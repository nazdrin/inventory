from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import MappingBranch
from app.business.reporting.orders.normalizer import normalize_salesdrive_order
from app.business.reporting.orders.repository import (
    create_sync_state,
    finish_sync_state,
    upsert_report_order,
)


logger = logging.getLogger("reporting.orders")


def _salesdrive_base_url() -> str | None:
    return os.getenv("SALESDRIVE_BASE_URL")


def _salesdrive_api_key() -> str | None:
    return os.getenv("SALESDRIVE_API_KEY")


async def _fetch_salesdrive_orders(period_from: datetime, period_to: datetime, page: int, limit: int) -> list[dict[str, Any]]:
    base_url = _salesdrive_base_url()
    api_key = _salesdrive_api_key()
    if not base_url:
        raise RuntimeError("SALESDRIVE_BASE_URL is not set")
    if not api_key:
        raise RuntimeError("SALESDRIVE_API_KEY is not set")

    params = {
        "limit": limit,
        "page": page,
        "filter[orderTime][from]": period_from.strftime("%Y-%m-%d %H:%M:%S"),
        "filter[orderTime][to]": period_to.strftime("%Y-%m-%d %H:%M:%S"),
    }
    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.get(
            f"{base_url.rstrip('/')}/api/order/list/",
            params=params,
            headers={"X-Api-Key": api_key},
        )
        response.raise_for_status()
        payload = response.json()

    data = payload.get("data") if isinstance(payload, dict) else None
    return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []


async def sync_salesdrive_orders(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    enterprise_code: str | None = None,
    limit: int = 100,
    max_pages: int = 20,
) -> dict[str, Any]:
    sync_state = await create_sync_state(
        session,
        source="salesdrive",
        enterprise_code=enterprise_code,
        sync_from=period_from,
        sync_to=period_to,
        request_params={
            "period_from": period_from.isoformat(),
            "period_to": period_to.isoformat(),
            "enterprise_code": enterprise_code,
            "limit": limit,
            "max_pages": max_pages,
        },
    )

    created = 0
    updated = 0
    failed = 0
    branch_enterprise_cache: dict[str, str | None] = {}

    async def resolve_enterprise(raw_order: dict[str, Any]) -> str | None:
        explicit = enterprise_code or str(raw_order.get("enterprise_code") or "").strip()
        if explicit:
            return explicit
        branch = str(raw_order.get("branch") or raw_order.get("utmSource") or raw_order.get("sajt") or "").strip()
        if not branch:
            return None
        if branch not in branch_enterprise_cache:
            branch_enterprise_cache[branch] = await session.scalar(
                select(MappingBranch.enterprise_code).where(MappingBranch.branch == branch).limit(1)
            )
        return branch_enterprise_cache[branch]

    try:
        for page in range(1, max_pages + 1):
            raw_orders = await _fetch_salesdrive_orders(period_from, period_to, page, limit)
            if not raw_orders:
                break
            for raw_order in raw_orders:
                target_enterprise = await resolve_enterprise(raw_order)
                if not target_enterprise:
                    branch = str(raw_order.get("branch") or raw_order.get("utmSource") or "").strip()
                    logger.warning(
                        "SalesDrive historical order skipped without enterprise_code: id=%s externalId=%s branch=%s",
                        raw_order.get("id"),
                        raw_order.get("externalId"),
                        branch,
                    )
                    failed += 1
                    continue
                try:
                    normalized = await normalize_salesdrive_order(
                        session,
                        order=raw_order,
                        enterprise_code=target_enterprise,
                    )
                    if normalized is None:
                        failed += 1
                        continue
                    _row, was_created = await upsert_report_order(session, normalized)
                    if was_created:
                        created += 1
                    else:
                        updated += 1
                except Exception:
                    failed += 1
                    logger.exception(
                        "SalesDrive historical reporting upsert failed: id=%s externalId=%s enterprise=%s",
                        raw_order.get("id"),
                        raw_order.get("externalId"),
                        target_enterprise,
                    )
            if len(raw_orders) < limit:
                break
        status = "success" if failed == 0 else "partial"
        await finish_sync_state(sync_state, status=status, created_count=created, updated_count=updated, failed_count=failed)
        return {"status": status, "created_count": created, "updated_count": updated, "failed_count": failed}
    except Exception as exc:
        await finish_sync_state(sync_state, status="failed", created_count=created, updated_count=updated, failed_count=failed, error_message=str(exc))
        raise

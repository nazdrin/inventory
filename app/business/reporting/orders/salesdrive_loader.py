from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessStore, MappingBranch
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


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_salesdrive_organization_id(raw_order: dict[str, Any]) -> str | None:
    for key in (
        "organizationId",
        "organization_id",
        "salesdrive_enterprise_id",
        "salesdriveOrganizationId",
    ):
        value = _clean(raw_order.get(key))
        if value:
            return value

    for key in ("organization", "enterprise", "business", "legalEntity"):
        value = raw_order.get(key)
        if isinstance(value, dict):
            nested = _clean(value.get("id") or value.get("organizationId") or value.get("value"))
            if nested:
                return nested
    return None


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
    organization_enterprise_cache: dict[str, str | None] = {}
    failed_reasons: dict[str, int] = {}
    sample_errors: list[dict[str, Any]] = []

    def add_failed(reason: str, raw_order: dict[str, Any], message: str | None = None) -> None:
        nonlocal failed
        failed += 1
        failed_reasons[reason] = failed_reasons.get(reason, 0) + 1
        if len(sample_errors) < 10:
            sample_errors.append(
                {
                    "reason": reason,
                    "message": message,
                    "id": raw_order.get("id"),
                    "externalId": raw_order.get("externalId"),
                    "branch": raw_order.get("branch") or raw_order.get("utmSource") or raw_order.get("sajt"),
                    "organizationId": _extract_salesdrive_organization_id(raw_order),
                }
            )

    async def resolve_enterprise(raw_order: dict[str, Any]) -> str | None:
        explicit = enterprise_code or str(raw_order.get("enterprise_code") or "").strip()
        if explicit:
            return explicit

        branch = _clean(raw_order.get("branch") or raw_order.get("utmSource") or raw_order.get("sajt"))
        if branch and branch not in branch_enterprise_cache:
            branch_enterprise_cache[branch] = await session.scalar(
                select(MappingBranch.enterprise_code).where(MappingBranch.branch == branch).limit(1)
            )
        if branch and branch_enterprise_cache.get(branch):
            return branch_enterprise_cache[branch]

        organization_id = _extract_salesdrive_organization_id(raw_order)
        if organization_id and organization_id not in organization_enterprise_cache:
            organization_filters = [BusinessStore.salesdrive_enterprise_code == organization_id]
            if organization_id.isdigit():
                organization_filters.append(BusinessStore.salesdrive_enterprise_id == int(organization_id))
            organization_enterprise_cache[organization_id] = await session.scalar(
                select(BusinessStore.enterprise_code)
                .where(or_(*organization_filters))
                .limit(1)
            )
        if organization_id and organization_enterprise_cache.get(organization_id):
            return organization_enterprise_cache[organization_id]

        return None

    try:
        for page in range(1, max_pages + 1):
            raw_orders = await _fetch_salesdrive_orders(period_from, period_to, page, limit)
            if not raw_orders:
                break
            for raw_order in raw_orders:
                target_enterprise = await resolve_enterprise(raw_order)
                if not target_enterprise:
                    logger.warning(
                        "SalesDrive historical order skipped without enterprise_code: id=%s externalId=%s branch=%s organizationId=%s",
                        raw_order.get("id"),
                        raw_order.get("externalId"),
                        raw_order.get("branch") or raw_order.get("utmSource") or raw_order.get("sajt"),
                        _extract_salesdrive_organization_id(raw_order),
                    )
                    add_failed("enterprise_not_resolved", raw_order)
                    continue
                try:
                    normalized = await normalize_salesdrive_order(
                        session,
                        order=raw_order,
                        enterprise_code=target_enterprise,
                    )
                    if normalized is None:
                        add_failed("normalizer_returned_none", raw_order)
                        continue
                    _row, was_created = await upsert_report_order(session, normalized)
                    if was_created:
                        created += 1
                    else:
                        updated += 1
                except Exception as exc:
                    add_failed("upsert_exception", raw_order, str(exc))
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
        return {
            "status": status,
            "created_count": created,
            "updated_count": updated,
            "failed_count": failed,
            "failed_reasons": failed_reasons,
            "sample_errors": sample_errors,
        }
    except Exception as exc:
        await finish_sync_state(sync_state, status="failed", created_count=created, updated_count=updated, failed_count=failed, error_message=str(exc))
        logger.exception("SalesDrive historical sync failed")
        return {
            "status": "failed",
            "created_count": created,
            "updated_count": updated,
            "failed_count": failed,
            "failed_reasons": failed_reasons,
            "sample_errors": sample_errors,
            "error_message": str(exc),
        }

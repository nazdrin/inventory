from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.business.reporting.orders.normalizer import normalize_salesdrive_order, normalize_tabletki_order
from app.business.reporting.orders.repository import upsert_report_order


logger = logging.getLogger("reporting.orders")


async def safe_upsert_tabletki_order(
    session: AsyncSession,
    *,
    order: dict[str, Any],
    enterprise_code: str,
    branch: str | None,
    fetched_status: int | float | str | None,
) -> None:
    try:
        normalized = await normalize_tabletki_order(
            session,
            order=order,
            enterprise_code=enterprise_code,
            branch=branch,
            fetched_status=fetched_status,
        )
        if normalized is None:
            logger.warning(
                "Reporting tabletki order skipped without id: enterprise_code=%s branch=%s",
                enterprise_code,
                branch,
            )
            return
        await upsert_report_order(session, normalized)
    except Exception:
        logger.exception(
            "Reporting tabletki order upsert failed: enterprise_code=%s branch=%s order_id=%s",
            enterprise_code,
            branch,
            order.get("id") if isinstance(order, dict) else None,
        )


async def safe_upsert_salesdrive_order(
    session: AsyncSession,
    *,
    order: dict[str, Any],
    enterprise_code: str,
) -> None:
    try:
        normalized = await normalize_salesdrive_order(session, order=order, enterprise_code=enterprise_code)
        if normalized is None:
            logger.warning(
                "Reporting SalesDrive order skipped without id: enterprise_code=%s salesdrive_id=%s",
                enterprise_code,
                order.get("id") if isinstance(order, dict) else None,
            )
            return
        await upsert_report_order(session, normalized)
    except Exception:
        logger.exception(
            "Reporting SalesDrive order upsert failed: enterprise_code=%s salesdrive_id=%s externalId=%s statusId=%s",
            enterprise_code,
            order.get("id") if isinstance(order, dict) else None,
            order.get("externalId") if isinstance(order, dict) else None,
            order.get("statusId") if isinstance(order, dict) else None,
        )

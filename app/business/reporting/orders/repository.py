from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    EnterpriseSettings,
    ReportEnterpriseExpenseSetting,
    ReportOrder,
    ReportOrderItem,
    ReportOrderSyncState,
)
from app.business.reporting.orders.normalizer import NormalizedOrder


async def upsert_report_order(session: AsyncSession, normalized: NormalizedOrder) -> tuple[ReportOrder, bool]:
    existing = await session.scalar(
        select(ReportOrder).where(
            ReportOrder.source == normalized.source,
            ReportOrder.enterprise_code == normalized.enterprise_code,
            ReportOrder.external_order_id == normalized.external_order_id,
        )
    )
    created = existing is None
    report_order = existing or ReportOrder(
        source=normalized.source,
        enterprise_code=normalized.enterprise_code,
        external_order_id=normalized.external_order_id,
    )
    if created:
        session.add(report_order)

    for field_name in (
        "business_store_id",
        "branch",
        "salesdrive_order_id",
        "tabletki_order_id",
        "order_number",
        "order_created_at",
        "order_updated_at",
        "sale_date",
        "status_id",
        "status_name",
        "status_group",
        "is_order",
        "is_sale",
        "is_return",
        "is_cancelled",
        "is_deleted",
        "customer_city",
        "payment_type",
        "delivery_type",
        "order_amount",
        "sale_amount",
        "items_quantity",
        "sale_quantity",
        "supplier_cost_total",
        "gross_profit_amount",
        "expense_percent",
        "expense_amount",
        "net_profit_amount",
        "last_synced_at",
        "raw_hash",
        "raw_json",
    ):
        setattr(report_order, field_name, getattr(normalized, field_name))

    await session.flush()
    await session.execute(delete(ReportOrderItem).where(ReportOrderItem.report_order_id == report_order.id))
    for item in normalized.items:
        session.add(
            ReportOrderItem(
                report_order_id=report_order.id,
                line_index=item.line_index,
                source_product_id=item.source_product_id,
                sku=item.sku,
                barcode=item.barcode,
                product_name=item.product_name,
                supplier_name=item.supplier_name,
                supplier_code=item.supplier_code,
                quantity=item.quantity,
                sale_price=item.sale_price,
                sale_amount=item.sale_amount,
                cost_price=item.cost_price,
                cost_amount=item.cost_amount,
                gross_profit_amount=item.gross_profit_amount,
                margin_percent=item.margin_percent,
            )
        )
    return report_order, created


async def list_business_enterprises(session: AsyncSession) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            select(EnterpriseSettings.enterprise_code, EnterpriseSettings.enterprise_name)
            .where(EnterpriseSettings.data_format == "Business")
            .order_by(EnterpriseSettings.enterprise_name.asc())
        )
    ).all()
    return [{"enterprise_code": code, "enterprise_name": name} for code, name in rows]


async def upsert_expense_setting(
    session: AsyncSession,
    *,
    enterprise_code: str,
    expense_percent,
    active_from: date,
    active_to: date | None = None,
) -> ReportEnterpriseExpenseSetting:
    current = await session.scalar(
        select(ReportEnterpriseExpenseSetting)
        .where(
            ReportEnterpriseExpenseSetting.enterprise_code == enterprise_code,
            ReportEnterpriseExpenseSetting.active_from == active_from,
        )
        .limit(1)
    )
    row = current or ReportEnterpriseExpenseSetting(enterprise_code=enterprise_code, active_from=active_from)
    if current is None:
        session.add(row)
    row.expense_percent = expense_percent
    row.active_to = active_to
    await session.flush()
    return row


async def create_sync_state(
    session: AsyncSession,
    *,
    source: str,
    enterprise_code: str | None,
    sync_from: datetime,
    sync_to: datetime,
    request_params: dict[str, Any],
) -> ReportOrderSyncState:
    row = ReportOrderSyncState(
        source=source,
        enterprise_code=enterprise_code,
        last_sync_from=sync_from,
        last_sync_to=sync_to,
        status="running",
        started_at=datetime.now(timezone.utc),
        request_params=request_params,
    )
    session.add(row)
    await session.flush()
    return row


async def finish_sync_state(
    row: ReportOrderSyncState,
    *,
    status: str,
    created_count: int = 0,
    updated_count: int = 0,
    failed_count: int = 0,
    error_message: str | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    row.status = status
    row.finished_at = now
    row.created_count = created_count
    row.updated_count = updated_count
    row.failed_count = failed_count
    row.error_message = error_message
    if status == "success":
        row.last_success_at = now

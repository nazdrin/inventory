from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    EnterpriseSettings,
    ReportEnterpriseExpenseSetting,
    ReportOrder,
    ReportOrderItem,
)
from app.business.reporting.orders.profit_calculator import percent
from app.business.reporting.orders.statuses import STATUS_FUNNEL_ORDER, STATUS_NAMES
from app.business.reporting.orders.repository import list_business_enterprises


def _fmt(value: Any) -> str:
    if value is None:
        return "0"
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _base_filters(period_from: datetime, period_to: datetime, enterprise_code: str | None) -> list[Any]:
    filters = [
        ReportOrder.order_created_at >= period_from,
        ReportOrder.order_created_at <= period_to,
    ]
    if enterprise_code:
        filters.append(ReportOrder.enterprise_code == enterprise_code)
    return filters


def _metrics_from_row(row: Any) -> dict[str, Any]:
    total_orders = int(row.total_orders or 0)
    sales_count = int(row.sales_count or 0)
    return_count = int(row.return_count or 0)
    cancelled_count = int(row.cancelled_count or 0)
    deleted_count = int(row.deleted_count or 0)
    return {
        "total_orders": total_orders,
        "active_orders": int(row.active_orders or 0),
        "sales_count": sales_count,
        "return_count": return_count,
        "cancelled_count": cancelled_count,
        "deleted_count": deleted_count,
        "refusal_rate": _fmt(percent(Decimal(cancelled_count), Decimal(total_orders))),
        "return_rate": _fmt(percent(Decimal(return_count), Decimal(total_orders))),
        "sales_return_rate": _fmt(percent(Decimal(return_count), Decimal(sales_count))),
        "order_amount": _fmt(row.order_amount),
        "sale_amount": _fmt(row.sale_amount),
        "items_quantity": _fmt(row.items_quantity),
        "sale_quantity": _fmt(row.sale_quantity),
        "supplier_cost_total": _fmt(row.supplier_cost_total),
        "gross_profit_amount": _fmt(row.gross_profit_amount),
        "expense_amount": _fmt(row.expense_amount),
        "net_profit_amount": _fmt(row.net_profit_amount),
    }


async def build_summary(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    enterprise_code: str | None = None,
) -> dict[str, Any]:
    filters = _base_filters(period_from, period_to, enterprise_code)
    stmt = select(
        func.count(ReportOrder.id).label("total_orders"),
        func.sum(case((ReportOrder.status_group == "active", 1), else_=0)).label("active_orders"),
        func.sum(case((ReportOrder.is_sale == True, 1), else_=0)).label("sales_count"),
        func.sum(case((ReportOrder.is_return == True, 1), else_=0)).label("return_count"),
        func.sum(case((ReportOrder.is_cancelled == True, 1), else_=0)).label("cancelled_count"),
        func.sum(case((ReportOrder.is_deleted == True, 1), else_=0)).label("deleted_count"),
        func.coalesce(func.sum(ReportOrder.order_amount), 0).label("order_amount"),
        func.coalesce(func.sum(ReportOrder.sale_amount), 0).label("sale_amount"),
        func.coalesce(func.sum(ReportOrder.items_quantity), 0).label("items_quantity"),
        func.coalesce(func.sum(ReportOrder.sale_quantity), 0).label("sale_quantity"),
        func.coalesce(func.sum(ReportOrder.supplier_cost_total), 0).label("supplier_cost_total"),
        func.coalesce(func.sum(ReportOrder.gross_profit_amount), 0).label("gross_profit_amount"),
        func.coalesce(func.sum(ReportOrder.expense_amount), 0).label("expense_amount"),
        func.coalesce(func.sum(ReportOrder.net_profit_amount), 0).label("net_profit_amount"),
    ).where(*filters)
    row = (await session.execute(stmt)).one()
    return {
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
        "enterprise_code": enterprise_code,
        **_metrics_from_row(row),
        "business_enterprises": await list_business_enterprises(session),
    }


async def build_funnel(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    enterprise_code: str | None = None,
) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            select(
                ReportOrder.status_id,
                ReportOrder.status_name,
                ReportOrder.status_group,
                func.count(ReportOrder.id),
                func.coalesce(func.sum(ReportOrder.order_amount), 0),
            )
            .where(*_base_filters(period_from, period_to, enterprise_code))
            .group_by(ReportOrder.status_id, ReportOrder.status_name, ReportOrder.status_group)
        )
    ).all()
    by_id = {int(status_id or 0): row for row in rows for status_id in [row[0]] if status_id is not None}
    result: list[dict[str, Any]] = []
    for status_id in STATUS_FUNNEL_ORDER:
        row = by_id.get(status_id)
        count = int(row[3] or 0) if row else 0
        result.append(
            {
                "status_id": status_id,
                "status_name": (row[1] if row and row[1] else STATUS_NAMES.get(status_id)),
                "status_group": row[2] if row else None,
                "count": count,
                "order_amount": _fmt(row[4]) if row else "0",
            }
        )
    return result


async def build_by_enterprise(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    enterprise_code: str | None = None,
) -> list[dict[str, Any]]:
    filters = _base_filters(period_from, period_to, enterprise_code)
    rows = (
        await session.execute(
            select(
                ReportOrder.enterprise_code,
                EnterpriseSettings.enterprise_name,
                func.count(ReportOrder.id).label("total_orders"),
                func.sum(case((ReportOrder.status_group == "active", 1), else_=0)).label("active_orders"),
                func.sum(case((ReportOrder.is_sale == True, 1), else_=0)).label("sales_count"),
                func.sum(case((ReportOrder.is_return == True, 1), else_=0)).label("return_count"),
                func.sum(case((ReportOrder.is_cancelled == True, 1), else_=0)).label("cancelled_count"),
                func.sum(case((ReportOrder.is_deleted == True, 1), else_=0)).label("deleted_count"),
                func.coalesce(func.sum(ReportOrder.order_amount), 0).label("order_amount"),
                func.coalesce(func.sum(ReportOrder.sale_amount), 0).label("sale_amount"),
                func.coalesce(func.sum(ReportOrder.items_quantity), 0).label("items_quantity"),
                func.coalesce(func.sum(ReportOrder.sale_quantity), 0).label("sale_quantity"),
                func.coalesce(func.sum(ReportOrder.supplier_cost_total), 0).label("supplier_cost_total"),
                func.coalesce(func.sum(ReportOrder.gross_profit_amount), 0).label("gross_profit_amount"),
                func.coalesce(func.sum(ReportOrder.expense_amount), 0).label("expense_amount"),
                func.coalesce(func.sum(ReportOrder.net_profit_amount), 0).label("net_profit_amount"),
            )
            .join(EnterpriseSettings, EnterpriseSettings.enterprise_code == ReportOrder.enterprise_code)
            .where(*filters)
            .group_by(ReportOrder.enterprise_code, EnterpriseSettings.enterprise_name)
            .order_by(EnterpriseSettings.enterprise_name.asc())
        )
    ).all()
    return [
        {
            "enterprise_code": row.enterprise_code,
            "enterprise_name": row.enterprise_name,
            **_metrics_from_row(row),
        }
        for row in rows
    ]


async def build_by_supplier(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    enterprise_code: str | None = None,
) -> list[dict[str, Any]]:
    filters = _base_filters(period_from, period_to, enterprise_code) + [ReportOrder.is_sale == True]
    order_expense_share = case(
        (ReportOrder.sale_amount > 0, ReportOrderItem.sale_amount / ReportOrder.sale_amount * ReportOrder.expense_amount),
        else_=0,
    )
    rows = (
        await session.execute(
            select(
                ReportOrderItem.supplier_code,
                func.max(ReportOrderItem.supplier_name),
                func.count(func.distinct(ReportOrder.id)),
                func.coalesce(func.sum(ReportOrderItem.quantity), 0),
                func.coalesce(func.sum(ReportOrderItem.sale_amount), 0),
                func.coalesce(func.sum(ReportOrderItem.cost_amount), 0),
                func.coalesce(func.sum(ReportOrderItem.gross_profit_amount), 0),
                func.coalesce(func.sum(order_expense_share), 0),
            )
            .join(ReportOrder, ReportOrder.id == ReportOrderItem.report_order_id)
            .where(*filters)
            .group_by(ReportOrderItem.supplier_code)
            .order_by(func.coalesce(func.sum(ReportOrderItem.sale_amount), 0).desc())
        )
    ).all()
    total_sales = sum((Decimal(str(row[4] or 0)) for row in rows), Decimal("0"))
    result: list[dict[str, Any]] = []
    for supplier_code, supplier_name, orders_count, quantity, sale_amount, cost_amount, gross_profit, expenses in rows:
        sale_dec = Decimal(str(sale_amount or 0))
        expense_dec = Decimal(str(expenses or 0))
        result.append(
            {
                "supplier_code": supplier_code or "unmapped",
                "supplier_name": supplier_name or supplier_code or "Unmapped",
                "orders_count": int(orders_count or 0),
                "quantity": _fmt(quantity),
                "sale_amount": _fmt(sale_amount),
                "cost_amount": _fmt(cost_amount),
                "gross_profit_amount": _fmt(gross_profit),
                "sales_share_percent": _fmt(percent(sale_dec, total_sales)),
                "allocated_expense_amount": _fmt(expense_dec),
                "net_profit_amount": _fmt(Decimal(str(gross_profit or 0)) - expense_dec),
            }
        )
    return result


async def build_details(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    enterprise_code: str | None = None,
    status_group: str | None = None,
    supplier_code: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    filters = _base_filters(period_from, period_to, enterprise_code)
    if status_group:
        filters.append(ReportOrder.status_group == status_group)
    if supplier_code:
        filters.append(ReportOrderItem.supplier_code == supplier_code)

    stmt = (
        select(ReportOrder)
        .outerjoin(ReportOrderItem, ReportOrderItem.report_order_id == ReportOrder.id)
        .where(*filters)
        .group_by(ReportOrder.id)
        .order_by(ReportOrder.order_created_at.desc().nullslast(), ReportOrder.id.desc())
        .limit(limit)
        .offset(offset)
    )
    orders = list((await session.execute(stmt)).scalars().all())
    order_ids = [item.id for item in orders]
    item_rows = []
    if order_ids:
        item_rows = (
            await session.execute(
                select(ReportOrderItem)
                .where(ReportOrderItem.report_order_id.in_(order_ids))
                .order_by(ReportOrderItem.report_order_id, ReportOrderItem.line_index)
            )
        ).scalars().all()
    items_by_order: dict[int, list[dict[str, Any]]] = {}
    for item in item_rows:
        items_by_order.setdefault(int(item.report_order_id), []).append(
            {
                "line_index": item.line_index,
                "sku": item.sku,
                "barcode": item.barcode,
                "product_name": item.product_name,
                "supplier_code": item.supplier_code,
                "supplier_name": item.supplier_name,
                "quantity": _fmt(item.quantity),
                "sale_price": _fmt(item.sale_price),
                "sale_amount": _fmt(item.sale_amount),
                "cost_price": _fmt(item.cost_price),
                "cost_amount": _fmt(item.cost_amount),
                "gross_profit_amount": _fmt(item.gross_profit_amount),
                "margin_percent": _fmt(item.margin_percent) if item.margin_percent is not None else None,
            }
        )
    return {
        "rows": [
            {
                "id": int(order.id),
                "source": order.source,
                "enterprise_code": order.enterprise_code,
                "branch": order.branch,
                "external_order_id": order.external_order_id,
                "salesdrive_order_id": order.salesdrive_order_id,
                "tabletki_order_id": order.tabletki_order_id,
                "order_number": order.order_number,
                "order_created_at": order.order_created_at.isoformat() if order.order_created_at else None,
                "status_id": order.status_id,
                "status_name": order.status_name,
                "status_group": order.status_group,
                "order_amount": _fmt(order.order_amount),
                "sale_amount": _fmt(order.sale_amount),
                "supplier_cost_total": _fmt(order.supplier_cost_total),
                "gross_profit_amount": _fmt(order.gross_profit_amount),
                "expense_amount": _fmt(order.expense_amount),
                "net_profit_amount": _fmt(order.net_profit_amount),
                "items": items_by_order.get(int(order.id), []),
            }
            for order in orders
        ],
        "limit": limit,
        "offset": offset,
    }


async def list_expense_settings(session: AsyncSession) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            select(
                EnterpriseSettings.enterprise_code,
                EnterpriseSettings.enterprise_name,
                ReportEnterpriseExpenseSetting.id,
                ReportEnterpriseExpenseSetting.expense_percent,
                ReportEnterpriseExpenseSetting.active_from,
                ReportEnterpriseExpenseSetting.active_to,
            )
            .outerjoin(
                ReportEnterpriseExpenseSetting,
                ReportEnterpriseExpenseSetting.enterprise_code == EnterpriseSettings.enterprise_code,
            )
            .where(EnterpriseSettings.data_format == "Business")
            .order_by(EnterpriseSettings.enterprise_name.asc(), ReportEnterpriseExpenseSetting.active_from.desc().nullslast())
        )
    ).all()
    return [
        {
            "enterprise_code": row[0],
            "enterprise_name": row[1],
            "setting_id": int(row[2]) if row[2] is not None else None,
            "expense_percent": _fmt(row[3] if row[3] is not None else 0),
            "active_from": row[4].isoformat() if row[4] else None,
            "active_to": row[5].isoformat() if row[5] else None,
        }
        for row in rows
    ]

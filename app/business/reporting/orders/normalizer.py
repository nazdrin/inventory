from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessStore, BusinessStoreOffer, DropshipEnterprise, Offer, ReportEnterpriseExpenseSetting
from app.business.reporting.orders.profit_calculator import as_decimal, money, percent
from app.business.reporting.orders.statuses import classify_status


@dataclass
class NormalizedOrderItem:
    line_index: int
    source_product_id: str | None
    sku: str | None
    barcode: str | None
    product_name: str | None
    supplier_name: str | None
    supplier_code: str | None
    quantity: Decimal
    sale_price: Decimal
    sale_amount: Decimal
    cost_price: Decimal
    cost_amount: Decimal
    gross_profit_amount: Decimal
    margin_percent: Decimal | None


@dataclass
class NormalizedOrder:
    source: str
    enterprise_code: str
    business_store_id: int | None
    branch: str | None
    external_order_id: str
    salesdrive_order_id: str | None
    tabletki_order_id: str | None
    order_number: str | None
    order_created_at: datetime | None
    order_updated_at: datetime | None
    sale_date: datetime | None
    status_id: int | None
    status_name: str | None
    status_group: str
    is_order: bool
    is_sale: bool
    is_return: bool
    is_cancelled: bool
    is_deleted: bool
    customer_city: str | None
    payment_type: str | None
    delivery_type: str | None
    order_amount: Decimal
    sale_amount: Decimal
    items_quantity: Decimal
    sale_quantity: Decimal
    supplier_cost_total: Decimal
    gross_profit_amount: Decimal
    expense_percent: Decimal
    expense_amount: Decimal
    net_profit_amount: Decimal
    last_synced_at: datetime
    raw_hash: str
    raw_json: dict[str, Any]
    items: list[NormalizedOrderItem]


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _raw_hash(raw: dict[str, Any]) -> str:
    payload = json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = _clean(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text[:19] if "T" in fmt else text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _status_id_from_any(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(str(value)))
    except Exception:
        return None


def _delivery_dict(order: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in order.get("deliveryData") or []:
        if isinstance(item, dict) and item.get("key"):
            out[str(item["key"])] = item.get("value")
    return out


def _salesdrive_products(order: dict[str, Any]) -> list[dict[str, Any]]:
    products = order.get("products")
    return [item for item in products if isinstance(item, dict)] if isinstance(products, list) else []


def _tabletki_rows(order: dict[str, Any]) -> list[dict[str, Any]]:
    rows = order.get("rows")
    return [item for item in rows if isinstance(item, dict)] if isinstance(rows, list) else []


async def _resolve_store_id(session: AsyncSession, enterprise_code: str, branch: str | None) -> int | None:
    if not branch:
        return None
    result = await session.execute(
        select(BusinessStore.id)
        .where(
            and_(
                BusinessStore.tabletki_enterprise_code == enterprise_code,
                BusinessStore.tabletki_branch == branch,
                BusinessStore.orders_enabled == True,
            )
        )
        .limit(1)
    )
    value = result.scalar_one_or_none()
    return int(value) if value is not None else None


async def _expense_percent(session: AsyncSession, enterprise_code: str, order_dt: datetime | None) -> Decimal:
    active_date = (order_dt or datetime.now(timezone.utc)).date()
    result = await session.execute(
        select(ReportEnterpriseExpenseSetting.expense_percent)
        .where(
            ReportEnterpriseExpenseSetting.enterprise_code == enterprise_code,
            ReportEnterpriseExpenseSetting.active_from <= active_date,
            (ReportEnterpriseExpenseSetting.active_to.is_(None))
            | (ReportEnterpriseExpenseSetting.active_to >= active_date),
        )
        .order_by(ReportEnterpriseExpenseSetting.active_from.desc(), ReportEnterpriseExpenseSetting.id.desc())
        .limit(1)
    )
    return as_decimal(result.scalar_one_or_none() or 0)


async def _offer_for_line(
    session: AsyncSession,
    *,
    sku: str | None,
    sale_price: Decimal,
    business_store_id: int | None,
) -> tuple[str | None, str | None, Decimal]:
    if not sku:
        return None, None, Decimal("0")

    rows: list[tuple[Any, Any, Any, Any]] = []
    if business_store_id is not None:
        result = await session.execute(
            select(
                BusinessStoreOffer.supplier_code,
                DropshipEnterprise.name,
                BusinessStoreOffer.effective_price,
                BusinessStoreOffer.wholesale_price,
            )
            .outerjoin(DropshipEnterprise, DropshipEnterprise.code == BusinessStoreOffer.supplier_code)
            .where(
                BusinessStoreOffer.store_id == business_store_id,
                BusinessStoreOffer.product_code == sku,
            )
        )
        rows = result.all()

    if not rows:
        result = await session.execute(
            select(Offer.supplier_code, DropshipEnterprise.name, Offer.price, Offer.wholesale_price)
            .outerjoin(DropshipEnterprise, DropshipEnterprise.code == Offer.supplier_code)
            .where(Offer.product_code == sku)
        )
        rows = result.all()

    if not rows:
        return None, None, Decimal("0")

    normalized = [(str(sc), name, as_decimal(price), as_decimal(cost)) for sc, name, price, cost in rows]
    normalized.sort(key=lambda item: (abs(as_decimal(sale_price) - item[2]), item[0]))
    supplier_code, supplier_name, _offer_price, cost_price = normalized[0]
    return supplier_code, str(supplier_name or supplier_code), money(cost_price)


async def _supplier_code_by_name(session: AsyncSession, supplier_name: str | None) -> str | None:
    normalized_name = _clean(supplier_name)
    if not normalized_name:
        return None
    result = await session.execute(
        select(DropshipEnterprise.code)
        .where(DropshipEnterprise.name == normalized_name)
        .limit(1)
    )
    exact = result.scalar_one_or_none()
    if exact:
        return str(exact)
    lowered = normalized_name.lower()
    result = await session.execute(
        select(DropshipEnterprise.code)
        .where(func.lower(DropshipEnterprise.name) == lowered)
        .limit(1)
    )
    value = result.scalar_one_or_none()
    return str(value) if value else None


async def normalize_tabletki_order(
    session: AsyncSession,
    *,
    order: dict[str, Any],
    enterprise_code: str,
    branch: str | None,
    fetched_status: int | float | str | None,
) -> NormalizedOrder | None:
    external_order_id = _clean(order.get("id"))
    if not external_order_id:
        return None

    status_id = _status_id_from_any(order.get("statusID")) or _status_id_from_any(fetched_status)
    status = classify_status(status_id)
    order_created_at = _parse_dt(order.get("orderTime") or order.get("createdAt") or order.get("date"))
    business_store_id = await _resolve_store_id(session, enterprise_code, _clean(order.get("branchID")) or branch)
    expense = await _expense_percent(session, enterprise_code, order_created_at)
    items: list[NormalizedOrderItem] = []

    for idx, row in enumerate(_tabletki_rows(order), start=1):
        sku = _clean(row.get("goodsCode"))
        quantity = as_decimal(row.get("qty", row.get("qtyShip", 0)))
        sale_price = money(as_decimal(row.get("price", row.get("priceShip", 0))))
        supplier_code, supplier_name, cost_price = await _offer_for_line(
            session,
            sku=sku,
            sale_price=sale_price,
            business_store_id=business_store_id,
        )
        sale_amount = money(sale_price * quantity)
        cost_amount = money(cost_price * quantity)
        gross_profit = money(sale_amount - cost_amount)
        items.append(
            NormalizedOrderItem(
                line_index=idx,
                source_product_id=sku,
                sku=sku,
                barcode=_clean(row.get("barcode")),
                product_name=_clean(row.get("goodsName")),
                supplier_name=supplier_name,
                supplier_code=supplier_code,
                quantity=quantity,
                sale_price=sale_price,
                sale_amount=sale_amount,
                cost_price=cost_price,
                cost_amount=cost_amount,
                gross_profit_amount=gross_profit,
                margin_percent=percent(gross_profit, sale_amount) if sale_amount else None,
            )
        )

    return _build_order(
        source="tabletki",
        enterprise_code=enterprise_code,
        business_store_id=business_store_id,
        branch=_clean(order.get("branchID")) or branch,
        external_order_id=external_order_id,
        salesdrive_order_id=None,
        tabletki_order_id=external_order_id,
        order_number=_clean(order.get("code") or order.get("tabletkiOrder")),
        order_created_at=order_created_at,
        order_updated_at=_parse_dt(order.get("updatedAt")),
        status=status,
        customer_city=None,
        payment_type=None,
        delivery_type=_clean(_delivery_dict(order).get("DeliveryServiceName")),
        expense_percent=expense,
        raw_json=order,
        items=items,
    )


async def normalize_salesdrive_order(
    session: AsyncSession,
    *,
    order: dict[str, Any],
    enterprise_code: str,
) -> NormalizedOrder | None:
    external_order_id = _clean(order.get("externalId") or order.get("tabletkiOrder") or order.get("id"))
    if not external_order_id:
        return None

    branch = _clean(order.get("branch") or order.get("utmSource") or order.get("sajt"))
    status_id = _status_id_from_any(order.get("statusId") or order.get("status_id"))
    status = classify_status(status_id, _clean(order.get("statusName") or order.get("status")))
    order_created_at = _parse_dt(order.get("orderTime") or order.get("createdAt") or order.get("date"))
    business_store_id = await _resolve_store_id(session, enterprise_code, branch)
    expense = await _expense_percent(session, enterprise_code, order_created_at)
    items: list[NormalizedOrderItem] = []

    for idx, product in enumerate(_salesdrive_products(order), start=1):
        sku = _clean(product.get("sku") or product.get("id") or product.get("parameter"))
        quantity = as_decimal(product.get("amount", 0))
        sale_price = money(as_decimal(product.get("costPerItem", product.get("price", 0))))
        cost_price = money(as_decimal(product.get("expenses", product.get("costPrice", 0))))
        supplier_code = _clean(product.get("supplier_code"))
        supplier_name = _clean(product.get("supplier") or order.get("supplier"))
        if not supplier_code:
            supplier_code = await _supplier_code_by_name(session, supplier_name)
        if not supplier_code and cost_price == 0:
            supplier_code, supplier_name, cost_price = await _offer_for_line(
                session,
                sku=sku,
                sale_price=sale_price,
                business_store_id=business_store_id,
            )
        sale_amount = money(sale_price * quantity)
        cost_amount = money(cost_price * quantity)
        gross_profit = money(sale_amount - cost_amount)
        items.append(
            NormalizedOrderItem(
                line_index=idx,
                source_product_id=_clean(product.get("id")),
                sku=sku,
                barcode=_clean(product.get("barcode")),
                product_name=_clean(product.get("name") or product.get("documentName")),
                supplier_name=supplier_name,
                supplier_code=supplier_code,
                quantity=quantity,
                sale_price=sale_price,
                sale_amount=sale_amount,
                cost_price=cost_price,
                cost_amount=cost_amount,
                gross_profit_amount=gross_profit,
                margin_percent=percent(gross_profit, sale_amount) if sale_amount else None,
            )
        )

    return _build_order(
        source="salesdrive",
        enterprise_code=enterprise_code,
        business_store_id=business_store_id,
        branch=branch,
        external_order_id=external_order_id,
        salesdrive_order_id=_clean(order.get("id")),
        tabletki_order_id=_clean(order.get("externalId") or order.get("tabletkiOrder") or order.get("TabletkiOrder")),
        order_number=_clean(order.get("tabletkiOrder") or order.get("TabletkiOrder") or order.get("externalId")),
        order_created_at=order_created_at,
        order_updated_at=_parse_dt(order.get("updateAt") or order.get("updatedAt")),
        status=status,
        customer_city=_clean(order.get("city")),
        payment_type=_clean(order.get("payment_method") or order.get("paymentType")),
        delivery_type=_clean(order.get("shipping_method") or order.get("deliveryType")),
        expense_percent=expense,
        raw_json=order,
        items=items,
    )


def _build_order(
    *,
    source: str,
    enterprise_code: str,
    business_store_id: int | None,
    branch: str | None,
    external_order_id: str,
    salesdrive_order_id: str | None,
    tabletki_order_id: str | None,
    order_number: str | None,
    order_created_at: datetime | None,
    order_updated_at: datetime | None,
    status,
    customer_city: str | None,
    payment_type: str | None,
    delivery_type: str | None,
    expense_percent: Decimal,
    raw_json: dict[str, Any],
    items: list[NormalizedOrderItem],
) -> NormalizedOrder:
    order_amount = money(sum((item.sale_amount for item in items), Decimal("0")))
    quantity = sum((item.quantity for item in items), Decimal("0"))
    cost_total = money(sum((item.cost_amount for item in items), Decimal("0")))
    sale_date = order_created_at if status.is_sale else None
    expense_amount = money(order_amount * as_decimal(expense_percent) / Decimal("100"))
    gross_profit = money(order_amount - cost_total)
    net_profit = money(gross_profit - expense_amount)
    return NormalizedOrder(
        source=source,
        enterprise_code=enterprise_code,
        business_store_id=business_store_id,
        branch=branch,
        external_order_id=external_order_id,
        salesdrive_order_id=salesdrive_order_id,
        tabletki_order_id=tabletki_order_id,
        order_number=order_number,
        order_created_at=order_created_at,
        order_updated_at=order_updated_at,
        sale_date=sale_date,
        status_id=status.status_id,
        status_name=status.status_name,
        status_group=status.status_group,
        is_order=status.is_order,
        is_sale=status.is_sale,
        is_return=status.is_return,
        is_cancelled=status.is_cancelled,
        is_deleted=status.is_deleted,
        customer_city=customer_city,
        payment_type=payment_type,
        delivery_type=delivery_type,
        order_amount=order_amount,
        sale_amount=order_amount if status.is_sale else Decimal("0"),
        items_quantity=quantity,
        sale_quantity=quantity if status.is_sale else Decimal("0"),
        supplier_cost_total=cost_total,
        gross_profit_amount=gross_profit,
        expense_percent=as_decimal(expense_percent),
        expense_amount=expense_amount,
        net_profit_amount=net_profit,
        last_synced_at=datetime.now(timezone.utc),
        raw_hash=_raw_hash(raw_json),
        raw_json=raw_json,
        items=items,
    )

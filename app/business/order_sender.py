# app/business/order_sender.py
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Iterable
import json
from sqlalchemy import select, and_, or_, func, literal
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

# === Ваши модели (проверьте реальные имена/поля) ===
from app.database import get_async_db
from app.models import Offer, DropshipEnterprise, CatalogMapping, EnterpriseSettings
import httpx

logger = logging.getLogger(__name__)


# ---------------------------
# ВСПОМОГАТЕЛЬНЫЕ СТРУКТУРЫ
# ---------------------------

@dataclass
class OrderRow:
    goodsCode: str
    goodsName: str
    qty: Decimal
    price: Decimal
    goodsProducer: Optional[str] = None


@dataclass
class SupplierPick:
    supplier_code: str
    supplier_name: str
    sku: Optional[str]
    price: Decimal
async def _send_to_salesdrive(payload: Dict[str, Any], api_key: str) -> None:
    """
    Отправка заказа в SalesDrive по API, с использованием X-Api-Key.
    """
    url = "https://petrenko.salesdrive.me/handler/"  # ← измените на ваш endpoint

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            logger.info("📦 Payload для SalesDrive:\n%s", json.dumps(payload, indent=2, ensure_ascii=False))
            response = await client.post(url, json=payload, headers=headers)
            logger.info("📤 Отправка в SalesDrive. Код ответа: %s", response.status_code)
            logger.info("📨 Ответ от SalesDrive: %s", response.text)
            response.raise_for_status()
        except httpx.RequestError as e:
            logger.error("❌ Ошибка подключения к SalesDrive: %s", str(e))
        except httpx.HTTPStatusError as e:
            logger.error("❌ Ошибка HTTP от SalesDrive: %s — %s", e.response.status_code, e.response.text)

def _as_decimal(x: Any) -> Decimal:
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(0)


def _normalize_order_rows(order: Dict[str, Any]) -> List[OrderRow]:
    rows = []
    for r in order.get("rows", []):
        rows.append(
            OrderRow(
                goodsCode=str(r.get("goodsCode")),
                goodsName=str(r.get("goodsName", "")),
                qty=_as_decimal(r.get("qty", 0)),
                price=_as_decimal(r.get("price", 0)),
                goodsProducer=r.get("goodsProducer"),
            )
        )
    return rows


def _delivery_dict(order: Dict[str, Any]) -> Dict[str, str]:
    """
    Превращаем массив deliveryData [{key, value, description}] в простой dict по key → value.
    """
    out = {}
    for item in order.get("deliveryData", []) or []:
        k = item.get("key")
        v = item.get("value")
        if k:
            out[k] = v
    return out


async def _get_salesdrive_api_key(session: AsyncSession, enterprise_code: str) -> Optional[str]:
    q = (
        select(EnterpriseSettings.token)
        .where(EnterpriseSettings.enterprise_code == str(enterprise_code))
        .limit(1)
    )
    res = await session.execute(q)
    return res.scalar_one_or_none()


async def _fetch_supplier_by_price(
    session: AsyncSession, product_code: str, price: Decimal
) -> Optional[str]:
    """
    Возвращает supplier_code из offers, если нашли точное совпадение по product_code и price.
    При необходимости добавьте доп. фильтры (город/branch/enterprise_code).
    """
    q = (
        select(Offer.supplier_code)
        .where(
            and_(
                Offer.product_code == str(product_code),
                Offer.price == price,  # точное равенство. При необходимости округлять.
            )
        )
        .limit(1)
    )
    res = await session.execute(q)
    return res.scalar_one_or_none()


async def _fetch_supplier_name(session: AsyncSession, supplier_code: str) -> Optional[str]:
    q = (
        select(DropshipEnterprise.name)
        .where(DropshipEnterprise.code == str(supplier_code))
        .limit(1)
    )
    res = await session.execute(q)
    return res.scalar_one_or_none()


async def _fetch_supplier_price(
    session: AsyncSession, supplier_code: str, product_code: str
) -> Optional[Decimal]:
    """
    Цена товара у конкретного поставщика (из offers).
    """
    q = (
        select(Offer.price)
        .where(
            and_(
                Offer.supplier_code == str(supplier_code),
                Offer.product_code == str(product_code),
            )
        )
        .limit(1)
    )
    res = await session.execute(q)
    return res.scalar_one_or_none()


async def _fetch_sku_from_catalog_mapping(
    session: AsyncSession, goods_code: str, supplier_code: str
) -> Optional[str]:
    """
    Берем SKU поставщика из CatalogMapping: поле Code_{supplier_code}, например Code_D1.
    """
    field_name = f"Code_{supplier_code}"
    code_col = getattr(CatalogMapping, field_name, None)
    if code_col is None:
        return None

    q = (
        select(code_col)
        .where(CatalogMapping.ID == str(goods_code))
        .limit(1)
    )
    res = await session.execute(q)
    return res.scalar_one_or_none()


def _build_novaposhta_block(d: Dict[str, str]) -> Dict[str, Any]:
    """
    Наполняем блок НП, если есть соответствующие ключи.
    Берём только value из входа.
    """
    if d.get("DeliveryServiceAlias") != "NP":
        return {}
    return {
        "ServiceType": d.get("ServiceType", "Warehouse"),
        "payer": d.get("payer", "recipient"),
        "area": d.get("area", ""),
        "region": d.get("region", ""),
        "city": d.get("CitySender", ""),         # из входа
        "cityNameFormat": d.get("cityNameFormat", ""),
        "WarehouseNumber": d.get("ID_Whs", ""),  # из входа
        "Street": d.get("Street", ""),
        "BuildingNumber": d.get("BuildingNumber", ""),
        "Flat": d.get("Flat", ""),
        "ttn": d.get("ttn", ""),
    }


def _build_ukrposhta_block(d: Dict[str, str]) -> Dict[str, Any]:
    if d.get("DeliveryServiceAlias") != "UP":
        return {}
    return {
        "ServiceType": d.get("ServiceType", ""),
        "payer": d.get("payer", ""),
        "type": d.get("type", ""),
        "city": d.get("CitySender", ""),
        "WarehouseNumber": d.get("ID_Whs", ""),
        "Street": d.get("Street", ""),
        "BuildingNumber": d.get("BuildingNumber", ""),
        "Flat": d.get("Flat", ""),
        "ttn": d.get("ttn", ""),
    }


def _build_meest_block(d: Dict[str, str]) -> Dict[str, Any]:
    if d.get("DeliveryServiceAlias") != "MEEST":
        return {}
    return {
        "ServiceType": d.get("ServiceType", ""),
        "payer": d.get("payer", ""),
        "area": d.get("area", ""),
        "city": d.get("CitySender", ""),
        "WarehouseNumber": d.get("ID_Whs", ""),
        "ttn": d.get("ttn", ""),
    }


def _build_rozetka_block(d: Dict[str, str]) -> Dict[str, Any]:
    if d.get("DeliveryServiceAlias") != "ROZ":
        return {}
    return {
        "WarehouseNumber": d.get("ID_Whs", ""),
        "payer": d.get("payer", ""),
        "ttn": d.get("ttn", ""),
    }


# async def _send_to_salesdrive_stub(payload: Dict[str, Any]) -> None:
#     """
#     Заглушка: вместо реальной отправки — подробный лог.
#     """
#     import json
#     logger.info("🧪 [SALES DRIVE STUB] Payload:\n%s", json.dumps(payload, indent=2, ensure_ascii=False))


async def _initiate_refusal_stub(order: Dict[str, Any], reason: str) -> None:
    logger.warning("🚫 Инициирован отказ по заказу %s: %s", order.get("id"), reason)


# ------------------------------------------------
# ЛОГИКА ОПРЕДЕЛЕНИЯ ПОСТАВЩИКА ДЛЯ MULTI-ITEM
# ------------------------------------------------

async def _try_pick_single_supplier_by_exact_prices(
    session: AsyncSession, rows: List[OrderRow]
) -> Optional[str]:
    """
    Если каждая позиция имеет точного поставщика по правилу (price == offers.price для product_code),
    и все эти supplier_code одинаковые — возвращаем его.
    """
    picked: List[str] = []
    for r in rows:
        sc = await _fetch_supplier_by_price(session, r.goodsCode, r.price)
        if not sc:
            return None
        picked.append(sc)
    if len(set(picked)) == 1:
        return picked[0]
    return None


async def _try_pick_alternative_supplier_by_total_cap(
    session: AsyncSession, rows: List[OrderRow], candidates: Iterable[str]
) -> Optional[str]:
    """
    Ищем такого поставщика s из списка candidates, чтобы сумма его цен по всем товарам
    была <= сумме исходных цен по заказу. Возвращаем supplier_code.
    """
    total_incoming = sum((r.price * r.qty for r in rows), Decimal(0))

    for supplier_code in candidates:
        ok = True
        total_alt = Decimal(0)
        for r in rows:
            price_s = await _fetch_supplier_price(session, supplier_code, r.goodsCode)
            if price_s is None:
                ok = False
                break
            total_alt += price_s * r.qty
        if ok and total_alt <= total_incoming:
            return supplier_code

    return None


async def _collect_all_supplier_candidates(session: AsyncSession) -> List[str]:
    """
    Собираем список кодов поставщиков из DropshipEnterprise (или ограничьте по активным).
    """
    q = select(DropshipEnterprise.code)
    res = await session.execute(q)
    rows = res.scalars().all()
    return [str(x) for x in rows]


# -------------------------------
# СБОРКА PAYLOAD ДЛЯ SALESDRIVE
# -------------------------------

async def _build_products_block(
    session: AsyncSession,
    rows: List[OrderRow],
    supplier_code: str,
    supplier_name: str,
    supplier_changed_note: Optional[str] = None
) -> List[Dict[str, Any]]:
    products = []
    for r in rows:
        sku = await _fetch_sku_from_catalog_mapping(session, r.goodsCode, supplier_code)
        description = supplier_name
        if supplier_changed_note:
            # расширяем описание при смене поставщика
            description = f"{supplier_name}. {supplier_changed_note}"

        products.append(
            {
                "id": r.goodsCode,
                "name": r.goodsName,
                "costPerItem": str(r.price),  # исх. цена позиции
                "amount": str(r.qty),
                "description": "",
                "discount": "",
                "sku": sku or "",
            }
        )
    return products


def _make_supplier_changed_note(rows: List[OrderRow]) -> str:
    parts = [f"{r.goodsName} — {str(r.price)}" for r in rows]
    return "Поставщик изменён. Оригинальные позиции и цены: " + "; ".join(parts)


def _extract_name_parts(order: Dict[str, Any], d: Dict[str, str]) -> Tuple[str, str, str]:
    # fName: Name, lName: LastName, mName: MiddleName
    f = d.get("Name") or order.get("customer") or ""
    l = d.get("LastName") or ""
    m = d.get("MiddleName") or ""
    return f, l, m


async def build_salesdrive_payload(
    session: AsyncSession,
    order: Dict[str, Any],
    enterprise_code: str,
    rows: List[OrderRow],
    supplier_code: str,
    supplier_name: str,
) -> Dict[str, Any]:
    d = _delivery_dict(order)
    fName, lName, mName = _extract_name_parts(order, d)

    # если был альтернативный выбор поставщика — добавим пометку
    supplier_changed_note = None
    if order.get("_supplier_changed"):
        supplier_changed_note = _make_supplier_changed_note(rows)

    products = await _build_products_block(
        session, rows, supplier_code, supplier_name, supplier_changed_note
    )

    #form_key = await _get_enterprise_salesdrive_form(session, enterprise_code)

    payload = {
        
        "getResultData": "1",
        "fName": fName,
        "lName": lName,
        "mName": mName,
        "phone": order.get("customerPhone", ""),
        "email": "",
        "company": "",
        "products": products,
        "payment_method": "",
        "shipping_method": d.get("DeliveryServiceName", ""),
        "shipping_address": d.get("ReceiverWhs", ""),
        "comment": supplier_changed_note or supplier_name,
        "sajt": "",
        "externalId": order.get("id", ""),
        "organizationId": "1",
        "stockId": "",
        "novaposhta": _build_novaposhta_block(d),
        "ukrposhta": _build_ukrposhta_block(d),
        "meest": _build_meest_block(d),
        "rozetka_delivery": _build_rozetka_block(d),
        "prodex24source_full": "",
        "prodex24source": "",
        "prodex24medium": "",
        "prodex24campaign": "",
        "prodex24content": "",
        "prodex24term": "",
        "prodex24page": "",
    }
    return payload


# -----------------------------------------
# ГЛАВНАЯ ТОЧКА: ПРОЦЕССОР ОТПРАВКИ ЗАКАЗА
# -----------------------------------------

async def process_and_send_order(
    order: Dict[str, Any],
    enterprise_code: str,
    branch: Optional[str] = None,
) -> None:
    rows = _normalize_order_rows(order)
    if not rows:
        await _initiate_refusal_stub(order, "Пустые позиции заказа")
        return

    async with get_async_db() as session:
        api_key = await _get_salesdrive_api_key(session, enterprise_code)
        if not api_key:
            await _initiate_refusal_stub(order, "❌ Отсутствует API‑ключ для SalesDrive")
            return

        if len(rows) == 1:
            r = rows[0]
            supplier_code = await _fetch_supplier_by_price(session, r.goodsCode, r.price)
            if not supplier_code:
                await _initiate_refusal_stub(order, "Не найден поставщик по цене")
                return

            supplier_name = (await _fetch_supplier_name(session, supplier_code)) or supplier_code
            payload = await build_salesdrive_payload(session, order, enterprise_code, rows, supplier_code, supplier_name)
            await _send_to_salesdrive(payload, api_key)
            return

        supplier_code = await _try_pick_single_supplier_by_exact_prices(session, rows)
        if not supplier_code:
            candidates = await _collect_all_supplier_candidates(session)
            alt = await _try_pick_alternative_supplier_by_total_cap(session, rows, candidates)
            if alt:
                supplier_code = alt
                order["_supplier_changed"] = True
            else:
                await _initiate_refusal_stub(order, "Не удалось подобрать поставщика по сумме заказа")
                return

        supplier_name = (await _fetch_supplier_name(session, supplier_code)) or supplier_code
        payload = await build_salesdrive_payload(session, order, enterprise_code, rows, supplier_code, supplier_name)
        await _send_to_salesdrive(payload, api_key)


# -----------------------------------------
# REGISTRY для вашего роутера/диспетчера
# -----------------------------------------

# Пример: регистрируем процессор в вашем словаре
ORDER_SEND_PROCESSORS = {
    # data_format → функция
    # Пример: "GoogleDrive": process_and_send_order,
    #         "JetVet": process_and_send_order,
    # У вас в вызывающем коде должно вызываться: await processor(order, enterprise_code, branch)
    "DEFAULT": process_and_send_order
}

# Пример вызова (для локального теста):
# asyncio.run(process_and_send_order(sample_order, "342"))
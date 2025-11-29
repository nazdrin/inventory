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
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, getcontext
from app.services.notification_service import send_notification
# БАЗОВЫЙ URL SalesDrive (используется для /handler/ и /api/order/update/)
SALESDRIVE_BASE_URL = "https://petrenko.salesdrive.me"  # ← при необходимости замените на ваш домен

# Импорт для cancelled-orders API
from app.business.cancelled_orders_fetcher import get_cancelled_orders, acknowledge_cancelled_orders

# === Ваши модели (проверьте реальные имена/поля) ===
from app.database import get_async_db
from app.models import Offer, DropshipEnterprise, CatalogMapping, EnterpriseSettings
import httpx
from app.services.order_sender import send_orders_to_tabletki

# Маппинг branch → город для utmSource
BRANCH_CITY_MAP = {
    "59677": "Kyiv",
    "59766": "Ivano-Frankivsk",
    "59770": "Kremenchuk",
    "59791": "Lviv",
}

def _branch_to_city(branch: Optional[str]) -> str:
    """
    Возвращает название города по коду branch.
    Если кода нет в словаре, возвращает исходный код.
    """
    if not branch:
        return ""
    code = str(branch)
    return BRANCH_CITY_MAP.get(code, code)
logger = logging.getLogger(__name__)

def _notify_business(msg: str) -> None:
    try:
        send_notification(msg, "Business")  # ← второй аргумент — канал
    except Exception:
        logger.exception("Не удалось отправить уведомление: %s", msg)
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
class OrderRow:
    goodsCode: str
    goodsName: str
    qty: Decimal
    price: Decimal
    goodsProducer: Optional[str] = None
    original_price: Optional[Decimal] = None  # ← NEW
async def _send_to_salesdrive(payload: Dict[str, Any], api_key: str) -> None:
    """
    Отправка заказа в SalesDrive по API, с использованием X-Api-Key.
    """
    url = f"{SALESDRIVE_BASE_URL.rstrip('/')}/handler/"  # ← базовый домен берём из SALESDRIVE_BASE_URL

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

# --- HELPER для обновления заявки в SalesDrive через /api/order/update/
async def _salesdrive_update_order(update_url: str, api_key: str, payload: Dict[str, Any]) -> Optional[httpx.Response]:
    """
    Обновление заявки в SalesDrive через /api/order/update/.
    Требует X-Api-Key. update_url — полный URL до /api/order/update/.
    payload — тело запроса с externalId и data.
    Возвращает httpx.Response или None при сетевой/HTTP ошибке.
    """
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(update_url, json=payload, headers=headers)
            resp.raise_for_status()
            return resp
    except httpx.RequestError:
        return None
    except httpx.HTTPStatusError:
        return None

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
        price = _as_decimal(r.get("price", 0))
        rows.append(
            OrderRow(
                goodsCode=str(r.get("goodsCode")),
                goodsName=str(r.get("goodsName", "")),
                qty=_as_decimal(r.get("qty", 0)),
                price=price,                         # текущая (может меняться далее)
                goodsProducer=r.get("goodsProducer"),
                original_price=price,                # ← исходная (не меняем)
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
async def _get_supplier_priority(session: AsyncSession, supplier_code: str) -> int:
    q = select(DropshipEnterprise.priority).where(DropshipEnterprise.code == str(supplier_code)).limit(1)
    res = await session.execute(q)
    val = res.scalar_one_or_none()
    return int(val or 0)

async def _get_supplier_profit_percent(session: AsyncSession, supplier_code: str) -> Decimal:
    q = select(DropshipEnterprise.profit_percent).where(DropshipEnterprise.code == str(supplier_code)).limit(1)
    res = await session.execute(q)
    v = res.scalar_one_or_none()
    return _as_decimal(v or 0)

async def _fetch_stock_qty(session: AsyncSession, supplier_code: str, product_code: str) -> Decimal:
    """
    Возвращает остаток по товару у поставщика.
    По умолчанию читаю из Offer.qty. Если у вас остаток в другой таблице,
    замените запрос внутри на вашу схему.
    """
    try:
        # Вариант через Offers (если там есть поле qty)
        q = (
            select(Offer.stock)
            .where(and_(Offer.supplier_code == str(supplier_code), Offer.product_code == str(product_code)))
            .limit(1)
        )
        res = await session.execute(q)
        v = res.scalar_one_or_none()
        return _as_decimal(v or 0)
    except Exception:
        return Decimal(0)
async def _pick_supplier_for_single_item(
    session: AsyncSession,
    product_code: str,
    order_price: Decimal,
) -> Optional[Tuple[str, Decimal, bool]]:
    """
    Возвращает (supplier_code, supplier_price, price_went_down_flag) для ОДНОЙ позиции.

    Правила:
      1) Если есть поставщики с ценой РОВНО как в заказе — выбираем любого из них
         (если хотите детерминизм — можно добавить ORDER BY priority DESC).
      2) Иначе, если все цены НИЖЕ цены заказа — берём поставщика с max(profit_percent).
      3) Иначе (все цены ВЫШЕ) — допустим только Offer.price <= order_price + 0.10;
         если таких нет — вернуть None (дальше будет отказ).
    """
    price_tolerance = Decimal("0.10")

    # все офферы по товару, которые не выходят за допуск вверх (чтобы отсечь заведомо неподходящих)
    q_all = (
        select(Offer.supplier_code, Offer.price)
        .where(
            and_(
                Offer.product_code == str(product_code),
                Offer.price <= order_price + price_tolerance,
            )
        )
    )
    res = await session.execute(q_all)
    rows = res.all()
    if not rows:
        return None

    # 1) поставщики с ценой ровно как в заказе
    equal_suppliers = [(sc, _as_decimal(p)) for sc, p in rows if _as_decimal(p) == order_price]
    if equal_suppliers:
        # при желании можно выбрать с max(stock), а затем max(priority)
        # сейчас берём первого подходящего
        supplier_code, supplier_price = equal_suppliers[0]
        return str(supplier_code), _as_decimal(supplier_price), False  # price_went_down=False

    # 2) все цены ниже?
    lower_suppliers = [(sc, _as_decimal(p)) for sc, p in rows if _as_decimal(p) < order_price]
    if lower_suppliers:
        # выбираем по максимальному profit_percent
        scored = []
        for sc, p in lower_suppliers:
            profit = await _get_supplier_profit_percent(session, sc)
            scored.append((profit, sc, p))
        scored.sort(key=lambda x: x[0], reverse=True)  # max profit_percent
        _, supplier_code, supplier_price = scored[0]
        return str(supplier_code), _as_decimal(supplier_price), True  # цена уменьшается

    # 3) иначе остались только цены >= order_price (и все > order_price, т.к. равных не было).
    # мы сюда попали уже с фильтром <= order_price+0.10; если здесь пусто — None.
    higher_suppliers = [(sc, _as_decimal(p)) for sc, p in rows if _as_decimal(p) > order_price]
    if not higher_suppliers:
        return None  # на всякий случай

    # берём любого из оставшихся — цена уйдёт "вверх" в пределах допуска; флаг снижения = False
    supplier_code, supplier_price = higher_suppliers[0]
    return str(supplier_code), _as_decimal(supplier_price), False

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

# NEW: fetch barcode and supplier item code from CatalogMapping
from typing import Optional, Tuple
async def _fetch_barcode_and_supplier_code(
    session: AsyncSession, goods_code: str, supplier_code: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает (barcode, supplier_item_code, supplier_item_name) из CatalogMapping по goods_code и supplier_code.
    """
    field_name = f"Code_{supplier_code}"
    name_field = f"Name_{supplier_code}"
    code_col = getattr(CatalogMapping, field_name, None)
    name_col = getattr(CatalogMapping, name_field, None)
    if code_col is None and name_col is None:
        return (None, None, None)
    q = (
        select(CatalogMapping.Barcode, code_col, name_col)
        .where(CatalogMapping.ID == str(goods_code))
        .limit(1)
    )
    res = await session.execute(q)
    row = res.first()
    if not row:
        return (None, None, None)
    return (row[0], row[1], row[2])


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

async def process_cancelled_orders_service(
    enterprise_code: str,
    verify_ssl: bool = True,
) -> None:
    """
    Внешний сервис вызывает только с enterprise_code.
      • SalesDrive API key берём из БД: EnterpriseSettings.token
      • SalesDrive base URL берём из SALESDRIVE_BASE_URL (константа в этом файле)
    Шаги:
      1) Получить отказы (get_cancelled_orders)
      2) Для каждого отказа сделать POST /api/order/update/ в SalesDrive:
         externalId = id, data.statusId = 6, data.comment = cancelReason
      3) Подтвердить обработку через acknowledge_cancelled_orders
    """
    try:
        cancelled = await get_cancelled_orders(enterprise_code=enterprise_code, verify_ssl=verify_ssl)
    except Exception as e:
        try:
            send_notification(f"Помилка отримання відмов | enterprise={enterprise_code} | err={e}", "Business")
        except Exception:
            pass
        return

    if not cancelled:
        return

    # Получаем API-ключ SalesDrive из БД по enterprise_code
    try:
        async with get_async_db() as session:
            api_key = await _get_salesdrive_api_key(session, enterprise_code)
    except Exception:
        api_key = None

    if not api_key:
        try:
            send_notification(
                f"🚫Відмова: немає API ключа SalesDrive для обробки відмов | enterprise={enterprise_code}",
                "Business",
            )
        except Exception:
            pass
        return

    update_url = f"{SALESDRIVE_BASE_URL.rstrip('/')}/api/order/update/"

    acknowledged_ids: List[str] = []
    for item in cancelled:
        ext_id = str(item.get("id", "")).strip()
        cancel_reason = str(item.get("cancelReason", "")).strip()
        if not ext_id:
            continue

        payload = {
            "externalId": ext_id,
            "data": {
                "statusId": 6,
                "comment": cancel_reason,
            },
        }

        resp = await _salesdrive_update_order(update_url, api_key, payload)
        if resp is not None:
            acknowledged_ids.append(ext_id)

    if acknowledged_ids:
        try:
            await acknowledge_cancelled_orders(
                enterprise_code=enterprise_code,
                request_ids=acknowledged_ids,
                verify_ssl=verify_ssl,
            )
        except Exception:
            pass


# async def _send_to_salesdrive_stub(payload: Dict[str, Any]) -> None:
#     """
#     Заглушка: вместо реальной отправки — подробный лог.
#     """
#     import json
#     logger.info("🧪 [SALES DRIVE STUB] Payload:\n%s", json.dumps(payload, indent=2, ensure_ascii=False))


async def _initiate_refusal_stub(order: Dict[str, Any], reason: str, enterprise_code: str) -> None:
    """
    Отправляет отказ по одному заказу в Tabletki.
    Требования:
      - statusID = 7 (принудительно)
      - в rows минимум один товар
      - tabletki_login/password берём из EnterpriseSettings по enterprise_code
      - cancel_reason по ТЗ = 5
      - Всегда шлём уведомление в канал "Business" с причиной отказа
    """
    logger.warning("🚫 Инициализация отказа по заказу %s: %s", order.get("id"), reason)

    # 1) Валидация входа
    if not isinstance(order, dict) or not order.get("rows"):
        msg = f"Відмова замовлення id={order.get('id')} | enterprise={enterprise_code} | причина: {reason} | помилка: порожні rows"
        try:
            send_notification(msg, "Business")
        except Exception:
            logger.exception("Не удалось отправить уведомление: %s", msg)
        logger.error("⛔ Заказ некорректен или отсутствуют rows — отказ не отправлен. id=%s", order.get("id"))
        return
    if not enterprise_code:
        msg = f"Відмова замовлення id={order.get('id')} | причина: {reason} | помилка: не передан enterprise_code"
        try:
            send_notification(msg, "Business")
        except Exception:
            logger.exception("Не удалось отправить уведомление: %s", msg)
        logger.error("⛔ Не передан enterprise_code — отказ не отправлен. id=%s", order.get("id"))
        return

    # Статус отказа
    order["statusID"] = 7

    # 2) Достаём креды по enterprise_code и отправляем отказ
    try:
        async with get_async_db() as session:
            res = await session.execute(
                select(
                    EnterpriseSettings.tabletki_login,
                    EnterpriseSettings.tabletki_password
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            row = res.first()
            if not row or not row[0] or not row[1]:
                msg = (
                    f"🚫Відмова замовлення id={order.get('id')} | enterprise={enterprise_code} | "
                    f"причина: {reason} | помилка: немає tabletki_login/password"
                )
                try:
                    send_notification(msg, "Business")
                except Exception:
                    logger.exception("Не удалось отправить уведомление: %s", msg)
                logger.error("⛔ tabletki_login/password не найдены для enterprise_code=%s — отказ не отправлен.", enterprise_code)
                return

            tabletki_login, tabletki_password = row[0], row[1]

            # 3) Фиксированный код причины отказа
            cancel_reason_code = 5

            # 4) Уведомление о том, что отправляем отказ
            msg = (
                f"🚫Відмова замовлення id={order.get('id')} | enterprise={enterprise_code} | "
                f"reason='{reason}' | cancel_reason_code={cancel_reason_code}"
            )
            try:
                send_notification(msg, "Business")
            except Exception:
                logger.exception("Не удалось отправить уведомление: %s", msg)

            # 5) Отправка в Tabletki
            await send_orders_to_tabletki(
                session=session,
                orders=[order],
                tabletki_login=tabletki_login,
                tabletki_password=tabletki_password,
                cancel_reason=cancel_reason_code,
            )
            logger.info(
                "✅ Отказ отправлен: id=%s, enterprise=%s, reason=%r → code=%s",
                order.get("id"), enterprise_code, reason, cancel_reason_code
            )
    except Exception as e:
        logger.exception("❌ Ошибка при отправке отказа: %s", e)
        err_msg = (
            f"Помилка під час відправки відмови id={order.get('id')} | enterprise={enterprise_code} | err={e}"
        )
        try:
            send_notification(err_msg, "Business")
        except Exception:
            logger.exception("Не удалось отправить уведомление: %s", err_msg)
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
    Ищем таких поставщиков, у кого для каждой позиции есть цена, и
    SUM(price_s * qty) <= SUM(order.price * qty).
    Из тех, кто прошел, выбираем поставщика с максимальным суммарным остатком
    по всем позициям (сумма stock_qty), при равенстве — больший priority.
    """
    total_incoming = sum((r.price * r.qty for r in rows), Decimal(0))
    passed = []

    for supplier_code in candidates:
        ok = True
        total_alt = Decimal(0)
        sum_stock = Decimal(0)

        for r in rows:
            price_s = await _fetch_supplier_price(session, supplier_code, r.goodsCode)
            if price_s is None:
                ok = False
                break
            total_alt += price_s * r.qty
            sum_stock += await _fetch_stock_qty(session, supplier_code, r.goodsCode)

        if ok and total_alt <= total_incoming:
            priority = await _get_supplier_priority(session, supplier_code)
            passed.append(
                {
                    "supplier_code": supplier_code,
                    "sum_stock": sum_stock,
                    "priority": int(priority),
                }
            )

    if not passed:
        return None

    # Выбираем лучшего: по суммарному остатку, затем по приоритету
    passed.sort(key=lambda x: (x["sum_stock"], x["priority"]), reverse=True)
    return passed[0]["supplier_code"]

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


# Helper to format goods name with quantity if qty > 1
def _format_goods_name_with_qty(row: OrderRow) -> str:
    """
    Возвращает название товара с пометкой, если количество > 1.
    Пример: '🔴x3 | Магний B6'
    """
    try:
        qty_int = int(row.qty)
    except (ValueError, TypeError):
        return row.goodsName

    if qty_int <= 1:
        return row.goodsName

    # Цветной ярлычок + количество перед названием
    return f"🔴x{qty_int} | {row.goodsName}"


async def _build_products_block(
    session: AsyncSession,
    rows: List[OrderRow],
    supplier_code: str,
    supplier_name: str,
    supplier_changed_note: Optional[str] = None
) -> List[Dict[str, Any]]:
    products = []
    for r in rows:
        display_name = _format_goods_name_with_qty(r)
        sku = await _fetch_sku_from_catalog_mapping(session, r.goodsCode, supplier_code)
        # Fetch barcode, supplier item code, and supplier item name
        barcode, supplier_item_code, supplier_item_name = await _fetch_barcode_and_supplier_code(session, r.goodsCode, supplier_code)
        # Build description string: supplier name, barcode, supplier code (if present), comma-separated
        parts: list[str] = []
        if supplier_item_name:
            parts.append(str(supplier_item_name))
        if barcode:
            parts.append(str(barcode))
        if supplier_item_code:
            parts.append(str(supplier_item_code))
        description = ", ".join(parts)

        # Добавляем ярлык количества в description, чтобы он был виден даже если SalesDrive
        # в списке заявок использует именно описание товара, а не поле name.
        try:
            qty_int = int(r.qty)
        except (ValueError, TypeError):
            qty_int = 0

        if qty_int > 1:
            qty_label = f"🔴x{qty_int}"
            description = f"{qty_label} | {description}" if description else qty_label

        products.append(
            {
                "id": r.goodsCode,
                "name": display_name,
                "costPerItem": str(r.price),  # исх. цена позиции
                "amount": str(r.qty),
                "description": description,
                "discount": "",
                "sku": sku or "",
            }
        )
    return products


def _make_supplier_changed_note(rows: List[OrderRow], supplier_name: Optional[str] = None) -> str:
    # показываем ЦЕНЫ ДО корректировки (если есть), иначе текущие
    parts = [f"{r.goodsName} — {str(r.original_price if r.original_price is not None else r.price)}" for r in rows]
    base = "Оригінальні позиції та ціни: " + "; ".join(parts)
    if supplier_name:
        return f"Постачальник: {supplier_name}. {base}"
    return base

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
    branch: Optional[str] = None, 
) -> Dict[str, Any]:
    d = _delivery_dict(order)
    fName, lName, mName = _extract_name_parts(order, d)
    supplier_changed_note = None
    if order.get("_supplier_changed"):
        supplier_changed_note = _make_supplier_changed_note(rows, supplier_name)
    # если был альтернативный выбор поставщика — добавим пометку

     # ЯВНАЯ пометка о снижении цены (если это был кейс single-item со снижением)
    if order.get("_price_went_down"):
        extra_note = "Ціна постачальника нижча за ціну в замовленні: застосовано нижчу ціну."
        supplier_changed_note = (supplier_changed_note + " | " + extra_note) if supplier_changed_note else extra_note

    products = await _build_products_block(
        session, rows, supplier_code, supplier_name, supplier_changed_note
    )

    #form_key = await _get_enterprise_salesdrive_form(session, enterprise_code)
    # --- Новый блок: комментарий не содержит supplier_name и code_val, они идут в UTM-поля
    raw_code = order.get("code")
    code_val = str(raw_code).strip() if raw_code is not None else ""   # ← вот так безопасно

    # Комментарий теперь не содержит supplier_name и code_val,
    # они используются в UTM-полях ниже.
    comment_text = supplier_changed_note or supplier_name

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
        "comment": comment_text,
        "sajt": str(branch or ""),
        "externalId": order.get("id", ""),
        "organizationId": "1",
        "stockId": "",
        "novaposhta": _build_novaposhta_block(d),
        "ukrposhta": _build_ukrposhta_block(d),
        "meest": _build_meest_block(d),
        "rozetka_delivery": _build_rozetka_block(d),
        # Новые UTM-поля вместо prodex24*
        "utmSourceFull": code_val,   # был supplier_name в comment_text
        "utmSource": _branch_to_city(branch),   # передаём город по коду branch (или сам код, если нет в словаре)
        "utmMedium": supplier_name or "",                        # заполняется при необходимости
        "utmCampaign": supplier_name or "",                  # был code_val в comment_text
        "utmContent": "",
        "utmTerm": "",
        "utmPage": "",
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
    """
    Логика:
      - Нормализация rows; отказ при пустых строках (уведомляем "Business").
      - Получение api_key SalesDrive; отказ при отсутствии (уведомляем "Business").
      - SINGLE-ITEM (_pick_supplier_for_single_item):
          1) Если есть цена ровно как в заказе — берём этого поставщика (цена остаётся как в заказе).
          2) Иначе, если все цены ниже — берём поставщика с max(profit_percent) и снижаем r.price.
          3) Иначе — допускаем цены <= order_price + 0.10; если нет — отказ (уведомляем).
      - MULTI-ITEM:
          * Пытаемся найти единого поставщика по точным ценам (_try_pick_single_supplier_by_exact_prices).
          * Иначе — альтернатива по сумме (_try_pick_alternative_supplier_by_total_cap).
          * Если поставщик найден — перезаписываем r.price на цены из БД выбранного поставщика.
          * Формируем payload и отправляем в SalesDrive.
    """
    supplier_code: Optional[str] = None  # защитная инициализация

    # 1) Нормализация позиций
    rows = _normalize_order_rows(order)
    if not rows:
        try:
            send_notification(
                f"Відмова: порожні позиції | id={order.get('id')} | enterprise={enterprise_code}",
                "Business",
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление о пустых позициях")
        await _initiate_refusal_stub(order, "Пустые позиции заказа", enterprise_code)
        return

    # 2) Сессия и api_key
    async with get_async_db() as session:
        api_key = await _get_salesdrive_api_key(session, enterprise_code)
        if not api_key:
            try:
                send_notification(
                    f"🚫Відмова: немає API ключа SalesDrive | id={order.get('id')} | enterprise={enterprise_code}",
                    "Business",
                )
            except Exception:
                logger.exception("Не удалось отправить уведомление об отсутствии API-ключа")
            await _initiate_refusal_stub(order, "❌ Отсутствует API-ключ для SalesDrive", enterprise_code)
            return

        # === SINGLE-ITEM ===
        if len(rows) == 1:
            r = rows[0]

            pick = await _pick_supplier_for_single_item(session, r.goodsCode, r.price)
            if not pick:
                try:
                    send_notification(
                        f"🚫Відмова: не знайдено постачальника (допуск +0.10) | id={order.get('id')} | enterprise={enterprise_code}",
                        "Business",
                    )
                except Exception:
                    logger.exception("Не удалось отправить уведомление об отсутствии поставщика (single)")
                await _initiate_refusal_stub(
                    order,
                    "Не найден подходящий поставщик (учтён допуск +0.10)",
                    enterprise_code,
                )
                return

            supplier_code, supplier_price, price_went_down = pick
            supplier_name = (await _fetch_supplier_name(session, supplier_code)) or supplier_code

            # Если у выбранного поставщика цена ниже — применяем её и помечаем
            if price_went_down:
                order["_supplier_changed"] = True
                order["_price_went_down"] = True
                r.price = supplier_price  # в SalesDrive уйдёт сниженная цена

            payload = await build_salesdrive_payload(
                session, order, enterprise_code, rows, supplier_code, supplier_name, branch=branch
            )
            await _send_to_salesdrive(payload, api_key)
            # После отправки заказа — обработать отказы из Reserve API и обновить заявки в SalesDrive
            # (Автоматический вызов удалён)
            return

        # === MULTI-ITEM ===
        # 1) Пробуем единого поставщика по точным ценам
        supplier_code = await _try_pick_single_supplier_by_exact_prices(session, rows)

        # 2) Иначе — альтернатива по сумме (выбор по суммарному остатку, затем по priority)
        if not supplier_code:
            candidates = await _collect_all_supplier_candidates(session)
            alt = await _try_pick_alternative_supplier_by_total_cap(session, rows, candidates)
            if alt:
                supplier_code = alt
                order["_supplier_changed"] = True
            else:
                try:
                    send_notification(
                        f"🚫Відмова: не підібрано єдиного постачальника під суму | id={order.get('id')} | enterprise={enterprise_code}",
                        "Business",
                    )
                except Exception:
                    logger.exception("Не удалось отправить уведомление (multi, не подобрали по сумме)")
                await _initiate_refusal_stub(
                    order,
                    "Не удалось подобрать единого поставщика под сумму заказа",
                    enterprise_code,
                )
                return

        # Страховка (на всякий случай)
        if not supplier_code:
            try:
                send_notification(
                    f"🚫Відмова: внутрішня помилка вибору постачальника | id={order.get('id')} | enterprise={enterprise_code}",
                    "Business",
                )
            except Exception:
                logger.exception("Не удалось отправить уведомление (multi, supplier_code is None)")
            await _initiate_refusal_stub(order, "Внутренняя ошибка: не выбран поставщик", enterprise_code)
            return

        # Теперь, когда выбран поставщик, перезаписываем цены строк на цены из БД выбранного поставщика
        for r in rows:
            db_price = await _fetch_supplier_price(session, supplier_code, r.goodsCode)
            if db_price is not None:
                r.price = _as_decimal(db_price)

        supplier_name = (await _fetch_supplier_name(session, supplier_code)) or supplier_code

        payload = await build_salesdrive_payload(
            session, order, enterprise_code, rows, supplier_code, supplier_name, branch=branch
        )
        await _send_to_salesdrive(payload, api_key)
        # После отправки заказа — обработать отказы из Reserve API и обновить заявки в SalesDrive
        # (Автоматический вызов удалён)

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
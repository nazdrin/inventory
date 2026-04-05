# app/business/order_sender.py
from __future__ import annotations

import asyncio
from asyncio import sleep
import logging
import os
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
from app.business.supplier_identity import (
    SUPPLIERLIST_MAP,
    get_supplier_display_name_by_code,
    get_supplier_id_by_code,
)

# === Ваши модели (проверьте реальные имена/поля) ===
from app.database import get_async_db
from app.models import Offer, DropshipEnterprise, CatalogMapping, CatalogSupplierMapping, EnterpriseSettings, MasterCatalog
import httpx
from app.services.order_sender import send_orders_to_tabletki




logger = logging.getLogger(__name__)

# --- Logging controls (env) ---
# ORDER_SENDER_LOG_LEVEL: DEBUG/INFO/WARNING/ERROR (default INFO)
# ORDER_SENDER_VERBOSE_SALESDRIVE_LOGS: 1 to log full payload/response bodies (default 0)
_LOG_LEVEL = os.getenv("ORDER_SENDER_LOG_LEVEL", "INFO").upper()
logger.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
VERBOSE_SD_LOGS = os.getenv("ORDER_SENDER_VERBOSE_SALESDRIVE_LOGS", "0") == "1"
SALESDRIVE_RETRY_ATTEMPTS = max(1, int(os.getenv("SALESDRIVE_RETRY_ATTEMPTS", "3")))
SALESDRIVE_RETRY_DELAY_SEC = max(0.0, float(os.getenv("SALESDRIVE_RETRY_DELAY_SEC", "2")))


# Глобальный допуск по цене для поиска поставщика
PRICE_TOLERANCE = Decimal("0.10")

# Допуск по маржинальности: если retail_sum / wholesale_sum < 1.05 — не выбираем единого поставщика,
# а оставляем/делаем разбиение по позициям (как в текущей реализации).
MIN_RETAIL_WHOLESALE_RATIO_FOR_SINGLE = Decimal("1.05")

# Для умного подбора (multi-item) используем наличие (Offer.stock) и оптовую цену (Offer.wholesale_price).
# ВНИМАНИЕ: если поле wholesale_price называется иначе в вашей БД — поправьте в запросе ниже.

# Маппинг branch (серийный номер аптеки) → город
BRANCH_CITY_MAP = {
    "59677": "Kyiv",
    "59766": "Ivano-Frankivsk",
    "59770": "Kremenchuk",
    "59791": "Lviv",
}

SUPPLIER_CITY_TAG_MAP = {
    "D1": "Київ",
    "D2": "Івано-Франківськ",
    "D3": "Кременчук",
    "D4": "Львів",
    "D5": "Чернівці",
    "D6": "Київ",
    "D7": "Київ",
    "D8": "Київ",
    "D9": "Львів",
    "D10": "Вінниця",
    "D11": "Київ",
    "D12": "Київ",
}

D14_SUPPLIER_CODE = "D14"
D14_SALESDRIVE_STOCK_ID = 2

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

    last_error: Exception | None = None
    async with httpx.AsyncClient(timeout=15) as client:
        for attempt in range(1, SALESDRIVE_RETRY_ATTEMPTS + 1):
            if VERBOSE_SD_LOGS:
                logger.info("📦 Payload для SalesDrive:\n%s", json.dumps(payload, indent=2, ensure_ascii=False))

            try:
                response = await client.post(url, json=payload, headers=headers)

                logger.info(
                    "📤 SalesDrive POST /handler/ attempt=%s/%s status=%s externalId=%s",
                    attempt,
                    SALESDRIVE_RETRY_ATTEMPTS,
                    response.status_code,
                    payload.get("externalId"),
                )
                if VERBOSE_SD_LOGS:
                    logger.info("📨 Ответ от SalesDrive: %s", response.text)
                else:
                    logger.debug("📨 SalesDrive response (truncated): %.2000s", response.text)
                response.raise_for_status()
                return
            except httpx.RequestError as e:
                last_error = e
                logger.warning(
                    "SalesDrive request error attempt=%s/%s externalId=%s err=%s",
                    attempt,
                    SALESDRIVE_RETRY_ATTEMPTS,
                    payload.get("externalId"),
                    e,
                )
            except httpx.HTTPStatusError as e:
                last_error = e
                logger.warning(
                    "SalesDrive HTTP error attempt=%s/%s externalId=%s status=%s",
                    attempt,
                    SALESDRIVE_RETRY_ATTEMPTS,
                    payload.get("externalId"),
                    e.response.status_code,
                )

            if attempt < SALESDRIVE_RETRY_ATTEMPTS:
                await sleep(SALESDRIVE_RETRY_DELAY_SEC)

    err_msg = (
        f"❌ SalesDrive send failed after {SALESDRIVE_RETRY_ATTEMPTS} attempts | "
        f"externalId={payload.get('externalId')} | err={last_error}"
    )
    logger.error(err_msg)
    try:
        send_notification(err_msg, "Business")
    except Exception:
        logger.exception("Не удалось отправить уведомление об ошибке SalesDrive")
    raise RuntimeError(err_msg) from last_error

# --- HELPER для обновления заявки в SalesDrive через /api/order/update/
async def _salesdrive_update_order(update_url: str, api_key: str, payload: Dict[str, Any]) -> tuple[Optional[httpx.Response], bool]:
    """
    Обновление заявки в SalesDrive через /api/order/update/.
    Требует X-Api-Key. update_url — полный URL до /api/order/update/.
    payload — тело запроса с externalId и data.
    Возвращает (response, should_acknowledge).
    Для HTTP 422 считаем ошибку терминальной и возвращаем should_acknowledge=True без retry.
    """
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }
    last_error: Exception | None = None
    async with httpx.AsyncClient(timeout=20) as client:
        for attempt in range(1, SALESDRIVE_RETRY_ATTEMPTS + 1):
            try:
                resp = await client.post(update_url, json=payload, headers=headers)
                resp.raise_for_status()
                return resp, True
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status_code = exc.response.status_code if exc.response is not None else None
                response_text = exc.response.text if exc.response is not None else ""
                if status_code == 422:
                    logger.info(
                        "SalesDrive update skipped as terminal 422: externalId=%s body=%s",
                        payload.get("externalId"),
                        response_text[:1000],
                    )
                    return None, True
                logger.warning(
                    "SalesDrive update retry attempt=%s/%s externalId=%s err=%s",
                    attempt,
                    SALESDRIVE_RETRY_ATTEMPTS,
                    payload.get("externalId"),
                    exc,
                )
            except httpx.RequestError as exc:
                last_error = exc
                logger.warning(
                    "SalesDrive update retry attempt=%s/%s externalId=%s err=%s",
                    attempt,
                    SALESDRIVE_RETRY_ATTEMPTS,
                    payload.get("externalId"),
                    exc,
                )
                if attempt < SALESDRIVE_RETRY_ATTEMPTS:
                    await sleep(SALESDRIVE_RETRY_DELAY_SEC)
    logger.error(
        "SalesDrive update failed after %s attempts | externalId=%s | err=%s",
        SALESDRIVE_RETRY_ATTEMPTS,
        payload.get("externalId"),
        last_error,
    )
    return None, False

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
    db_name = res.scalar_one_or_none()
    return db_name or get_supplier_display_name_by_code(supplier_code)


def _is_d14_supplier(supplier_code: Optional[str]) -> bool:
    return str(supplier_code or "").strip().upper() == D14_SUPPLIER_CODE
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


# --- NEW: Оптова ціна (wholesale_price) товару у конкретного постачальника ---
async def _fetch_supplier_wholesale_price(
    session: AsyncSession, supplier_code: str, product_code: str
) -> Optional[Decimal]:
    """Оптова ціна (wholesale_price) товару у конкретного постачальника (з таблиці offers)."""
    q = (
        select(Offer.wholesale_price)
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

# === NEW: поиск поставщиков по допуску и ближайшей цене ===
from typing import List, Tuple, Optional

async def _find_suppliers_within_tolerance(
    session: AsyncSession,
    product_code: str,
    order_price: Decimal,
    tolerance: Decimal = PRICE_TOLERANCE,
) -> List[Tuple[str, Decimal]]:
    """
    Возвращает список (supplier_code, supplier_price) для товара, где
    |price - order_price| <= tolerance, отсортированный по модулю разницы.
    """
    q = select(Offer.supplier_code, Offer.price).where(Offer.product_code == str(product_code))
    res = await session.execute(q)
    rows = res.all()
    matches: List[Tuple[str, Decimal]] = []
    for sc, p in rows:
        p_dec = _as_decimal(p)
        if abs(p_dec - order_price) <= tolerance:
            matches.append((str(sc), p_dec))
    matches.sort(key=lambda x: (abs(x[1] - order_price), x[1]))
    return matches



async def _find_nearest_supplier_by_price(
    session: AsyncSession,
    product_code: str,
    order_price: Decimal,
) -> Optional[Tuple[str, Decimal]]:
    """
    Возвращает (supplier_code, supplier_price) для поставщика с ценой,
    наиболее близкой к order_price (без ограничения по допуску).
    """
    q = select(Offer.supplier_code, Offer.price).where(Offer.product_code == str(product_code))
    res = await session.execute(q)
    rows = res.all()
    if not rows:
        return None
    candidates: List[Tuple[str, Decimal]] = []
    for sc, p in rows:
        p_dec = _as_decimal(p)
        candidates.append((str(sc), p_dec))
    candidates.sort(key=lambda x: (abs(x[1] - order_price), x[1]))
    return candidates[0] if candidates else None


# --- SMART multi-item helpers ---

async def _prefetch_offers_for_products(
    session: AsyncSession,
    product_codes: List[str],
) -> Dict[str, Dict[str, Dict[str, Decimal]]]:
    """ 
    Prefetch offers for a list of product codes.

    Returns mapping:
      offers[supplier_code][product_code] = {
          "price": Decimal,
          "wholesale_price": Decimal,
          "stock": Decimal,
      }

    NOTE: expects columns Offer.price, Offer.wholesale_price, Offer.stock.
    """
    q = (
        select(
            Offer.supplier_code,
            Offer.product_code,
            Offer.price,
            Offer.wholesale_price,
            Offer.stock,
        )
        .where(Offer.product_code.in_([str(x) for x in product_codes]))
    )
    res = await session.execute(q)
    rows = res.all()

    out: Dict[str, Dict[str, Dict[str, Decimal]]] = {}
    for sc, pc, price, wprice, stock in rows:
        sc_s = str(sc)
        pc_s = str(pc)
        out.setdefault(sc_s, {})
        out[sc_s][pc_s] = {
            "price": _as_decimal(price),
            "wholesale_price": _as_decimal(wprice),
            "stock": _as_decimal(stock),
        }
    return out


def _supplier_can_fulfill_all(
    rows: List[OrderRow],
    supplier_code: str,
    offers_map: Dict[str, Dict[str, Dict[str, Decimal]]],
) -> bool:
    """True if supplier has offers for ALL rows and stock >= qty for each row."""
    by_supplier = offers_map.get(str(supplier_code)) or {}
    for r in rows:
        rec = by_supplier.get(str(r.goodsCode))
        if not rec:
            return False
        if rec.get("stock", Decimal(0)) < _as_decimal(r.qty):
            return False
    return True


def _calc_order_retail_sum(rows: List[OrderRow]) -> Decimal:
    return sum((_as_decimal(r.price) * _as_decimal(r.qty) for r in rows), Decimal(0))


def _calc_supplier_wholesale_sum(
    rows: List[OrderRow],
    supplier_code: str,
    offers_map: Dict[str, Dict[str, Dict[str, Decimal]]],
) -> Decimal:
    by_supplier = offers_map.get(str(supplier_code)) or {}
    total = Decimal(0)
    for r in rows:
        rec = by_supplier.get(str(r.goodsCode))
        if not rec:
            return Decimal("Infinity")
        total += _as_decimal(rec.get("wholesale_price", 0)) * _as_decimal(r.qty)
    return total


def _pick_best_single_supplier_by_margin(
    rows: List[OrderRow],
    candidates: List[str],
    offers_map: Dict[str, Dict[str, Dict[str, Decimal]]],
) -> Optional[Tuple[str, Decimal, Decimal, Decimal]]:
    """ 
    Choose a single supplier among candidates maximizing:
      retail_sum - wholesale_sum

    Returns tuple:
      (supplier_code, delta, retail_sum, wholesale_sum)
    """
    retail_sum = _calc_order_retail_sum(rows)
    best: Optional[Tuple[str, Decimal, Decimal, Decimal]] = None

    for sc in candidates:
        wholesale_sum = _calc_supplier_wholesale_sum(rows, sc, offers_map)
        if wholesale_sum == Decimal("Infinity"):
            continue
        delta = retail_sum - wholesale_sum
        if best is None or delta > best[1]:
            best = (str(sc), delta, retail_sum, wholesale_sum)

    return best


def _greedy_group_rows_min_suppliers(
    rows: List[OrderRow],
    offers_map: Dict[str, Dict[str, Dict[str, Decimal]]],
) -> Dict[str, str]:
    """ 
    Greedy grouping to minimize number of suppliers.

    Strategy:
      - While there are remaining rows, pick supplier that can fulfill the largest number of remaining rows
        (stock>=qty and offer exists). Tie-breaker: highest (retail_sum_subset - wholesale_sum_subset).
      - Assign those rows to that supplier and remove from remaining.

    Returns mapping: goodsCode -> supplier_code
    """
    remaining = list(rows)
    mapping: Dict[str, str] = {}

    # Pre-calc retail sum per row for tie-breaking
    def subset_delta(sc: str, subset: List[OrderRow]) -> Decimal:
        retail = sum((_as_decimal(r.price) * _as_decimal(r.qty) for r in subset), Decimal(0))
        wholesale = Decimal(0)
        by_supplier = offers_map.get(str(sc)) or {}
        for r in subset:
            rec = by_supplier.get(str(r.goodsCode))
            if not rec:
                return Decimal("-Infinity")
            wholesale += _as_decimal(rec.get("wholesale_price", 0)) * _as_decimal(r.qty)
        return retail - wholesale

    supplier_codes = list(offers_map.keys())

    while remaining:
        best_sc: Optional[str] = None
        best_cover: List[OrderRow] = []
        best_score = Decimal("-Infinity")

        for sc in supplier_codes:
            by_supplier = offers_map.get(str(sc)) or {}
            cover = [
                r for r in remaining
                if (str(r.goodsCode) in by_supplier)
                and (by_supplier[str(r.goodsCode)].get("stock", Decimal(0)) >= _as_decimal(r.qty))
            ]
            if not cover:
                continue

            score = subset_delta(sc, cover)
            # prefer larger cover; tie-breaker by delta
            if (len(cover) > len(best_cover)) or (len(cover) == len(best_cover) and score > best_score):
                best_sc = str(sc)
                best_cover = cover
                best_score = score

        if not best_sc or not best_cover:
            # cannot cover remaining with any supplier — stop
            break

        for r in best_cover:
            mapping[str(r.goodsCode)] = best_sc
        # remove assigned
        assigned_codes = {str(r.goodsCode) for r in best_cover}
        remaining = [r for r in remaining if str(r.goodsCode) not in assigned_codes]

    return mapping



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


def _use_master_mapping_for_orders() -> bool:
    return os.getenv("USE_MASTER_MAPPING_FOR_STOCK", "0").strip().lower() in {"1", "true", "yes", "on"}


async def _fetch_sku_from_master_mapping(
    session: AsyncSession, goods_code: str, supplier_code: str
) -> Optional[str]:
    supplier_id = get_supplier_id_by_code(supplier_code)
    if not supplier_id:
        return None

    row = (
        await session.execute(
            select(CatalogSupplierMapping.supplier_code)
            .where(
                CatalogSupplierMapping.sku == str(goods_code),
                CatalogSupplierMapping.supplier_id == supplier_id,
                CatalogSupplierMapping.is_active.is_(True),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    return row


async def _fetch_barcode_and_supplier_code_master(
    session: AsyncSession, goods_code: str, supplier_code: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    supplier_id = get_supplier_id_by_code(supplier_code)
    if not supplier_id:
        return (None, None, None)

    row = (
        await session.execute(
            select(
                MasterCatalog.barcode,
                CatalogSupplierMapping.supplier_code,
                CatalogSupplierMapping.supplier_product_name_raw,
                CatalogSupplierMapping.barcode,
            )
            .join(MasterCatalog, MasterCatalog.sku == CatalogSupplierMapping.sku)
            .where(
                CatalogSupplierMapping.sku == str(goods_code),
                CatalogSupplierMapping.supplier_id == supplier_id,
                CatalogSupplierMapping.is_active.is_(True),
            )
            .limit(1)
        )
    ).first()
    if not row:
        return (None, None, None)

    master_barcode, supplier_item_code, supplier_item_name, mapping_barcode = row
    return (
        master_barcode or mapping_barcode,
        supplier_item_code,
        supplier_item_name,
    )


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


async def _fetch_sku_for_order_line(
    session: AsyncSession, goods_code: str, supplier_code: str
) -> Optional[str]:
    if _use_master_mapping_for_orders():
        master_value = await _fetch_sku_from_master_mapping(session, goods_code, supplier_code)
        if master_value:
            return master_value
    return await _fetch_sku_from_catalog_mapping(session, goods_code, supplier_code)


async def _fetch_barcode_and_supplier_code_for_order_line(
    session: AsyncSession, goods_code: str, supplier_code: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if _use_master_mapping_for_orders():
        master_row = await _fetch_barcode_and_supplier_code_master(session, goods_code, supplier_code)
        if any(master_row):
            return master_row
    return await _fetch_barcode_and_supplier_code(session, goods_code, supplier_code)


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

        resp, should_ack = await _salesdrive_update_order(update_url, api_key, payload)
        if should_ack:
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
                enterprise_code=enterprise_code,
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

# === NEW: Detect multi-supplier exact match ("мікс") ===
async def _detect_multi_supplier_exact_match(
    session: AsyncSession,
    rows: List[OrderRow],
) -> Optional[List[Tuple[OrderRow, str]]]:
    """
    Детектируем кейс, когда КАЖДАЯ позиция имеет точное совпадение по цене,
    но поставщики для разных позиций могут отличаться.

    Возвращаем список пар (строка заказа, supplier_code) или None,
    если хотя бы для одной строки совпадение не найдено или все строки
    относятся к одному и тому же поставщику.
    """
    mapping: List[Tuple[OrderRow, str]] = []
    suppliers: List[str] = []

    for r in rows:
        sc = await _fetch_supplier_by_price(session, r.goodsCode, r.price)
        if not sc:
            return None
        mapping.append((r, sc))
        suppliers.append(sc)

    if len(set(suppliers)) <= 1:
        # либо один поставщик, либо пусто — это не "мікс"
        return None

    return mapping


def _make_mixed_suppliers_comment(rows_with_suppliers: List[Tuple[OrderRow, str]]) -> str:
    """
    Формирует комментарий для кейса, когда позиции соответствуют ценам разных поставщиков.
    """
    suppliers = sorted({sc for _, sc in rows_with_suppliers})
    supplier_list = ", ".join(suppliers) if suppliers else ""
    parts = [
        f"{r.goodsName} — {str(r.price)} ({sc})"
        for r, sc in rows_with_suppliers
    ]
    details = "; ".join(parts)
    base = (
        "Увага: у замовленні товари з цінами, що відповідають різним постачальникам"
    )
    if supplier_list:
        base += f" ({supplier_list})"
    return f"{base}. Потрібна ручна перевірка. Деталі: {details}"


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
    Возвращает только название товара (без количества и значков).
    """
    return row.goodsName


async def _build_products_block(
    session: AsyncSession,
    rows: List[OrderRow],
    supplier_code: Optional[str],
    supplier_name: str,
    supplier_changed_note: Optional[str] = None,
    row_supplier_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    products = []
    for r in rows:
        display_name = _format_goods_name_with_qty(r)

        sku: Optional[str] = None
        barcode: Optional[str] = None
        supplier_item_code: Optional[str] = None
        supplier_item_name: Optional[str] = None

        # ВАЖНО: если в заказе несколько поставщиков, supplier_code на уровне заказа будет None.
        # Тогда берём поставщика для конкретной строки из row_supplier_map.
        effective_supplier_code: Optional[str] = supplier_code
        if not effective_supplier_code and row_supplier_map:
            effective_supplier_code = row_supplier_map.get(str(r.goodsCode))

        effective_supplier_name: Optional[str] = None
        if effective_supplier_code:
            # SKU/Barcode/Code/Name тянем по поставщику конкретной строки
            sku = await _fetch_sku_for_order_line(session, r.goodsCode, effective_supplier_code)
            barcode, supplier_item_code, supplier_item_name = await _fetch_barcode_and_supplier_code_for_order_line(
                session, r.goodsCode, effective_supplier_code
            )
            # Имя поставщика тоже подтягиваем (для отображения в description)
            effective_supplier_name = (await _fetch_supplier_name(session, effective_supplier_code)) or effective_supplier_code

        description = str(supplier_item_code) if supplier_item_code else ""

        product_payload = {
            "id": r.goodsCode,
            "name": display_name,
            "costPerItem": str(r.price),  # исх. цена позиции
            "amount": str(r.qty),
            "expenses": "0",
            "description": description,
            "barcode": str(barcode) if barcode else "",
            "discount": "",
            "sku": str(r.goodsCode),
        }

        if _is_d14_supplier(effective_supplier_code):
            product_payload["stockId"] = D14_SALESDRIVE_STOCK_ID
            logger.info(
                "SalesDrive line stockId override applied: goodsCode=%s supplier=%s stockId=%s",
                r.goodsCode,
                effective_supplier_code,
                D14_SALESDRIVE_STOCK_ID,
            )

        products.append(product_payload)
    return products


def _make_supplier_changed_note(rows: List[OrderRow], supplier_name: Optional[str] = None) -> str:
    # показываем ЦЕНЫ ДО корректировки (если есть), иначе текущие
    parts = [f"{r.goodsName} — {str(r.original_price if r.original_price is not None else r.price)}" for r in rows]
    base = "Оригінальні позиції та ціни: " + "; ".join(parts)
    if supplier_name:
        return f"Постачальник: {supplier_name}. {base}"
    return base

# --- Multiline comment helpers ---
def _format_multi_supplier_list(items: List[Tuple[str, str]]) -> str:
    """items: list of (goods_name, supplier_name). Returns multiline comment."""
    lines = ["⚠️ Для товарів знайдені різні постачальники:", ""]
    for goods_name, supplier_name in items:
        lines.append(f"⚪️ {goods_name} — {supplier_name}")
    return "\n".join(lines)

def _format_smart_single_supplier_comment(supplier_name: str, delta: Decimal, retail_sum: Decimal, wholesale_sum: Decimal) -> str:
    return "\n".join(
        [
            "⚠️ Підібраний єдиний постачальник:",
            "",
            f"🔵 Постачальник: {supplier_name}",
            "",
            f"▫️ Нова маржа (Δ): {delta}",
            f"▫️ Роздрібна сума: {retail_sum}",
            f"▫️ Оптова сума: {wholesale_sum}",
        ]
    )

# Multiline comment for grouped-suppliers (greedy split)
def _format_grouped_suppliers_comment(supplier_to_goods: Dict[str, List[str]], name_map: Dict[str, str]) -> str:
    """Multiline comment for case when we split order into minimal number of suppliers."""
    lines: List[str] = ["⚠️ Єдиного постачальника не знайдено", ""]

    # Keep deterministic order: by supplier display name, then by supplier code
    ordered = sorted(supplier_to_goods.items(), key=lambda kv: (name_map.get(kv[0], kv[0]), kv[0]))

    for idx, (sc, goods_list) in enumerate(ordered):
        supplier_label = name_map.get(sc, sc)
        lines.append(f"🔵 {supplier_label}")
        for g in goods_list:
            lines.append(f"▫️ {g}")
        # blank line between suppliers (not after the last one)
        if idx != len(ordered) - 1:
            lines.append("")

    return "\n".join(lines)

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
    supplier_code: Optional[str],
    supplier_name: str,
    branch: Optional[str] = None,
    comment_override: Optional[str] = None,
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

    # Для multi-supplier кейса процессор может положить в order карту поставщиков по строкам
    row_supplier_map = order.get("_row_supplier_map") if isinstance(order.get("_row_supplier_map"), dict) else None

    products = await _build_products_block(
        session,
        rows,
        supplier_code,
        supplier_name,
        supplier_changed_note,
        row_supplier_map=row_supplier_map,
    )

    # --- NEW: opt (тільки total: сума оптових цін з урахуванням qty) ---
    row_supplier_map = order.get("_row_supplier_map") if isinstance(order.get("_row_supplier_map"), dict) else None

    opt_total = Decimal(0)

    for idx, r in enumerate(rows):
        # визначаємо постачальника для конкретної позиції
        effective_supplier_code: Optional[str] = supplier_code
        if not effective_supplier_code and row_supplier_map:
            effective_supplier_code = row_supplier_map.get(str(r.goodsCode))

        w_price: Optional[Decimal] = None
        if effective_supplier_code:
            w_price = await _fetch_supplier_wholesale_price(session, effective_supplier_code, r.goodsCode)

        w_dec = _as_decimal(w_price or 0)
        line_opt = w_dec * _as_decimal(r.qty)
        opt_total += line_opt

        if idx < len(products):
            products[idx]["expenses"] = str(w_dec)

    # В SalesDrive передаємо лише total
    opt_text = str(opt_total)

    #form_key = await _get_enterprise_salesdrive_form(session, enterprise_code)
    # --- Новый блок: комментарий не содержит supplier_name и code_val, они идут в UTM-поля
    raw_code = order.get("code")
    code_val = str(raw_code).strip() if raw_code is not None else ""   # ← вот так безопасно

    # Комментарий: либо переданный явно, либо сформированный по изменению постачальника, либо просто имя постачальника
    if comment_override:
        comment_text = comment_override
    else:
        comment_text = supplier_changed_note or supplier_name

    # Общее количество единиц товара в заказе
    try:
        total_qty = sum(int(r.qty) for r in rows)
    except (ValueError, TypeError):
        total_qty = 0

    supplierlist_val = ""
    if supplier_code:
        supplierlist_val = SUPPLIERLIST_MAP.get(str(supplier_code), "")
        if _is_d14_supplier(supplier_code):
            logger.info(
                "SalesDrive supplier recognized as D14: supplier_code=%s supplierlist=%s",
                supplier_code,
                supplierlist_val,
            )

    supplier_city_tag = ""
    if supplier_code:
        supplier_city_tag = SUPPLIER_CITY_TAG_MAP.get(str(supplier_code), "")

    city = BRANCH_CITY_MAP.get(str(branch), str(branch or ""))
    if supplier_city_tag:
        if isinstance(city, str) and "(" in city and city.endswith(")"):
            city = city[:-1] + f", {supplier_city_tag})"
        else:
            city = f"{city} ({supplier_city_tag})"

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
        "stockId": D14_SALESDRIVE_STOCK_ID if _is_d14_supplier(supplier_code) else "",
        "novaposhta": _build_novaposhta_block(d),
        "ukrposhta": _build_ukrposhta_block(d),
        "meest": _build_meest_block(d),
        "rozetka_delivery": _build_rozetka_block(d),
        # Новые поля для интеграции с SalesDrive
        "city": city,
        "branch": str(branch or ""),              # серийный номер аптеки
        "tabletkiOrder": code_val,               # номер заказа Tabletki.ua (бывший utmSourceFull)
        "supplier": supplier_name or "",         # поставщик (бывший utmMedium/utmCampaign)
        "opt": opt_text,                         # оптові ціни (wholesale_price) + total
        # qtyOrder: значок-предупреждение, если суммарное количество > 1
        "qtyOrder": f"🔴x{total_qty}" if total_qty > 1 else "",
        "supplierlist": supplierlist_val,
    }
    if _is_d14_supplier(supplier_code):
        logger.info(
            "SalesDrive root stockId override applied: externalId=%s supplier=%s stockId=%s",
            order.get("id", ""),
            supplier_code,
            D14_SALESDRIVE_STOCK_ID,
        )
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
      - SINGLE-ITEM:
          1) Если есть поставщик с ценой в допуске 10 копеек — берём его (comment = название поставщика).
          2) Если нет — ищем ближайшего по цене (comment = ⚠️ с предупреждением и разницей в цене).
          3) Если вообще нет офферов — comment = ⚠️ не найдено, supplier пустой.
      - MULTI-ITEM:
          4) Если у всех позиций найден поставщик по цене с допуском 10 копеек и он един для всех — supplier = название, comment = название.
          5) Если у всех позиций найдены разные поставщики с допуском — supplier пустой, comment = ⚠️ с перечислением товаров и их постачальників.
          6) Если есть позиции без поставщика в допуске — supplier пустой, comment = ⚠️ с деталями по найденным/ненайденным.
      - После формирования payload — отправка в SalesDrive.
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
            # Нет API-ключа SalesDrive — уведомляем, но отказ не формируем.
            # Заказ останется для повторной отправки при следующем запуске, когда ключ будет добавлен.
            try:
                send_notification(
                    f"❌ Немає API ключа SalesDrive | id={order.get('id')} | enterprise={enterprise_code}",
                    "Business",
                )
            except Exception:
                logger.exception("Не удалось отправить уведомление об отсутствии API-ключа")
            return

        # === SINGLE-ITEM ===
        if len(rows) == 1:
            r = rows[0]

            comment_override: Optional[str] = None

            # 1–3. Поиск поставщика по логике допуска / ближайшей цены / полного отсутствия
            matches = await _find_suppliers_within_tolerance(session, r.goodsCode, r.price)
            if matches:
                # 1) Есть поставщик по цене с допуском 10 копеек — берём первого (самый близкий)
                supplier_code = matches[0][0]
                supplier_name = (await _fetch_supplier_name(session, supplier_code)) or supplier_code
                # comment и supplier = название поставщика
                comment_override = supplier_name
            else:
                # 2) Нет поставщика в допуске 10 копеек — ищем ближайшего по цене
                nearest = await _find_nearest_supplier_by_price(session, r.goodsCode, r.price)
                if nearest:
                    supplier_code, _supplier_price = nearest
                    supplier_name = (await _fetch_supplier_name(session, supplier_code)) or supplier_code
                    # supplier заполняем именем поставщика
                    comment_override = supplier_name
                else:
                    # 3) Вообще нет офферов этого товара — supplier пустой, comment с предупреждением
                    supplier_code = None
                    supplier_name = ""
                    comment_override = (
                        "⚠️ Не знайдено постачальника для товару: "
                        f"{r.goodsName} (код {r.goodsCode}, ціна {r.price})."
                    )

            payload = await build_salesdrive_payload(
                session,
                order,
                enterprise_code,
                rows,
                supplier_code,
                supplier_name,
                branch=branch,
                comment_override=comment_override,
            )
            await _send_to_salesdrive(payload, api_key)
            # После отправки заказа — обработать отказы из Reserve API и обновить заявки в SalesDrive
            # (Автоматический вызов удалён)
            return

        # === MULTI-ITEM ===
        # Для каждой позиции ищем поставщика по цене с допуском 10 копеек
        rows_with_supplier: List[Tuple[OrderRow, str, Decimal]] = []  # row, supplier_code, supplier_price
        rows_without_supplier: List[OrderRow] = []

        for r in rows:
            matches = await _find_suppliers_within_tolerance(session, r.goodsCode, r.price)
            if matches:
                sc, sp = matches[0]
                rows_with_supplier.append((r, sc, sp))
            else:
                rows_without_supplier.append(r)

        # Карта: goodsCode -> supplier_code (по строкам). Используется для формирования description
        # даже когда весь заказ является "мікс" (supplier_code=None на уровне заказа).
        order["_row_supplier_map"] = {str(r.goodsCode): str(sc) for (r, sc, _sp) in rows_with_supplier}

        # --- SMART multi-item algorithm (единый поставщик по наличию и оптовой цене) ---
        # Условия запуска:
        #   - если НЕ найден единый поставщик по цене (в допуске) ИЛИ есть позиции без поставщика в допуске.
        # Алгоритм:
        #   1) Собираем поставщиков, которые могут отгрузить ВСЕ товары (stock>=qty по каждой позиции).
        #   2) Среди них выбираем того, у кого (sum(retail) - sum(wholesale)) максимальна.
        #   3) Если retail_sum / wholesale_sum < 1.05 — НЕ выбираем единого поставщика (мало маржи),
        #      оставляем разбиение по позициям (текущая логика).
        #   4) Если товаров > 2 и нет единого поставщика — пробуем разбить на группы, минимизируя число поставщиков.

        try:
            product_codes = [str(r.goodsCode) for r in rows]
            offers_map = await _prefetch_offers_for_products(session, product_codes)
        except Exception:
            offers_map = {}

        smart_single_supplier: Optional[Tuple[str, Decimal, Decimal, Decimal]] = None
        if offers_map:
            all_candidates = [sc for sc in offers_map.keys() if _supplier_can_fulfill_all(rows, sc, offers_map)]
            if all_candidates:
                smart_single_supplier = _pick_best_single_supplier_by_margin(rows, all_candidates, offers_map)

        # Если нашли единого поставщика по наличию+оптовой цене — применяем, но с проверкой на минимальную маржинальность
        if smart_single_supplier is not None:
            sc, delta, retail_sum, wholesale_sum = smart_single_supplier
            ratio_ok = True
            if wholesale_sum and wholesale_sum != 0:
                ratio = (retail_sum / wholesale_sum)
                ratio_ok = ratio >= MIN_RETAIL_WHOLESALE_RATIO_FOR_SINGLE

            if ratio_ok:
                supplier_code = sc
                supplier_name = (await _fetch_supplier_name(session, supplier_code)) or supplier_code
                comment_override = _format_smart_single_supplier_comment(
                    supplier_name=supplier_name,
                    delta=delta,
                    retail_sum=retail_sum,
                    wholesale_sum=wholesale_sum,
                )
                payload = await build_salesdrive_payload(
                    session,
                    order,
                    enterprise_code,
                    rows,
                    supplier_code,
                    supplier_name,
                    branch=branch,
                    comment_override=comment_override,
                )
                await _send_to_salesdrive(payload, api_key)
                return
            else:
                # маржа слишком мала — оставляем текущую модель разбиения
                logger.info(
                    "SMART single supplier rejected due to low ratio: retail/wholesale < %s",
                    MIN_RETAIL_WHOLESALE_RATIO_FOR_SINGLE,
                )

        # Если товаров > 2 и нет единого поставщика — пробуем умное разбиение на группы
        if len(rows) > 2 and offers_map:
            grouped_map = _greedy_group_rows_min_suppliers(rows, offers_map)
            # применяем только если покрыли все товары
            if grouped_map and len(grouped_map) == len(rows):
                # Перезапишем карту по строкам для description
                order["_row_supplier_map"] = {str(r.goodsCode): grouped_map[str(r.goodsCode)] for r in rows}

                # Формируем читабельный комментарий: группы по поставщикам (multiline)
                supplier_to_goods: Dict[str, List[str]] = {}
                for r in rows:
                    sc = grouped_map.get(str(r.goodsCode))
                    supplier_to_goods.setdefault(str(sc), []).append(str(r.goodsName))

                name_map: Dict[str, str] = {}
                for sc in supplier_to_goods.keys():
                    name_map[sc] = (await _fetch_supplier_name(session, sc)) or sc

                supplier_code = None
                supplier_name = ""
                comment_override = _format_grouped_suppliers_comment(supplier_to_goods, name_map)

                payload = await build_salesdrive_payload(
                    session,
                    order,
                    enterprise_code,
                    rows,
                    supplier_code,
                    supplier_name,
                    branch=branch,
                    comment_override=comment_override,
                )
                await _send_to_salesdrive(payload, api_key)
                return

        comment_override: Optional[str] = None

        if rows_with_supplier and not rows_without_supplier:
            # У всех товаров найден поставщик в допуске
            unique_codes = sorted({sc for _, sc, _ in rows_with_supplier})
            if len(unique_codes) == 1:
                # 4) Единый поставщик по цене с допуском 10 копеек
                supplier_code = unique_codes[0]
                supplier_name = (await _fetch_supplier_name(session, supplier_code)) or supplier_code
                # comment и supplier = название поставщика
                comment_override = supplier_name
            else:
                # 5) Для товаров найдены разные поставщики с допуском — supplier пустой,
                # в comment перечисляем товары и их постачальників
                supplier_code = None
                supplier_name = ""
                # мапа код поставщика -> имя
                name_map: Dict[str, str] = {}
                for _, sc, _ in rows_with_supplier:
                    if sc not in name_map:
                        name_map[sc] = (await _fetch_supplier_name(session, sc)) or sc
                items = [(r.goodsName, name_map[sc]) for (r, sc, _) in rows_with_supplier]
                comment_override = _format_multi_supplier_list(items)
        else:
            # Есть товары без поставщика в допуске (или вообще никому не нашли)
            supplier_code = None
            supplier_name = ""
            found_parts: List[str] = []
            missing_parts: List[str] = []

            if rows_with_supplier:
                name_map: Dict[str, str] = {}
                for _, sc, _ in rows_with_supplier:
                    if sc not in name_map:
                        name_map[sc] = (await _fetch_supplier_name(session, sc)) or sc
                for r, sc, _ in rows_with_supplier:
                    found_parts.append(f"{r.goodsName} — {name_map[sc]}")

            if rows_without_supplier:
                for r in rows_without_supplier:
                    missing_parts.append(f"{r.goodsName} — постачальник не знайдений")

            lines: List[str] = ["⚠️"]
            if found_parts:
                lines.append("Товари з знайденими постачальниками:")
                for fp in found_parts:
                    lines.append(f"⚪️ {fp}")
            if missing_parts:
                if found_parts:
                    lines.append("")
                lines.append("Товари без постачальника:")
                for mp in missing_parts:
                    lines.append(f"⚪️ {mp}")

            if len(lines) > 1:
                comment_override = "\n".join(lines)
            else:
                comment_override = "⚠️ Не знайдено постачальників для товарів у замовленні."

        payload = await build_salesdrive_payload(
            session,
            order,
            enterprise_code,
            rows,
            supplier_code,
            supplier_name,
            branch=branch,
            comment_override=comment_override,
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

# app/business/dropship_pipeline.py
import asyncio
import inspect
import logging
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

# === ВАША ИНФРАСТРУКТУРА / МОДЕЛИ ===
from app.database import get_async_db
from app.models import Offer, DropshipEnterprise, CompetitorPrice  # CompetitorPrice имеет поля: code, city, competitor_price
from app.business.feed_biotus import parse_feed_stock_to_json


logger = logging.getLogger("dropship")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --------------------------------------------------------------------------------------
# Утилиты
# --------------------------------------------------------------------------------------
def _to_decimal(x: Optional[float | Decimal]) -> Decimal:
    if x is None:
        return Decimal("0")
    return Decimal(str(x))

def _as_share(x: Optional[float | Decimal]) -> Decimal:
    """25 -> 0.25; 0.25 -> 0.25; None -> 0"""
    d = _to_decimal(x)
    if d == 0:
        return Decimal("0")
    if d > 1:
        return d / Decimal("100")
    return d

def _round_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _split_cities(city_field: str) -> List[str]:
    """Разбиваем по , ; | и обрезаем пробелы."""
    if not city_field:
        return []
    parts = re.split(r"[;,|]", city_field)
    return [p.strip() for p in parts if p.strip()]

# --------------------------------------------------------------------------------------
# Реестр парсеров (подставьте свои реализации)
# --------------------------------------------------------------------------------------
ParserFn = Callable[..., List[dict[str, Any]]]

async def parse_feed_stock_to_json_template(*, code: str, timeout: int = 20, **kwargs) -> List[dict]:
    """
    Заглушка. Реальный парсер должен вернуть список словарей:
      {"code_sup": "...", "qty": int, "price_retail": float, "price_opt": float}
    """
    logger.warning("Parser for supplier %s not implemented; returning empty list.", code)
    return []

PARSERS: Dict[str, ParserFn] = {
    "D1": parse_feed_stock_to_json,
}

# --------------------------------------------------------------------------------------
# Универсальный ИМЕНОВАННЫЙ вызов парсера: code=<ent.code>, timeout=20 (+ session/enterprise если поддерживаются)
# --------------------------------------------------------------------------------------
async def _call_parser_kw(parser: ParserFn, session: AsyncSession, ent: DropshipEnterprise) -> List[dict]:
    if parser is None:
        return []
    # базовые kwargs
    base_kwargs: Dict[str, Any] = {
        "code": ent.code,
        "timeout": 20,
        "session": session,
        "enterprise": ent,
        "ent": ent,
    }
    # оставляем только те, что реально есть в сигнатуре
    sig = inspect.signature(parser)
    accepted: Dict[str, Any] = {}
    for name in sig.parameters.keys():
        if name in base_kwargs:
            accepted[name] = base_kwargs[name]

    result = parser(**accepted)
    if inspect.isawaitable(result):
        result = await result

    if result is None:
        return []
    if isinstance(result, dict):
        return [result]
    if isinstance(result, list):
        return result
    if isinstance(result, str):
        import json
        parsed = json.loads(result)
        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            return parsed
        raise TypeError("JSON из парсера должен быть dict или list.")
    raise TypeError("Парсер должен вернуть list[dict] / dict / JSON-строку этих структур.")

# --------------------------------------------------------------------------------------
# 1) Активные поставщики
# --------------------------------------------------------------------------------------
async def fetch_active_enterprises(session: AsyncSession) -> List[DropshipEnterprise]:
    q = select(DropshipEnterprise).where(DropshipEnterprise.is_active.is_(True))
    res = await session.execute(q)
    return list(res.scalars().all())

# --------------------------------------------------------------------------------------
# 2) Маппинг Code_<supplier> -> product_code (берём "ID" ВЕРХНИМИ из catalog_mapping)
# --------------------------------------------------------------------------------------
CATALOG_MAPPING_ID_COL = "ID"  # у вас именно так (UPPER)

async def map_supplier_codes(
    session: AsyncSession,
    supplier_code: str,
    items: List[dict],
) -> List[dict]:
    """
    Для каждого item ищем в catalog_mapping колонку "Code_<supplier_code>" по значению code_sup.
    Возвращаем [{"product_code": <ID>, "qty": ..., "price_retail": ..., "price_opt": ...}, ...]
    """
    mapped: List[dict] = []
    column_name = f'Code_{supplier_code}'  # например "Code_D1"

    for it in items:
        code_sup = it.get("code_sup")
        if not code_sup:
            continue

        sql = text(f'''
            SELECT "{CATALOG_MAPPING_ID_COL}"
            FROM catalog_mapping
            WHERE "{column_name}" = :code_sup
            LIMIT 1
        ''')
        res = await session.execute(sql, {"code_sup": code_sup})
        row = res.first()
        if not row:
            logger.info("Mapping not found: supplier=%s code_sup=%s", supplier_code, code_sup)
            continue

        product_code = str(row[0])  # master-код (ваш ID)
        mapped.append({
            "product_code": product_code,
            "qty": int(it.get("qty") or 0),
            "price_retail": it.get("price_retail"),
            "price_opt": it.get("price_opt"),
        })

    return mapped

# --------------------------------------------------------------------------------------
# 3) Цена конкурента и расчёт нашей цены
# --------------------------------------------------------------------------------------
async def fetch_competitor_price(session: AsyncSession, product_code: str, city: str) -> Optional[Decimal]:
    """
    ВАЖНО: в вашей таблице competitor_prices поле кода — 'code', а не 'product_code'.
    Полей времени нет — берём первую запись (LIMIT 1).
    """
    q = (
        select(CompetitorPrice.competitor_price)
        .where(
            CompetitorPrice.code == product_code,
            CompetitorPrice.city == city
        )
        .limit(1)
    )
    res = await session.execute(q)
    row = res.first()
    return _to_decimal(row[0]) if row else None

def compute_price_for_item(
    *,
    competitor_price: Optional[Decimal],
    is_rrp: bool,
    is_wholesale: bool,
    price_retail: Optional[float | Decimal],
    price_opt: Optional[float | Decimal],
    profit_percent: Optional[float | Decimal],
    min_markup_threshold: Optional[float | Decimal],
) -> Decimal:
    """
    Алгоритм:
      base = competitor*0.99 (если есть) иначе price_retail (если >0) иначе price_opt
      если is_rrp -> не ниже price_retail
      нижний порог:
        - wholesale:  rr * (1 - profit + minmk)
        - retail:     po * (1 + minmk)
      итог = max(base, RRP, floor), округление до копейки
    """
    rr = _to_decimal(price_retail)
    po = _to_decimal(price_opt)
    profit = _as_share(profit_percent)
    minmk  = _as_share(min_markup_threshold)

    if competitor_price is not None:
        candidate = competitor_price * Decimal("0.99")
    else:
        candidate = rr if rr > 0 else po

    price = candidate

    if is_rrp and rr > 0:
        price = max(price, rr)

    floor = Decimal("0")
    if is_wholesale:
        if rr > 0:
            floor = rr * (Decimal("1") - profit + minmk)
    else:
        if po > 0:
            floor = po * (Decimal("1") + minmk)

    if floor > 0:
        price = max(price, floor)

    return _round_money(price)

# --------------------------------------------------------------------------------------
# 4) UPSERT в offers
# --------------------------------------------------------------------------------------
async def upsert_offer(
    session: AsyncSession,
    *,
    product_code: str,
    supplier_code: str,
    city: str,
    price: Decimal,
    stock: int,
) -> None:
    """
    UPSERT по UNIQUE(product_code, supplier_code, city) — см. constraint 'uq_offers_product_supplier_city'
    """
    stmt = insert(Offer).values(
        product_code=product_code,
        supplier_code=supplier_code,
        city=city,
        price=price,
        stock=stock,
    ).on_conflict_do_update(
        constraint="uq_offers_product_supplier_city",
        set_={
            "price": price,
            "stock": stock,
        }
    )
    await session.execute(stmt)

# --------------------------------------------------------------------------------------
# 5) Обработка одного поставщика end-to-end
# --------------------------------------------------------------------------------------
async def process_supplier(
    session: AsyncSession,
    ent: DropshipEnterprise,
    parser_registry: Dict[str, ParserFn],
) -> None:
    code = ent.code
    parser = parser_registry.get(code, parse_feed_stock_to_json_template)

    # 5.1 сырые данные из парсера (именованно: code=<ent.code>, timeout=20, + session/enterprise если поддерживаются)
    raw_items = await _call_parser_kw(parser, session, ent)
    if not raw_items:
        logger.info("Supplier %s: parser returned no items.", code)
        return

    # 5.2 маппинг кодов (Code_<supplier> -> ID из catalog_mapping."ID")
    mapped = await map_supplier_codes(session, code, raw_items)
    if not mapped:
        logger.info("Supplier %s: no mapped items.", code)
        return

    # 5.3 параметры ценообразования
    is_rrp = bool(ent.is_rrp)
    is_wholesale = bool(ent.is_wholesale)
    profit_percent = ent.profit_percent or 0
    min_markup_threshold = ent.min_markup_threshold or 0

    # 5.4 города поставщика
    cities = _split_cities(ent.city or "")
    if not cities:
        logger.warning("Supplier %s: empty 'city' field; skipping.", code)
        return

    # 5.5 цикл по городам и товарам
    for city in cities:
        logger.info("Supplier %s / city %s / items %d", code, city, len(mapped))
        for it in mapped:
            product_code = str(it["product_code"])  # это ID из catalog_mapping."ID"
            qty = int(it.get("qty") or 0)
            rr = it.get("price_retail")
            po = it.get("price_opt")

            competitor = await fetch_competitor_price(session, product_code, city)
            price = compute_price_for_item(
                competitor_price=competitor,
                is_rrp=is_rrp,
                is_wholesale=is_wholesale,
                price_retail=rr,
                price_opt=po,
                profit_percent=profit_percent,
                min_markup_threshold=min_markup_threshold,
            )

            await upsert_offer(
                session,
                product_code=product_code,
                supplier_code=code,
                city=city,
                price=price,
                stock=qty,
            )

# --------------------------------------------------------------------------------------
# 6) Главный раннер
# --------------------------------------------------------------------------------------
async def run_pipeline() -> None:
    async with get_async_db() as session:
        suppliers = await fetch_active_enterprises(session)
        if not suppliers:
            logger.info("No active dropship enterprises.")
            return

        for ent in suppliers:
            try:
                await process_supplier(session, ent, PARSERS)
                await session.commit()
            except Exception as exc:
                logger.exception("Failed supplier %s: %s", ent.code, exc)
                await session.rollback()

if __name__ == "__main__":
    asyncio.run(run_pipeline())

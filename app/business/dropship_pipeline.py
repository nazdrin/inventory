# dropship_pipeline.py
import asyncio
import logging
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import text, select, asc, desc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

# === ІМПОРТИ ВАШИХ ІНФРАСТРУКТУР ===
# Налаштуйте під ваш проєкт:
from app.database import get_async_db  # ваш async session maker

# Якщо у вас є ORM-моделі, імпортуйте їх; інакше можна працювати через text() запити
from app.models import Offer, DropshipEnterprise  # припускаю, що у вас ці моделі вже є

logger = logging.getLogger("dropship")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


# -------------------------------
# УТИЛІТИ
# -------------------------------

def _to_decimal(x: Optional[float | Decimal]) -> Decimal:
    if x is None:
        return Decimal("0")
    return Decimal(str(x))

def _as_share(x: Optional[float | Decimal]) -> Decimal:
    """
    Перетворює 25 -> 0.25, 0.25 -> 0.25, None -> 0
    """
    d = _to_decimal(x)
    if d == 0:
        return Decimal("0")
    if d > 1:
        return (d / Decimal("100"))
    return d

def _round_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _split_cities(city_field: str) -> List[str]:
    """
    Розбиваємо поле city на список міст. Підтримує: ',', ';', '|'
    """
    if not city_field:
        return []
    parts = re.split(r"[;,|]", city_field)
    return [p.strip() for p in parts if p.strip()]


# -------------------------------
# РЕЄСТР ПАРСЕРІВ ПОСТАЧАЛЬНИКІВ
# -------------------------------

ParserFn = Callable[[AsyncSession, DropshipEnterprise], "list[dict[str, Any]]"]

async def parse_feed_stock_to_json_template(session: AsyncSession, ent: DropshipEnterprise) -> List[dict]:
    """
    Заглушка. Сюди підключаєте реальну реалізацію (парсинг XML/CSV із ent.feed_url або з GDrive).
    ПОВЕРТАЄ:
      [
        {"code_sup": "ABC123", "qty": 8, "price_retail": 259.0, "price_opt": 219.0},
        ...
      ]
    """
    # TODO: Реалізувати під конкретного постачальника
    logger.warning("Parser for %s is not implemented; returning empty list", ent.code)
    return []

PARSERS: Dict[str, ParserFn] = {
    # Приклади:
    # "D1": parse_d1_feed,
    # "D2": parse_d2_from_gdrive,
    # На старті — все через шаблон
}


# -------------------------------
# 1) ЗАВАНТАЖЕННЯ АКТИВНИХ ПОСТАЧАЛЬНИКІВ
# -------------------------------

async def fetch_active_enterprises(session: AsyncSession) -> List[DropshipEnterprise]:
    """
    Всі активні dropship-постачальники.
    """
    q = select(DropshipEnterprise).where(DropshipEnterprise.is_active.is_(True))
    res = await session.execute(q)
    return list(res.scalars().all())


# -------------------------------
# 2) МАПІНГ КОДІВ ПОСТАЧАЛЬНИКА -> ВАШ product_code (ID)
# -------------------------------

async def map_supplier_codes(
    session: AsyncSession,
    supplier_code: str,
    items: List[dict],
) -> List[dict]:
    """
    Для кожного елемента шукає у catalog_mapping значення ID по колонці Code_<supplier_code>.
    Повертає список елементів вигляду:
      {"product_code": <ID>, "qty": ..., "price_retail": ..., "price_opt": ...}
    Пропускає ті, де не знайдено відповідності.
    """
    column_name = f'Code_{supplier_code}'
    mapped: List[dict] = []

    for it in items:
        code_sup = it.get("code_sup")
        if not code_sup:
            continue

        # ВАЖЛИВО: лапки навколо назви колонки, якщо у вас CamelCase
        sql = text(f'''
            SELECT id
            FROM catalog_mapping
            WHERE "{column_name}" = :code_sup
            LIMIT 1
        ''')
        res = await session.execute(sql, {"code_sup": code_sup})
        row = res.first()
        if not row:
            logger.info("Mapping not found for supplier=%s code_sup=%s", supplier_code, code_sup)
            continue

        product_code = row[0]
        mapped.append({
            "product_code": product_code,
            "qty": it.get("qty") or 0,
            "price_retail": it.get("price_retail"),
            "price_opt": it.get("price_opt"),
        })

    return mapped


# -------------------------------
# 3) РОЗРАХУНОК ЦІНИ + UPSERT У OFFERS
# -------------------------------

async def fetch_competitor_price(session: AsyncSession, product_code: str, city: str) -> Optional[Decimal]:
    """
    Беремо останню відому ціну конкурента для пари (product_code, city).
    """
    sql = text("""
        SELECT competitor_price
        FROM competitor_prices
        WHERE product_code = :product_code AND city = :city
        ORDER BY updated_at DESC
        LIMIT 1
    """)
    res = await session.execute(sql, {"product_code": product_code, "city": city})
    row = res.first()
    if not row:
        return None
    return _to_decimal(row[0])

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
    Реалізація описаного алгоритму з «порогами».
    """
    rr = _to_decimal(price_retail)
    po = _to_decimal(price_opt)

    profit = _as_share(profit_percent)            # 25 -> 0.25
    minmk  = _as_share(min_markup_threshold)      # 25 -> 0.25

    # 1) Базова кандидат-ціна
    if competitor_price is not None:
        candidate = competitor_price * Decimal("0.99")
    else:
        candidate = rr if rr > 0 else po

    price = candidate

    # 2) RRP: не нижче price_retail (як РРЦ)
    if is_rrp and rr > 0:
        price = max(price, rr)

    # 3) Нижня межа (floor)
    floor = Decimal("0")
    if is_wholesale:
        # floor = price_retail * (1 - profit_percent + min_markup_threshold)
        if rr > 0:
            floor = rr * (Decimal("1") - profit + minmk)
    else:
        # floor = price_opt * (1 + min_markup_threshold)
        if po > 0:
            floor = po * (Decimal("1") + minmk)

    if floor > 0:
        price = max(price, floor)

    # 4) фінальне округлення
    return _round_money(price)

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
    UPSERT у таблицю offers по унікальному ключу (product_code, supplier_code, city).
    Використовує ORM-модель Offer і constraint 'uq_offers_product_supplier_city'.
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


async def process_supplier(
    session: AsyncSession,
    ent: DropshipEnterprise,
    parser_registry: Dict[str, ParserFn],
) -> None:
    """
    Повний цикл по одному постачальнику:
    - зчитати сирові дані (parser)
    - мапнути коди
    - для кожного міста: врахувати конкурентні ціни та зробити upsert у offers
    """
    code = ent.code
    parser = parser_registry.get(code, parse_feed_stock_to_json_template)
    raw_items = await parser(session, ent)

    if not raw_items:
        logger.info("No items returned by parser for supplier %s", code)
        return

    mapped = await map_supplier_codes(session, code, raw_items)
    if not mapped:
        logger.info("No mapped items for supplier %s", code)
        return

    is_rrp = bool(ent.is_rrp)
    is_wholesale = bool(ent.is_wholesale)
    profit_percent = ent.profit_percent or 0
    min_markup_threshold = ent.min_markup_threshold or 0

    cities = _split_cities(ent.city or "")
    if not cities:
        logger.warning("Supplier %s has empty 'city' field; skipping city-specific offers", code)
        return

    for city in cities:
        logger.info("Processing supplier=%s city=%s (items=%d)", code, city, len(mapped))
        for it in mapped:
            product_code = str(it["product_code"])
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


# -------------------------------
# 4) ЗАПУСК УСЬОГО КОНВЕЄРА
# -------------------------------

async def run_pipeline() -> None:
    """
    Головний вхід: обробляє всіх активних постачальників і комітить результати.
    """
    async with get_async_db() as session:   # ваш контекст менеджер повертає AsyncSession
        suppliers = await fetch_active_enterprises(session)
        if not suppliers:
            logger.info("No active dropship enterprises found.")
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
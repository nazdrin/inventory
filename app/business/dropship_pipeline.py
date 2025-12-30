# app/business/dropship_pipeline.py
import asyncio
import inspect
import logging
import re
import argparse
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional
from pathlib import Path
import tempfile
import json
from datetime import datetime, timezone

from sqlalchemy import text, select, func, asc, desc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

# === ВАША ИНФРАСТРУКТУРА / МОДЕЛИ ===
from app.database import get_async_db
from app.models import Offer, DropshipEnterprise, CompetitorPrice  # CompetitorPrice: code, city, competitor_price
from app.business.feed_biotus import parse_feed_stock_to_json
from app.business.feed_dsn import parse_dsn_stock_to_json
from app.business.feed_proteinplus import parse_feed_stock_to_json as parse_feed_D3
from app.business.feed_dobavki import parse_d4_stock_to_json as parse_feed_D4
from app.business.feed_monstr import parse_feed_stock_to_json as parse_feed_D5
from app.business.feed_sportatlet import parse_d6_stock_to_json as parse_feed_D6
from app.business.feed_pediakid import parse_pediakid_stock_to_json as parse_feed_D7
# опционально: сервис "куда отдать массив"
try:
    from app.services.database_service import process_database_service
except Exception:
    async def process_database_service(file_path, file_type, enterprise_code):
        logging.getLogger("dropship").warning(
            "process_database_service() недоступен. Заглушка: file_type=%s enterprise_code=%s items=%d",
            file_type, enterprise_code, len(file_path)
        )

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
    "D2": parse_dsn_stock_to_json,
    "D3": parse_feed_D3,
    "D4": parse_feed_D4,
    "D5": parse_feed_D5,
    "D6": parse_feed_D6,
    "D7": parse_feed_D7,
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
    column_name = f'Code_{supplier_code}'

    # 1. собираем все уникальные коды поставщика
    codes_set = {it.get("code_sup") for it in items if it.get("code_sup")}
    if not codes_set:
        return []

    # 2. одним запросом вытягиваем все соответствия
    sql = text(f'''
        SELECT "{CATALOG_MAPPING_ID_COL}" AS id,
               "{column_name}" AS code_sup
        FROM catalog_mapping
        WHERE "{column_name}" = ANY(:codes)
    ''')
    res = await session.execute(sql, {"codes": list(codes_set)})
    rows = res.fetchall()

    # 3. строим dict code_sup -> ID
    code_to_id: Dict[str, str] = {}
    for r in rows:
        m = r._mapping
        cs = str(m["code_sup"])
        pid = str(m["id"])
        code_to_id[cs] = pid

    # 4. собираем результирующий список, сохраняя логику и порядок
    for it in items:
        code_sup = it.get("code_sup")
        if not code_sup:
            continue
        product_code = code_to_id.get(str(code_sup))
        if not product_code:
            continue
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



# --- Новый хелпер: получить последнюю применённую политику балансировщика для city+supplier ---
async def fetch_latest_balancer_policy(
    session: AsyncSession,
    *,
    city: str,
    supplier_code: str,
) -> Optional[dict]:
    """
    Берём последнюю применённую политику из balancer_policy_log:
      - is_applied = true
      - city = :city
      - supplier = :supplier_code

    Возвращает dict:
      {
        "rules": [{"band_id": "...", "porog": 0.15}, ...],
        "min_porog_by_band": {"B1": 0.15, ...},
        "price_bands": [{"band_id": "B1", "min": 0, "max": 300}, ...]  # из config_snapshot профиля
        "segment_id": "...",
        "segment_start": "...",
        "segment_end": "..."
      }
    Если записи нет — None.
    """
    sql = text("""
        SELECT
            rules,
            min_porog_by_band,
            config_snapshot,
            segment_id,
            segment_start,
            segment_end
        FROM balancer_policy_log
        WHERE is_applied = true
          AND city = :city
          AND supplier = :supplier
        ORDER BY segment_start DESC, id DESC
        LIMIT 1
    """)
    res = await session.execute(sql, {"city": city, "supplier": supplier_code})
    row = res.first()
    if not row:
        return None

    rules, min_porog_by_band, config_snapshot, segment_id, segment_start, segment_end = row

    # config_snapshot может быть NULL
    price_bands = []
    try:
        if isinstance(config_snapshot, dict):
            # ищем профиль, который подходит под city+supplier
            profiles = config_snapshot.get("profiles") or []
            for p in profiles:
                scope = (p.get("scope") or {})
                cities = scope.get("cities") or []
                suppliers = scope.get("suppliers") or []
                if city in cities and supplier_code in suppliers:
                    price_bands = p.get("price_bands") or []
                    break
    except Exception:
        price_bands = []

    return {
        "rules": rules or [],
        "min_porog_by_band": min_porog_by_band or {},
        "price_bands": price_bands or [],
        "segment_id": segment_id,
        "segment_start": segment_start,
        "segment_end": segment_end,
    }


def resolve_band_id_from_bands(price: Decimal, bands: list[dict]) -> Optional[str]:
    """
    Определяем band_id по цене (берём rr, а если rr=0 — можно подать любую базовую цену).
    bands: [{"band_id":"B1","min":0,"max":300}, ...]
    Правило: min <= price < max, если max отсутствует/null -> price >= min.
    """
    if price is None:
        return None
    p = _to_decimal(price)
    if p <= 0:
        return None

    for b in bands or []:
        try:
            band_id = str(b.get("band_id"))
            mn = _to_decimal(b.get("min"))
            mx_raw = b.get("max")
            mx = None if mx_raw is None else _to_decimal(mx_raw)

            if mx is None:
                if p >= mn:
                    return band_id
            else:
                if p >= mn and p < mx:
                    return band_id
        except Exception:
            continue
    return None


def rule_porog_by_band(rules: list[dict], band_id: str) -> Optional[Decimal]:
    for r in rules or []:
        if str(r.get("band_id")) == str(band_id):
            try:
                return _to_decimal(r.get("porog"))
            except Exception:
                return None
    return None




# --- Новая логика расчёта цены ---
def compute_price_for_item(
    *,
    competitor_price: Optional[Decimal],
    is_rrp: bool,
    is_dumping: bool,
    retail_markup: Optional[float | Decimal],
    price_retail: Optional[float | Decimal],
    price_opt: Optional[float | Decimal],
    threshold_percent_effective: Optional[float | Decimal],
    discount_min: Decimal = Decimal("0.001"),   # 0.1%
    discount_max: Decimal = Decimal("0.01"),    # 1%
) -> Decimal:
    """
    Новая логика (для НЕ-RRP):

    Вход:
      - rr: розничная цена
      - po: оптовая цена
      - competitor_price
      - threshold_percent_effective: порог (в процентах или доле), уже с учётом cap (store_serial)

    Примечание: если rr отсутствует/0, то в местах где rr требуется, берём threshold_price.
    """
    rr = _to_decimal(price_retail)
    po = _to_decimal(price_opt)

    # 1) ЖЁСТКАЯ РРЦ
    if is_rrp and rr > 0:
        return _round_money(rr)

    # 1.1) ЖЁСТКИЙ "демпинг": price = price_opt * (1 + retail_markup)
    # Приоритет ниже РРЦ, но выше режима конкурентов/порогов.
    if is_dumping:
        mu = _as_share(retail_markup)  # 25 -> 0.25; 0.25 -> 0.25; None -> 0
        if po > 0:
            return _round_money(po * (Decimal("1") + mu))
        # Если оптовой нет — fallback на rr (если есть), иначе 0
        if rr > 0:
            return _round_money(rr)
        return Decimal("0")

    # 2) Эффективный порог
    thr = _as_share(threshold_percent_effective)
    threshold_price = Decimal("0")
    if po > 0 and thr >= 0:
        threshold_price = po * (Decimal("1") + thr)

    # 4) Цена под конкурента (если есть конкурент)
    under_competitor: Optional[Decimal] = None
    if competitor_price is not None and competitor_price > 0:
        # random discount in [discount_min, discount_max]
        disc = Decimal(str(__import__("random").uniform(float(discount_min), float(discount_max))))
        candidate = competitor_price * (Decimal("1") - disc)

        # clamp delta between 2 and 15 UAH
        delta = competitor_price - candidate
        if delta < Decimal("2"):
            candidate = competitor_price - Decimal("2")
        elif delta > Decimal("15"):
            candidate = competitor_price - Decimal("15")

        # safety: never above competitor
        if candidate > competitor_price:
            candidate = competitor_price

        under_competitor = candidate

    # 5) Ограничение: цена под конкурента не может быть выше rr
    if under_competitor is not None and rr > 0:
        under_competitor = min(under_competitor, rr)

    # 6) Выбор итоговой цены (без enterprise_settings диапазонов)
    # Если rr отсутствует/0, то в местах где rr требуется, берём threshold_price.
    def _fallback() -> Decimal:
        if rr > 0:
            return rr
        if threshold_price > 0:
            return threshold_price
        if under_competitor is not None and under_competitor > 0:
            return under_competitor
        return Decimal("0")

    # Если конкурента нет
    if under_competitor is None:
        if threshold_price > 0:
            # не поднимаем выше rr, если rr задан и меньше пороговой
            if rr > 0 and rr < threshold_price:
                return _round_money(rr)
            return _round_money(threshold_price)
        return _round_money(_fallback())

    # Конкурент есть
    if threshold_price > 0 and under_competitor >= threshold_price:
        return _round_money(under_competitor)

    # under_competitor < threshold_price
    if threshold_price > 0:
        # не поднимаем выше rr, если rr задан и меньше пороговой
        if rr > 0 and rr < threshold_price:
            return _round_money(rr)
        return _round_money(threshold_price)

    return _round_money(_fallback())

# --------------------------------------------------------------------------------------
# 4) UPSERT в offers
# --------------------------------------------------------------------------------------
from typing import Optional
async def upsert_offer(
    session: AsyncSession,
    *,
    product_code: str,
    supplier_code: str,
    city: str,
    price: Decimal,
    wholesale_price: Optional[Decimal] = None,
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
        wholesale_price=wholesale_price,
        stock=stock,
    ).on_conflict_do_update(
        constraint="uq_offers_product_supplier_city",
        set_={
            "price": price,
            "wholesale_price": wholesale_price,
            "stock": stock,
        }
    )
    await session.execute(stmt)


async def clear_offers_for_supplier(session: AsyncSession, supplier_code: str) -> int:
    """
    Удаляет все офферы поставщика из offers.
    Возвращает число удалённых строк (для логирования).
    """
    res = await session.execute(
        text("DELETE FROM offers WHERE supplier_code = :supplier_code"),
        {"supplier_code": supplier_code},
    )
    deleted = res.rowcount or 0
    logger.info("Удалено старых офферов: %d для supplier=%s", deleted, supplier_code)
    return deleted


# --------------------------------------------------------------------------------------
# Определяет, какие поставщики подлежат очистке (отсутствуют или неактивны)
# --------------------------------------------------------------------------------------
from typing import List
async def fetch_suppliers_to_clear(session: AsyncSession) -> List[str]:
    """
    Возвращает список supplier_code, офферы которых нужно очистить:
      - Поставщики отсутствуют в dropship_enterprises
      - Поставщики присутствуют, но неактивны (is_active = false/NULL)
    """
    sql = text("""
        SELECT DISTINCT o.supplier_code
        FROM offers o
        LEFT JOIN dropship_enterprises d
          ON d.code = o.supplier_code
        WHERE d.code IS NULL
           OR (d.is_active IS NULL OR d.is_active = :inactive)
    """)
    res = await session.execute(sql, {"inactive": False})
    rows = res.fetchall()
    return [r[0] for r in rows if r[0] is not None]


# --------------------------------------------------------------------------------------
# Очищает офферы для неактивных или отсутствующих поставщиков
# --------------------------------------------------------------------------------------
async def clear_offers_for_inactive_or_missing(session: AsyncSession) -> int:
    """
    Очищает офферы для поставщиков, которые:
      1) существуют в dropship_enterprises, но помечены как неактивные (is_active = false)
      2) отсутствуют в dropship_enterprises вовсе (например, были удалены)
    Возвращает количество удалённых строк.
    """
    sql = text("""
        DELETE FROM offers o
        WHERE EXISTS (
            SELECT 1
            FROM dropship_enterprises d
            WHERE d.code = o.supplier_code
              AND COALESCE(d.is_active, false) = false
        )
        OR NOT EXISTS (
            SELECT 1
            FROM dropship_enterprises d2
            WHERE d2.code = o.supplier_code
        )
    """)
    res = await session.execute(sql)
    deleted = res.rowcount or 0
    return deleted

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
    # <<< ДОБАВЛЕНО: полная очистка старых офферов поставщика
    await clear_offers_for_supplier(session, code)

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

    # Новый режим "демпинга" (временное поле): если включён, цена считается жёстко по формуле
    # price = price_opt * (1 + retail_markup)
    is_dumping = bool(getattr(ent, "use_feed_instead_of_gdrive", False))
    retail_markup = getattr(ent, "retail_markup", None)

    min_markup_threshold = ent.min_markup_threshold or 0
    # Порог поставщика (из dropship_enterprises). Если 0/None — берём порог из балансировщика по band_id.
    supplier_threshold_percent = Decimal(str(min_markup_threshold)) if min_markup_threshold else Decimal("0")

    # 5.4 города поставщика
    cities = _split_cities(ent.city or "")
    if not cities:
        logger.warning("Supplier %s: empty 'city' field; skipping.", code)
        return

    # 5.5 bulk-загрузка цен конкурентов по (product_code, city)
    product_codes_set = {str(it["product_code"]) for it in mapped if it.get("product_code")}
    comp_map: Dict[tuple[str, str], Decimal] = {}
    if product_codes_set:
        comp_q = (
            select(CompetitorPrice.code, CompetitorPrice.city, CompetitorPrice.competitor_price)
            .where(
                CompetitorPrice.code.in_(product_codes_set),
                CompetitorPrice.city.in_(cities)
            )
        )
        res = await session.execute(comp_q)
        for code_val, city_val, price_val in res.fetchall():
            if price_val is None:
                continue
            comp_map[(str(code_val), city_val)] = _to_decimal(price_val)

    # Кэш политик балансировщика на город
    balancer_policy_cache: Dict[str, Optional[dict]] = {}

    # 5.6 цикл по городам и товарам
    for city in cities:
        # Подтягиваем последнюю применённую политику балансировщика для (city, supplier_code)
        if city not in balancer_policy_cache:
            balancer_policy_cache[city] = await fetch_latest_balancer_policy(
                session,
                city=city,
                supplier_code=code,
            )
        bal_policy = balancer_policy_cache[city]
        logger.info("Supplier %s / city %s / items %d", code, city, len(mapped))
        for it in mapped:
            product_code = str(it["product_code"])  # это ID из catalog_mapping."ID"
            qty = int(it.get("qty") or 0)
            rr = it.get("price_retail")
            po = it.get("price_opt")

            competitor = comp_map.get((product_code, city))

            # Выбор порога:
            # 1) если у поставщика задан min_markup_threshold > 0 -> используем его для всех товаров
            # 2) иначе пытаемся взять порог из балансировщика по band_id (band определяем по rr)
            threshold_percent_effective = supplier_threshold_percent

            if threshold_percent_effective <= 0 and bal_policy:
                bands = bal_policy.get("price_bands") or []
                rules = bal_policy.get("rules") or []
                min_map = bal_policy.get("min_porog_by_band") or {}

                rr_dec = _to_decimal(rr)
                band_id = resolve_band_id_from_bands(rr_dec, bands)

                # если band_id не определился (rr=0), пробуем от competitor_price, иначе от po
                if not band_id and competitor is not None:
                    band_id = resolve_band_id_from_bands(_to_decimal(competitor), bands)
                if not band_id:
                    band_id = resolve_band_id_from_bands(_to_decimal(po), bands)

                if band_id:
                    porog_from_rules = rule_porog_by_band(rules, band_id)
                    min_porog = _to_decimal(min_map.get(band_id)) if min_map else Decimal("0")
                    if porog_from_rules is not None and porog_from_rules > 0:
                        threshold_percent_effective = porog_from_rules
                    else:
                        threshold_percent_effective = min_porog

            price = compute_price_for_item(
                competitor_price=competitor,
                is_rrp=is_rrp,
                is_dumping=is_dumping,
                retail_markup=retail_markup,
                price_retail=rr,
                price_opt=po,
                threshold_percent_effective=threshold_percent_effective,
            )

            # Сохраняем price_opt в offers.wholesale_price (если пришло)
            wholesale_price = None
            if po is not None:
                wp = _to_decimal(po)
                if wp > 0:
                    wholesale_price = _round_money(wp)

            await upsert_offer(
                session,
                product_code=product_code,
                supplier_code=code,
                city=city,
                price=price,
                wholesale_price=wholesale_price,
                stock=qty,
            )

# --------------------------------------------------------------------------------------
# 6) Построение "stock"-пакета из offers и отправка в БД-сервис
# --------------------------------------------------------------------------------------
async def _load_branch_mapping(session: AsyncSession, enterprise_code: str) -> Dict[str, str]:
    """
    Читает mapping_branch для enterprise_code и возвращает dict: {store_id (city) -> branch}.
    """
    sql = text("""
        SELECT store_id, branch
        FROM mapping_branch
        WHERE enterprise_code = :enterprise_code
    """)
    res = await session.execute(sql, {"enterprise_code": enterprise_code})
    rows = res.fetchall()
    return {r[0]: r[1] for r in rows}

# --- ВМЕСТО старой build_best_offers_by_city ---
async def build_best_offers_by_city(session: AsyncSession) -> List[dict]:
    """
    Лучший оффер по каждой паре (city, product_code) ТОЛЬКО из записей со stock > 0.
    Тай-брейки: price ASC → supplier.priority DESC → stock DESC → updated_at DESC.
    """
    # coalesce(priority, 0) — если поставщик не найден в dropship_enterprises
    priority = func.coalesce(DropshipEnterprise.priority, 0)

    ranked = (
        select(
            Offer.city.label("city"),
            Offer.product_code.label("product_code"),
            Offer.price.label("price"),
            Offer.stock.label("stock"),
            Offer.updated_at.label("updated_at"),
            func.row_number().over(
                partition_by=(Offer.city, Offer.product_code),
                order_by=(
                    asc(Offer.price),
                    desc(priority),
                    desc(Offer.stock),
                    desc(Offer.updated_at),
                ),
            ).label("rn"),
        )
        .select_from(Offer)
        .join(
            DropshipEnterprise,
            DropshipEnterprise.code == Offer.supplier_code,
            isouter=True,
        )
        .where(Offer.stock > 0)   # ← ключевое условие: исключаем нулевые остатки
        .subquery()
    )

    best = (
        select(
            ranked.c.city,
            ranked.c.product_code,
            ranked.c.price,
            ranked.c.stock,
            ranked.c.updated_at,
        )
        .where(ranked.c.rn == 1)
    )

    res = await session.execute(best)
    return [dict(r._mapping) for r in res.fetchall()]

# --- ВМЕСТО старой build_stock_payload ---
async def build_stock_payload(session: AsyncSession, enterprise_code: str) -> List[dict]:
    """
    Формирует массив для process_database_service:
      [{"branch": "...", "code": <product_code>, "price": <min_price>, "qty": <stock>, "price_reserve": <min_price>}]
    Берём только офферы с qty > 0 (доп. предохранитель).
    """
    best = await build_best_offers_by_city(session)
    city2branch = await _load_branch_mapping(session, enterprise_code)

    payload: List[dict] = []
    skipped_no_branch = 0
    skipped_zero_stock = 0

    for row in best:
        if (row.get("stock") or 0) <= 0:
            skipped_zero_stock += 1
            continue

        city = row["city"]
        branch = city2branch.get(city)
        if not branch:
            skipped_no_branch += 1
            continue

        price = float(row["price"])
        payload.append({
            "branch": branch,
            "code": str(row["product_code"]),
            "price": price,
            "qty": int(row["stock"]),
            "price_reserve": price,
        })

    if skipped_no_branch:
        logger.warning("Пропущено позиций без маппинга branch: %d", skipped_no_branch)
    if skipped_zero_stock:
        logger.warning("Пропущено позиций с нулевым остатком (safety): %d", skipped_zero_stock)

    logger.info("Stock payload size: %d", len(payload))
    return payload

def _dump_payload_to_file(payload: list[dict], enterprise_code: str, file_type: str) -> Path:
    """
    Сохраняет payload в JSON-файл и возвращает путь.
    Пишем во временную директорию ОС: <tmp>/inventory_exports/<file_type>_<enterprise>_<UTC>.json
    """
    base_dir = Path(tempfile.gettempdir()) / "inventory_exports"
    base_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fname = f"{file_type}_{enterprise_code}_{ts}.json"
    path = base_dir / fname
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    logger.info("Сохранил %s (%d позиций) в %s", file_type, len(payload), path)
    return path

async def generate_and_send_stock(session: AsyncSession, enterprise_code: str) -> None:
    data = await build_stock_payload(session, enterprise_code)
    json_path = _dump_payload_to_file(data, enterprise_code, "stock")
    # сервис ожидает путь к файлу
    await process_database_service(str(json_path), "stock", enterprise_code)

# --------------------------------------------------------------------------------------
# 7) Главный раннер с аргументами (enterprise_code, file_type, optional --supplier)
# --------------------------------------------------------------------------------------
async def run_pipeline(
    enterprise_code: Optional[str] = None,
    file_type: Optional[str] = None,
) -> None:
    async with get_async_db() as session:
        # 0) Санитарная очистка: удаляем офферы по списку поставщиков, которых нужно очистить
        try:
            to_clear = await fetch_suppliers_to_clear(session)
            total_deleted = 0
            for scode in to_clear:
                total_deleted += await clear_offers_for_supplier(session, scode)
            if total_deleted:
                logger.info(
                    "Удалены офферы неактивных/отсутствующих поставщиков: %d (поставщиков: %d)",
                    total_deleted, len(to_clear)
                )
            await session.commit()
        except Exception as exc:
            logger.exception("Очистка офферов неактивных/удалённых поставщиков завершилась ошибкой: %s", exc)
            await session.rollback()

        # 1) Обновляем offers по всем активным поставщикам
        suppliers = await fetch_active_enterprises(session)
        # Пробрасываем enterprise_code пайплайна в ent, чтобы process_supplier мог читать enterprise_settings по нему
        # (enterprise_code — код предприятия, НЕ код поставщика)
        if enterprise_code:
            for ent in suppliers:
                setattr(ent, "_pipeline_enterprise_code", enterprise_code)
        if not suppliers:
            logger.info("No active dropship enterprises.")
        else:
            for ent in suppliers:
                try:
                    await process_supplier(session, ent, PARSERS)
                    await session.commit()
                except Exception as exc:
                    logger.exception("Failed supplier %s: %s", ent.code, exc)
                    await session.rollback()

        # 2) Если нужно, формируем и отправляем пакет
        if enterprise_code and file_type:
            ft = file_type.lower()
            if ft == "stock":
                try:
                    await generate_and_send_stock(session, enterprise_code)
                except Exception as exc:
                    logger.exception("Build/send stock payload failed: %s", exc)
                    await session.rollback()
            else:
                logger.warning("Неизвестный file_type: %s (поддерживается только 'stock')", file_type)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dropship pipeline runner")
    parser.add_argument("-e", "--enterprise_code", type=str, default=None,
                        help="Код предприятия (для mapping_branch и отдачи в БД-сервис)")
    parser.add_argument("-t", "--file_type", type=str, default=None,
                        help="Тип выдачи (сейчас поддерживается: stock)")
    args = parser.parse_args()

    asyncio.run(run_pipeline(
        enterprise_code=args.enterprise_code,
        file_type=args.file_type,
    ))
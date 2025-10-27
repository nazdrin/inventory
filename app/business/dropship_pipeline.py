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
            #logger.info("Mapping not found: supplier=%s code_sup=%s", supplier_code, code_sup)
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
        # 1) Обновляем offers по всем активным поставщикам
        suppliers = await fetch_active_enterprises(session)
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
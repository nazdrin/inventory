# app/business/dropship_pipeline.py
import asyncio
import inspect
import logging
import re
import argparse
import os
import random
from decimal import Decimal, ROUND_HALF_UP, ROUND_CEILING, ROUND_FLOOR
PRICE_BAND_LOW_MAX = Decimal(os.getenv("PRICE_BAND_LOW_MAX", "100"))
PRICE_BAND_MID_MAX = Decimal(os.getenv("PRICE_BAND_MID_MAX", "400"))
PRICE_BAND_HIGH_MAX = Decimal(os.getenv("PRICE_BAND_HIGH_MAX", "999999"))
THR_MULT_LOW = Decimal(os.getenv("THR_MULT_LOW", "1.0"))
THR_MULT_MID = Decimal(os.getenv("THR_MULT_MID", "1.0"))
THR_MULT_HIGH = Decimal(os.getenv("THR_MULT_HIGH", "1.0"))
THR_MULT_PREMIUM = Decimal(os.getenv("THR_MULT_PREMIUM", "1.0"))
NO_COMP_MULT_LOW = Decimal(os.getenv("NO_COMP_MULT_LOW", "1.0"))
NO_COMP_MULT_MID = Decimal(os.getenv("NO_COMP_MULT_MID", "1.0"))
NO_COMP_MULT_HIGH = Decimal(os.getenv("NO_COMP_MULT_HIGH", "1.0"))
NO_COMP_MULT_PREMIUM = Decimal(os.getenv("NO_COMP_MULT_PREMIUM", "1.0"))
# --- Новый базовый порог (доля, например 0.08 = 8%) ---
BASE_THR = Decimal(os.getenv("BASE_THR", "0.08"))
# --- Competitor undercut behavior (env-controlled) ---
# fixed discount share (default 1%)
COMP_DISCOUNT_SHARE = Decimal(os.getenv("COMP_DISCOUNT_SHARE", "0.01"))
# clamp undercut in UAH
COMP_DELTA_MIN_UAH = Decimal(os.getenv("COMP_DELTA_MIN_UAH", "2"))
COMP_DELTA_MAX_UAH = Decimal(os.getenv("COMP_DELTA_MAX_UAH", "15"))
from typing import Any, Callable, Dict, List, Optional
from pathlib import Path
import tempfile
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Fix: Ensure AsyncSession is imported at the top, before usage
from sqlalchemy.ext.asyncio import AsyncSession

# --- Balancer (read-only) integration (Variant A) ---
try:
    # Preferred single source of truth for pricing policies
    from app.business.balancer.repository import get_active_policy_for_pricing as _get_policy_for_pricing
except Exception:  # pragma: no cover
    _get_policy_for_pricing = None
# --- Helper: fetch active policy from balancer repository (preferred) ---
async def _fetch_policy_for_pricing_repo(
    session: AsyncSession,
    *,
    city: str,
    supplier_code: str,
    now_utc: datetime,
) -> Optional[dict]:
    """Try to fetch active policy from balancer repository (preferred).

    We keep this wrapper to:
      - avoid spreading repository signature across the pipeline
      - normalize output keys used below (rules/min_porog_by_band/price_bands/policy_id/mode/segment_id/...)

    Returns None if repository is unavailable or no policy found.
    """
    if _get_policy_for_pricing is None:
        return None

    try:
        import os
        import json
        sig = inspect.signature(_get_policy_for_pricing)
        kwargs: Dict[str, Any] = {}

        # Common parameter names we might support
        for name in sig.parameters.keys():
            if name in ("session", "db"):
                kwargs[name] = session
            elif name == "city":
                kwargs[name] = city
            elif name in ("supplier", "supplier_code"):
                kwargs[name] = supplier_code
            elif name in ("as_of", "now", "now_utc", "dt"):
                kwargs[name] = now_utc
            elif name in ("mode", "run_mode"):
                # Pricing context mode (LIVE/TEST). Default LIVE, can be overridden for experiments.
                kwargs[name] = os.getenv("PRICING_POLICY_MODE", "LIVE").upper()

        res = _get_policy_for_pricing(**kwargs)
        if inspect.isawaitable(res):
            res = await res
        if not res:
            return None

        # Normalize shapes:
        # repo might return {id, hash, mode, rules, price_bands, min_porog_by_band, segment_id, segment_start, segment_end, reason_details}
        policy_id = res.get("policy_id") or res.get("id")
        mode = res.get("mode") or os.getenv("PRICING_POLICY_MODE", "LIVE").upper()
        rules = res.get("rules") or []
        min_porog_by_band = res.get("min_porog_by_band") or {}
        price_bands = res.get("price_bands") or []
        segment_id = res.get("segment_id")
        segment_start = res.get("segment_start")
        segment_end = res.get("segment_end")

        # Some implementations may put config data under config_snapshot
        if not price_bands:
            cfg = res.get("config_snapshot")

            # repo may return jsonb as a string
            if isinstance(cfg, str):
                try:
                    cfg = json.loads(cfg)
                except Exception:
                    cfg = None

            if isinstance(cfg, dict):
                try:
                    profiles = cfg.get("profiles") or []
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
            "policy_id": policy_id,
            "mode": mode,
            "rules": rules,
            "min_porog_by_band": min_porog_by_band,
            "price_bands": price_bands,
            "segment_id": segment_id,
            "segment_start": segment_start,
            "segment_end": segment_end,
            # keep original for debugging
            "_raw": res,
        }
    except Exception:
        logger.exception(
            "Failed to fetch policy via balancer repository: supplier=%s city=%s",
            supplier_code,
            city,
        )
        return None

from sqlalchemy import text, select, func, asc, desc, or_
from sqlalchemy.dialects.postgresql import insert

# === ВАША ИНФРАСТРУКТУРА / МОДЕЛИ ===
from app.database import get_async_db
from app.models import (
    CatalogSupplierMapping,
    CompetitorPrice,
    DropshipEnterprise,
    MasterCatalog,
    Offer,
    OfferBlockRule,
)  # CompetitorPrice: code, city, competitor_price
from app.business.supplier_identity import get_supplier_id_by_code
from app.business.feed_biotus import parse_feed_stock_to_json
from app.business.feed_dsn import parse_dsn_stock_to_json
from app.business.feed_proteinplus import parse_feed_stock_to_json as parse_feed_D3
from app.business.feed_dobavki import parse_d4_feed_to_json
from app.business.feed_monstr import parse_feed_stock_to_json as parse_feed_D5
from app.business.feed_sportatlet import parse_d6_stock_to_json as parse_feed_D6
from app.business.feed_pediakid import parse_pediakid_stock_to_json as parse_feed_D7
from app.business.feed_suziria import parse_suziria_stock_to_json as parse_feed_D8
from app.business.feed_ortomedika import parse_feed_stock_to_json as parse_feed_D9
from app.business.feed_zoohub import parse_feed_stock_to_json as parse_feed_D10
from app.business.feed_toros import parse_feed_stock_to_json as parse_feed_D11
from app.business.feed_vetstar import parse_feed_stock_to_json as parse_feed_D12
from app.business.feed_zoocomplex import parse_zoocomplex_stock_to_json as parse_feed_D13
# опционально: сервис "куда отдать массив"
try:
    from app.services.database_service import process_database_service
except Exception:
    async def process_database_service(file_path, file_type, enterprise_code):
        logging.getLogger("dropship").warning(
            "process_database_service() недоступен. Заглушка: file_type=%s enterprise_code=%s items=%d",
            file_type, enterprise_code, len(file_path)
        )

# опционально: нотификации (Telegram/Email/whatever implemented in notification_service)
try:
    from app.services.notification_service import send_notification
except Exception:  # pragma: no cover
    async def send_notification(message: str, recipient: str) -> None:
        logging.getLogger("dropship").info("send_notification() недоступен. recipient=%s msg=%s", recipient, message)

logger = logging.getLogger("dropship")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Logging controls (env) ---
# DROPSHIP_LOG_LEVEL: DEBUG/INFO/WARNING/ERROR (default INFO)
# DROPSHIP_VERBOSE_ITEM_LOGS: 1 to log per-item pricing lines at INFO (default 0)
_LOG_LEVEL = os.getenv("DROPSHIP_LOG_LEVEL", "INFO").upper()
logger.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
VERBOSE_ITEM_LOGS = os.getenv("DROPSHIP_VERBOSE_ITEM_LOGS", "0") == "1"

# --------------------------------------------------------------------------------------
# Утилиты
# --------------------------------------------------------------------------------------
def _to_decimal(x: Optional[float | Decimal]) -> Decimal:
    if x is None:
        return Decimal("0")
    return Decimal(str(x))

def _as_share(x: Optional[float | Decimal]) -> Decimal:
    """Convert percent to share.

    Examples:
      25   -> 0.25
      0.25 -> 0.25
      -2   -> -0.02
      -0.02 -> -0.02
      None -> 0
      1    -> 0.01
      -1   -> -0.01

    NOTE: We treat absolute values >= 1 as percentages (so 1 == 1%).
    """
    d = _to_decimal(x)
    if d == 0:
        return Decimal("0")
    if abs(d) >= 1:
        return d / Decimal("100")
    return d

def _round_money(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

# Округление итоговой цены для экспорта/записи.
def _round_price_to_step_up(x: Decimal, step: Decimal) -> Decimal:
    if x is None:
        return Decimal("0.00")
    d = _to_decimal(x)
    if d <= 0:
        return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    q = (d / step).to_integral_value(rounding=ROUND_CEILING) * step
    return q.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _round_price_to_step_down(x: Decimal, step: Decimal) -> Decimal:
    d = _to_decimal(x)
    if d <= 0:
        return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    q = (d / step).to_integral_value(rounding=ROUND_FLOOR) * step
    return q.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _price_round_step_for_supplier(supplier_code: str) -> Decimal:
    if str(supplier_code).upper() == "D3":
        return Decimal("1.00")
    return Decimal("0.50")


def _round_price_export_for_supplier(x: Decimal, supplier_code: str) -> Decimal:
    return _round_price_to_step_up(x, _price_round_step_for_supplier(supplier_code))


def _cap_price_not_above_competitor(competitor_price: Decimal, supplier_code: str) -> Decimal:
    comp = _to_decimal(competitor_price)
    if comp <= 0:
        return Decimal("0.00")
    step = _price_round_step_for_supplier(supplier_code)
    capped = _round_price_to_step_down(comp, step)
    if capped <= 0:
        return comp.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return capped


def _env_decimal(name: str, default: str) -> Decimal:
    raw = (os.getenv(name, default) or default).strip()
    try:
        return Decimal(raw)
    except Exception:
        logging.getLogger("dropship").warning(
            "Invalid decimal env %s=%r, fallback to %s", name, raw, default
        )
        return Decimal(default)


def _env_optional_decimal(name: str) -> Decimal | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except Exception:
        logging.getLogger("dropship").warning(
            "Invalid decimal env %s=%r, ignored", name, raw
        )
        return None


PRICE_JITTER_RANGE_UAH = _env_decimal("PRICE_JITTER_RANGE_UAH", "1.0")
PRICE_JITTER_STEP_UAH = _env_decimal("PRICE_JITTER_STEP_UAH", "0.5")
PRICE_JITTER_MIN_UAH = _env_optional_decimal("PRICE_JITTER_MIN_UAH")
PRICE_JITTER_MAX_UAH = _env_optional_decimal("PRICE_JITTER_MAX_UAH")
_PRICE_JITTER_WARNED: set[str] = set()


def _price_jitter_enabled() -> bool:
    return os.getenv("PRICE_JITTER_ENABLED", "0") == "1"


def _warn_price_jitter_once(key: str, msg: str, *args: Any) -> None:
    if key in _PRICE_JITTER_WARNED:
        return
    _PRICE_JITTER_WARNED.add(key)
    logger.warning(msg, *args)


def _build_price_jitter_deltas() -> list[Decimal]:
    jitter_step = _to_decimal(PRICE_JITTER_STEP_UAH)
    if jitter_step <= 0:
        _warn_price_jitter_once(
            "step_non_positive",
            "price jitter disabled: PRICE_JITTER_STEP_UAH must be > 0, got %s",
            jitter_step,
        )
        return []

    jitter_min = PRICE_JITTER_MIN_UAH
    jitter_max = PRICE_JITTER_MAX_UAH

    # Backward compatibility: if MIN/MAX not fully set, use symmetric range [-R; +R].
    if jitter_min is None or jitter_max is None:
        jitter_range = abs(_to_decimal(PRICE_JITTER_RANGE_UAH))
        jitter_min = -jitter_range
        jitter_max = jitter_range

    if jitter_min > jitter_max:
        _warn_price_jitter_once(
            "invalid_min_max",
            "price jitter disabled: PRICE_JITTER_MIN_UAH (%s) > PRICE_JITTER_MAX_UAH (%s)",
            jitter_min,
            jitter_max,
        )
        return []

    deltas: list[Decimal] = []
    cur = jitter_min
    while cur <= jitter_max:
        deltas.append(cur)
        cur += jitter_step
    return deltas


def _apply_price_jitter(price: Decimal) -> tuple[Decimal, Decimal]:
    if not _price_jitter_enabled():
        return price, Decimal("0")

    p = _to_decimal(price)
    deltas = _build_price_jitter_deltas()
    if not deltas:
        return p, Decimal("0")

    delta = random.choice(deltas)
    new_price = p + delta
    if new_price <= 0:
        return p, Decimal("0")
    return new_price, delta

def resolve_price_band(base_price: Decimal) -> str:
    if base_price <= PRICE_BAND_LOW_MAX:
        return "LOW"
    elif base_price <= PRICE_BAND_MID_MAX:
        return "MID"
    else:
        return "HIGH"
def _split_cities(city_field: str) -> List[str]:
    """Разбиваем по , ; | и обрезаем пробелы."""
    if not city_field:
        return []
    parts = re.split(r"[;,|]", city_field)
    return [p.strip() for p in parts if p.strip()]

def is_supplier_blocked(supplier_code: str, now: datetime | None = None) -> bool:
    # .env:
    # SUPPLIER_SCHEDULE_ENABLED=true|false
    # SUPPLIER_{CODE}_BLOCK_START_DAY=1..7, SUPPLIER_{CODE}_BLOCK_START_TIME=HH:MM
    # SUPPLIER_{CODE}_BLOCK_END_DAY=1..7,   SUPPLIER_{CODE}_BLOCK_END_TIME=HH:MM
    if os.getenv("SUPPLIER_SCHEDULE_ENABLED", "").strip().lower() != "true":
        return False

    code = (supplier_code or "").strip().upper()
    prefix = f"SUPPLIER_{code}_BLOCK_"
    start_day_raw = os.getenv(f"{prefix}START_DAY")
    start_time_raw = os.getenv(f"{prefix}START_TIME")
    end_day_raw = os.getenv(f"{prefix}END_DAY")
    end_time_raw = os.getenv(f"{prefix}END_TIME")

    # Неполный набор переменных для поставщика => блокировка не применяется.
    if not all([start_day_raw, start_time_raw, end_day_raw, end_time_raw]):
        return False

    warned = False

    def _warn_once(msg: str, *args: Any) -> None:
        nonlocal warned
        if warned:
            return
        warned = True
        logger.warning(msg, *args)

    def _parse_day(raw: str, label: str) -> Optional[int]:
        try:
            day = int(raw)
        except Exception:
            _warn_once(
                "Некорректный %s для поставщика %s: %r. Блокировка отключена для этого вызова.",
                label, code, raw
            )
            return None
        if day < 1 or day > 7:
            _warn_once(
                "Некорректный %s для поставщика %s: %r (ожидается 1..7). Блокировка отключена для этого вызова.",
                label, code, raw
            )
            return None
        return day

    def _parse_time_minutes(raw: str, label: str) -> Optional[int]:
        try:
            parts = raw.split(":")
            if len(parts) != 2:
                raise ValueError("format")
            hour = int(parts[0])
            minute = int(parts[1])
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                raise ValueError("range")
            return hour * 60 + minute
        except Exception:
            _warn_once(
                "Некорректный %s для поставщика %s: %r (ожидается HH:MM). Блокировка отключена для этого вызова.",
                label, code, raw
            )
            return None

    start_day = _parse_day(start_day_raw, "BLOCK_START_DAY")
    end_day = _parse_day(end_day_raw, "BLOCK_END_DAY")
    start_time = _parse_time_minutes(start_time_raw, "BLOCK_START_TIME")
    end_time = _parse_time_minutes(end_time_raw, "BLOCK_END_TIME")
    if None in (start_day, end_day, start_time, end_time):
        return False

    schedule_tz: Optional[ZoneInfo] = None
    try:
        schedule_tz = ZoneInfo("Europe/Kiev")
    except Exception:
        try:
            schedule_tz = ZoneInfo("Europe/Kyiv")
        except Exception:
            schedule_tz = None

    if now is None:
        current = datetime.now(tz=schedule_tz) if schedule_tz else datetime.now()
    else:
        current = now
        if schedule_tz:
            if current.tzinfo is None or current.utcoffset() is None:
                current = current.replace(tzinfo=schedule_tz)
            else:
                current = current.astimezone(schedule_tz)
        elif current.tzinfo is not None and current.utcoffset() is not None:
            current = current.astimezone()
    now_minutes = current.weekday() * 1440 + current.hour * 60 + current.minute

    start_minutes = (start_day - 1) * 1440 + start_time
    end_minutes = (end_day - 1) * 1440 + end_time

    if start_minutes <= end_minutes:
        return start_minutes <= now_minutes <= end_minutes
    return now_minutes >= start_minutes or now_minutes <= end_minutes

# --------------------------------------------------------------------------------------
# Реестр парсеров (подставьте свои реализации)
# --------------------------------------------------------------------------------------
ParserFn = Callable[..., List[dict[str, Any]]]


async def parse_feed_D4(*, code: str = "D4", timeout: int = 20, **kwargs) -> str:
    # D4 now supports Drive-first strategy inside unified parser; stock pipeline must force stock mode.
    return await parse_d4_feed_to_json(mode="stock", code=code, timeout=timeout)

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
    "D8": parse_feed_D8,
    "D9": parse_feed_D9,
    "D10": parse_feed_D10,
    "D11": parse_feed_D11,
    "D12": parse_feed_D12,
    "D13": parse_feed_D13,
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
# 2) Маппинг supplier item -> product_code
# --------------------------------------------------------------------------------------
CATALOG_MAPPING_ID_COL = "ID"  # у вас именно так (UPPER)

def _use_master_mapping_for_stock() -> bool:
    return os.getenv("USE_MASTER_MAPPING_FOR_STOCK", "0").strip().lower() in {"1", "true", "yes", "on"}


async def _map_supplier_codes_legacy(
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

    logger.info(
        "Supplier %s: legacy mapping backend used, input=%d mapped=%d",
        supplier_code,
        len(items),
        len(mapped),
    )
    return mapped


async def _map_supplier_codes_master(
    session: AsyncSession,
    supplier_code: str,
    items: List[dict],
) -> List[dict]:
    """
    Новый backend:
      supplier string code -> numeric supplier_id
      (supplier_id, supplier_code=code_sup) -> catalog_supplier_mapping
      sku -> product_code

    Архивные master_catalog.sku исключаются уже на этапе lookup.
    """
    supplier_id = get_supplier_id_by_code(supplier_code)
    if supplier_id is None:
        logger.warning(
            "Supplier %s: numeric supplier_id not found in unified supplier map, fallback to legacy mapping backend",
            supplier_code,
        )
        return await _map_supplier_codes_legacy(session, supplier_code, items)

    normalized_codes: List[str] = []
    seen_codes: set[str] = set()
    for item in items:
        code_sup = str(item.get("code_sup") or "").strip()
        if not code_sup or code_sup in seen_codes:
            continue
        seen_codes.add(code_sup)
        normalized_codes.append(code_sup)

    if not normalized_codes:
        logger.info("Supplier %s: master mapping backend skipped, no code_sup values", supplier_code)
        return []

    stmt = (
        select(
            CatalogSupplierMapping.supplier_code,
            CatalogSupplierMapping.sku,
            MasterCatalog.is_archived,
        )
        .join(MasterCatalog, MasterCatalog.sku == CatalogSupplierMapping.sku)
        .where(
            CatalogSupplierMapping.supplier_id == supplier_id,
            CatalogSupplierMapping.supplier_code.in_(normalized_codes),
            CatalogSupplierMapping.is_active.is_(True),
        )
    )
    rows = (await session.execute(stmt)).all()

    active_lookup: Dict[str, str] = {}
    archived_codes: set[str] = set()
    for supplier_item_code, sku, is_archived in rows:
        code_sup = str(supplier_item_code or "").strip()
        if not code_sup:
            continue
        if bool(is_archived):
            archived_codes.add(code_sup)
            continue
        active_lookup[code_sup] = str(sku)

    mapped: List[dict] = []
    missing_count = 0
    archived_count = 0
    for item in items:
        code_sup = str(item.get("code_sup") or "").strip()
        if not code_sup:
            continue

        if code_sup in archived_codes:
            archived_count += 1
            continue

        product_code = active_lookup.get(code_sup)
        if not product_code:
            missing_count += 1
            continue

        mapped.append(
            {
                "product_code": product_code,
                "qty": int(item.get("qty") or 0),
                "price_retail": item.get("price_retail"),
                "price_opt": item.get("price_opt"),
            }
        )

    logger.info(
        "Supplier %s: master mapping backend used, supplier_id=%s input=%d matched=%d missing=%d archived=%d",
        supplier_code,
        supplier_id,
        len(items),
        len(mapped),
        missing_count,
        archived_count,
    )
    return mapped


async def map_supplier_codes(
    session: AsyncSession,
    supplier_code: str,
    items: List[dict],
) -> List[dict]:
    if _use_master_mapping_for_stock():
        return await _map_supplier_codes_master(session, supplier_code, items)
    return await _map_supplier_codes_legacy(session, supplier_code, items)


async def fetch_active_offer_blocks(
    session: AsyncSession,
    supplier_code: str,
) -> tuple[set[str], set[str]]:
    now_utc = datetime.now(timezone.utc)
    q = (
        select(OfferBlockRule.product_code, OfferBlockRule.supplier_code)
        .where(
            OfferBlockRule.is_active.is_(True),
            OfferBlockRule.blocked_until > now_utc,
            or_(
                OfferBlockRule.supplier_code.is_(None),
                OfferBlockRule.supplier_code == supplier_code,
            ),
        )
    )
    res = await session.execute(q)

    globally_blocked: set[str] = set()
    supplier_blocked: set[str] = set()

    for product_code, rule_supplier_code in res.fetchall():
        if not product_code:
            continue
        product_code_str = str(product_code)
        if rule_supplier_code is None:
            globally_blocked.add(product_code_str)
        else:
            supplier_blocked.add(product_code_str)

    return globally_blocked, supplier_blocked

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




# --- Новый хелпер (Вариант A): получить АКТИВНУЮ политику балансировщика для city+supplier на текущий момент ---
async def fetch_active_balancer_policy(
    session: AsyncSession,
    *,
    city: str,
    supplier_code: str,
    now_utc: Optional[datetime] = None,
) -> Optional[dict]:
    """
    Вариант A:
      - берём ТОЛЬКО активную (по времени) политику: segment_start <= now < segment_end
      - is_applied = true
      - city = :city
      - supplier = :supplier_code
      - приоритет: LIVE > TEST

    Возвращает dict:
      {
        "policy_id": <int>,
        "mode": "LIVE"|"TEST",
        "rules": [{"band_id": "...", "porog": 0.15}, ...],
        "min_porog_by_band": {"B1": 0.15, ...},
        "price_bands": [{"band_id": "B1", "min": 0, "max": 300}, ...],
        "segment_id": "...",
        "segment_start": <datetime>,
        "segment_end": <datetime>,
      }

    Если активной политики нет — None.

    ВАЖНО: сравнение делаем в UTC (now_utc). segment_start/segment_end в БД должны быть timestamptz.
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    sql = text("""
        SELECT
            id,
            mode,
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
          AND segment_start <= :now_utc
          AND segment_end   >  :now_utc
        ORDER BY
            CASE WHEN mode = 'LIVE' THEN 1 ELSE 0 END DESC,
            segment_start DESC,
            id DESC
        LIMIT 1
    """)

    res = await session.execute(sql, {"city": city, "supplier": supplier_code, "now_utc": now_utc})
    row = res.first()
    if not row:
        return None

    policy_id, mode, rules, min_porog_by_band, config_snapshot, segment_id, segment_start, segment_end = row

    # config_snapshot может быть NULL
    price_bands: list[dict] = []
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
        "policy_id": policy_id,
        "mode": mode,
        "rules": rules or [],
        "min_porog_by_band": min_porog_by_band or {},
        "price_bands": price_bands or [],
        "segment_id": segment_id,
        "segment_start": segment_start,
        "segment_end": segment_end,
    }


# --- Fallback: если активной политики нет, берём последнюю применённую (как было раньше) ---
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

    ВАЖНО: приоритет LIVE > TEST.

    Возвращает dict:
      {
        "policy_id": <int>,
        "mode": "LIVE"|"TEST",
        "rules": [{"band_id": "...", "porog": 0.15}, ...],
        "min_porog_by_band": {"B1": 0.15, ...},
        "price_bands": [{"band_id": "B1", "min": 0, "max": 300}, ...],
        "segment_id": "...",
        "segment_start": <datetime>,
        "segment_end": <datetime>,
      }
    Если записи нет — None.
    """
    sql = text("""
        SELECT
            id,
            mode,
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
        ORDER BY
            CASE WHEN mode = 'LIVE' THEN 1 ELSE 0 END DESC,
            segment_start DESC,
            id DESC
        LIMIT 1
    """)
    res = await session.execute(sql, {"city": city, "supplier": supplier_code})
    row = res.first()
    if not row:
        return None

    policy_id, mode, rules, min_porog_by_band, config_snapshot, segment_id, segment_start, segment_end = row

    # config_snapshot может быть NULL
    price_bands: list[dict] = []
    try:
        if isinstance(config_snapshot, dict):
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
        "policy_id": policy_id,
        "mode": mode,
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
def _compute_price_for_item_with_source(
    *,
    competitor_price: Optional[Decimal],
    is_rrp: bool,
    is_dumping: bool,
    retail_markup: Optional[float | Decimal],
    price_retail: Optional[float | Decimal],
    price_opt: Optional[float | Decimal],
    threshold_percent_effective: Optional[float | Decimal],
) -> tuple[Decimal, str]:
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
        return _round_money(rr), "rrp"

    # 1.1) ЖЁСТКИЙ "демпинг": price = price_opt * (1 + retail_markup)
    # Приоритет ниже РРЦ, но выше режима конкурентов/порогов.
    if is_dumping:
        mu = _as_share(retail_markup)  # 25 -> 0.25; 0.25 -> 0.25; None -> 0
        if po > 0:
            return _round_money(po * (Decimal("1") + mu)), "dumping_po"
        # Если оптовой нет — fallback на rr (если есть), иначе 0
        if rr > 0:
            return _round_money(rr), "dumping_rr"
        return Decimal("0"), "dumping_zero"

    # 2) Эффективный порог (УЖЕ доля/коэффициент, а не проценты). Может быть > 1 для дешёвых товаров.
    thr = _to_decimal(threshold_percent_effective)
    threshold_price = Decimal("0")
    if po > 0:
        threshold_price = po * (Decimal("1") + thr)
        # safety: do not allow negative prices
        if threshold_price < 0:
            threshold_price = Decimal("0")

    # 4) Цена под конкурента (если есть конкурент)
    under_competitor: Optional[Decimal] = None
    if competitor_price is not None and competitor_price > 0:
        candidate = competitor_price * (Decimal("1") - COMP_DISCOUNT_SHARE)
        delta = competitor_price - candidate
        if delta < COMP_DELTA_MIN_UAH:
            candidate = competitor_price - COMP_DELTA_MIN_UAH
        elif delta > COMP_DELTA_MAX_UAH:
            candidate = competitor_price - COMP_DELTA_MAX_UAH
        # safety: never above competitor
        if candidate > competitor_price:
            candidate = competitor_price
        under_competitor = candidate

    # 5) RR ceiling (for non-RRP suppliers) applies ONLY when there is NO wholesale price (po<=0)
    if under_competitor is not None and rr > 0 and po <= 0:
        under_competitor = min(under_competitor, rr)

    # 6) Выбор итоговой цены (без enterprise_settings диапазонов)
    # Если rr отсутствует/0, то в местах где rr требуется, берём threshold_price.
    def _fallback() -> tuple[Decimal, str]:
        if rr > 0:
            return rr, "fallback_rr"
        if threshold_price > 0:
            return threshold_price, "fallback_threshold"
        if under_competitor is not None and under_competitor > 0:
            return under_competitor, "fallback_under_competitor"
        return Decimal("0"), "fallback_zero"

    # Если конкурента нет
    if under_competitor is None:
        if threshold_price > 0:
            return _round_money(threshold_price), "no_comp_threshold"
        fallback_price, fallback_source = _fallback()
        return _round_money(fallback_price), f"no_comp_{fallback_source}"

    # Конкурент есть
    if threshold_price > 0 and under_competitor >= threshold_price:
        return _round_money(under_competitor), "under_competitor"

    # under_competitor < threshold_price
    if threshold_price > 0:
        return _round_money(threshold_price), "threshold_floor"

    fallback_price, fallback_source = _fallback()
    return _round_money(fallback_price), f"comp_{fallback_source}"


def compute_price_for_item(
    *,
    competitor_price: Optional[Decimal],
    is_rrp: bool,
    is_dumping: bool,
    retail_markup: Optional[float | Decimal],
    price_retail: Optional[float | Decimal],
    price_opt: Optional[float | Decimal],
    threshold_percent_effective: Optional[float | Decimal],
) -> Decimal:
    price, _ = _compute_price_for_item_with_source(
        competitor_price=competitor_price,
        is_rrp=is_rrp,
        is_dumping=is_dumping,
        retail_markup=retail_markup,
        price_retail=price_retail,
        price_opt=price_opt,
        threshold_percent_effective=threshold_percent_effective,
    )
    return price

# --------------------------------------------------------------------------------------
# 4) UPSERT в offers
# --------------------------------------------------------------------------------------

# Batched upsert helper: yields seq in chunks of chunk_size
def _iter_chunks(seq: List[dict], chunk_size: int):
    """Yield seq in chunks of chunk_size."""
    if chunk_size <= 0:
        chunk_size = 1000
    for i in range(0, len(seq), chunk_size):
        yield seq[i : i + chunk_size]
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


# Batched UPSERT into offers (removes per-row INSERT ... ON CONFLICT round-trips)
async def bulk_upsert_offers(
    session: AsyncSession,
    *,
    rows: List[dict],
    batch_size: int = 1000,
) -> None:
    """Batched UPSERT into offers.

    Removes per-row INSERT ... ON CONFLICT round-trips.
    Expects each row dict to contain:
      product_code, supplier_code, city, price, wholesale_price, stock
    """
    if not rows:
        return

    for chunk in _iter_chunks(rows, batch_size):
        # Deduplicate rows inside the batch by the same key as the UNIQUE constraint.
        # Postgres throws: "ON CONFLICT DO UPDATE command cannot affect row a second time"
        # if the same (product_code, supplier_code, city) appears more than once in a single INSERT statement.
        dedup: Dict[tuple[str, str, str], dict] = {}
        dup_count = 0

        for r in chunk:
            k = (
                str(r.get("product_code")),
                str(r.get("supplier_code")),
                str(r.get("city")),
            )
            if k in dedup:
                dup_count += 1
                prev = dedup[k]
                # Keep the latest price/wholesale_price, but do not reduce stock on duplicates.
                # (Duplicates can happen due to feed/catalog mapping repetitions.)
                prev_stock = int(prev.get("stock") or 0)
                new_stock = int(r.get("stock") or 0)
                prev["stock"] = max(prev_stock, new_stock)
                prev["price"] = r.get("price")
                prev["wholesale_price"] = r.get("wholesale_price")
            else:
                dedup[k] = r

        if dup_count:
            logger.warning(
                "bulk_upsert_offers: deduplicated %d duplicate rows inside one batch (unique key: product_code+supplier_code+city)",
                dup_count,
            )

        deduped_chunk = list(dedup.values())

        ins = insert(Offer).values(deduped_chunk)
        stmt = ins.on_conflict_do_update(
            constraint="uq_offers_product_supplier_city",
            set_={
                "price": ins.excluded.price,
                "wholesale_price": ins.excluded.wholesale_price,
                "stock": ins.excluded.stock,
            },
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

    blocked_global_codes, blocked_supplier_codes = await fetch_active_offer_blocks(session, code)

    mapped_before_filter = len(mapped)
    blocked_global_count = 0
    blocked_supplier_count = 0
    filtered_mapped: List[dict] = []

    for item in mapped:
        product_code = item.get("product_code")
        if not product_code:
            filtered_mapped.append(item)
            continue

        product_code_str = str(product_code)
        if product_code_str in blocked_global_codes:
            blocked_global_count += 1
            continue
        if product_code_str in blocked_supplier_codes:
            blocked_supplier_count += 1
            continue

        filtered_mapped.append(item)

    mapped = filtered_mapped
    logger.info(
        "Supplier %s: mapped=%d blocked_global=%d blocked_supplier=%d remaining=%d",
        code,
        mapped_before_filter,
        blocked_global_count,
        blocked_supplier_count,
        len(mapped),
    )
    if not mapped:
        logger.info("Supplier %s: no mapped items after offer block filtering.", code)
        return

    # 5.3 параметры ценообразования
    is_rrp = bool(ent.is_rrp)

    # Новый режим "демпинга" (временное поле): если включён, цена считается жёстко по формуле
    # price = price_opt * (1 + retail_markup)
    is_dumping = bool(getattr(ent, "use_feed_instead_of_gdrive", False))
    retail_markup = getattr(ent, "retail_markup", None)

    min_markup_threshold = ent.min_markup_threshold or 0
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

    # Фиксируем "сейчас" один раз на поставщика, чтобы все товары/города в прогоне брали один и тот же сегмент
    now_utc = datetime.now(timezone.utc)

    # 5.6 цикл по городам и товарам
    for city in cities:
        # Collect rows for batched upsert per-city (reduces SQL round-trips)
        rows_to_upsert: List[dict] = []
        # Вариант A: сначала пробуем взять АКТИВНУЮ политику через balancer repository (single source of truth)
        if city not in balancer_policy_cache:
            # Preferred: single source of truth (balancer repository)
            active = await _fetch_policy_for_pricing_repo(
                session,
                city=city,
                supplier_code=code,
                now_utc=now_utc,
            )

            # If repo returned a policy but without price_bands, treat as incomplete and fallback to SQL.
            if active is not None and not (active.get("price_bands") or []):
                logger.warning(
                    "Balancer repo returned policy without price_bands; fallback to SQL. supplier=%s city=%s policy_id=%s",
                    code,
                    city,
                    active.get("policy_id") or active.get("id"),
                )
                active = None

            # Fallbacks (legacy): active-by-time, then last applied
            if active is None:
                active = await fetch_active_balancer_policy(
                    session,
                    city=city,
                    supplier_code=code,
                    now_utc=now_utc,
                )
            if active is None:
                active = await fetch_latest_balancer_policy(
                    session,
                    city=city,
                    supplier_code=code,
                )

            balancer_policy_cache[city] = active
        bal_policy = balancer_policy_cache[city]
        # --- Extract reason and band_sources for debug/notification ---
        reason_dbg = None
        band_sources_dbg = None
        raw = bal_policy.get("_raw") if bal_policy else None
        if isinstance(raw, dict):
            reason_dbg = raw.get("reason")
            rd = raw.get("reason_details") or {}
            band_sources_dbg = rd.get("band_sources")
        if bal_policy:
            (logger.info if VERBOSE_ITEM_LOGS else logger.debug)(
                "Balancer policy selected: supplier=%s city=%s mode=%s policy_id=%s segment=%s [%s..%s] now_utc=%s reason=%s band_sources=%s",
                code,
                city,
                bal_policy.get("mode"),
                bal_policy.get("policy_id") or bal_policy.get("id"),
                bal_policy.get("segment_id"),
                bal_policy.get("segment_start"),
                bal_policy.get("segment_end"),
                now_utc,
                (bal_policy.get("_raw") or {}).get("reason"),
                ((bal_policy.get("_raw") or {}).get("reason_details") or {}).get("band_sources"),
            )
        else:
            (logger.info if VERBOSE_ITEM_LOGS else logger.debug)(
                "Balancer policy selected: supplier=%s city=%s NONE (will use supplier min_markup_threshold/fallback)",
                code,
                city,
            )
        logger.info("Supplier %s / city %s / items %d", code, city, len(mapped))
        # --- Debug/notify: показать, какую политику и какие пороги реально применяем ---
        sample_limit = 8
        try:
            sample_limit = int(__import__("os").getenv("PRICING_POLICY_SAMPLE_LIMIT", "8"))
        except Exception:
            sample_limit = 8

        sample_rows: list[str] = []
        policy_id_dbg = None
        policy_mode_dbg = None
        segment_dbg = None
        if bal_policy:
            policy_id_dbg = bal_policy.get("policy_id") or bal_policy.get("id")
            policy_mode_dbg = bal_policy.get("mode")
            segment_dbg = bal_policy.get("segment_id")

        logger.info(
            "Pricing policy context: supplier=%s city=%s policy_id=%s mode=%s segment=%s now_utc=%s",
            code, city, policy_id_dbg, policy_mode_dbg, segment_dbg, now_utc,
        )
        for it in mapped:
            product_code = str(it["product_code"])
            qty = int(it.get("qty") or 0)
            rr = it.get("price_retail")
            po = it.get("price_opt")
            competitor = comp_map.get((product_code, city))

            rr_dec = _to_decimal(rr)
            po_dec = _to_decimal(po)
            competitor_dec = competitor if competitor is not None else Decimal("0")

            # Определяем band по себестоимости (по по_dec)
            band = resolve_price_band(po_dec)

            # supplier_add_uah — абсолютная надбавка из таблицы (min_markup_threshold)
            supplier_add_uah = supplier_threshold_percent

            # Выбор абсолютной надбавки по бэнду
            if competitor_dec > 0:
                if band == "LOW":
                    band_add_uah = THR_MULT_LOW
                elif band == "MID":
                    band_add_uah = THR_MULT_MID
                elif band == "HIGH":
                    band_add_uah = THR_MULT_HIGH
                else:
                    band_add_uah = THR_MULT_PREMIUM
            else:
                if band == "LOW":
                    band_add_uah = NO_COMP_MULT_LOW
                elif band == "MID":
                    band_add_uah = NO_COMP_MULT_MID
                elif band == "HIGH":
                    band_add_uah = NO_COMP_MULT_HIGH
                else:
                    band_add_uah = NO_COMP_MULT_PREMIUM

            # Расчёт thr_effective по Путь A
            if po_dec > 0:
                thr_effective = BASE_THR + (band_add_uah + supplier_add_uah) / po_dec
            else:
                thr_effective = Decimal("0")

            price, price_source = _compute_price_for_item_with_source(
                competitor_price=competitor,
                is_rrp=is_rrp,
                is_dumping=is_dumping,
                retail_markup=retail_markup,
                price_retail=rr,
                price_opt=po,
                threshold_percent_effective=thr_effective,
            )
            price = _round_price_export_for_supplier(price, code)
            if _price_jitter_enabled():
                base_price = price
                price, delta = _apply_price_jitter(price)
                price = _round_price_export_for_supplier(price, code)
                if delta != 0:
                    logger.debug(
                        "price jitter applied: base=%s, delta=%s, final=%s",
                        base_price,
                        delta,
                        price,
                    )
            # If final price originated from "under competitor" branch, never allow price to exceed competitor after jitter.
            if (
                competitor_dec > 0
                and price > competitor_dec
                and price_source in {"under_competitor", "comp_fallback_under_competitor"}
            ):
                capped = _cap_price_not_above_competitor(competitor_dec, code)
                logger.debug(
                    "price capped by competitor: source=%s before=%s competitor=%s after=%s",
                    price_source,
                    price,
                    competitor_dec,
                    capped,
                )
                price = capped

            # Лог: supplier, city, product_code, band, thr_supplier, thr_effective, competitor_price, final_price
            (logger.info if VERBOSE_ITEM_LOGS else logger.debug)(
                "Price: supplier=%s city=%s product_code=%s band=%s supplier_add_uah=%s band_add_uah=%s thr_effective=%s competitor_price=%s final_price=%s",
                code, city, product_code, band, supplier_add_uah, band_add_uah, thr_effective, competitor, price
            )

            # --- собрать несколько строк для нотификации: какие пороги реально применились ---
            if len(sample_rows) < sample_limit:
                thr_share = _as_share(thr_effective)
                thr_pct = (thr_share * Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                sample_rows.append(
                    f"• {product_code} band={band} thr={thr_pct}% rr={rr_dec} po={po_dec} comp={(competitor or Decimal('0'))} -> price={price}"
                )

            wholesale_price = None
            if po is not None:
                wp = _to_decimal(po)
                if wp > 0:
                    wholesale_price = _round_money(wp)

            rows_to_upsert.append({
                "product_code": product_code,
                "supplier_code": code,
                "city": city,
                "price": price,
                "wholesale_price": wholesale_price,
                "stock": qty,
            })

        # Batched UPSERT for this city
        if rows_to_upsert:
            await bulk_upsert_offers(
                session,
                rows=rows_to_upsert,
                batch_size=int(os.getenv("OFFERS_UPSERT_BATCH_SIZE", "1000")),
            )
            logger.info(
                "Offers upserted (batched): supplier=%s city=%s rows=%d",
                code,
                city,
                len(rows_to_upsert),
            )

        # --- отправляем нотификацию 1 раз на supplier+city за прогон ---
        # ВРЕМЕННО ОТКЛЮЧЕНО по умолчанию, чтобы не спамить в проде.
        # Включить можно явным флагом окружения: PRICING_POLICY_NOTIFY=1
        if sample_rows and os.getenv("PRICING_POLICY_NOTIFY", "0") == "1":
            header = (
                f"📌 Pricing policy applied\n"
                f"Supplier: {code}\n"
                f"City: {city}\n"
                f"Policy: id={policy_id_dbg} mode={policy_mode_dbg} seg={segment_dbg}\n"
                f"Reason: {reason_dbg or '-'}\n"
                f"Band sources: {band_sources_dbg or '-'}\n"
                f"Now(UTC): {now_utc}\n"
            )
            msg = header + "\n".join(sample_rows)
            try:
                res = send_notification(msg, "Разработчик")
                if inspect.isawaitable(res):
                    await res
            except Exception:
                logger.exception("send_notification failed: supplier=%s city=%s", code, city)

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
                    if is_supplier_blocked(ent.code):
                        logger.info("Выгрузка для поставщика %s остановлена по расписанию.", ent.code)
                        continue
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

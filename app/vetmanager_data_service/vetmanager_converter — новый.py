from __future__ import annotations

"""
Vetmanager → Tabletki Data Service (минимум кода, БЕЗ классов; простые импорты)

Изменения для ускорения:
- Жёстко работаем по клинике PREFERRED_CLINIC_ID="2".
- Перед запросом остатков отбрасываем товары, у которых НЕТ ценового параметра (goodSaleParams) для этой клиники.
- Остатки по складу TARGET_STORE_ID тянем ПАРАЛЛЕЛЬНО (aiohttp) с ограничением конкурентности.
- Цены берём из good.goodSaleParams (active, coeff=1 в приоритете).

Точка входа: async def run_service(enterprise_code: str, file_type: str)
"""

import os
import json
import logging
import asyncio
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import aiohttp
from aiohttp import ClientSession, ClientTimeout

from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings
from app.models import MappingBranch
from app.services.database_service import process_database_service

# ===== НАСТРОЙКИ =====
PREFERRED_CLINIC_ID = "2"           # фиксированная клиника
TARGET_STORE_ID = 12                # склад, по которому считаем qty
MAX_GOODS_FOR_STOCK = None          # None/0 → обработать ВСЕ подходящие (по клинике) товары
X_REST_TIME_ZONE = "Europe/Kiev"    # важно: на вашем инстансе 'Europe/Kyiv' даёт 500
LOG_PROGRESS_EVERY = 200            # шаг прогресс-логов при сканировании остатков
MAX_CONCURRENCY = 12                # параллельные HTTP к остаткам
REQUEST_TIMEOUT_SEC = 20            # таймаут одного HTTP-запроса остатков

# ===== ЛОГИ (только консоль) =====
def _ts() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"vetmanager.min.{name}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(h)
    return logger


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

# ===== БД =====
async def get_domain_and_key(session, enterprise_code: str) -> Tuple[str, str]:
    stmt = select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    row = (await session.execute(stmt)).scalar_one_or_none()
    if not row or not getattr(row, "token", None):
        raise ValueError(f"Не найден token для enterprise_code={enterprise_code}")
    parts = [p.strip() for p in row.token.strip().split(",")]
    if len(parts) < 2:
        raise ValueError("Некорректный token. Ожидается 'domain, api_key'")
    return parts[0], parts[1]


async def get_branches(session, enterprise_code: str) -> List[str]:
    stmt = select(MappingBranch).where(MappingBranch.enterprise_code == enterprise_code)
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        raise ValueError(f"В mapping_branch нет записей для enterprise_code={enterprise_code}")
    return [str(getattr(r, "branch", "")) for r in rows if getattr(r, "branch", None)]

# ===== HTTP (sync для каталога) =====
def vet_headers(api_key: str) -> Dict[str, str]:
    return {
        "X-REST-API-KEY": api_key,
        "Content-Type": "application/json",
        "X-REST-TIME-ZONE": X_REST_TIME_ZONЕ,  # опечатки не допускаются
    }

# исправим опечатку:
X_REST_TIME_ZONЕ = X_REST_TIME_ZONE  # на случай копипаста выше


def http_get(url: str, headers: Dict[str, str], logger: logging.Logger) -> requests.Response:
    logger.info(f"HTTP GET → {url}")
    resp = requests.get(url, headers=headers, timeout=30)
    logger.info(f"HTTP {resp.status_code} ← {url}")
    resp.raise_for_status()
    return resp

# ===== УТИЛИТЫ =====
def _to_int01(x: Any) -> int:
    try:
        return 1 if int(str(x).strip()) == 1 else 0
    except Exception:
        return 0


def parse_qty_to_int_floor(value: Any) -> int:
    """Парсим qty ('47.500', 47.5, 47) → целое неотрицательное: floor."""
    try:
        s = str(value).strip().replace(",", ".")
        d = Decimal(s)
        if d < 0:
            d = Decimal(0)
        return int(d.to_integral_value(rounding=ROUND_FLOOR))
    except (InvalidOperation, ValueError, TypeError):
        return 0


def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def extract_store_id(row: Dict[str, Any]) -> Optional[str]:
    """Возвращает store_id как строку: поддерживает row['store_id'] и row['store']['id']."""
    sid = row.get("store_id", None)
    if sid is None and isinstance(row.get("store"), dict):
        sid = row["store"].get("id", None)
    if sid is None:
        return None
    return str(sid)

# ===== АВТОДЕТЕКТ ПОЛЬЗОВАТЕЛЯ и ЖЁСТКАЯ КЛИНИКА=2 =====
def discover_user_id(domain: str, api_key: str, logger: logging.Logger) -> str:
    url = (f"https://{domain}/rest/api/User?filter="
           "[{\"property\":\"is_limited\",\"value\":0,\"operator\":\"=\"}]")
    r = http_get(url, {"X-REST-API-KEY": api_key, "Content-Type": "application/json", "X-REST-TIME-ZONE": X_REST_TIME_ZONE}, logger)
    data = r.json()
    items = (data.get("data") or {}).get("user") or data.get("items") or []
    if not isinstance(items, list) or not items:
        raise ValueError("Cannot autodetect user_id: empty user list")
    for u in items:
        if str(u.get("is_active", "1")) == "1":
            return str(u.get("id"))
    return str(items[0].get("id"))


def discover_clinics(domain: str, api_key: str, logger: logging.Logger, user_id: str) -> List[str]:
    url = f"https://{domain}/rest/api/user/allowedClinicsByUserId?user_id={user_id}"
    r = http_get(url, {"X-REST-API-KEY": api_key, "Content-Type": "application/json", "X-REST-TIME-ZONE": X_REST_TIME_ZONE}, logger)
    data = r.json()
    items = (data.get("data") or {}).get("clinics") or data.get("items") or []
    if not isinstance(items, list) or not items:
        raise ValueError("Cannot autodetect clinics: empty clinics list")
    return [str(c.get("id")) for c in items if c.get("id") is not None]


def discover_single_clinic(domain: str, api_key: str, logger: logging.Logger) -> Tuple[str, str]:
    """
    Жёстко используем clinic_id = PREFERRED_CLINIC_ID ("2").
    Проверяем, что пользователю эта клиника доступна.
    """
    uid = discover_user_id(domain, api_key, logger)
    clinics = discover_clinics(domain, api_key, logger, uid)
    if PREFERRED_CLINIC_ID in clinics:
        logger.info(f"Using FIXED clinic_id={PREFERRED_CLINIC_ID} (allowed={clinics})")
        return uid, PREFERRED_CLINIC_ID
    logger.error(
        f"Requested fixed clinic_id={PREFERRED_CLINIC_ID} is not allowed for user_id={uid}. Allowed clinics: {clinics}"
    )
    raise ValueError(
        f"clinic_id={PREFERRED_CLINIC_ID} is not available for this API key/user. "
        f"Allowed clinics: {clinics}"
    )

# ===== ЦЕНА ИЗ КАТАЛОГА ДЛЯ КЛИНИКИ 2 =====
def select_gsp_and_price_from_catalog(good: Dict[str, Any], clinic_id: str) -> Tuple[Optional[str], float]:
    """
    Выбираем gsp_id и price из good.goodSaleParams для ЗАДАННОЙ клиники.
    Приоритет: active & coefficient==1; затем любой active; иначе None, 0.0
    """
    gsps = good.get("goodSaleParams") or []
    best_any = None
    # 1) exact: active & coeff==1
    for p in gsps:
        if str(p.get("clinic_id")) != str(clinic_id):
            continue
        if str(p.get("status", "")).lower() != "active":
            continue
        if int(p.get("coefficient", 1)) == 1:
            pid = p.get("id")
            pr = safe_float(p.get("price"))
            return (str(pid) if pid is not None else None, pr)
    # 2) any active
    for p in gsps:
        if str(p.get("clinic_id")) != str(clinic_id):
            continue
        if str(p.get("status", "")).lower() == "active":
            best_any = p
            break
    if best_any:
        pid = best_any.get("id")
        pr = safe_float(best_any.get("price"))
        return (str(pid) if pid is not None else None, pr)
    # 3) not found
    return (None, 0.0)

# ===== КАТАЛОГ =====
def fetch_goods_paginated(domain: str, api_key: str, logger: logging.Logger, limit: int = 200) -> List[Dict[str, Any]]:
    """Каталог: /rest/api/Good → берём массив из data.good (или data.items)."""
    base = f"https://{domain}/rest/api/Good"
    headers = {"X-REST-API-KEY": api_key, "Content-Type": "application/json", "X-REST-TIME-ZONE": X_REST_TIME_ZONE}
    goods: List[Dict[str, Any]] = []
    offset = 0
    page = 1
    while True:
        url = f"{base}?limit={limit}&offset={offset}"
        r = http_get(url, headers, logger)
        payload = r.json()
        data_node = payload.get("data") if isinstance(payload, dict) else None
        items: List[Dict[str, Any]] = []
        if isinstance(data_node, dict):
            if isinstance(data_node.get("good"), list):
                items = data_node["good"]
            elif isinstance(data_node.get("items"), list):
                items = data_node["items"]
        count = len(items)
        logger.info(f"Goods page {page}: got {count} items")
        if count == 0:
            break
        goods.extend(items)
        if len(items) < limit:
            break
        offset += limit
        page += 1
    logger.info(f"Fetched goods total={len(goods)}")
    return goods


def filter_goods_for_catalog(all_goods: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
    """Оставляем только is_warehouse_account=1, is_active=1, is_for_sale=1."""
    filtered: List[Dict[str, Any]] = []
    for g in all_goods:
        if _to_int01(g.get("is_warehouse_account")) != 1:
            continue
        if _to_int01(g.get("is_active")) != 1:
            continue
        if _to_int01(g.get("is_for_sale")) != 1:
            continue
        filtered.append(g)
    logger.info(f"filter_goods_for_catalog: in={len(all_goods)} out={len(filtered)}")
    return filtered


def transform_catalog(goods: List[Dict[str, Any]], logger: logging.Logger) -> List[Dict[str, Any]]:
    """Формируем catalog.json (без цен)."""
    by_id: Dict[str, Dict[str, Any]] = {}
    for g in goods:
        gid = str(g.get("id")) if g.get("id") is not None else None
        if gid:
            by_id[gid] = g
    catalog: List[Dict[str, Any]] = []
    for gid, g in by_id.items():
        catalog.append({
            "code": gid,
            "name": g.get("title") or "",
            "barcode": g.get("barcode") or "",
            "producer": "",
            "vat": 20.0,
        })
    logger.info(f"transform_catalog: unique={len(catalog)}")
    return catalog

# ===== ПРЕДФИЛЬТР ПОД КЛИНИКУ 2 (ускоряет) =====
def filter_goods_with_price_for_clinic(goods: List[Dict[str, Any]], clinic_id: str, logger: logging.Logger) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    """
    Оставляем только те товары, у которых есть goodSaleParams для clinic_id.
    Заодно формируем price_by_good из каталога.
    """
    out_goods: List[Dict[str, Any]] = []
    price_by_good: Dict[str, float] = {}
    for g in goods:
        gid = g.get("id")
        if gid is None:
            continue
        gsp_id, price_val = select_gsp_and_price_from_catalog(g, clinic_id)
        if gsp_id is None:
            continue  # нет цены в клинике -> не тратим HTTP на остаток
        out_goods.append(g)
        price_by_good[str(gid)] = float(price_val)
    logger.info(f"clinic prefilter: in={len(goods)} out={len(out_goods)} (have price in clinic={clinic_id})")
    return out_goods, price_by_good

# ===== ПАРАЛЛЕЛЬНАЯ ЗАГРУЗКА ОСТАТКОВ ПО ВСЕМУ СПИСКУ =====
async def fetch_qty_for_good(session: ClientSession, base_stock: str, clinic_id: str, user_id: str, gid: str) -> Tuple[str, int]:
    """
    Возвращает (gid, qty_on_TARGET_STORE_ID). Ошибки даём как qty=0.
    """
    url = f"{base_stock}?clinic_id={clinic_id}&good_id={gid}&user_id={user_id}"
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return gid, 0
            pj = await resp.json(content_type=None)
    except Exception:
        return gid, 0

    balances = (pj.get("data") or {}).get("stock_balances") or []
    qty_store = 0
    if isinstance(balances, list):
        for row in balances:
            sid = extract_store_id(row)
            if sid != str(TARGET_STORE_ID):
                continue
            raw_q = row.get("qty") or row.get("quantity") or 0
            qty_store += parse_qty_to_int_floor(raw_q)
    return gid, int(qty_store)


async def fetch_stock_concurrent(
    domain: str,
    api_key: str,
    logger: logging.Logger,
    goods: List[Dict[str, Any]],
) -> Dict[str, int]:
    """
    Асинхронно, с ограничением конкурентности, тянем остатки по TARGET_STORE_ID для всех goods.
    """
    uid, clinic_id = discover_single_clinic(domain, api_key, logger)
    base_stock = f"https://{domain}/rest/api/Good/StockBalancesForProduct"

    timeout = ClientTimeout(total=REQUEST_TIMEOUT_SEC)
    connector_kwargs = dict(limit_per_host=MAX_CONCURRENCY)  # ограничим одновременные коннекты
    qty_by_good: Dict[str, int] = {}

    headers = {
        "X-REST-API-KEY": api_key,
        "Content-Type": "application/json",
        "X-REST-TIME-ZONE": X_REST_TIME_ZONE,
    }

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async with aiohttp.TCPConnector(limit=None) as connector:
        async with ClientSession(headers=headers, timeout=timeout, connector=connector) as session:

            async def bounded_fetch(gid: str) -> None:
                async with sem:
                    g, q = await fetch_qty_for_good(session, base_stock, clinic_id, uid, gid)
                    qty_by_good[g] = q

            # ограничим список, если MAX_GOODS_FOR_STOCK задан
            subset = goods if MAX_GOODS_FOR_STOCK in (None, 0) else goods[:MAX_GOODS_FOR_STOCK]
            total = len(subset)

            tasks = []
            for idx, g in enumerate(subset, start=1):
                gid_val = g.get("id")
                if gid_val is None:
                    continue
                gid = str(gid_val)
                tasks.append(asyncio.create_task(bounded_fetch(gid)))
                if idx % LOG_PROGRESS_EVERY == 0:
                    logger.info(f"[progress] queued {idx}/{total}")

            if tasks:
                await asyncio.gather(*tasks)

            logger.info(f"[done] fetched qty for {len(qty_by_good)}/{total} goods (store_id={TARGET_STORE_ID})")
            return qty_by_good

# ===== СБОРКА STOCK =====
def build_stock(
    branches: List[str],
    price_by_good: Dict[str, float],
    qty_by_good: Dict[str, int],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for gid, price in price_by_good.items():
        qty = int(qty_by_good.get(gid, 0))
        for branch in branches:
            out.append({
                "branch": branch,
                "code": gid,
                "price": float(price),
                "price_reserve": float(price),
                "qty": qty,
            })
    logger.info(f"build_stock: rows={len(out)} (branches={len(branches)} goods={len(price_by_good)})")
    return out

# ===== ОТПРАВКА =====
async def send_catalog_data(file_path, enterprise_code):
    await process_database_service(file_path, "catalog", enterprise_code)


async def send_stock_data(file_path, enterprise_code):
    await process_database_service(file_path, "stock", enterprise_code)

# ===== ТОЧКА ВХОДА =====
async def run_service(enterprise_code: str, file_type: str) -> None:
    logger = get_logger(enterprise_code)
    logger.info(f"START run_service enterprise={enterprise_code} type={file_type}")

    async with get_async_db() as session:
        domain, api_key = await get_domain_and_key(session, enterprise_code)
        branches = await get_branches(session, enterprise_code)
        logger.info(f"branches loaded: {len(branches)} -> {branches}")
        logger.info(f"settings: domain={domain}, api_key_masked={api_key[:4]}{'*'*(len(api_key)-4)}")
        logger.info(f"fixed clinic_id={PREFERRED_CLINIC_ID}, target store_id={TARGET_STORE_ID}")

        # 1) Каталог
        logger.info("===== STAGE: Fetch GOODS =====")
        all_goods = fetch_goods_paginated(domain, api_key, logger, limit=200)
        active_goods = filter_goods_for_catalog(all_goods, logger)
        catalog = transform_catalog(active_goods, logger)

        if file_type == "catalog":
            out_dir = f"temp/{enterprise_code}"
            ensure_dir(out_dir)
            final = os.path.join(out_dir, "catalog.json")
            with open(final, "w", encoding="utf-8") as f:
                json.dump(catalog, f, ensure_ascii=False, indent=2)
            await send_catalog_data(final, enterprise_code)
            logger.info("DONE catalog")
            logger.info("FINISH run_service")
            return

        # 2) Предфильтр под клинику=2 (оставляем только товары с ценой в этой клинике)
        logger.info("===== STAGE: Prefilter GOODS by clinic (have price) =====")
        goods_for_clinic, price_by_good = filter_goods_with_price_for_clinic(active_goods, PREFERRED_CLINIC_ID, logger)

        # 3) Остатки — параллельно, по всем отфильтрованным товарам
        logger.info(f"===== STAGE: Fetch STOCK (concurrent; store_id={TARGET_STORE_ID}) =====")
        qty_by_good = await fetch_stock_concurrent(domain, api_key, logger, goods_for_clinic)

        stock = build_stock(branches, price_by_good, qty_by_good, logger)
        out_dir = f"temp/{enterprise_code}"
        ensure_dir(out_dir)
        final = os.path.join(out_dir, "stock.json")
        with open(final, "w", encoding="utf-8") as f:
            json.dump(stock, f, ensure_ascii=False, indent=2)
        await send_stock_data(final, enterprise_code)
        logger.info("DONE stock")

    logger.info("FINISH run_service")

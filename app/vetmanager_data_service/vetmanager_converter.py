from __future__ import annotations

"""
Vetmanager → Tabletki Data Service (минимум кода, БЕЗ классов; простые импорты)

Что делает модуль:
- Каталог: включаем ТОЛЬКО товары с is_warehouse_account=1, is_active=1, is_for_sale=1.
- Сток: идём по ВЕСЕМ отфильтрованным товарам каталога (MAX_GOODS_FOR_STOCK=None -> все),
        для каждого тянем остаток по складу TARGET_STORE_ID и ставим qty (может быть 0).
- Цена: берётся из good.goodSaleParams по выбранной клинике:
        приоритет: status=active & coefficient=1, затем любой active; иначе 0.0.
- Логи: только в консоль (без сохранения «сырья» в файлы).
- Точка входа: async def run_service(enterprise_code: str, file_type: str)
"""

import os
import json
import logging
import requests
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings
from app.models import MappingBranch
from app.services.database_service import process_database_service

# ===== НАСТРОЙКИ =====
TARGET_STORE_ID = 10                # склад, по которому считаем qty
MAX_GOODS_FOR_STOCK = None          # None/0 → обработать ВСЕ отфильтрованные товары
X_REST_TIME_ZONE = "Europe/Kiev"    # важно: на вашем инстансе 'Europe/Kyiv' даёт 500
LOG_PROGRESS_EVERY = 200            # шаг прогресс-логов при сканировании остатков
# добавьте рядом с настройками/константами модуля
PREFERRED_CLINIC_ID = "2"
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

# ===== HTTP =====
def vet_headers(api_key: str) -> Dict[str, str]:
    return {
        "X-REST-API-KEY": api_key,
        "Content-Type": "application/json",
        "X-REST-TIME-ZONE": X_REST_TIME_ZONE,
    }


def http_get(url: str, headers: Dict[str, str], logger: logging.Logger) -> requests.Response:
    logger.info(f"HTTP GET → {url}")
    resp = requests.get(url, headers=headers, timeout=30)
    logger.info(f"HTTP {resp.status_code} ← {url}")
    resp.raise_for_status()
    return resp


def http_get_quiet(url: str, headers: Dict[str, str]) -> requests.Response:
    """Тихий GET для per-good запросов (меньше шума в логе)."""
    resp = requests.get(url, headers=headers, timeout=30)
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

# ===== АВТОДЕТЕКТ ПОЛЬЗОВАТЕЛЯ/КЛИНИКИ =====
def discover_user_id(domain: str, api_key: str, logger: logging.Logger) -> str:
    url = (f"https://{domain}/rest/api/User?filter="
           "[{\"property\":\"is_limited\",\"value\":0,\"operator\":\"=\"}]")
    r = http_get(url, vet_headers(api_key), logger)
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
    r = http_get(url, vet_headers(api_key), logger)
    data = r.json()
    items = (data.get("data") or {}).get("clinics") or data.get("items") or []
    if not isinstance(items, list) or not items:
        raise ValueError("Cannot autodetect clinics: empty clinics list")
    ids: List[str] = []
    for c in items:
        cid = c.get("id")
        if cid is not None:
            ids.append(str(cid))
    return ids


def discover_single_clinic(domain: str, api_key: str, logger: logging.Logger) -> Tuple[str, str]:
    """
    Жёстко используем clinic_id = PREFERRED_CLINIC_ID (по умолчанию "2").
    Проверяем, что пользователю эта клиника доступна.
    Возвращаем (user_id, clinic_id).
    """
    uid = discover_user_id(domain, api_key, logger)
    clinics = discover_clinics(domain, api_key, logger, uid)  # список строковых id
    if PREFERRED_CLINIC_ID in clinics:
        logger.info(f"Using FIXED clinic_id={PREFERRED_CLINIC_ID} (allowed={clinics})")
        return uid, PREFERRED_CLINIC_ID

    # если нужной клиники нет среди доступных — падаем с понятным сообщением
    logger.error(
        f"Requested fixed clinic_id={PREFERRED_CLINIC_ID} is not allowed for user_id={uid}. "
        f"Allowed clinics: {clinics}"
    )
    raise ValueError(
        f"clinic_id={PREFERRED_CLINIC_ID} is not available for this API key/user. "
        f"Allowed clinics: {clinics}"
    )


# ===== ЦЕНА ИЗ КАТАЛОГА =====
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
    goods: List[Dict[str, Any]] = []
    offset = 0
    page = 1
    while True:
        url = f"{base}?limit={limit}&offset={offset}"
        r = http_get(url, vet_headers(api_key), logger)
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

# ===== СТОК + ЦЕНЫ: идём по ВСЕМ товарам (или первым N, если N задан) =====
def fetch_stock_and_prices_first_n_no_qty_filter(
    domain: str,
    api_key: str,
    logger: logging.Logger,
    goods: List[Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, float]]:
    """
    Берём первые N товаров (если MAX_GOODS_FOR_STOCK задан), иначе — все.
    Для каждого:
      - тянем остаток по складу TARGET_STORE_ID (qty может быть 0);
      - цену берём из good.goodSaleParams для выбранной клиники.
    """
    headers = vet_headers(api_key)
    uid, clinic_id = discover_single_clinic(domain, api_key, logger)
    logger.info(
        f"clinic mode: user_id={uid}, clinic_id={clinic_id}; "
        f"taking {'ALL' if not MAX_GOODS_FOR_STOCK else MAX_GOODS_FOR_STOCK} goods "
        f"(no qty>0 filter) on store_id={TARGET_STORE_ID}"
    )

    base_stock = f"https://{domain}/rest/api/Good/StockBalancesForProduct"

    qty_by_good: Dict[str, int] = {}
    price_by_good: Dict[str, float] = {}

    subset = goods if MAX_GOODS_FOR_STOCK in (None, 0) else goods[:MAX_GOODS_FOR_STOCK]
    total = len(subset)

    for idx, g in enumerate(subset, start=1):
        gid_val = g.get("id")
        if gid_val is None:
            continue
        gid = str(gid_val)

        # Остаток по конкретному товару (может быть 0)
        stock_url = f"{base_stock}?clinic_id={clinic_id}&good_id={gid}&user_id={uid}"
        qty_store = 0
        try:
            resp = http_get_quiet(stock_url, headers)
            pj = resp.json()
            balances = (pj.get("data") or {}).get("stock_balances") or []
            if isinstance(balances, list):
                for row in balances:
                    sid = extract_store_id(row)
                    if sid != str(TARGET_STORE_ID):
                        continue
                    raw_q = row.get("qty") or row.get("quantity") or 0
                    qty_store += parse_qty_to_int_floor(raw_q)
        except Exception as ex:
            logging.getLogger().error(f"stock failed: gid={gid} err={ex} → qty=0")

        # Цена — из каталога (goodSaleParams) по той же клинике
        gsp_id, price_from_catalog = select_gsp_and_price_from_catalog(g, clinic_id)
        if gsp_id is None:
            logger.warning(f"gid={gid}: no goodSaleParams for clinic={clinic_id} -> price=0")
            price_from_catalog = 0.0

        qty_by_good[gid] = int(qty_store)
        price_by_good[gid] = float(price_from_catalog)

        # прогресс-лог
        if idx % LOG_PROGRESS_EVERY == 0 or idx == total:
            logger.info(f"[progress] processed {idx}/{total}")

    logger.info(f"SUMMARY: produced {len(qty_by_good)} items (processed {total})")
    return qty_by_good, price_by_good

# ===== СБОРКА STOCK =====
def build_stock(
    branches: List[str],
    price_by_good: Dict[str, float],
    qty_by_good: Dict[str, int],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for gid, qty in qty_by_good.items():
        price = float(price_by_good.get(gid, 0.0))
        for branch in branches:
            out.append({
                "branch": branch,
                "code": gid,
                "price": price,
                "price_reserve": price,
                "qty": int(qty),
            })
    logger.info(f"build_stock: rows={len(out)} (branches={len(branches)} goods={len(qty_by_good)})")
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

        # 1) Каталог
        logger.info("===== STAGE: Fetch GOODS =====")
        all_goods = fetch_goods_paginated(domain, api_key, logger, limit=200)
        goods = filter_goods_for_catalog(all_goods, logger)
        catalog = transform_catalog(goods, logger)

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

        # 2) Сток (все товары; цена из каталога; остатки по складу TARGET_STORE_ID)
        logger.info(f"===== STAGE: Fetch STOCK & PRICES (ALL goods; store_id={TARGET_STORE_ID}) =====")
        qty_by_good, price_by_good = fetch_stock_and_prices_first_n_no_qty_filter(
            domain, api_key, logger, goods
        )

        stock = build_stock(branches, price_by_good, qty_by_good, logger)
        out_dir = f"temp/{enterprise_code}"
        ensure_dir(out_dir)
        final = os.path.join(out_dir, "stock.json")
        with open(final, "w", encoding="utf-8") as f:
            json.dump(stock, f, ensure_ascii=False, indent=2)
        await send_stock_data(final, enterprise_code)
        logger.info("DONE stock")

    logger.info("FINISH run_service")

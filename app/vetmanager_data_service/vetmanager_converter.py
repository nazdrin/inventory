from __future__ import annotations

"""
Vetmanager → Tabletki Data Service (минимум кода, БЕЗ классов; простые импорты)

⚠️ ИЗМЕНЕНИЕ СУЩЕСТВУЮЩЕГО КОДА: модуль меняет точки интеграции (импорты ORM и process_database_service).
Логи — ТОЛЬКО в консоль. Сырые входящие сохраняются в temp/{enterprise_code}/raw/...
Сток запрашивается одним вызовом БЕЗ clinic_id / good_id / user_id (если падает/пусто — включается фолбэк по товарам).
Каталог берём через /rest/api/Good, а если там пусто/500 — через ProductsDataForInvoice с ЯВНОЙ клиникой.
"""

import os
import json
import logging
import requests
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings
from app.models import MappingBranch
from app.services.database_service import process_database_service

# ===== ПАРАМЕТРЫ ФАЙЛОВ =====
BASE_SAVE_DIR = Path("temp")
RAW_DIR_NAME = "raw"

# ===== ЛОГИРОВАНИЕ (только консоль) =====

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


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def save_json(data: Any, path: Path) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ===== БД: ЧТЕНИЕ НАСТРОЕК И BRANCH =====

async def get_domain_and_key(session, enterprise_code: str) -> Tuple[str, str]:
    """Ожидаемый формат token: "domain, api_key"."""
    stmt = select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    row = (await session.execute(stmt)).scalar_one_or_none()
    if not row or not getattr(row, "token", None):
        raise ValueError(f"Не найден token для enterprise_code={enterprise_code}")

    parts = [p.strip() for p in row.token.strip().split(",")]
    if len(parts) < 2:
        raise ValueError("Некорректный token. Ожидается 'domain, api_key'")
    return parts[0], parts[1]


async def get_branches(session, enterprise_code: str) -> List[str]:
    """Возвращает список branch для enterprise_code."""
    stmt = select(MappingBranch).where(MappingBranch.enterprise_code == enterprise_code)
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        raise ValueError(f"В mapping_branch нет записей для enterprise_code={enterprise_code}")
    return [str(getattr(r, "branch", "")) for r in rows if getattr(r, "branch", None)]

# ===== HTTP ВСПОМОГАТЕЛЬНЫЕ (sync, через requests) =====

def vet_headers(api_key: str) -> Dict[str, str]:
    return {
        "X-REST-API-KEY": api_key,
        "Content-Type": "application/json",
        "X-REST-TIME-ZONE": "Europe/Kiev",
    }


def http_get(url: str, headers: Dict[str, str], logger: logging.Logger) -> requests.Response:
    logger.info(f"HTTP GET → {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        logger.info(f"HTTP {resp.status_code} ← {url}")
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        status = getattr(e.response, "status_code", None)
        body_preview = None
        try:
            if getattr(e, "response", None) is not None and e.response.text:
                body_preview = e.response.text[:500]
        except Exception:
            body_preview = None
        if status:
            logger.error(f"HTTP GET failed: {status} for {url}")
        else:
            logger.error(f"HTTP GET failed (no status): {e} for {url}")
        if body_preview:
            logger.error(f"HTTP response body (first 500 chars): {body_preview}")
        raise

# ===== АВТОДЕТЕКТ user_id и clinic_id =====

def discover_user_id(domain: str, api_key: str, logger: logging.Logger) -> str:
    """/rest/api/User?filter=[{"property":"is_limited","value":0,"operator":"="}] → предпочитаем активного."""
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


def discover_clinic_id(domain: str, api_key: str, logger: logging.Logger, user_id: str) -> str:
    """/rest/api/user/allowedClinicsByUserId?user_id={id} → берём первую клинику."""
    url = f"https://{domain}/rest/api/user/allowedClinicsByUserId?user_id={user_id}"
    r = http_get(url, vet_headers(api_key), logger)
    data = r.json()
    items = (data.get("data") or {}).get("clinics") or data.get("items") or []
    if not isinstance(items, list) or not items or items[0].get("id") is None:
        raise ValueError("Cannot autodetect clinic_id: empty clinics list")
    return str(items[0]["id"])


def discover_clinics(domain: str, api_key: str, logger: logging.Logger, user_id: str) -> List[str]:
    """Возвращает *все* clinic_id, доступные пользователю."""
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

# ===== ВЫГРУЗКА ДАННЫХ (sync HTTP) =====

# --- Каталог: основной /Good и фолбэк ProductsDataForInvoice с clinic_id ---

def fetch_goods_paginated(domain: str, api_key: str, logger: logging.Logger, limit: int = 200) -> List[Dict[str, Any]]:
    base = f"https://{domain}/rest/api/Good"
    goods: List[Dict[str, Any]] = []
    offset = 0
    page = 1
    while True:
        url = f"{base}?limit={limit}&offset={offset}"
        logger.info(f"GET Goods page={page} limit={limit} offset={offset}")
        resp = http_get(url, vet_headers(api_key), logger)
        payload = resp.json()
        items = payload.get("data") or payload.get("items") or payload
        if isinstance(items, dict) and "items" in items:
            items = items["items"]
        count = len(items) if isinstance(items, list) else 0
        logger.info(f"Goods page {page}: got {count} items")
        if not isinstance(items, list) or not items:
            break
        goods.extend(items)
        if len(items) < limit:
            break
        offset += limit
        page += 1
    logger.info(f"Fetched goods total={len(goods)}")
    return goods


def fetch_goods_via_pdi(domain: str, api_key: str, logger: logging.Logger, clinic_id: str, page_size: int = 200) -> List[Dict[str, Any]]:
    """Фолбэк: берём товары через ProductsDataForInvoice с явной клиникой.
    /rest/api/Good/ProductsDataForInvoice/?page[number]=1&page[size]=50&clinic_id=1&page[no_paging]=0&search_query=
    """
    base = f"https://{domain}/rest/api/Good/ProductsDataForInvoice/"
    page = 1
    goods: List[Dict[str, Any]] = []
    while True:
        url = (f"{base}?page[number]={page}&page[size]={page_size}&page[no_paging]=0"
               f"&clinic_id={clinic_id}&search_query=")
        logger.info(f"GET Goods PDI page={page} size={page_size} clinic_id={clinic_id}")
        r = http_get(url, vet_headers(api_key), logger)
        pj = r.json()
        items = (pj.get("data") or {}).get("products") or pj.get("items") or []
        if not isinstance(items, list) or not items:
            break
        goods.extend(items)
        logger.info(f"Goods PDI page {page}: got {len(items)} items (total {len(goods)})")
        page += 1
    logger.info(f"Fetched goods via PDI total={len(goods)} (clinic_id={clinic_id})")
    return goods


def _price_from_good(rec: Dict[str, Any]) -> float:
    if isinstance(rec.get("goodSaleParams"), list):
        for p in rec["goodSaleParams"]:
            if isinstance(p.get("price"), (int, float)):
                return float(p["price"])
    if isinstance(rec.get("price"), (int, float)):
        return float(rec["price"])  # формат PDI
    return 0.0


def merge_goods_prefer_nonzero_price(goods_lists: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    for lst in goods_lists:
        for g in lst:
            gid = g.get("id")
            if gid is None:
                continue
            key = str(gid)
            if key not in by_id:
                by_id[key] = g
            else:
                if _price_from_good(by_id[key]) == 0.0 and _price_from_good(g) > 0.0:
                    by_id[key] = g
    return list(by_id.values())


def fetch_goods_via_pdi_all_clinics(domain: str, api_key: str, logger: logging.Logger, clinic_ids: List[str], page_size: int = 200) -> List[Dict[str, Any]]:
    bundles: List[List[Dict[str, Any]]] = []
    for cid in clinic_ids:
        try:
            bundle = fetch_goods_via_pdi(domain, api_key, logger, clinic_id=cid, page_size=page_size)
            bundles.append(bundle)
        except Exception as ex:
            logger.error(f"Goods PDI failed for clinic_id={cid}: {ex}")
    merged = merge_goods_prefer_nonzero_price(bundles)
    logger.info(f"Merged goods across clinics: clinics={len(clinic_ids)} total={sum(len(b) for b in bundles)} unique={len(merged)}")
    return merged


def fetch_goods_resilient(domain: str, api_key: str, logger: logging.Logger, limit: int = 200, clinic_ids: Optional[List[str]] = None, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Пробуем обычный /Good. Если пусто/ошибка — берём PDI по *всем* клиникам."""
    try:
        goods = fetch_goods_paginated(domain, api_key, logger, limit=limit)
        if goods:
            return goods
        logger.warning("/Good returned empty list; switching to ProductsDataForInvoice with clinic_id(s)")
    except Exception as e:
        logger.warning(f"/Good failed: {e}; switching to ProductsDataForInvoice with clinic_id(s)")
    # autodetect if not provided
    if user_id is None:
        user_id = discover_user_id(domain, api_key, logger)
    if clinic_ids is None:
        clinic_ids = discover_clinics(domain, api_key, logger, user_id)
    logger.info(f"using clinics={clinic_ids} (user_id={user_id}) for PDI")
    return fetch_goods_via_pdi_all_clinics(domain, api_key, logger, clinic_ids=clinic_ids, page_size=limit)

# --- Остатки: общий вызов + фолбэк по товарам при ошибке/пусто ---

def fetch_stock_all(domain: str, api_key: str, logger: logging.Logger, goods: Optional[List[Dict[str, Any]]] = None, clinic_ids: Optional[List[str]] = None, user_id: Optional[str] = None) -> Any:
    """Сначала пытаемся без параметров. Если 4xx/5xx/пусто — фолбэк по товарам с clinic_id/user_id по *всем* клиникам."""
    url = f"https://{domain}/rest/api/Good/StockBalancesForProduct"
    logger.info("GET StockBalancesForProduct (ALL)")
    try:
        resp = http_get(url, vet_headers(api_key), logger)
        payload = resp.json()
        items = (payload.get("data") or {}).get("stock_balances")
        if not isinstance(items, list) or len(items) == 0:
            raise ValueError("Empty stock_balances")
        return payload
    except Exception as e:
        logger.warning(f"ALL-stock endpoint failed/empty, switching to per-good fallback across clinics: {e}")
        return fetch_stock_fallback(domain, api_key, logger, goods=goods, clinic_ids=clinic_ids, user_id=user_id)


def fetch_stock_fallback(domain: str, api_key: str, logger: logging.Logger, goods: Optional[List[Dict[str, Any]]] = None, clinic_ids: Optional[List[str]] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
    if goods is None:
        goods = fetch_goods_resilient(domain, api_key, logger, limit=200, clinic_ids=clinic_ids, user_id=user_id)
    if user_id is None:
        user_id = discover_user_id(domain, api_key, logger)
    if clinic_ids is None:
        clinic_ids = discover_clinics(domain, api_key, logger, user_id)
    logger.info(f"fallback stock: user_id={user_id} clinics={clinic_ids}")

    collected: List[Dict[str, Any]] = []
    base = f"https://{domain}/rest/api/Good/StockBalancesForProduct"
    total_goods = len(goods)
    for cid in clinic_ids:
        logger.info(f"fallback stock: processing clinic_id={cid} goods={total_goods}")
        for idx, g in enumerate(goods, 1):
            gid = g.get("id")
            if gid is None:
                continue
            url = f"{base}?clinic_id={cid}&good_id={gid}&user_id={user_id}"
            try:
                r = http_get(url, vet_headers(api_key), logger)
                pj = r.json()
                part = (pj.get("data") or {}).get("stock_balances") or []
                if isinstance(part, list):
                    collected.extend(part)
            except Exception as ex:
                logger.error(f"fallback stock: clinic_id={cid} good_id={gid} failed: {ex}")
            if idx % 200 == 0 or idx == total_goods:
                logger.info(f"fallback stock progress clinic={cid}: {idx}/{total_goods}")
    return {"data": {"stock_balances": collected}}

# ===== ТРАНСФОРМАЦИИ =====

def transform_catalog(goods: List[Dict[str, Any]], logger: logging.Logger) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    """Возвращает (catalog_list, prices_by_code). При дублях id берём последний.
    Поддерживаем форматы /Good (goodSaleParams[].price) и PDI (price на верхнем уровне).
    """
    by_id: Dict[str, Dict[str, Any]] = {}
    for g in goods:
        gid = str(g.get("id")) if g.get("id") is not None else None
        if gid:
            by_id[gid] = g

    catalog: List[Dict[str, Any]] = []
    prices: Dict[str, float] = {}
    for gid, g in by_id.items():
        price = 0.0
        gsp = g.get("goodSaleParams") or []
        if isinstance(gsp, list):
            for p in gsp:
                if isinstance(p.get("price"), (int, float)):
                    price = float(p["price"])
                    break
        if price == 0.0 and isinstance(g.get("price"), (int, float)):
            price = float(g["price"])  # формат PDI

        prices[gid] = price
        catalog.append({
            "code": gid,
            "name": g.get("title") or g.get("name") or "",
            "barcode": g.get("barcode") or g.get("bar_code") or "",
            "producer": "N/A",
            "vat": 20.0,
        })
    logger.info(f"transform_catalog: in={len(goods)} unique={len(catalog)}")
    return catalog, prices


def parse_stock_all_to_qty(payload: Any, logger: logging.Logger) -> Dict[str, int]:
    """Строит словарь { good_id: qty_total } из общего ответа, суммируя по всем складам."""
    if isinstance(payload, dict):
        items = (payload.get("data") or {}).get("stock_balances") or payload.get("items")
    else:
        items = payload

    if not isinstance(items, list):
        logger.warning("Unexpected stock payload; will treat as empty list")
        items = []

    total_by_good: Dict[str, int] = {}
    for r in items:
        gid = r.get("good_id") or r.get("id") or r.get("goodId")
        if gid is None:
            continue
        qv = r.get("qty") or r.get("quantity") or 0
        try:
            q = int(qv) if isinstance(qv, (int, float, str)) else 0
        except Exception:
            q = 0
        key = str(gid)
        total_by_good[key] = total_by_good.get(key, 0) + max(q, 0)
    logger.info(f"parse_stock_all_to_qty: goods={len(total_by_good)} rows={len(items)}")
    return total_by_good


def build_stock(
    branches: List[str],
    prices: Dict[str, float],
    qty_by_good: Dict[str, int],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """Строит итоговый stock.json: для КАЖДОГО branch и КАЖДОГО товара пишет строку.
    qty — суммарный по всем складам.
    """
    out: List[Dict[str, Any]] = []
    miss_price = 0
    for gid, qty in qty_by_good.items():
        price = prices.get(gid, 0.0)
        if price == 0.0:
            miss_price += 1
        for branch in branches:
            out.append({
                "branch": branch,
                "code": gid,
                "price": float(price),
                "price_reserve": float(price),
                "qty": max(int(qty), 0),
            })
    if miss_price:
        logger.warning(f"build_stock: товаров без цены={miss_price}")
    logger.info(f"build_stock: rows={len(out)} (branches={len(branches)} goods={len(qty_by_good)})")
    return out

# ===== ОТПРАВКА (реальный вызов process_database_service) =====

async def send_catalog_data(file_path, enterprise_code):
    await process_database_service(file_path, "catalog", enterprise_code)


async def send_stock_data(file_path, enterprise_code):
    await process_database_service(file_path, "stock", enterprise_code)

# ===== ТОЧКА ВХОДА (as requested) =====

async def run_service(enterprise_code: str, file_type: str) -> None:
    logger = get_logger(enterprise_code)
    logger.info(f"START run_service enterprise={enterprise_code} type={file_type}")

    async with get_async_db() as session:
        domain, api_key = await get_domain_and_key(session, enterprise_code)
        branches = await get_branches(session, enterprise_code)
        logger.info(f"branches loaded: {len(branches)} -> {branches}")
        logger.info(f"settings: domain={domain}, api_key_masked={api_key[:4]}{'*'*(len(api_key)-4)}")

        # 1) Каталог (нужен также для цен в стоке)
        logger.info("===== STAGE: Fetch GOODS =====")
        goods = fetch_goods_resilient(domain, api_key, logger, limit=200)
        raw_goods_path = BASE_SAVE_DIR / enterprise_code / RAW_DIR_NAME / f"goods_all_{_ts()}.json"
        save_json(goods, raw_goods_path)
        catalog, prices = transform_catalog(goods, logger)

        if file_type == "catalog":
            final = BASE_SAVE_DIR / enterprise_code / "catalog.json"
            save_json(catalog, final)
            await send_catalog_data(final, enterprise_code)
            logger.info("DONE catalog")
            logger.info("FINISH run_service")
            return

        # 2) Сток — общий вызов или фолбэк по товарам
        stock_payload = fetch_stock_all(domain, api_key, logger, goods=goods, clinic_ids=clinics, user_id=uid)
        raw_stock_path = BASE_SAVE_DIR / enterprise_code / RAW_DIR_NAME / f"stock_all_{_ts()}.json"
        save_json(stock_payload, raw_stock_path)

        qty_by_good = parse_stock_all_to_qty(stock_payload, logger)
        stock = build_stock(branches, prices, qty_by_good, logger)
        final = BASE_SAVE_DIR / enterprise_code / "stock.json"
        save_json(stock, final)
        await send_stock_data(final, enterprise_code)
        logger.info("DONE stock")

    logger.info("FINISH run_service")

# Пример запуска:
# async def main():
#     await run_service("221", "catalog")
#     await run_service("221", "stock")
#
# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(main())

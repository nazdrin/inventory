import argparse
import asyncio
import hashlib
import json
import os
from decimal import Decimal
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv
from sqlalchemy import select, text

from app.database import get_async_db
from app.models import CatalogCategory, MasterCatalog


def _env_required(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Не задано обязательное окружение: {name}")
    return value


def _resolve_enterprise_code(enterprise_code: str = "") -> str:
    load_dotenv()
    value = (enterprise_code or "").strip() or (os.getenv("MASTER_CATALOG_ENTERPRISE_CODE") or "").strip()
    if not value:
        raise RuntimeError("Не задан enterprise_code: передайте --enterprise или MASTER_CATALOG_ENTERPRISE_CODE")
    return value


async def _get_salesdrive_token(enterprise_code: str) -> str:
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT token FROM enterprise_settings WHERE enterprise_code = :c LIMIT 1"),
            {"c": enterprise_code},
        )
        token = res.scalar_one_or_none()
    token = (token or "").strip()
    if not token:
        raise RuntimeError(f"Не найден token в enterprise_settings для enterprise_code={enterprise_code}")
    return token


def _chunk(items: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    max_retries: int = 6,
) -> httpx.Response:
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = await client.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code in (429, 502, 503, 504):
                if attempt == max_retries:
                    return resp
                await asyncio.sleep(delay)
                delay = min(delay * 2, 20.0)
                continue
            return resp
        except httpx.RequestError:
            if attempt == max_retries:
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 2, 20.0)
    raise RuntimeError("Неожиданное завершение retry-цикла")


def _cache_path(enterprise_code: str) -> str:
    return os.path.join(os.getcwd(), f".salesdrive_master_catalog_cache_{enterprise_code}.json")


def _load_cache(path: str) -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return {}


def _save_cache_atomic(path: str, cache: Dict[str, str]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, ensure_ascii=False)
    os.replace(tmp, path)


def _decimal_to_float(value: Optional[Decimal], divisor: str = "1") -> Optional[float]:
    if value is None:
        return None
    result = Decimal(value) / Decimal(divisor)
    return float(result)


def _stable_hash_product(item: Dict[str, Any]) -> str:
    payload = {
        "sku": item.get("sku"),
        "name": item.get("name"),
        "nameTranslate": item.get("nameTranslate"),
        "description": item.get("description"),
        "descriptionTranslate": item.get("descriptionTranslate"),
        "manufacturer": item.get("manufacturer"),
        "barcode": item.get("barcode"),
        "category": item.get("category"),
        "main_image_url": item.get("main_image_url"),
        "is_archived": item.get("is_archived"),
        "weight": item.get("weight"),
        "length": item.get("length"),
        "width": item.get("width"),
        "height": item.get("height"),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


async def export_master_catalog_to_salesdrive(
    enterprise_code: str,
    batch_size: int = 200,
    limit: int = 0,
) -> Dict[str, Any]:
    load_dotenv()
    enterprise_code = _resolve_enterprise_code(enterprise_code)
    endpoint = _env_required("SALESDRIVE_PRODUCT_HANDLER_URL")
    token = await _get_salesdrive_token(enterprise_code)

    async with get_async_db() as session:
        category_rows = (await session.execute(select(CatalogCategory))).scalars().all()
        categories = {row.category_code: row for row in category_rows}

        stmt = select(MasterCatalog).order_by(MasterCatalog.sku.asc())
        if limit and limit > 0:
            stmt = stmt.limit(limit)
        master_rows = (await session.execute(stmt)).scalars().all()

    products: List[Dict[str, Any]] = []
    for row in master_rows:
        category = None
        category_code = row.category_l2_code or row.category_l1_code
        if category_code:
            category_row = categories.get(category_code)
            category = {"id": category_code}
            if category_row and category_row.name_ua:
                category["name"] = category_row.name_ua

        item: Dict[str, Any] = {
            "id": row.sku,
            "sku": row.sku,
            "name": row.name_ua,
            "nameTranslate": row.name_ru,
            "nameForDocuments": row.name_ua,
            "description": row.description_ua,
            "descriptionTranslate": row.description_ru,
            "manufacturer": row.manufacturer,
            "barcode": row.barcode,
            "main_image_url": row.main_image_url,
            "is_archived": bool(row.is_archived),
        }
        if category:
            item["category"] = category
        if row.main_image_url:
            item["images"] = [{"fullsize": row.main_image_url}]
        if row.is_archived:
            item["label"] = ["Архив"]
            item["labelMode"] = "replace"

        weight = _decimal_to_float(row.weight_g, "1000")
        length = _decimal_to_float(row.length_mm, "10")
        width = _decimal_to_float(row.width_mm, "10")
        height = _decimal_to_float(row.height_mm, "10")
        if weight is not None:
            item["weight"] = weight
        if length is not None:
            item["length"] = length
        if width is not None:
            item["width"] = width
        if height is not None:
            item["height"] = height
        products.append(item)

    cache_file = _cache_path(enterprise_code)
    cache = _load_cache(cache_file)
    ids_in_source = {str(item["id"]) for item in products}
    to_send: List[Dict[str, Any]] = []
    hashes_to_apply: Dict[str, str] = {}

    for item in products:
        item_id = str(item["id"])
        item_hash = _stable_hash_product(item)
        hashes_to_apply[item_id] = item_hash
        if cache.get(item_id) != item_hash:
            send_item = {k: v for k, v in item.items() if k not in {"main_image_url", "is_archived"} and v is not None}
            to_send.append(send_item)

    if not to_send:
        cache = {k: v for k, v in cache.items() if k in ids_in_source}
        _save_cache_atomic(cache_file, cache)
        return {"sent": 0, "batches": 0, "errors": 0, "cache_path": cache_file}

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": token,
    }
    batches = _chunk(to_send, batch_size)
    sent_total = 0
    errors = 0

    async with httpx.AsyncClient() as client:
        for part in batches:
            payload = {
                "action": "update",
                "dontUpdateFields": ["price"],
                "product": part,
            }
            resp = await _post_with_retry(client, endpoint, headers, payload)
            if 200 <= resp.status_code < 300:
                sent_total += len(part)
                for item in part:
                    cache[str(item["id"])] = hashes_to_apply[str(item["id"])]
            else:
                errors += 1

    cache = {k: v for k, v in cache.items() if k in ids_in_source}
    _save_cache_atomic(cache_file, cache)
    return {"sent": sent_total, "batches": len(batches), "errors": errors, "cache_path": cache_file}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Экспорт master_catalog в SalesDrive product-handler")
    parser.add_argument("--enterprise", default="")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await export_master_catalog_to_salesdrive(
        enterprise_code=str(args.enterprise),
        batch_size=int(args.batch_size),
        limit=int(args.limit),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

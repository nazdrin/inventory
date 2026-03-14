import argparse
import asyncio
import hashlib
import json
import logging
import os
from typing import Any, Dict, List

import httpx
from dotenv import load_dotenv
from sqlalchemy import select, text

from app.database import get_async_db
from app.models import CatalogCategory


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("salesdrive_category_exporter")


def _env_required(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Не задано обязательное окружение: {name}")
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
    return os.path.join(os.getcwd(), f".salesdrive_category_cache_{enterprise_code}.json")


def _load_cache(path: str) -> Dict[str, str]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except FileNotFoundError:
        return {}
    except Exception:
        logger.exception("Не удалось прочитать cache-файл: %s", path)
    return {}


def _save_cache_atomic(path: str, cache: Dict[str, str]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, ensure_ascii=False)
    os.replace(tmp, path)


def _stable_hash_category(item: Dict[str, Any]) -> str:
    payload = {
        "id": item.get("id"),
        "name": item.get("name"),
        "parentId": item.get("parentId"),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


async def export_categories_to_salesdrive(
    enterprise_code: str,
    batch_size: int = 200,
    limit: int = 0,
) -> Dict[str, Any]:
    load_dotenv()
    endpoint = _env_required("SALESDRIVE_CATEGORY_HANDLER_URL")
    token = await _get_salesdrive_token(enterprise_code)

    async with get_async_db() as session:
        stmt = (
            select(CatalogCategory)
            .where(CatalogCategory.is_active.is_(True))
            .order_by(CatalogCategory.category_code.asc())
        )
        if limit and limit > 0:
            stmt = stmt.limit(limit)
        rows = (await session.execute(stmt)).scalars().all()

    categories = [
        {
            "id": row.category_code,
            "name": row.name_ua,
            "parentId": row.parent_category_code or None,
        }
        for row in rows
        if row.category_code and row.name_ua
    ]

    cache_file = _cache_path(enterprise_code)
    cache = _load_cache(cache_file)
    ids_in_source = {str(item["id"]) for item in categories}
    to_send: List[Dict[str, Any]] = []
    hashes_to_apply: Dict[str, str] = {}

    for item in categories:
        item_id = str(item["id"])
        item_hash = _stable_hash_category(item)
        hashes_to_apply[item_id] = item_hash
        if cache.get(item_id) != item_hash:
            to_send.append(item)

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
            payload = {"action": "update", "category": part}
            resp = await _post_with_retry(client, endpoint, headers, payload)
            if 200 <= resp.status_code < 300:
                sent_total += len(part)
                for item in part:
                    cache[str(item["id"])] = hashes_to_apply[str(item["id"])]
            else:
                errors += 1
                logger.error("Category batch FAIL: HTTP %d body=%s", resp.status_code, resp.text[:4000])

    cache = {k: v for k, v in cache.items() if k in ids_in_source}
    _save_cache_atomic(cache_file, cache)
    return {"sent": sent_total, "batches": len(batches), "errors": errors, "cache_path": cache_file}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Экспорт catalog_categories в SalesDrive category-handler")
    parser.add_argument("--enterprise", default="223")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await export_categories_to_salesdrive(
        enterprise_code=str(args.enterprise),
        batch_size=int(args.batch_size),
        limit=int(args.limit),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

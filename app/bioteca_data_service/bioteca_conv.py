import json
import logging
import os
import tempfile
from typing import Any

import aiohttp
from sqlalchemy.future import select

from app.database import get_async_db
from app.models import EnterpriseSettings, MappingBranch
from app.services.database_service import process_database_service

DEFAULT_VAT = 20.0
AINUR_PRODUCTS_URL = "https://connect.ainur.app/api/v4/product"
REQUEST_TIMEOUT_SEC = 60

logger = logging.getLogger(__name__)


def _normalize_producer(value: Any) -> str:
    if value is None:
        return ""

    producer = str(value).strip()
    if producer.lower() in {"n/a", "n\\a", "na", "none", "null", "-"}:
        return ""
    return producer


async def fetch_enterprise_settings(enterprise_code: str) -> EnterpriseSettings | None:
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()


async def fetch_mapping_branches(enterprise_code: str) -> list[dict[str, str]]:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.store_id, MappingBranch.branch, MappingBranch.enterprise_code).where(
                MappingBranch.enterprise_code == enterprise_code
            )
        )
        rows = result.all()

    mappings: list[dict[str, str]] = []
    for store_id, branch, enterprise_code_value in rows:
        if store_id is None:
            logger.warning("Bioteca mapping row without store_id: enterprise_code=%s", enterprise_code)
            continue
        mappings.append(
            {
                "store_id": str(store_id),
                "branch": "" if branch is None else str(branch),
                "enterprise_code": "" if enterprise_code_value is None else str(enterprise_code_value),
            }
        )
    return mappings


def _extract_products(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("results", "products", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    raise ValueError("AINUR API returned unsupported payload format")


async def fetch_products_for_store(
    session: aiohttp.ClientSession,
    enterprise_code: str,
    token: str,
    store_id: str,
    min_stock: int = 1,
) -> list[dict[str, Any]]:
    headers = {
        "accept": "application/json",
        "X-AINUR-API-Access-Token": token,
    }
    params = {
        "store_id": store_id,
        "min_stock": min_stock,
    }

    try:
        async with session.get(
            AINUR_PRODUCTS_URL,
            headers=headers,
            params=params,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC),
        ) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)
            products = _extract_products(payload)
            logger.info(
                "Bioteca fetched products: enterprise_code=%s store_id=%s count=%s",
                enterprise_code,
                store_id,
                len(products),
            )
            return products
    except aiohttp.ClientResponseError:
        logger.exception("Bioteca HTTP error: store_id=%s", store_id)
        raise
    except aiohttp.ClientError:
        logger.exception("Bioteca request failed: store_id=%s", store_id)
        raise


async def fetch_all_products_grouped_by_store(
    enterprise_code: str,
    token: str,
    store_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SEC)
    grouped: dict[str, list[dict[str, Any]]] = {}

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for store_id in store_ids:
            try:
                grouped[store_id] = await fetch_products_for_store(
                    session,
                    enterprise_code,
                    token,
                    store_id,
                    min_stock=1,
                )
                logger.info(
                    "Bioteca store processed: enterprise_code=%s store_id=%s count=%s",
                    enterprise_code,
                    store_id,
                    len(grouped[store_id]),
                )
            except Exception as exc:
                logger.error(
                    "Bioteca store fetch failed: enterprise_code=%s store_id=%s error=%s",
                    enterprise_code,
                    store_id,
                    exc,
                )
    return grouped


def transform_catalog(products_by_store: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    deduplicated: dict[str, dict[str, Any]] = {}

    for products in products_by_store.values():
        for product in products:
            code = product.get("code")
            if not code:
                continue

            options = product.get("options") or {}
            name = options.get("name") or product.get("name") or ""

            if str(code) in deduplicated:
                continue

            deduplicated[str(code)] = {
                "code": str(code),
                "name": str(name),
                "vat": DEFAULT_VAT,
                "producer": _normalize_producer(product.get("sku")),
                "barcode": "" if product.get("barcode") is None else str(product.get("barcode")),
            }

    return list(deduplicated.values())


def transform_stock(
    products_by_store: dict[str, list[dict[str, Any]]],
    store_to_branch: dict[str, str],
    enterprise_code: str,
) -> list[dict[str, Any]]:
    stock_rows: list[dict[str, Any]] = []

    for store_id, products in products_by_store.items():
        branch = store_to_branch.get(store_id)
        if not branch:
            logger.warning(
                "Bioteca branch missing for store_id: enterprise_code=%s store_id=%s",
                enterprise_code,
                store_id,
            )
            continue

        for product in products:
            code = product.get("code")
            if not code:
                continue

            stock_map = product.get("stock") or {}
            qty_raw = 0
            if isinstance(stock_map, dict):
                qty_raw = stock_map.get(store_id, 0)

            try:
                qty = int(float(qty_raw or 0))
            except (TypeError, ValueError):
                qty = 0

            price_raw = product.get("price", 0)
            try:
                price = float(price_raw or 0)
            except (TypeError, ValueError):
                price = 0.0

            stock_rows.append(
                {
                    "branch": branch,
                    "code": str(code),
                    "price": price,
                    "qty": qty,
                    "price_reserve": price,
                }
            )

    return stock_rows


def save_to_json(data: list[dict[str, Any]], enterprise_code: str, file_type: str) -> str:
    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    os.makedirs(temp_dir, exist_ok=True)
    json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

    logger.info(
        "Bioteca JSON saved: enterprise_code=%s file_type=%s path=%s records=%s",
        enterprise_code,
        file_type,
        json_file_path,
        len(data),
    )
    return json_file_path


async def run_service(enterprise_code: str, file_type: str) -> None:
    logger.info("Bioteca service started: enterprise_code=%s file_type=%s", enterprise_code, file_type)

    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        logger.error("Bioteca settings not found: enterprise_code=%s", enterprise_code)
        raise ValueError(f"EnterpriseSettings not found for enterprise_code={enterprise_code}")

    token = (enterprise_settings.token or "").strip()
    if not token:
        logger.error("Bioteca token missing: enterprise_code=%s", enterprise_code)
        raise ValueError(f"Bioteca token is empty for enterprise_code={enterprise_code}")

    mappings = await fetch_mapping_branches(enterprise_code)
    if not mappings:
        logger.warning("Bioteca mapping_branch records not found: enterprise_code=%s", enterprise_code)
        return

    unique_store_ids = sorted({item["store_id"] for item in mappings if item.get("store_id")})
    logger.info(
        "Bioteca mappings loaded: enterprise_code=%s store_ids=%s",
        enterprise_code,
        len(unique_store_ids),
    )
    if not unique_store_ids:
        logger.warning("Bioteca no store_ids found: enterprise_code=%s", enterprise_code)
        return

    products_by_store = await fetch_all_products_grouped_by_store(enterprise_code, token, unique_store_ids)
    if not products_by_store:
        logger.warning("Bioteca no products fetched: enterprise_code=%s file_type=%s", enterprise_code, file_type)
        return

    if file_type == "catalog":
        transformed_data = transform_catalog(products_by_store)
        logger.info(
            "Bioteca catalog transformed: enterprise_code=%s records=%s",
            enterprise_code,
            len(transformed_data),
        )
    elif file_type == "stock":
        store_to_branch = {
            item["store_id"]: item["branch"]
            for item in mappings
            if item.get("store_id") and item.get("branch")
        }
        transformed_data = transform_stock(products_by_store, store_to_branch, enterprise_code)
        logger.info(
            "Bioteca stock transformed: enterprise_code=%s records=%s",
            enterprise_code,
            len(transformed_data),
        )
    else:
        raise ValueError("file_type must be 'catalog' or 'stock'")

    if not transformed_data:
        logger.warning(
            "Bioteca transformed payload is empty, skip database processing: enterprise_code=%s file_type=%s",
            enterprise_code,
            file_type,
        )
        return

    json_file_path = save_to_json(transformed_data, enterprise_code, file_type)
    await process_database_service(json_file_path, file_type, enterprise_code)
    logger.info("Bioteca service finished: enterprise_code=%s file_type=%s", enterprise_code, file_type)

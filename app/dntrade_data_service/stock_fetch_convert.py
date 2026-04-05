import asyncio
import os
import logging
from dotenv import load_dotenv
import traceback
from typing import Dict, List, Optional, Tuple
from time import perf_counter

import aiohttp
from sqlalchemy.future import select

from app.database import EnterpriseSettings, MappingBranch, get_async_db
from app.dntrade_data_service.client import DEFAULT_LIMIT, fetch_products_page
from app.dntrade_data_service.runtime import maybe_dump_raw_json, save_to_json
from app.services.database_service import process_database_service

# === Logging setup (non-intrusive: only if not already configured) ===
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
logger = logging.getLogger(__name__)

load_dotenv()

LIMIT = DEFAULT_LIMIT  # Лимит количества записей за один запрос
MAX_PAGES_PER_STORE = int(os.getenv("DNTRADE_STOCK_MAX_PAGES_PER_STORE", "2000"))
MAX_REPEAT_PAGES = int(os.getenv("DNTRADE_STOCK_MAX_REPEAT_PAGES", "3"))
logger.debug("Module loaded. LIMIT=%s", LIMIT)

async def fetch_enterprise_settings(enterprise_code, db):
    """Получение настроек предприятия по enterprise_code из EnterpriseSettings."""
    result = await db.execute(
        select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    )
    settings = result.scalars().first()
    if settings:
        logger.info("Enterprise settings found for enterprise_code=%s", enterprise_code)
    else:
        logger.error("Enterprise settings NOT found for enterprise_code=%s", enterprise_code)
    return settings

async def fetch_store_branch_map(enterprise_code, db) -> Dict[str, str]:
    """Получение словаря store_id -> branch из mapping_branch по enterprise_code."""
    result = await db.execute(
        select(MappingBranch.store_id, MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
    )
    rows = result.all()
    mapping = {
        str(store_id): str(branch)
        for store_id, branch in rows
        if store_id is not None and branch is not None
    }
    logger.info(
        "Fetched %d mapping rows (%d unique stores) for enterprise_code=%s",
        len(rows),
        len(mapping),
        enterprise_code,
    )
    return mapping


def transform_stock(products, store_to_branch: Dict[str, str]) -> Tuple[List[dict], Dict[str, int]]:
    """Трансформация данных продуктов в целевой формат для стока."""
    logger.info("Transform stock: incoming products=%d", len(products))
    transformed_by_key: Dict[Tuple[str, str], dict] = {}
    skipped_missing_product_id = 0
    missing_price_entries = 0
    missing_store_id = 0
    missing_branch = 0
    bad_price = 0
    balance_coerced_to_zero = 0
    duplicate_branch_code = 0
    duplicate_conflicting_values = 0
    duplicate_samples: List[str] = []

    for product in products:
        product_id = product.get("product_id")
        if not product_id:
            skipped_missing_product_id += 1
            continue
        # API может отдавать ключ как "prices"; оставляем обратную совместимость с опечаткой "pices"
        price_data = product.get("prices") or product.get("pices", [])
        if not price_data:
            missing_price_entries += 1
            logger.warning("No price entries for product_id=%s", product_id)
        balance = product.get("balance")

        # Обрабатываем balance
        try:
            balance = float(balance)
        except (TypeError, ValueError):
            balance = 0
            balance_coerced_to_zero += 1

        qty = max(balance, 0)

        for price_entry in price_data:
            price_title = (price_entry.get("price_title") or "").strip().lower()
            # Разрешаем несколько вариантов наименования розничной цены
            if price_title in {"роздрібна", "розничная", "retail"}:
                store_id = price_entry.get("store_id")
                if not store_id:
                    missing_store_id += 1
                    logger.warning("Missing store_id in price_entry for product_id=%s", product_id)
                    continue
                branch = store_to_branch.get(str(store_id))

                if branch:
                    try:
                        price_val = float(price_entry.get("price", 0))
                    except (TypeError, ValueError):
                        bad_price += 1
                        logger.warning("Bad price format for product_id=%s store_id=%s value=%r", product_id, store_id, price_entry.get("price"))
                        continue
                    record = {
                        "branch": branch,
                        "code": product_id,
                        "price": price_val,
                        "price_reserve": price_val,
                        "qty": qty,
                    }
                    key = (branch, str(product_id))
                    existing = transformed_by_key.get(key)
                    if existing is not None:
                        duplicate_branch_code += 1
                        if (
                            existing.get("price") != record["price"]
                            or existing.get("price_reserve") != record["price_reserve"]
                            or existing.get("qty") != record["qty"]
                        ):
                            duplicate_conflicting_values += 1
                            if len(duplicate_samples) < 10:
                                duplicate_samples.append(f"{branch}:{product_id}")
                        # Keep the latest record for the same (branch, code).
                    transformed_by_key[key] = record
                else:
                    missing_branch += 1
                    logger.warning("Branch not found for store_id=%s (product_id=%s)", store_id, product_id)
            else:
                # Сбор статистики встречающихся названий цен для анализа
                if price_title:
                    logger.debug("Non-retail price skipped: title=%s product_id=%s", price_title, product_id)

    if duplicate_branch_code:
        logger.warning(
            "Dntrade stock duplicate (branch, code) detected: total_duplicates=%d conflicting=%d samples=%s",
            duplicate_branch_code,
            duplicate_conflicting_values,
            duplicate_samples,
        )

    transformed = list(transformed_by_key.values())
    stats = {
        "skipped_missing_product_id": skipped_missing_product_id,
        "missing_price_entries": missing_price_entries,
        "missing_store_id": missing_store_id,
        "missing_branch": missing_branch,
        "bad_price": bad_price,
        "balance_coerced_to_zero": balance_coerced_to_zero,
        "duplicate_branch_code": duplicate_branch_code,
        "duplicate_conflicting_values": duplicate_conflicting_values,
    }
    logger.info("Transform stock finished: produced %d records", len(transformed))
    return transformed, stats

async def run_service(enterprise_code, file_type):
    """Основной сервис выполнения задачи."""
    started = perf_counter()
    logger.info("Run service started: enterprise_code=%s file_type=%s", enterprise_code, file_type)
    try:
        metadata_started = perf_counter()
        logger.info("Dntrade stock: opening read-only session for metadata enterprise_code=%s", enterprise_code)
        async with get_async_db(commit_on_exit=False) as db:
            enterprise_settings = await fetch_enterprise_settings(enterprise_code, db)
            if not enterprise_settings:
                logger.error("Stop: enterprise settings missing for enterprise_code=%s", enterprise_code)
                return

            api_key = enterprise_settings.token
            if not api_key:
                logger.error("Stop: api_key missing for enterprise_code=%s", enterprise_code)
                return

            store_to_branch = await fetch_store_branch_map(enterprise_code, db)
            store_ids = sorted(store_to_branch.keys())
            if not store_ids:
                logger.warning("No store_ids for enterprise_code=%s", enterprise_code)
                return  # Нет store_id для данного предприятия

        logger.info(
            "Dntrade stock: metadata loaded enterprise_code=%s stores=%d branches=%d elapsed=%.3fs",
            enterprise_code,
            len(store_ids),
            len(set(store_to_branch.values())),
            perf_counter() - metadata_started,
        )

        all_products = []
        total_pages_fetched = 0
        store_failures = 0

        fetch_started = perf_counter()
        async with aiohttp.ClientSession() as session:
            # Цикл по каждому store_id
            for store_id in store_ids:
                offset = 0
                pages_fetched = 0
                repeated_page_count = 0
                last_fingerprint: Optional[Tuple[int, str, str]] = None
                logger.info("Fetching products for store_id=%s", store_id)

                while True:
                    if pages_fetched >= MAX_PAGES_PER_STORE:
                        logger.warning(
                            "Dntrade stock stop for store_id=%s: max pages reached (%s) enterprise_code=%s",
                            store_id,
                            MAX_PAGES_PER_STORE,
                            enterprise_code,
                        )
                        break

                    response = await fetch_products_page(
                        session=session,
                        api_key=api_key,
                        store_id=store_id,
                        offset=offset,
                        limit=LIMIT,
                    )

                    if response is None:
                        store_failures += 1
                        logger.warning(
                            "Breaking on store_id=%s offset=%s due to API error/None response",
                            store_id,
                            offset,
                        )
                        break  # Ошибка API - останавливаем обработку этого store_id

                    products = response.get("products", [])
                    logger.debug("Received %d products for store_id=%s offset=%s", len(products), store_id, offset)
                    if not products:
                        logger.info("No more products for store_id=%s at offset=%s", store_id, offset)
                        break  # Если список `products` пустой, прекращаем цикл для store_id

                    first_id = str(products[0].get("product_id", ""))
                    last_id = str(products[-1].get("product_id", ""))
                    current_fingerprint = (len(products), first_id, last_id)
                    if current_fingerprint == last_fingerprint:
                        repeated_page_count += 1
                        if repeated_page_count >= MAX_REPEAT_PAGES:
                            logger.warning(
                                "Dntrade stock stop for store_id=%s: repeating page detected %s times at offset=%s enterprise_code=%s",
                                store_id,
                                repeated_page_count,
                                offset,
                                enterprise_code,
                            )
                            break
                    else:
                        repeated_page_count = 0
                    last_fingerprint = current_fingerprint

                    all_products.extend(products)
                    offset += len(products)
                    pages_fetched += 1
                    total_pages_fetched += 1

                logger.info(
                    "Dntrade stock store fetch finished: enterprise_code=%s store_id=%s pages=%d accumulated_products=%d",
                    enterprise_code,
                    store_id,
                    pages_fetched,
                    len(all_products),
                )

        logger.info(
            "Dntrade stock: fetch finished enterprise_code=%s stores=%d products=%d pages=%d store_failures=%d elapsed=%.3fs",
            enterprise_code,
            len(store_ids),
            len(all_products),
            total_pages_fetched,
            store_failures,
            perf_counter() - fetch_started,
        )

        if not all_products:
            logger.warning("No products collected. Nothing to save.")
            return  # Нет данных для сохранения

        maybe_dump_raw_json(all_products, enterprise_code, "stock", label="raw_input")

        transformed_data, transform_stats = transform_stock(all_products, store_to_branch)
        file_type = "stock"
        json_file_path = save_to_json(transformed_data, enterprise_code, file_type)
        if not json_file_path:
            logger.error("Stop: failed to save JSON for enterprise_code=%s", enterprise_code)
            return  # Ошибка сохранения JSON

        logger.info("Sending data to database_service: file=%s enterprise_code=%s type=%s", json_file_path, enterprise_code, file_type)
        await process_database_service(json_file_path, file_type, enterprise_code)
        logger.info(
            "Run service finished successfully: enterprise_code=%s records=%d "
            "skipped_missing_product_id=%s missing_price_entries=%s missing_store_id=%s "
            "missing_branch=%s bad_price=%s balance_coerced_to_zero=%s duplicate_branch_code=%s "
            "duplicate_conflicting_values=%s elapsed=%.3fs",
            enterprise_code,
            len(transformed_data),
            transform_stats["skipped_missing_product_id"],
            transform_stats["missing_price_entries"],
            transform_stats["missing_store_id"],
            transform_stats["missing_branch"],
            transform_stats["bad_price"],
            transform_stats["balance_coerced_to_zero"],
            transform_stats["duplicate_branch_code"],
            transform_stats["duplicate_conflicting_values"],
            perf_counter() - started,
        )
    except Exception:
        logger.error("Run service failed: enterprise_code=%s file_type=%s", enterprise_code, file_type)
        logger.debug(traceback.format_exc())
        raise
    

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "238"
    logger.info("__main__ start: enterprise_code=%s", TEST_ENTERPRISE_CODE)
    asyncio.run(run_service(TEST_ENTERPRISE_CODE, "stock"))
    logger.info("__main__ finished")

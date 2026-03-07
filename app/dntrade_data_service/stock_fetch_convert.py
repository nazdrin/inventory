import json
import asyncio
import tempfile
import os
import logging
from dotenv import load_dotenv
import traceback
from typing import Dict, List

import aiohttp
from sqlalchemy.future import select

from app.database import EnterpriseSettings, MappingBranch, get_async_db
from app.dntrade_data_service.client import DEFAULT_LIMIT, fetch_products_page
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


def transform_stock(products, store_to_branch: Dict[str, str]) -> List[dict]:
    """Трансформация данных продуктов в целевой формат для стока."""
    logger.info("Transform stock: incoming products=%d", len(products))
    transformed = []

    for product in products:
        product_id = product.get("product_id")
        if not product_id:
            continue
        # API может отдавать ключ как "prices"; оставляем обратную совместимость с опечаткой "pices"
        price_data = product.get("prices") or product.get("pices", [])
        if not price_data:
            logger.warning("No price entries for product_id=%s", product_id)
        balance = product.get("balance")

        # Обрабатываем balance
        try:
            balance = float(balance)
        except (TypeError, ValueError):
            balance = 0

        qty = max(balance, 0)

        for price_entry in price_data:
            price_title = (price_entry.get("price_title") or "").strip().lower()
            # Разрешаем несколько вариантов наименования розничной цены
            if price_title in {"роздрібна", "розничная", "retail"}:
                store_id = price_entry.get("store_id")
                if not store_id:
                    logger.warning("Missing store_id in price_entry for product_id=%s", product_id)
                    continue
                branch = store_to_branch.get(str(store_id))

                if branch:
                    try:
                        price_val = float(price_entry.get("price", 0))
                    except (TypeError, ValueError):
                        logger.warning("Bad price format for product_id=%s store_id=%s value=%r", product_id, store_id, price_entry.get("price"))
                        continue
                    transformed.append({
                        "branch": branch,
                        "code": product_id,
                        "price": price_val,
                        "price_reserve": price_val,
                        "qty": qty,
                    })
                else:
                    logger.warning("Branch not found for store_id=%s (product_id=%s)", store_id, product_id)
            else:
                # Сбор статистики встречающихся названий цен для анализа
                if price_title:
                    logger.debug("Non-retail price skipped: title=%s product_id=%s", price_title, product_id)

    logger.info("Transform stock finished: produced %d records", len(transformed))
    return transformed

def save_to_json(data, enterprise_code, file_type):
    """Сохранение данных в JSON-файл в указанную временную директорию из .env."""
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)

        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        try:
            file_size = os.path.getsize(json_file_path)
        except OSError:
            file_size = -1

        logger.info("JSON записан: path=%s size=%s bytes", json_file_path, file_size)
        return json_file_path
    except IOError as e:
        logger.error(f"Ошибка при сохранении JSON-файла: {e}")
        return None

async def run_service(enterprise_code, file_type):
    """Основной сервис выполнения задачи."""
    logger.info("Run service started: enterprise_code=%s file_type=%s", enterprise_code, file_type)
    try:
        async with get_async_db() as db:  # ОДНА сессия для всех запросов
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

            all_products = []

            async with aiohttp.ClientSession() as session:
                # Цикл по каждому store_id
                for store_id in store_ids:
                    offset = 0
                    logger.info("Fetching products for store_id=%s", store_id)

                    while True:
                        response = await fetch_products_page(
                            session=session,
                            api_key=api_key,
                            store_id=store_id,
                            offset=offset,
                            limit=LIMIT,
                        )

                        if response is None:
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

                        all_products.extend(products)
                        offset += LIMIT

            if not all_products:
                logger.warning("No products collected. Nothing to save.")
                return  # Нет данных для сохранения

            transformed_data = transform_stock(all_products, store_to_branch)
            file_type = "stock"
            json_file_path = save_to_json(transformed_data, enterprise_code, file_type)
            if not json_file_path:
                logger.error("Stop: failed to save JSON for enterprise_code=%s", enterprise_code)
                return  # Ошибка сохранения JSON

            logger.info("Sending data to database_service: file=%s enterprise_code=%s type=%s", json_file_path, enterprise_code, file_type)
            await process_database_service(json_file_path, file_type, enterprise_code)
            logger.info("Run service finished successfully: enterprise_code=%s records=%d", enterprise_code, len(transformed_data))
    except Exception:
        logger.error("Run service failed: enterprise_code=%s file_type=%s", enterprise_code, file_type)
        logger.debug(traceback.format_exc())
        raise
    

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "238"
    logger.info("__main__ start: enterprise_code=%s", TEST_ENTERPRISE_CODE)
    asyncio.run(run_service(TEST_ENTERPRISE_CODE, "stock"))
    logger.info("__main__ finished")

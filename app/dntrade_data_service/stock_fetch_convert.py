import sys
sys.path.append('C:/Users/nazdr/Documents/inventory')
import requests
import json
import asyncio
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings, MappingBranch
from app.services.database_service import process_database_service
from sqlalchemy.future import select
import tempfile
import os
import logging
from dotenv import load_dotenv
import traceback

# === Logging setup (non-intrusive: only if not already configured) ===
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
logger = logging.getLogger(__name__)

load_dotenv()

LIMIT = 100  # Лимит количества записей за один запрос
logger.debug("Module loaded. LIMIT=%s", LIMIT)

def log_progress(offset, count):
    """Логирование процесса обновляемой строкой в консоли"""
    sys.stdout.write(f"\rЗапрос: offset={offset} | Получено: {count} записей")
    sys.stdout.flush()
    logging.debug("Progress: offset=%s, received=%s", offset, count)

async def fetch_developer_settings(db):
    """Получение API_ENDPOINT из DeveloperSettings."""
    result = await db.execute(select(DeveloperSettings))
    settings = result.scalars().first()
    if settings:
        logger.info("Developer settings fetched successfully")
    else:
        logger.error("Developer settings not found")
    return settings

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

async def fetch_store_ids(enterprise_code, db):
    """Получение списка store_id из mapping_branch по enterprise_code."""
    result = await db.execute(
        select(MappingBranch.store_id).where(MappingBranch.enterprise_code == enterprise_code)
    )
    store_ids = result.scalars().all()
    logger.info("Fetched %d store_id(s) for enterprise_code=%s", len(store_ids), enterprise_code)
    return store_ids

async def fetch_branch_by_store_id(store_id, db):
    """Получение branch из mapping_branch по store_id."""
    result = await db.execute(
        select(MappingBranch.branch).where(MappingBranch.store_id == store_id)
    )
    branch = result.scalars().first()
    if branch:
        logger.debug("Resolved branch %s for store_id=%s", branch, store_id)
    else:
        logger.warning("Branch not found for store_id=%s", store_id)
    return branch

def fetch_products(api_endpoint, api_key, store_id, offset=0, limit=LIMIT):
    """Запрос данных продуктов через API для конкретного store_id с корректным форматом запроса."""
    headers = {
        "ApiKey": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    url = f"{api_endpoint}?limit={limit}&offset={offset}&store_id={store_id}"
    logger.debug("Requesting products: store_id=%s offset=%s limit=%s url=%s", store_id, offset, limit, url)

    try:
        response = requests.post(url, headers=headers, json={})
        if response.status_code != 200:
            logger.error("API error: status=%s store_id=%s offset=%s response=%s", response.status_code, store_id, offset, response.text[:500])
            return None  # Ошибка запроса

        data = response.json()
        if not isinstance(data, dict):
            logger.error("Unexpected API payload type: %s", type(data))
            return None
        logger.debug("API ok: store_id=%s offset=%s keys=%s", store_id, offset, list(data.keys()))
        return data
    except requests.RequestException as e:
        logger.exception("RequestException while fetching products: store_id=%s offset=%s error=%s", store_id, offset, e)
        return None
    except Exception:
        logger.exception("Unexpected error while fetching products: store_id=%s offset=%s", store_id, offset)
        return None

async def transform_stock(products, db):
    """Трансформация данных продуктов в целевой формат для стока."""
    logger.info("Transform stock: incoming products=%d", len(products))
    transformed = []
    log_filename = "stock_logs.json"

    for product in products:
        product_id = product.get("product_id")
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
                branch = await fetch_branch_by_store_id(store_id, db)  # Передаем db

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

    # Запись логов
    try:
        with open(log_filename, "w", encoding="utf-8") as log_file:
            json.dump(transformed, log_file, ensure_ascii=False, indent=4)
    except IOError as e:
        logger.exception("Ошибка записи лога: %s", e)
    
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
            developer_settings = await fetch_developer_settings(db)
            if not developer_settings:
                logger.error("Stop: developer settings missing")
                return

            api_endpoint = developer_settings.telegram_token_developer
            logger.debug("API endpoint from developer settings: %s", api_endpoint)

            enterprise_settings = await fetch_enterprise_settings(enterprise_code, db)
            if not enterprise_settings:
                logger.error("Stop: enterprise settings missing for enterprise_code=%s", enterprise_code)
                return

            api_key = enterprise_settings.token
            if not api_key:
                logger.error("Stop: api_key missing for enterprise_code=%s", enterprise_code)
                return

            store_ids = await fetch_store_ids(enterprise_code, db)
            if not store_ids:
                logger.warning("No store_ids for enterprise_code=%s", enterprise_code)
                return  # Нет store_id для данного предприятия

            all_products = []

            # Цикл по каждому store_id
            for store_id in store_ids:
                offset = 0
                logger.info("Fetching products for store_id=%s", store_id)

                while True:
                    response = fetch_products(api_endpoint, api_key, store_id, offset=offset, limit=LIMIT)

                    if response is None:
                        logger.warning("Breaking on store_id=%s offset=%s due to API error/None response", store_id, offset)
                        break  # Ошибка API - останавливаем обработку этого store_id

                    products = response.get("products", [])
                    logger.debug("Received %d products for store_id=%s offset=%s", len(products), store_id, offset)
                    if not products:
                        logger.info("No more products for store_id=%s at offset=%s", store_id, offset)
                        break  # Если список `products` пустой, прекращаем цикл для store_id

                    all_products.extend(products)
                    log_progress(offset, len(products))
                    offset += LIMIT

            if not all_products:
                logger.warning("No products collected. Nothing to save.")
                return  # Нет данных для сохранения

            transformed_data = await transform_stock(all_products, db)  # Передаем db в transform_stock
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
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))
    logger.info("__main__ finished")

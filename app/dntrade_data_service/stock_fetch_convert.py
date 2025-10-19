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

load_dotenv()

LIMIT = 100  # Лимит количества записей за один запрос

def log_progress(offset, count):
    """Логирование процесса обновляемой строкой в консоли"""
    sys.stdout.write(f"\rЗапрос: offset={offset} | Получено: {count} записей")
    sys.stdout.flush()

async def fetch_developer_settings(db):
    """Получение API_ENDPOINT из DeveloperSettings."""
    result = await db.execute(select(DeveloperSettings))
    return result.scalars().first()

async def fetch_enterprise_settings(enterprise_code, db):
    """Получение настроек предприятия по enterprise_code из EnterpriseSettings."""
    result = await db.execute(
        select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    )
    return result.scalars().first()

async def fetch_store_ids(enterprise_code, db):
    """Получение списка store_id из mapping_branch по enterprise_code."""
    result = await db.execute(
        select(MappingBranch.store_id).where(MappingBranch.enterprise_code == enterprise_code)
    )
    return result.scalars().all()

async def fetch_branch_by_store_id(store_id, db):
    """Получение branch из mapping_branch по store_id."""
    result = await db.execute(
        select(MappingBranch.branch).where(MappingBranch.store_id == store_id)
    )
    return result.scalars().first()

def fetch_products(api_endpoint, api_key, store_id, offset=0, limit=LIMIT):
    """Запрос данных продуктов через API для конкретного store_id с корректным форматом запроса."""
    headers = {
        "ApiKey": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    url = f"{api_endpoint}?limit={limit}&offset={offset}&store_id={store_id}"

    try:
        response = requests.post(url, headers=headers, json={})
        if response.status_code != 200:
            return None  # Ошибка запроса

        data = response.json()
        return data if isinstance(data, dict) else None
    except requests.RequestException:
        return None

async def transform_stock(products, db):
    """Трансформация данных продуктов в целевой формат для стока."""
    transformed = []
    log_filename = "stock_logs.json"

    for product in products:
        product_id = product.get("product_id")
        price_data = product.get("pices", [])
        balance = product.get("balance")

        # Обрабатываем balance
        try:
            balance = float(balance)
        except (TypeError, ValueError):
            balance = 0

        qty = max(balance, 0)

        for price_entry in price_data:
            if price_entry.get("price_title") == "Роздрібна":
                store_id = price_entry.get("store_id")
                branch = await fetch_branch_by_store_id(store_id, db)  # Передаем db

                if branch:
                    transformed.append({
                        "branch": branch,
                        "code": product_id,
                        "price": float(price_entry.get("price", 0)),
                        "price_reserve": float(price_entry.get("price", 0)),
                        "qty": qty,
                    })

    # Запись логов
    try:
        with open(log_filename, "w", encoding="utf-8") as log_file:
            json.dump(transformed, log_file, ensure_ascii=False, indent=4)
    except IOError as e:
        print(f"Ошибка записи лога: {e}")
    
    return transformed

def save_to_json(data, enterprise_code, file_type):
    """Сохранение данных в JSON-файл в указанную временную директорию из .env."""
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)

        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logging.info(f"JSON записан в файл: {json_file_path}")
        return json_file_path
    except IOError as e:
        logging.error(f"Ошибка при сохранении JSON-файла: {e}")
        return None

async def run_service(enterprise_code, file_type):
    """Основной сервис выполнения задачи."""
    async with get_async_db() as db:  # ОДНА сессия для всех запросов
        developer_settings = await fetch_developer_settings(db)
        if not developer_settings:
            return

        api_endpoint = developer_settings.telegram_token_developer

        enterprise_settings = await fetch_enterprise_settings(enterprise_code, db)
        if not enterprise_settings:
            return

        api_key = enterprise_settings.token
        if not api_key:
            return

        store_ids = await fetch_store_ids(enterprise_code, db)
        if not store_ids:
            return  # Нет store_id для данного предприятия

        all_products = []

        # Цикл по каждому store_id
        for store_id in store_ids:
            offset = 0

            while True:
                response = fetch_products(api_endpoint, api_key, store_id, offset=offset, limit=LIMIT)

                if response is None:
                    break  # Ошибка API - останавливаем обработку этого store_id

                products = response.get("products", [])
                if not products:
                    break  # Если список `products` пустой, прекращаем цикл для store_id

                all_products.extend(products)
                log_progress(offset, len(products))
                offset += LIMIT

        if not all_products:
            return  # Нет данных для сохранения

        transformed_data = await transform_stock(all_products, db)  # Передаем db в transform_stock
        file_type = "stock"
        json_file_path = save_to_json(transformed_data, enterprise_code, file_type)
        if not json_file_path:
            return  # Ошибка сохранения JSON

        await process_database_service(json_file_path, file_type, enterprise_code)
    

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "238"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

import sys
import requests
import json
import asyncio
from app.database import get_async_db, EnterpriseSettings, MappingBranch
from app.services.database_service import process_database_service
from sqlalchemy.future import select
import tempfile
import os
import logging
from dotenv import load_dotenv
load_dotenv()

API_URL = "https://openapi.keycrm.app/v1/products"
REQUEST_LIMIT_PER_MINUTE = 60


def log_progress(page, count):
    sys.stdout.write(f"\rЗапрос: page={page} | Получено: {count} записей")
    sys.stdout.flush()


async def fetch_enterprise_settings(enterprise_code):
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()


async def fetch_branch_id(enterprise_code): 
    """Получение branch из таблицы MappingBranch.""" 
    async with get_async_db() as session: 
        result = await session.execute( 
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code) 
        ) 
        mapping = result.scalars().first() 
        return mapping if mapping else "unknown"



def fetch_all_products(api_key):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    all_products = []
    page = 1

    while True:
        params = {
            "limit": 15,
            "page": page
        }

        response = requests.get(API_URL, headers=headers, params=params)

        if response.status_code != 200:
            logging.error(f"Ошибка при запросе страницы {page}: {response.status_code}")
            break

        json_data = response.json()
        products = json_data.get("data", [])
        if not products:
            break

        all_products.extend(products)
        log_progress(page, len(products))

        if not json_data.get("next_page_url"):
            break

        page += 1
        asyncio.sleep(1)  # контроль лимита API

    print(f"\nВсего получено: {len(all_products)} записей")
    return all_products


def transform_stock_data(products, branch_id):
    transformed = []
    for item in products:
        quantity = item.get("quantity", 0)
        if quantity <= 0:
            continue

        transformed.append({
            "branch": branch_id,
            "code": str(item.get("id")),
            "price": float(item.get("max_price", 0)),
            "qty": int(quantity),
            "price_reserve": float(item.get("max_price", 0)),
        })
    return transformed


def save_to_tempfile(data):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode='w', encoding='utf-8') as temp_file:
            json.dump(data, temp_file, ensure_ascii=False, indent=4)
            return temp_file.name
    except IOError as e:
        logging.error(f"Ошибка при сохранении временного файла: {e}")
        return None


async def run_service(enterprise_code):
    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        print("Настройки предприятия не найдены.")
        return

    api_key = enterprise_settings.token
    if not api_key:
        print("API ключ не найден.")
        return

    try:
        branch_id = await fetch_branch_id(enterprise_code)
    except ValueError as e:
        print(str(e))
        return

    all_products = fetch_all_products(api_key)
    if not all_products:
        print("Данные не получены.")
        return

    transformed_data = transform_stock_data(all_products, branch_id)
    if not transformed_data:
        print("Нет данных для сохранения после фильтрации.")
        return

    json_file_path = save_to_tempfile(transformed_data)
    if not json_file_path:
        print("Ошибка сохранения JSON-файла.")
        return

    await process_database_service(json_file_path, "stock", enterprise_code)


if __name__ == "__main__":
    enterprise_code = "272"
    asyncio.run(run_service(enterprise_code))

import sys
import requests
import json
import asyncio
from app.database import get_async_db, EnterpriseSettings
from app.services.database_service import process_database_service
from sqlalchemy.future import select
import tempfile
import os
import logging
from dotenv import load_dotenv
load_dotenv()

DEFAULT_VAT = 20
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

        # Проверка на конец страниц
        if not json_data.get("next_page_url"):
            break

        page += 1
        asyncio.sleep(1)  # контроль лимита по API

    print(f"\nВсего получено: {len(all_products)} записей")
    return all_products


def transform_products(products):
    transformed = []
    for item in products:
        transformed.append({
            "code": str(item.get("id")),
            "name": item.get("name"),
            "vat": DEFAULT_VAT,
            "producer": "",
            "barcode": item.get("barcode", "")
        })
    return transformed


def save_to_json(data, enterprise_code, file_type):
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logging.info(f"JSON сохранен в файл: {json_file_path}")
        return json_file_path
    except IOError as e:
        logging.error(f"Ошибка при сохранении JSON-файла: {e}")
        return None
    
def save_raw_input(data, enterprise_code):
    try:
        raw_dir = os.path.join(os.getcwd(), "input_raw", str(enterprise_code))
        os.makedirs(raw_dir, exist_ok=True)
        file_path = os.path.join(raw_dir, "raw_catalog.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Исходные данные сохранены в файл: {file_path}")
        return file_path
    except IOError as e:
        logging.error(f"Ошибка при сохранении исходного файла: {e}")
        return None



async def run_service(enterprise_code, file_type):
    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        print("Настройки предприятия не найдены.")
        return

    api_key = enterprise_settings.token
    if not api_key:
        print("API ключ не найден.у")
        return

    all_products = fetch_all_products(api_key)
    if not all_products:
        print("Данные не получены.")
        return
    
    # Сохраняем исходные (входящие) данные
    save_raw_input(all_products, enterprise_code)
    

    transformed_data = transform_products(all_products)
    file_type = "catalog"
    json_file_path = save_to_json(transformed_data, enterprise_code, file_type)

    if not json_file_path:
        print("Ошибка сохранения JSON-файла.")
        return

    await process_database_service(json_file_path, file_type, enterprise_code)


if __name__ == "__main__":
    enterprise_code = "2"  
    asyncio.run(run_service(enterprise_code))

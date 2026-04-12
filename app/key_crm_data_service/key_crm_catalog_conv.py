import sys
import requests
import json
import asyncio
import time
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
PAGE_LIMIT = 15
REQUEST_TIMEOUT_SEC = 30
REQUEST_DELAY_SEC = 60 / REQUEST_LIMIT_PER_MINUTE if REQUEST_LIMIT_PER_MINUTE > 0 else 0


def log_progress(page, count):
    sys.stdout.write(f"\rЗапрос: page={page} | Получено: {count} записей")
    sys.stdout.flush()


class KeyCRMFetchError(RuntimeError):
    pass

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
    pages_fetched = 0
    started = time.perf_counter()

    while True:
        params = {
            "limit": PAGE_LIMIT,
            "page": page
        }

        response = requests.get(API_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SEC)

        if response.status_code != 200:
            raise KeyCRMFetchError(
                f"KeyCRM catalog request failed: page={page} status={response.status_code}"
            )

        json_data = response.json()
        products = json_data.get("data", [])
        if not products:
            break

        pages_fetched += 1
        all_products.extend(products)
        log_progress(page, len(products))

        # Проверка на конец страниц
        if not json_data.get("next_page_url"):
            break

        page += 1
        if REQUEST_DELAY_SEC > 0:
            time.sleep(REQUEST_DELAY_SEC)

    print(f"\nВсего получено: {len(all_products)} записей")
    elapsed = time.perf_counter() - started
    logging.info(
        "KeyCRM catalog fetch summary: pages=%s fetched=%s page_limit=%s elapsed=%.3fs",
        pages_fetched,
        len(all_products),
        PAGE_LIMIT,
        elapsed,
    )
    return all_products, {
        "pages_fetched": pages_fetched,
        "fetched_records": len(all_products),
        "elapsed": elapsed,
    }


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
    logging.info(
        "KeyCRM catalog transform summary: incoming=%s transformed=%s",
        len(products),
        len(transformed),
    )
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
        # TODO(runtime): replace cwd-dependent path with BASE_DIR-based path
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
    started = time.perf_counter()
    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        print("Настройки предприятия не найдены.")
        return

    api_key = enterprise_settings.token
    if not api_key:
        print("API ключ не найден.у")
        return

    all_products, fetch_summary = fetch_all_products(api_key)
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

    logging.info(
        "KeyCRM catalog run summary: enterprise_code=%s pages=%s fetched=%s transformed=%s elapsed=%.3fs",
        enterprise_code,
        fetch_summary["pages_fetched"],
        fetch_summary["fetched_records"],
        len(transformed_data),
        time.perf_counter() - started,
    )
    await process_database_service(json_file_path, file_type, enterprise_code)


if __name__ == "__main__":
    enterprise_code = "2"  
    asyncio.run(run_service(enterprise_code))

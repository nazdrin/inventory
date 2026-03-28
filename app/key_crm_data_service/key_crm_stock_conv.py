import sys
import requests
import json
import asyncio
import time
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


async def fetch_branch_id(enterprise_code): 
    """Получение branch из таблицы MappingBranch.""" 
    async with get_async_db() as session: 
        result = await session.execute( 
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code) 
        ) 
        mapping = result.scalars().first() 
        if mapping:
            return mapping
        raise ValueError(
            f"KeyCRM stock misconfiguration: branch mapping not found for enterprise_code={enterprise_code}"
        )



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
                f"KeyCRM stock request failed: page={page} status={response.status_code}"
            )

        json_data = response.json()
        products = json_data.get("data", [])
        if not products:
            break

        pages_fetched += 1
        all_products.extend(products)
        log_progress(page, len(products))

        if not json_data.get("next_page_url"):
            break

        page += 1
        if REQUEST_DELAY_SEC > 0:
            time.sleep(REQUEST_DELAY_SEC)

    print(f"\nВсего получено: {len(all_products)} записей")
    elapsed = time.perf_counter() - started
    logging.info(
        "KeyCRM stock fetch summary: pages=%s fetched=%s page_limit=%s elapsed=%.3fs",
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

def save_raw_input(data, enterprise_code):
    try:
        raw_dir = os.path.join(os.getcwd(), "input_raw", str(enterprise_code))
        os.makedirs(raw_dir, exist_ok=True)
        file_path = os.path.join(raw_dir, "raw_stock.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Исходные данные сохранены в файл: {file_path}")
        return file_path
    except IOError as e:
        logging.error(f"Ошибка при сохранении исходного файла: {e}")
        return None


def transform_stock_data(products, branch_id):
    transformed = []
    skipped_non_positive_qty = 0
    for item in products:
        quantity = item.get("quantity", 0)
        if quantity <= 0:
            skipped_non_positive_qty += 1
            continue

        transformed.append({
            "branch": branch_id,
            "code": str(item.get("id")),
            "price": float(item.get("max_price", 0)),
            "qty": int(quantity),
            "price_reserve": float(item.get("max_price", 0)),
        })
    logging.info(
        "KeyCRM stock transform summary: incoming=%s transformed=%s skipped_non_positive_qty=%s branch=%s",
        len(products),
        len(transformed),
        skipped_non_positive_qty,
        branch_id,
    )
    return transformed, skipped_non_positive_qty


def save_to_tempfile(data):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode='w', encoding='utf-8') as temp_file:
            json.dump(data, temp_file, ensure_ascii=False, indent=4)
            return temp_file.name
    except IOError as e:
        logging.error(f"Ошибка при сохранении временного файла: {e}")
        return None


async def run_service(enterprise_code, file_type):
    started = time.perf_counter()
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

    all_products, fetch_summary = fetch_all_products(api_key)
    if not all_products:
        print("Данные не получены.")
        return

    # ✅ Сохраняем входящий файл до фильтрации
    save_raw_input(all_products, enterprise_code)

    transformed_data, skipped_non_positive_qty = transform_stock_data(all_products, branch_id)
    if not transformed_data:
        print("Нет данных для сохранения после фильтрации.")
        return

    json_file_path = save_to_tempfile(transformed_data)
    if not json_file_path:
        print("Ошибка сохранения JSON-файла.")
        return

    logging.info(
        "KeyCRM stock run summary: enterprise_code=%s pages=%s fetched=%s transformed=%s skipped_non_positive_qty=%s elapsed=%.3fs",
        enterprise_code,
        fetch_summary["pages_fetched"],
        fetch_summary["fetched_records"],
        len(transformed_data),
        skipped_non_positive_qty,
        time.perf_counter() - started,
    )
    await process_database_service(json_file_path, "stock", enterprise_code)


if __name__ == "__main__":
    enterprise_code = "272"
    asyncio.run(run_service(enterprise_code))

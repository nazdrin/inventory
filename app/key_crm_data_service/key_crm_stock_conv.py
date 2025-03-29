import sys
import requests
import json
import asyncio
import os
import logging
from app.database import get_async_db, EnterpriseSettings
from app.models import MappingBranch
from app.services.database_service import process_database_service
from sqlalchemy.future import select
from dotenv import load_dotenv
load_dotenv()

API_URL = "https://openapi.keycrm.app/v1/offers/stocks"
DEFAULT_FILE_TYPE = "stock"


def log_progress(page, count):
    sys.stdout.write(f"\rЗапрос: page={page} | Получено: {count} записей")
    sys.stdout.flush()


async def fetch_enterprise_settings(enterprise_code):
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()


async def fetch_branch_mapping():
    async with get_async_db() as session:
        result = await session.execute(select(MappingBranch))
        return {str(row.store_id): str(row.branch) for row in result.scalars().all()}


def fetch_all_stock(api_key):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    all_items = []
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
        items = json_data.get("data", [])
        if not items:
            break

        all_items.extend(items)
        log_progress(page, len(items))

        if not json_data.get("next_page_url"):
            break

        page += 1

    print(f"\nВсего получено: {len(all_items)} записей")
    return all_items


def transform_stock(data, branch_mapping, fallback_branch):
    result = []
    for item in data:
        item_id = str(item.get("id"))
        price = item.get("price")
        quantity = item.get("quantity", 0)
        reserve = item.get("reserve", 0)
        warehouses = item.get("warehouse")

        if warehouses:
            for w in warehouses:
                store_id = str(w.get("id"))
                branch = branch_mapping.get(store_id)
                if not branch:
                    continue

                qty = w.get("quantity", 0) - w.get("reserve", 0)
                result.append({
                    "branch": branch,
                    "code": item_id,
                    "price": price,
                    "qty": qty,
                    "price_reserve": price
                })
        elif quantity is not None:
            if fallback_branch:
                qty = quantity - reserve
                result.append({
                    "branch": fallback_branch,
                    "code": item_id,
                    "price": price,
                    "qty": qty,
                    "price_reserve": price
                })
        else:
            if fallback_branch:
                result.append({
                    "branch": fallback_branch,
                    "code": item_id,
                    "price": price,
                    "qty": 0,
                    "price_reserve": price
                })
    return result


async def fetch_branch_by_enterprise_code(enterprise_code):
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        record = result.scalars().first()
        return str(record.branch) if record else None


def save_to_json(data, enterprise_code, file_type):
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", os.path.join(os.getcwd(), "temp"))
        dir_path = os.path.join(temp_dir, str(enterprise_code))
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, f"{file_type}.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"JSON сохранен в файл: {file_path}")
        return file_path
    except IOError as e:
        logging.error(f"Ошибка сохранения файла: {e}")
        return None


async def run_service(enterprise_code):

    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        print("Не найдены настройки предприятия.")
        return

    api_key = enterprise_settings.token
    if not api_key:
        print("Не найден API ключ.")
        return

    branch_mapping = await fetch_branch_mapping()
    if not branch_mapping:
        print("Не найдены соответствия branch → store_id.")
        return

    # fallback branch по enterprise_code
    # fallback_branch = branch_mapping.get(str(enterprise_code))
    fallback_branch = await fetch_branch_by_enterprise_code(enterprise_code)


    stock_data = fetch_all_stock(api_key)
    if not stock_data:
        print("Данные по складам не получены.")
        return

    transformed = transform_stock(stock_data, branch_mapping, fallback_branch)
    file_path = save_to_json(transformed, enterprise_code, DEFAULT_FILE_TYPE)

    if not file_path:
        print("Не удалось сохранить файл.")
        return

    await process_database_service(file_path, DEFAULT_FILE_TYPE, enterprise_code)


if __name__ == "__main__":
    enterprise_code = "2"

    asyncio.run(run_service(enterprise_code))
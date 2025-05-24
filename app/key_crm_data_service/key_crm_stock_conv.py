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

logging.basicConfig(level=logging.INFO)


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
        return result.scalars().all()  # возвращаем список объектов


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
            "page": page,
            "filter[details]": "true"
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


def transform_stock(raw_input, branch_mapping: dict, enterprise_code: str) -> list:
    """
    Универсальный парсер остатков. Если нет соответствия (enterprise_code, store_id),
    запись пропускается, в лог выводится предупреждение.
    """
    import logging

    if isinstance(raw_input, dict) and "data" in raw_input:
        data = raw_input["data"]
    else:
        data = raw_input

    result = []
    unknown_stores_logged = set()

    for item in data:
        item_id = str(item.get("id"))
        price = item.get("price")
        warehouses = item.get("warehouse", [])

        for w in warehouses:
            store_id = w.get("id")
            key = (str(enterprise_code), str(store_id))

            if key not in branch_mapping:
                if key not in unknown_stores_logged:
                    logging.warning(f"⚠️ Нет соответствия для enterprise_code={enterprise_code}, store_id={store_id} — склад будет пропущен.")
                    unknown_stores_logged.add(key)
                continue  # пропускаем warehouse

            branch = branch_mapping[key]
            quantity = w.get("quantity", 0)
            reserve = w.get("reserve", 0)

            try:
                qty = int(quantity) - int(reserve)
            except (TypeError, ValueError):
                qty = 0

            if qty < 0:
                qty = 0

            result.append({
                "branch": branch,
                "code": item_id,
                "price": price,
                "qty": qty,
                "price_reserve": price
            })

    return result



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
        logging.error(f"Ошибка сохранения исходного файла: {e}")
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

    stock_data = fetch_all_stock(api_key)
    if not stock_data:
        print("Данные по складам не получены.")
        return

# Сохраняем исходные данные
    save_raw_input(stock_data, enterprise_code)

    if not stock_data:
        print("Данные по складам не получены.")
        return

    # Загружаем mapping и строим словарь {(enterprise_code, store_id): branch}
    mapping_rows = await fetch_branch_mapping()
    branch_mapping = {
        (str(row.enterprise_code), str(row.store_id)): str(row.branch)
        for row in mapping_rows
        if row.enterprise_code and row.store_id
    }

    transformed = transform_stock(stock_data, branch_mapping, enterprise_code)
    file_path = save_to_json(transformed, enterprise_code, DEFAULT_FILE_TYPE)

    if not file_path:
        print("Не удалось сохранить файл.")
        return

    await process_database_service(file_path, DEFAULT_FILE_TYPE, enterprise_code)


if __name__ == "__main__":
    enterprise_code = "2"
    asyncio.run(run_service(enterprise_code))

import sys
sys.path.append('C:/Users/nazdr/Documents/inventory')
import requests
import logging
import json
import asyncio
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings
from app.database_service import process_database_service
from sqlalchemy.future import select

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

LIMIT = 100  # Лимит количества записей за один запрос

# Тестовый словарь сопоставления store_id -> branch
STORE_BRANCH_MAP = {
    "833a605c-fa32-46b6-9735-067239c68634": "111",
    "84267716-9373-4C36-BC3C-33912CF55BC9": "222",
}

async def fetch_developer_settings():
    """Получение API_ENDPOINT из DeveloperSettings."""
    async with get_async_db() as session:
        result = await session.execute(select(DeveloperSettings))
        return result.scalars().first()

async def fetch_enterprise_settings(enterprise_code):
    """Получение настроек предприятия по enterprise_code из EnterpriseSettings."""
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()

def fetch_products(api_endpoint, api_key, offset=0, limit=LIMIT):
    """Запрос данных продуктов через API с постраничной выборкой."""
    headers = {
        "ApiKey": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {"limit": limit, "offset": offset}

    try:
        logging.info(f"Fetching products with offset: {offset} and limit: {limit}")
        response = requests.post(api_endpoint, headers=headers, json=payload)
        logging.info(f"Response status code: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"API responded with error: {response.status_code}")
            return None
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to connect to API: {e}")
        return None

def transform_stock(products):
    """Трансформация данных продуктов в целевой формат для стока."""
    transformed = []
    for product in products:
        product_id = product.get("product_id")
        price_data = product.get("pices", [])
        balance = product.get("balance", 0)

        # Если у товара только один store_id, qty = balance
        if len(price_data) == 1:
            price_entry = price_data[0]
            if price_entry.get("price_title") == "Роздрібна":
                store_id = price_entry.get("store_id")
                branch = STORE_BRANCH_MAP.get(store_id)

                if branch:
                    transformed.append({
                        "branch": branch,
                        "code": product_id,
                        "price": float(price_entry.get("price", 0)),
                        "price_reserve": float(price_entry.get("price", 0)),
                        "qty": float(balance),  # Используем balance
                    })
        else:
            # Если у товара несколько store_id, qty = 1 для каждого
            for price_entry in price_data:
                if price_entry.get("price_title") == "Роздрібна":
                    store_id = price_entry.get("store_id")
                    branch = STORE_BRANCH_MAP.get(store_id)

                    if branch:
                        transformed.append({
                            "branch": branch,
                            "code": product_id,
                            "price": float(price_entry.get("price", 0)),
                            "price_reserve": float(price_entry.get("price", 0)),
                            "qty": 1,  # Устанавливаем qty = 1
                        })
    return transformed

def save_to_json(data, filename):
    """Сохранение данных в файл JSON."""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Data successfully saved to {filename}")
        return filename
    except IOError as e:
        logging.error(f"Failed to save JSON file: {e}")
        return None

async def run_service(enterprise_code):
    """Основной сервис выполнения задачи."""
    developer_settings = await fetch_developer_settings()
    if not developer_settings:
        logging.error("Failed to fetch developer settings from the database.")
        return

    api_endpoint = developer_settings.telegram_token_developer

    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        logging.error(f"No settings found for enterprise_code: {enterprise_code}")
        return

    api_key = enterprise_settings.token

    if not api_key:
        logging.error(f"No API key (token) found for enterprise_code: {enterprise_code}")
        return

    all_products = []
    offset = 0

    while True:
        response = fetch_products(api_endpoint, api_key, offset=offset, limit=LIMIT)
        if not response or response.get("status") != 1:
            logging.error("Failed to fetch products from API or invalid response format.")
            break

        products = response.get("products", [])
        if not products:
            logging.info("No more products to fetch.")
            break

        all_products.extend(products)
        offset += LIMIT

        if len(products) < LIMIT:
            break

    if not all_products:
        logging.error("No products were fetched from the API.")
        return

    # Преобразование данных
    transformed_data = transform_stock(all_products)

    # Сохранение данных в JSON
    json_file_path = save_to_json(transformed_data, "stock.json")
    if not json_file_path:
        logging.error("Failed to save transformed data to JSON file.")
        return

    # Передача данных в process_database_service
    await process_database_service(json_file_path, "stock", enterprise_code)

    logging.info("Service completed successfully.")

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

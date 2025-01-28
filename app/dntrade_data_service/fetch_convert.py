
# import sys
# sys.path.append('C:/Users/nazdr/Documents/inventory')
# import requests
# import logging
# import json
# import asyncio
# from app.database import get_async_db, DeveloperSettings, EnterpriseSettings
# from app.database_service import process_database_service
# from sqlalchemy.future import select

# # Настройка логирования
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# DEFAULT_VAT = 20

# async def fetch_developer_settings():
#     """Получение API_ENDPOINT из DeveloperSettings."""
#     async with get_async_db() as session:
#         result = await session.execute(select(DeveloperSettings))
#         return result.scalars().first()

# async def fetch_enterprise_settings(enterprise_code):
#     """Получение настроек предприятия по enterprise_code из EnterpriseSettings."""
#     async with get_async_db() as session:
#         result = await session.execute(
#             select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
#         )
#         return result.scalars().first()

# def fetch_products(api_endpoint, api_key):
#     """Запрос данных продуктов через API."""
#     headers = {
#         "ApiKey": api_key,  # Изменено на ApiKey
#         "Content-Type": "application/json",
#         "Accept": "application/json"
#     }

#     try:
#         logging.info(f"Using API endpoint: {api_endpoint}")
#         logging.info(f"Using API_KEY: {api_key}")

#         response = requests.post(api_endpoint, headers=headers, json={})  # Пустое тело запроса
#         logging.info(f"Response status code: {response.status_code}")
#         logging.info(f"Response content: {response.text}")

#         if response.status_code != 200:
#             logging.error(f"API responded with error: {response.status_code} - {response.text}")
#             return None
#         return response.json()
#     except requests.RequestException as e:
#         logging.error(f"Failed to connect to API: {e}")
#         return None

# def transform_products(products, branch_id):
#     """Трансформация данных продуктов в целевой формат."""
#     transformed = []
#     for product in products:
#         producer = product.get("short_description")
#         if not producer:  # Если `producer` пустой или None, задаем значение по умолчанию
#             producer = "N/A"

#         transformed.append({
#             "code": product.get("product_id"),
#             "name": product.get("title"),
#             "vat": DEFAULT_VAT,
#             "producer": producer,  # Гарантируем, что producer не будет None
#             "barcode": product.get("barcode"),
#             "branch_id": branch_id
#         })
#     return transformed

# def save_to_json(data, filename):
#     """Сохранение данных в файл JSON."""
#     try:
#         with open(filename, "w", encoding="utf-8") as f:
#             json.dump(data, f, ensure_ascii=False, indent=4)
#         logging.info(f"Data successfully saved to {filename}")
#         return filename
#     except IOError as e:
#         logging.error(f"Failed to save JSON file: {e}")
#         return None

# async def run_service(enterprise_code):
#     """Основной сервис выполнения задачи."""
#     # Получение данных из настроек
#     developer_settings = await fetch_developer_settings()
#     if not developer_settings:
#         logging.error("Failed to fetch developer settings from the database.")
#         return

#     api_endpoint = developer_settings.telegram_token_developer

#     enterprise_settings = await fetch_enterprise_settings(enterprise_code)
#     if not enterprise_settings:
#         logging.error(f"No settings found for enterprise_code: {enterprise_code}")
#         return

#     branch_id = enterprise_settings.branch_id
#     api_key = enterprise_settings.token  # Получение API key из поля token

#     if not api_key:
#         logging.error(f"No API key (token) found for enterprise_code: {enterprise_code}")
#         return

#     # Запрос данных из API
#     response = fetch_products(api_endpoint, api_key)
#     if not response or response.get("status") != 1:
#         logging.error("Failed to fetch products from API or invalid response format.")
#         return

#     products = response.get("products", [])

#     # Преобразование данных
#     transformed_data = transform_products(products, branch_id)

#     # Сохранение данных в JSON
#     json_file_path = save_to_json(transformed_data, "products.json")
#     if not json_file_path:
#         logging.error("Failed to save transformed data to JSON file.")
#         return

#     # Передача данных в process_database_service
#     await process_database_service(json_file_path, "catalog", enterprise_code)

#     logging.info("Service completed successfully.")

# if __name__ == "__main__":
#     # Для тестирования можно задать enterprise_code вручную
#     TEST_ENTERPRISE_CODE = "2"
#     asyncio.run(run_service(TEST_ENTERPRISE_CODE))

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

DEFAULT_VAT = 20
LIMIT = 100  # Лимит количества записей за один запрос

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

    payload = {
        "limit": limit,
        "offset": offset
    }

    try:
        logging.info(f"Fetching products with offset: {offset} and limit: {limit}")
        response = requests.post(api_endpoint, headers=headers, json=payload)
        logging.info(f"Response status code: {response.status_code}")
        logging.info(f"Response content: {response.text}")

        if response.status_code != 200:
            logging.error(f"API responded with error: {response.status_code} - {response.text}")
            return None
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to connect to API: {e}")
        return None

def transform_products(products, branch_id):
    """Трансформация данных продуктов в целевой формат."""
    transformed = []
    for product in products:
        producer = product.get("short_description")
        if not producer:  # Если `producer` пустой или None, задаем значение по умолчанию
            producer = "N/A"

        transformed.append({
            "code": product.get("product_id"),
            "name": product.get("title"),
            "vat": DEFAULT_VAT,
            "producer": producer,
            "barcode": product.get("barcode"),
            "branch_id": branch_id
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
    # Получение данных из настроек
    developer_settings = await fetch_developer_settings()
    if not developer_settings:
        logging.error("Failed to fetch developer settings from the database.")
        return

    api_endpoint = developer_settings.telegram_token_developer

    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        logging.error(f"No settings found for enterprise_code: {enterprise_code}")
        return

    branch_id = enterprise_settings.branch_id
    api_key = enterprise_settings.token  # Получение API key из поля token

    if not api_key:
        logging.error(f"No API key (token) found for enterprise_code: {enterprise_code}")
        return

    # Запрос данных из API с постраничной выборкой
    all_products = []
    offset = 0

    while True:
        response = fetch_products(api_endpoint, api_key, offset=offset, limit=LIMIT)
        if not response or response.get("status") != 1:
            logging.error("Failed to fetch products from API or invalid response format.")
            break

        products = response.get("products", [])
        if not products:  # Если данных больше нет, выходим из цикла
            logging.info("No more products to fetch.")
            break

        all_products.extend(products)
        offset += LIMIT

        logging.info(f"Fetched {len(products)} products. Total: {len(all_products)}")

        # Проверяем, если размер текущего ответа меньше LIMIT, то это последняя страница
        if len(products) < LIMIT:
            logging.info("Fetched the last page of products.")
            break

    if not all_products:
        logging.error("No products were fetched from the API.")
        return

    # Преобразование данных
    transformed_data = transform_products(all_products, branch_id)

    # Сохранение данных в JSON
    json_file_path = save_to_json(transformed_data, "products.json")
    if not json_file_path:
        logging.error("Failed to save transformed data to JSON file.")
        return

    # Передача данных в process_database_service
    await process_database_service(json_file_path, "catalog", enterprise_code)

    logging.info("Service completed successfully.")

if __name__ == "__main__":
    # Для тестирования можно задать enterprise_code вручную
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

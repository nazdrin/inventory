import sys
sys.path.append('C:/Users/nazdr/Documents/inventory')
import requests
import json
import asyncio
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings
from app.database_service import process_database_service
from sqlalchemy.future import select

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
    """Запрос данных продуктов через API с использованием query-параметров."""
    headers = {
        "ApiKey": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    url = f"{api_endpoint}?limit={limit}&offset={offset}"

    try:
        response = requests.post(url, headers=headers)

        if response.status_code != 200:
            return None  # Ошибка запроса, игнорируем

        data = response.json()
        return data if isinstance(data, dict) else None
    except requests.RequestException:
        return None

def transform_products(products, branch_id):
    """Трансформация данных продуктов в целевой формат."""
    transformed = []
    for product in products:
        producer = product.get("short_description")
        if not producer or producer in [None, "", 0]:  # Фильтрация некорректных значений
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
        return filename
    except IOError:
        return None

async def run_service(enterprise_code):
    """Основной сервис выполнения задачи."""
    developer_settings = await fetch_developer_settings()
    if not developer_settings:
        return

    api_endpoint = developer_settings.telegram_token_developer

    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        return

    branch_id = enterprise_settings.branch_id
    api_key = enterprise_settings.token
    if not api_key:
        return

    all_products = []
    offset = 0

    while True:
        response = fetch_products(api_endpoint, api_key, offset=offset, limit=LIMIT)

        if response is None:
            break  # Если нет ответа от API, прерываем цикл

        products = response.get("products", [])
        if not products:
            break  # Если список продуктов пуст, заканчиваем

        all_products.extend(products)
        offset += LIMIT  # Увеличиваем offset

    if not all_products:
        return  # Нет данных для сохранения

    transformed_data = transform_products(all_products, branch_id)
    json_file_path = save_to_json(transformed_data, "products.json")
    if not json_file_path:
        return  # Ошибка сохранения JSON

    await process_database_service(json_file_path, "catalog", enterprise_code)

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

import json
import requests
import asyncio
from app.database import get_async_db, EnterpriseSettings
from sqlalchemy.future import select
from app.services.database_service import process_database_service
import tempfile
import os
import logging
from dotenv import load_dotenv
load_dotenv()

PROM_API_URL = "https://my.prom.ua/api/v1/products/list"
LIMIT = 100000  # Фиксированный лимит записей
JSON_FILENAME = "products.json"

def fetch_products(api_key, limit=LIMIT):
    """Запрос данных продуктов через API."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }
    params = {"limit": limit}
    try:
        response = requests.get(PROM_API_URL, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Ошибка запроса: {response.status_code}, {response.text}")
            return None
        return response.json()
    except requests.RequestException as e:
        print(f"Ошибка соединения: {e}")
        return None

async def fetch_enterprise_settings(enterprise_code):
    """Получение настроек предприятия по enterprise_code из EnterpriseSettings."""
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()

def transform_products(products, branch_id):
    """Трансформация данных в нужный формат для стока."""
    transformed = []
    for product in products.get("products", []):
        quantity = product.get("quantity_in_stock", 0)
        quantity = max(quantity, 0) if quantity is not None else 0  # Если None, ставим 0
        transformed.append({
            "branch": str(branch_id),  # Преобразуем branch_id в строку
            "code": str(product.get("id")),  # Преобразуем id в строку
            "price": float(product.get("price", 0.0)),  # Преобразуем price в float
            "qty": quantity,  # Количество в наличии (не отрицательное)
            "price_reserve": float(product.get("price", 0.0))  # Дублируем price
        })
    return transformed

def save_to_json(data, enterprise_code, file_type):
    """Сохранение данных в JSON-файл в указанную временную директорию из .env."""
    try:
        # Получаем временный путь из переменной окружения, иначе используем системный temp
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)

        # Формируем путь к JSON-файлу
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logging.info(f"JSON записан в файл: {json_file_path}")
        return json_file_path
    except IOError as e:
        logging.error(f"Ошибка при сохранении JSON-файла: {e}")
        return None

async def run_prom(enterprise_code, file_type):
    """Основной сервис для получения и обработки данных стока."""
    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings or not enterprise_settings.token:
        print("Ошибка: Токен не найден")
        return

    api_key = enterprise_settings.token
    response = fetch_products(api_key, limit=LIMIT)
    if response:
        transformed_data = transform_products(response, enterprise_settings.branch_id)
        file_type = "stock"
        json_file_path = save_to_json(transformed_data, enterprise_code, file_type)
        if json_file_path:
            await process_database_service(json_file_path, "stock", enterprise_code)
    else:
        print("Не удалось получить данные")

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "777"
    asyncio.run(run_prom(TEST_ENTERPRISE_CODE))

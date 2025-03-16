import sys
import requests
import json
import asyncio
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings
from app.services.database_service import process_database_service
from sqlalchemy.future import select
from collections import Counter
import tempfile
import os
import logging
from dotenv import load_dotenv
load_dotenv()

DEFAULT_VAT = 20
LIMIT = 100  # Лимит количества записей за один запрос
def log_progress(offset, count):
#     """Логирование процесса обновляемой строкой в консоли"""
    sys.stdout.write(f"\rЗапрос: offset={offset} | Получено: {count} записей")
    sys.stdout.flush()

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
    seen_product_ids = set()
    product_id_counts = Counter(product.get("product_id") for product in products)
    
    for product in products:
        product_id = product.get("product_id")
        if product_id in seen_product_ids:
            continue  # Пропускаем дублирующийся product_id
        
        producer = product.get("short_description")
        if not producer or producer in [None, "", 0]:  # Фильтрация некорректных значений
            producer = "N/A"
        transformed.append({
            "code": product_id,
            "name": product.get("title"),
            "vat": DEFAULT_VAT,
            "producer": producer,
            "barcode": product.get("barcode"),
            # "branch_id": branch_id
        })
        seen_product_ids.add(product_id)  # Запоминаем обработанный product_id
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
        log_progress(offset, len(products))
        offset += LIMIT  # Увеличиваем offset

    if not all_products:
        return  # Нет данных для сохранения

    transformed_data = transform_products(all_products, branch_id)
    file_type = "catalog"
    json_file_path = save_to_json(transformed_data, enterprise_code, file_type)

    if not json_file_path:
        return  # Ошибка сохранения JSON

    await process_database_service(json_file_path, file_type, enterprise_code)
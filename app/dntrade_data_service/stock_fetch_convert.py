import sys
sys.path.append('C:/Users/nazdr/Documents/inventory')
import requests
import json
import asyncio
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings
from app.services.database_service import process_database_service
from sqlalchemy.future import select
import tempfile
import os
import logging
from dotenv import load_dotenv
load_dotenv()

LIMIT = 100  # Лимит количества записей за один запрос
def log_progress(offset, count):
    """Логирование процесса обновляемой строкой в консоли"""
    sys.stdout.write(f"\rЗапрос: offset={offset} | Получено: {count} записей")
    sys.stdout.flush()

STORE_BRANCH_MAP = {
    "833a605c-fa32-46b6-9735-067239c68634": "30447"
    # "E0D20C48-9BF2-499D-A7BA-466C97BEC6B7": "222",
    # "AB160935-AD2E-4539-A937-2F05F9F4775F": "333",
    # "99AB8090-5090-4638-BC02-B781CA861976": "444",
    # "A86790BA-CBDC-44D7-A780-F05FE7B1FFB7": "555",
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

def fetch_products(api_endpoint, api_key, store_id, offset=0, limit=LIMIT):
    """Запрос данных продуктов через API для конкретного store_id с корректным форматом запроса."""
    headers = {
        "ApiKey": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    # Корректный URL с параметрами запроса
    url = f"{api_endpoint}?limit={limit}&offset={offset}&store_id={store_id}"

    try:
        response = requests.post(url, headers=headers, json={})  # Пустое тело запроса `-d ''`
        if response.status_code != 200:
            return None  # Ошибка запроса

        data = response.json()
        return data if isinstance(data, dict) else None
    except requests.RequestException:
        return None

def transform_stock(products):
    """Трансформация данных продуктов в целевой формат для стока."""
    transformed = []
    log_data = []
    full_input_log_filename = "full_input_data.json"  # Файл для логирования всех входящих данных
    log_filename = "stock_logs.json"  # Файл для логирования конкретных товаров
    
    # Логирование всех входящих данных
    try:
        with open(full_input_log_filename, "w", encoding="utf-8") as full_log_file:
            json.dump(products, full_log_file, ensure_ascii=False, indent=4)
    except IOError as e:
        print(f"Ошибка записи полного входного лога: {e}")
    
    for product in products:
        product_id = product.get("product_id")
        price_data = product.get("pices", [])
        balance = product.get("balance")
        
        # Проверка, если balance не число или None, устанавливаем в 0
        try:
            balance = float(balance)
        except (TypeError, ValueError):
            balance = 0
        
        # Устанавливаем qty в 0, если balance отрицательный
        qty = max(balance, 0)
        
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
                        "qty": qty,  # Используем скорректированное значение qty
                    })
    
    # Запись логов в файл, если есть данные для логирования
    if log_data:
        try:
            with open(log_filename, "w", encoding="utf-8") as log_file:
                json.dump(log_data, log_file, ensure_ascii=False, indent=4)
        except IOError as e:
            print(f"Ошибка записи лога: {e}")
    
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

    api_key = enterprise_settings.token
    if not api_key:
        return

    all_products = []

    # Цикл по каждому store_id
    for store_id in STORE_BRANCH_MAP.keys():
        offset = 0  # Начальный offset для каждого store_id

        while True:
            response = fetch_products(api_endpoint, api_key, store_id, offset=offset, limit=LIMIT)

            if response is None:
                break  # Ошибка API - останавливаем обработку этого store_id

            products = response.get("products", [])
            if not products:
                break  # Если список `products` пустой, прекращаем цикл для store_id

            all_products.extend(products)
            # Логируем прогресс одной строкой
            log_progress(offset, len(products))
            offset += LIMIT  # Увеличиваем offset

    if not all_products:
        return  # Нет данных для сохранения

    transformed_data = transform_stock(all_products)
    file_type = "stock"
    json_file_path = save_to_json(transformed_data, enterprise_code, file_type)
    if not json_file_path:
        return  # Ошибка сохранения JSON

    await process_database_service(json_file_path, file_type, enterprise_code)
    

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "238"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))
import sys
import requests
import json
import asyncio
import os
import tempfile
import logging
from dotenv import load_dotenv
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings, MappingBranch
from app.services.database_service import process_database_service
from sqlalchemy.future import select

load_dotenv()

LIMIT = 1000  # Лимит количества записей за один запрос
API_URL = "https://api.checkbox.ua/api/v1/goods"

def log_progress(offset, count):
    sys.stdout.write(f"\rЗапрос: offset={offset} | Получено: {count} записей")
    sys.stdout.flush()

async def fetch_enterprise_settings(enterprise_code):
    """Получение настроек предприятия."""
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()

async def fetch_branch_by_enterprise(enterprise_code):
    """Получение branch из MappingBranch по enterprise_code."""
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        return result.scalars().first()

def fetch_stock(api_key, offset=0, limit=LIMIT):
    """Запрос данных стока через API."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }
    params = {"offset": offset, "limit": limit}
    try:
        response = requests.get(API_URL, headers=headers, params=params)
        if response.status_code != 200:
            return None
        return response.json()
    except requests.RequestException:
        return None

def transform_stock(products, branch):
    """Трансформация данных стока в целевой формат."""
    def safe_div(value, divisor):
        return (value or 0) / divisor

    return [{
        "branch": branch,
        "code": product.get("id"),
        "price": safe_div(product.get("price"), 100),
        "qty": safe_div(product.get("count"), 1000),
        "price_reserve": safe_div(product.get("price"), 100)
    } for product in products]



def save_to_json(data, enterprise_code, file_type):
    """Сохранение данных в JSON-файл."""
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")
        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        logging.info(f"JSON записан в файл: {json_file_path}")
        return json_file_path
    except IOError as e:
        logging.error(f"Ошибка при сохранении JSON-файла: {e}")
        return None

async def run_service(enterprise_code, file_type):
    """Основной сервис выполнения задачи."""
    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        return
    
    api_key = enterprise_settings.token
    if not api_key:
        return
    
    branch = await fetch_branch_by_enterprise(enterprise_code)
    if not branch:
        return
    
    all_products = []
    offset = 0
    
    while True:
        response = fetch_stock(api_key, offset=offset, limit=LIMIT)
        if response is None:
            break
        products = response.get("results", [])
        if not products:
            break
        all_products.extend(products)
        log_progress(offset, len(products))
        offset += LIMIT
    
    if not all_products:
        return
    
    transformed_data = transform_stock(all_products, branch)
    file_type = "stock"
    json_file_path = save_to_json(transformed_data, enterprise_code, file_type)
    if not json_file_path:
        return
    
    await process_database_service(json_file_path, file_type, enterprise_code)

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "256"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))
import sys
import requests
import json
import asyncio
import xml.etree.ElementTree as ET
from app.database import get_async_db, EnterpriseSettings, MappingBranch
from app.services.database_service import process_database_service
from sqlalchemy.future import select
import tempfile
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
async def fetch_feed_url(enterprise_code):
    """Получение URL фида из таблицы EnterpriseSettings по enterprise_code."""
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        feed_url = result.scalars().first()
        return feed_url if feed_url else None

def download_xml(url):
    """Загрузка XML-файла с подменой User-Agent."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text  # Возвращаем XML как строку
    else:
        raise Exception(f"Ошибка загрузки: {response.status_code}")
import xml.etree.ElementTree as ET
import os
import tempfile
import json
import logging

import xml.etree.ElementTree as ET
import os
import tempfile
import json
import logging

def parse_xml(xml_string, enterprise_code):
    """Разбор XML и извлечение данных о наличии и ценах."""
    root = ET.fromstring(xml_string)
    stock_data = []
    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    debug_file_path = os.path.join(temp_dir, f"{enterprise_code}_debug_stock_data.json")
    
    for offer in root.findall(".//offer"):
        code = offer.findtext("code")
        if not code:
            continue  # Пропускаем товары без кода
        
        # Логика выбора цены
        price = offer.findtext("newprice") or offer.findtext("price", "0")
        price = float(price) if price else 0.0
        
        # Количество на складе
        stock_quantity = offer.findtext("quantity", "0")
        stock_quantity = int(stock_quantity) if stock_quantity.isdigit() else 0
        
        item_data = {
            "code": code,
            "price": price,
            "qty": stock_quantity,
            "price_reserve": price,  # Используем ту же логику, что и для price
        }
        stock_data.append(item_data)
    
    with open(debug_file_path, "w", encoding="utf-8") as debug_file:
        json.dump(stock_data, debug_file, ensure_ascii=False, indent=4)
    
    logging.info(f"Входящие данные сохранены в {debug_file_path}")
    return stock_data

async def fetch_branch_id(enterprise_code):
    """Получение branch из таблицы MappingBranch."""
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        mapping = result.scalars().first()
        return mapping if mapping else "unknown"

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
    """Основной процесс загрузки и обработки XML-фида."""
    feed_url = await fetch_feed_url(enterprise_code)
    if not feed_url:
        logging.error(f"Не найден URL фида для enterprise_code: {enterprise_code}")
        return
    branch_id = await fetch_branch_id(enterprise_code)
    xml_data = download_xml(feed_url)
    parsed_data = parse_xml(xml_data, enterprise_code)
    
    # Добавляем branch ко всем записям
    for item in parsed_data:
        item["branch"] = branch_id
    
    file_type = "stock"
    json_file_path = save_to_json(parsed_data, enterprise_code, file_type)
    
    if json_file_path:
        await process_database_service(json_file_path, file_type, enterprise_code)

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

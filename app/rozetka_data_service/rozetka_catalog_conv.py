import sys
import requests
import json
import asyncio
import xml.etree.ElementTree as ET
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings
from app.services.database_service import process_database_service
from sqlalchemy.future import select
import tempfile
import os
import logging
from dotenv import load_dotenv

load_dotenv()

DEFAULT_VAT = 20

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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Cache-Control": "no-cache"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.text  # Возвращаем XML как строку
    else:
        raise Exception(f"Ошибка загрузки: {response.status_code}")


def parse_xml(xml_string):
    """Разбор XML и извлечение данных о товарах."""
    root = ET.fromstring(xml_string)
    offers = []
    
    for offer in root.findall(".//offer"):
        code = offer.findtext("code")
        name = offer.findtext("name")
        barcode = offer.findtext("barcode", "")
        producer = offer.findtext("brand", "N/A")
        
        # Пропускаем товары без кода или названия
        if not code or not name:
            continue
        
        offer_data = {
            "code": code,
            "name": name.strip() if name else "no_name",
            "vat": DEFAULT_VAT,
            "producer": producer.strip() if producer else "N/A",
            "barcode": barcode.strip() if barcode else ""
        }
        
        offers.append(offer_data)
    
    return offers

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

async def fetch_enterprise_settings(enterprise_code):
    """Получение настроек предприятия."""
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()

async def run_service(enterprise_code, file_type):
    """Основной процесс загрузки и обработки XML-фида."""
    feed_url = await fetch_feed_url(enterprise_code)
    if not feed_url:
        logging.error(f"Не найден URL фида для enterprise_code: {enterprise_code}")
        return
    
    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        return
    
    xml_data = download_xml(feed_url)
    parsed_data = parse_xml(xml_data)
    
    file_type = "catalog"
    json_file_path = save_to_json(parsed_data, enterprise_code, file_type)
    
    if json_file_path:
        await process_database_service(json_file_path, file_type, enterprise_code)

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

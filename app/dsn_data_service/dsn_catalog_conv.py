import sys
import requests
import json
import asyncio
import xml.etree.ElementTree as ET
from app.database import get_async_db, EnterpriseSettings
from app.services.database_service import process_database_service
from sqlalchemy.future import select
import tempfile
import os
import logging
import re
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

async def fetch_feed_url(enterprise_code):
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        feed_url = result.scalars().first()
        return feed_url if feed_url else None

def download_xml(url):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Ошибка загрузки XML: {response.status_code}")

def extract_barcode(description: str) -> str:
    match = re.search(r"Штрихкод:\s*(\d+)", description)
    return match.group(1) if match else ""

def parse_xml_to_catalog(xml_string, enterprise_code):
    root = ET.fromstring(xml_string)
    catalog_data = []

    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    debug_file_path = os.path.join(temp_dir, f"{enterprise_code}_debug_catalog_data.json")

    for offer in root.findall(".//offer"):
        offer_id = offer.attrib.get("id")
        name_el = offer.find("name")
        vendor_el = offer.find("vendor")
        description_el = offer.find("description")

        if not offer_id or name_el is None or vendor_el is None:
            continue

        name_text = name_el.text.strip() if name_el.text else ""
        if name_text.startswith("<![CDATA[") and name_text.endswith("]]>"):
            name_text = name_text[9:-3].strip()

        description_text = description_el.text if description_el is not None and description_el.text else ""
        barcode = extract_barcode(description_text)

        item = {
            "code": offer_id,
            "name": name_text,
            "vat": 20,
            "producer": vendor_el.text.strip() if vendor_el.text else "",
            "barcode": barcode
        }
        catalog_data.append(item)

    with open(debug_file_path, "w", encoding="utf-8") as debug_file:
        json.dump(catalog_data, debug_file, ensure_ascii=False, indent=4)

    logging.info(f"Каталог сохранён в {debug_file_path}")
    return catalog_data

def save_to_json(data, enterprise_code, file_type):
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
    feed_url = await fetch_feed_url(enterprise_code)
    if not feed_url:
        logging.error(f"Не найден URL фида для enterprise_code: {enterprise_code}")
        return

    xml_data = download_xml(feed_url)
    parsed_data = parse_xml_to_catalog(xml_data, enterprise_code)

    file_type = "catalog"
    json_file_path = save_to_json(parsed_data, enterprise_code, file_type)

    if json_file_path:
        await process_database_service(json_file_path, file_type, enterprise_code)

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

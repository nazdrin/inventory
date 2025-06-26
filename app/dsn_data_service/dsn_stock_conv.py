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
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        feed_url = result.scalars().first()
        return feed_url if feed_url else None


async def fetch_branch_id(enterprise_code):
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        mapping = result.scalars().first()
        return mapping if mapping else "unknown"


def download_xml(url):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Ошибка загрузки XML: {response.status_code}")


def parse_stock_data(xml_string: str, branch_id: str, enterprise_code: str):
    root = ET.fromstring(xml_string)
    stock_data = []

    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    debug_file_path = os.path.join(temp_dir, f"{enterprise_code}_debug_stock_data.json")

    for offer in root.findall(".//offer"):
        offer_id = offer.attrib.get("id")
        price_text = offer.findtext("price")
        # quantity_in_stock = int(offer.findtext("quantity_in_stock"))
        try:
            quantity_in_stock = int(offer.findtext("quantity_in_stock", "0"))
            if quantity_in_stock < 0:
                quantity_in_stock = 0
        except (ValueError, TypeError):
            quantity_in_stock = 0

        if not offer_id or not price_text:
            continue

        try:
            price = float(price_text)
        except ValueError:
            continue

        stock_data.append({
            "branch": branch_id,
            "code": offer_id,
            "price": price,
            "qty": quantity_in_stock,
            "price_reserve": price
        })

    with open(debug_file_path, "w", encoding="utf-8") as debug_file:
        json.dump(stock_data, debug_file, ensure_ascii=False, indent=4)

    logging.info(f"Отладочный JSON сохранен в {debug_file_path}")
    return stock_data


def save_to_json(data, enterprise_code, file_type):
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logging.info(f"JSON сохранён в {json_file_path}")
        return json_file_path
    except IOError as e:
        logging.error(f"Ошибка при сохранении JSON: {e}")
        return None


async def run_service(enterprise_code, file_type):
    feed_url = await fetch_feed_url(enterprise_code)
    if not feed_url:
        logging.error(f"Не найден URL фида для enterprise_code: {enterprise_code}")
        return

    xml_data = download_xml(feed_url)
    branch_id = await fetch_branch_id(enterprise_code)
    parsed_data = parse_stock_data(xml_data, branch_id, enterprise_code)

    file_type = "stock"
    json_file_path = save_to_json(parsed_data, enterprise_code, file_type)

    if json_file_path:
        await process_database_service(json_file_path, file_type, enterprise_code)


if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

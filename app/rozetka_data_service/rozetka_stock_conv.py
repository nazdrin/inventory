import requests
import json
import asyncio
import xml.etree.ElementTree as ET
import time
from app.database import get_async_db, EnterpriseSettings, MappingBranch
from app.services.database_service import process_database_service
from sqlalchemy.future import select
import tempfile
import os
import logging
from dotenv import load_dotenv

load_dotenv()

REQUEST_TIMEOUT_SEC = 30

logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
logger.propagate = False


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
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
    if response.status_code == 200:
        return response.text  # Возвращаем XML как строку
    else:
        raise Exception(f"Ошибка загрузки: {response.status_code}")

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
    
    logger.info("Rozetka stock debug JSON saved: path=%s records=%s", debug_file_path, len(stock_data))
    return stock_data

async def fetch_branch_id(enterprise_code):
    """Получение branch из таблицы MappingBranch."""
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        mapping = result.scalars().first()
        if not mapping:
            raise ValueError(
                f"Rozetka stock misconfiguration: branch mapping not found for enterprise_code={enterprise_code}"
            )
        return str(mapping)

def save_to_json(data, enterprise_code, file_type):
    """Сохранение данных в JSON-файл."""
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")
        
        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logger.info("Rozetka stock JSON saved: path=%s records=%s", json_file_path, len(data))
        return json_file_path
    except IOError as e:
        logger.error("Ошибка при сохранении JSON-файла: %s", e)
        return None

async def run_service(enterprise_code, file_type):
    """Основной процесс загрузки и обработки XML-фида."""
    run_started_at = time.monotonic()
    feed_url = await fetch_feed_url(enterprise_code)
    if not feed_url:
        logger.error("Не найден URL фида для enterprise_code=%s", enterprise_code)
        return
    branch_id = await fetch_branch_id(enterprise_code)
    logger.info(
        "Rozetka stock run start: enterprise_code=%s branch=%s source_url=%s",
        enterprise_code,
        branch_id,
        feed_url,
    )
    download_started_at = time.monotonic()
    xml_data = download_xml(feed_url)
    logger.info(
        "Rozetka stock download summary: enterprise_code=%s bytes=%s elapsed=%.2fs",
        enterprise_code,
        len(xml_data.encode("utf-8")),
        time.monotonic() - download_started_at,
    )

    parse_started_at = time.monotonic()
    parsed_data = parse_xml(xml_data, enterprise_code)
    logger.info(
        "Rozetka stock parse summary: enterprise_code=%s branch=%s records=%s elapsed=%.2fs",
        enterprise_code,
        branch_id,
        len(parsed_data),
        time.monotonic() - parse_started_at,
    )
    
    # Добавляем branch ко всем записям
    for item in parsed_data:
        item["branch"] = branch_id
    
    file_type = "stock"
    json_file_path = save_to_json(parsed_data, enterprise_code, file_type)
    
    if json_file_path:
        await process_database_service(json_file_path, file_type, enterprise_code)
        logger.info(
            "Rozetka stock run summary: enterprise_code=%s branch=%s records=%s elapsed=%.2fs",
            enterprise_code,
            branch_id,
            len(parsed_data),
            time.monotonic() - run_started_at,
        )

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

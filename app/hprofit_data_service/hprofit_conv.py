import os
import json
import logging
import requests
from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings
from app.models import MappingBranch
from app.services.database_service import process_database_service
import xml.etree.ElementTree as ET



async def fetch_feed_url(enterprise_code):
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(
                EnterpriseSettings.enterprise_code == enterprise_code
            )
        )
        return result.scalars().first()

def parse_xml_feed(xml_text: str) -> list:
    root = ET.fromstring(xml_text)
    offers = root.findall(".//offer")
    result = []
    for offer in offers:
        item = {
            "productId": offer.get("id"),
            "productName": offer.findtext("name"),
            "brand": offer.findtext("brand"),
            "barcode": offer.findtext("barcode"),
            "price": float(offer.findtext("price") or 0),
            "quantity": float(offer.findtext("quantity_in_stock") or 0),

            "reserve": 0  # если нет в XML — по умолчанию
        }
        result.append(item)
    return result


async def fetch_branch_by_enterprise_code(enterprise_code):
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(
                MappingBranch.enterprise_code == enterprise_code
            )
        )
        branch = result.scalars().first()
        if not branch:
            raise ValueError(f"Branch не найден для enterprise_code={enterprise_code}")
        return str(branch)


def download_feed(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Ошибка загрузки: {response.status_code}")
    return response.text


def transform_catalog(data: list) -> list:
    return [
        {
            "code": item.get("productId"),
            "name": item.get("productName"),
            "producer": item.get("brand"),
            "barcode": item.get("barcode"),
            "vat": 20.0
        }
        for item in data
    ]


def transform_stock(data: list, branch: str) -> list:
    result = []
    for item in data:
        try:
            qty = int(item.get("quantity", 0)) - int(item.get("reserve", 0))
        except (ValueError, TypeError):
            qty = 0

        result.append({
            "branch": branch,
            "code": item.get("productId"),
            "price": item.get("price"),
            "qty": max(qty, 0),
            "price_reserve": item.get("price")
        })
    return result


def save_to_json(data, enterprise_code, file_type):
    dir_path = os.path.join("temp", str(enterprise_code))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{file_type}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    logging.info(f"✅ Данные сохранены: {file_path}")
    return file_path


async def send_catalog_data(file_path, enterprise_code):
    await process_database_service(file_path, "catalog", enterprise_code)


async def send_stock_data(file_path, enterprise_code):
    await process_database_service(file_path, "stock", enterprise_code)

async def run_service(enterprise_code: str, file_type: str):
    url = await fetch_feed_url(enterprise_code)
    if not url:
        raise ValueError("❌ URL фида не найден")

    feed_text = download_feed(url)
    if not feed_text.strip():
        raise ValueError("❌ Получен пустой фид")

    try:
        raw_data = parse_xml_feed(feed_text)
    except Exception as e:
        raise ValueError(f"❌ Ошибка разбора XML: {e}")

    if file_type == "catalog":
        data = transform_catalog(raw_data)
        path = save_to_json(data, enterprise_code, "catalog")
        await send_catalog_data(path, enterprise_code)

    elif file_type == "stock":
        branch = await fetch_branch_by_enterprise_code(enterprise_code)
        data = transform_stock(raw_data, branch)
        path = save_to_json(data, enterprise_code, "stock")
        await send_stock_data(path, enterprise_code)

    else:
        raise ValueError("Тип файла должен быть 'catalog' или 'stock'")


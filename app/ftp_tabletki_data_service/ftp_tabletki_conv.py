import os
import zipfile
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from app.database import get_async_db, MappingBranch, EnterpriseSettings, InventoryData
from sqlalchemy import select
import logging
import json

TEMP_DIR = "/opt/test_project/uploads"
FTP_UPLOADS_DIR = Path("/var/ftp/tabletki-uploads")

logging.basicConfig(level=logging.INFO)


def parse_branch_from_filename(filename: str) -> tuple[str, datetime]:
    try:
        name = Path(filename).stem  # Rest_12345_20250705120000
        _, branch_str, datetime_str = name.split("_")
        dt = datetime.strptime(datetime_str, "%Y%m%d%H%M%S")
        return branch_str, dt
    except Exception:
        return None, None


def find_latest_zip_for_branch(branch: str) -> Path | None:
    matched = []
    for file in FTP_UPLOADS_DIR.glob(f"Rest_{branch}_*.zip"):
        _, dt = parse_branch_from_filename(file.name)
        if dt:
            matched.append((dt, file))

    if not matched:
        return None

    matched.sort(reverse=True)
    return matched[0][1]


def cleanup_old_files(branch: str, keep_file: Path):
    for file in FTP_UPLOADS_DIR.glob(f"Rest_{branch}_*.zip"):
        if file != keep_file:
            file.unlink(missing_ok=True)


def extract_zip_file(zip_path: Path, extract_to: Path) -> Path:
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_to)
    for file in extract_to.glob("*.xml"):
        return file
    raise FileNotFoundError("XML-файл внутри архива не найден")


def parse_xml_file(file_path: Path) -> list[dict]:
    tree = ET.parse(file_path)
    root = tree.getroot()
    offers = []
    for offer in root.findall("Offer"):
        item = offer.attrib
        offers.append({
            "code": item.get("Code"),
            "name": item.get("Name"),
            "producer": item.get("Producer"),
            "barcode": item.get("Barcode"),
            "morion": item.get("Code1"),
            "optima": item.get("Code2"),
            "badm": item.get("Code7"),
            "venta": item.get("Code9"),
            "tabletki": item.get("tabletki"),
            "vat": float(item.get("Tax", 0)),
            "price": float(item.get("Price", 0)),
            "qty": float(item.get("Quantity", 0)),
            "price_reserve": float(item.get("PriceReserve", 0)),
        })
    return offers


async def validate_catalog_data(enterprise_code: str, offers: list[dict]) -> bool:
    async with get_async_db() as session:
        for i, item in enumerate(offers):
            if i >= 10:
                break
            stmt = select(InventoryData).where(
                InventoryData.enterprise_code == enterprise_code,
                InventoryData.code == item["code"]
            )
            result = await session.execute(stmt)
            db_item = result.scalars().first()
            if db_item and db_item.name == item["name"]:
                return True  # Валидный товар найден
    return False


def transform_catalog(data: list) -> list:
    return [
        {
            "code": item["code"],
            "name": item["name"],
            "producer": item["producer"],
            "barcode": item["barcode"],
            "morion": item["morion"],
            "optima": item["optima"],
            "badm": item["badm"],
            "venta": item["venta"],
            "tabletki": item["tabletki"],
            "vat": item["vat"]
        }
        for item in data
    ]


def transform_stock(data: list, branch: str) -> list:
    return [
        {
            "branch": branch,
            "code": item["code"],
            "price": item["price"],
            "qty": item["qty"],
            "price_reserve": item["price_reserve"]
        }
        for item in data
    ]


def save_to_json(data: list, enterprise_code: str, file_type: str) -> Path:
    dir_path = Path(TEMP_DIR) / enterprise_code
    dir_path.mkdir(parents=True, exist_ok=True)
    out_path = dir_path / f"{file_type}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out_path


async def run_service(enterprise_code: str, file_type: str):
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        branches = result.scalars().all()

    for branch in branches:
        zip_file = find_latest_zip_for_branch(branch)
        if not zip_file:
            logging.warning(f"Файл для branch {branch} не найден")
            continue

        cleanup_old_files(branch, zip_file)
        extract_path = Path(TEMP_DIR) / f"{enterprise_code}_{branch}"
        extract_path.mkdir(parents=True, exist_ok=True)
        xml_file = extract_zip_file(zip_file, extract_path)
        offers = parse_xml_file(xml_file)

        if file_type == "catalog":
            is_valid = await validate_catalog_data(enterprise_code, offers)
            if not is_valid:
                logging.warning(f"❌ Каталог {zip_file.name} не прошёл валидацию")
                continue
            data = transform_catalog(offers)

        elif file_type == "stock":
            data = transform_stock(offers, branch)

        else:
            logging.error(f"❌ Неизвестный тип: {file_type}")
            continue

        save_path = save_to_json(data, enterprise_code, file_type)
        logging.info(f"✅ Данные подготовлены: {save_path}")
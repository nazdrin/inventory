import json
import logging
import os
import zipfile
import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from app.database import EnterpriseSettings, InventoryData, MappingBranch, get_async_db
from app.services.database_service import process_database_service

TEMP_DIR = Path(os.getenv("FTP_TABLETKI_TEMP_DIR", "/opt/test_project/uploads"))
FTP_UPLOADS_DIR = Path(os.getenv("FTP_TABLETKI_UPLOADS_DIR", "/var/ftp/tabletki-uploads"))
CLEANUP_OLD_ARCHIVES = os.getenv("FTP_TABLETKI_CLEANUP_OLD_ARCHIVES", "0") == "1"


def get_logger() -> logging.Logger:
    logger = logging.getLogger("ftp_tabletki")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = get_logger()


def parse_branch_from_filename(filename: str) -> tuple[str | None, datetime | None]:
    try:
        name = Path(filename).stem
        _, branch_str, datetime_str = name.split("_")
        dt = datetime.strptime(datetime_str, "%Y%m%d%H%M%S")
        return branch_str, dt
    except Exception:
        return None, None


def find_latest_zip_for_branch(branch: str) -> Path | None:
    matched: list[tuple[datetime, Path]] = []
    for file in FTP_UPLOADS_DIR.glob(f"Rest_{branch}_*.zip"):
        _, dt = parse_branch_from_filename(file.name)
        if dt:
            matched.append((dt, file))

    if not matched:
        return None

    matched.sort(reverse=True)
    return matched[0][1]


def cleanup_old_files(branch: str, keep_file: Path) -> int:
    removed = 0
    for file in FTP_UPLOADS_DIR.glob(f"Rest_{branch}_*.zip"):
        if file != keep_file:
            file.unlink(missing_ok=True)
            removed += 1
    return removed


def extract_zip_file(zip_path: Path, extract_to: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zf:
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
        offers.append(
            {
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
            }
        )
    return offers


async def validate_catalog_data(enterprise_code: str, offers: list[dict]) -> bool:
    async with get_async_db() as session:
        for i, item in enumerate(offers):
            if i >= 10:
                break
            stmt = select(InventoryData).where(
                InventoryData.enterprise_code == enterprise_code,
                InventoryData.code == item["code"],
            )
            result = await session.execute(stmt)
            db_item = result.scalars().first()
            if db_item and db_item.name == item["name"]:
                return True
    return False


def transform_catalog(data: list[dict]) -> list[dict]:
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
            "vat": item["vat"],
        }
        for item in data
    ]


def transform_stock(data: list[dict], branch: str) -> list[dict]:
    return [
        {
            "branch": branch,
            "code": item["code"],
            "price": item["price"],
            "qty": item["qty"],
            "price_reserve": item["price_reserve"],
        }
        for item in data
    ]


def save_to_json(data: list[dict], enterprise_code: str, file_type: str, branch: str) -> Path:
    dir_path = TEMP_DIR / enterprise_code
    dir_path.mkdir(parents=True, exist_ok=True)
    out_path = dir_path / f"{file_type}_{branch}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("FTP Tabletki JSON saved: path=%s records=%s", out_path, len(data))
    return out_path


async def send_catalog_data(file_path: Path, enterprise_code: str):
    await process_database_service(file_path, "catalog", enterprise_code)
    logger.info("FTP Tabletki catalog sent: path=%s", file_path)


async def send_stock_data(file_path: Path, enterprise_code: str):
    await process_database_service(file_path, "stock", enterprise_code)
    logger.info("FTP Tabletki stock sent: path=%s", file_path)


async def fetch_branches(enterprise_code: str) -> list[str]:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        return [branch for branch in result.scalars().all() if branch]


async def fetch_enterprise_settings(enterprise_code: str) -> EnterpriseSettings | None:
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()


async def run_service(enterprise_code: str, file_type: str):
    run_started_at = datetime.now()
    settings = await fetch_enterprise_settings(enterprise_code)
    branches = await fetch_branches(enterprise_code)

    if not settings:
        logger.warning("FTP Tabletki settings not found: enterprise_code=%s", enterprise_code)
    if not branches:
        logger.warning("FTP Tabletki branches not found: enterprise_code=%s", enterprise_code)
        return

    logger.info(
        "FTP Tabletki run start: enterprise_code=%s type=%s branches=%s uploads_dir=%s temp_dir=%s cleanup_old_archives=%s",
        enterprise_code,
        file_type,
        len(branches),
        FTP_UPLOADS_DIR,
        TEMP_DIR,
        CLEANUP_OLD_ARCHIVES,
    )

    processed_branches = 0
    skipped_branches = 0
    total_offers = 0
    total_records = 0
    validation_failed = 0
    cleaned_archives = 0

    for branch in branches:
        zip_file = find_latest_zip_for_branch(branch)
        if not zip_file:
            skipped_branches += 1
            logger.warning("FTP Tabletki source file not found: branch=%s", branch)
            continue

        if CLEANUP_OLD_ARCHIVES:
            cleaned_archives += cleanup_old_files(branch, zip_file)

        extract_path = TEMP_DIR / f"{enterprise_code}_{branch}_{file_type}"
        extract_path.mkdir(parents=True, exist_ok=True)
        xml_file = extract_zip_file(zip_file, extract_path)
        offers = parse_xml_file(xml_file)
        total_offers += len(offers)

        if file_type == "catalog":
            is_valid = await validate_catalog_data(enterprise_code, offers)
            if not is_valid:
                validation_failed += 1
                logger.warning(
                    "FTP Tabletki catalog validation failed: enterprise_code=%s branch=%s zip=%s offers=%s",
                    enterprise_code,
                    branch,
                    zip_file.name,
                    len(offers),
                )
                continue
            data = transform_catalog(offers)
            save_path = save_to_json(data, enterprise_code, file_type, branch)
            await send_catalog_data(save_path, enterprise_code)
        elif file_type == "stock":
            data = transform_stock(offers, branch)
            save_path = save_to_json(data, enterprise_code, file_type, branch)
            await send_stock_data(save_path, enterprise_code)
        else:
            logger.error("FTP Tabletki unknown file type: %s", file_type)
            return

        processed_branches += 1
        total_records += len(data)
        logger.info(
            "FTP Tabletki branch summary: enterprise_code=%s type=%s branch=%s zip=%s offers=%s records=%s",
            enterprise_code,
            file_type,
            branch,
            zip_file.name,
            len(offers),
            len(data),
        )

    elapsed = (datetime.now() - run_started_at).total_seconds()
    logger.info(
        "FTP Tabletki run summary: enterprise_code=%s type=%s branches=%s processed=%s skipped=%s validation_failed=%s offers=%s records=%s cleaned_archives=%s elapsed=%.2fs",
        enterprise_code,
        file_type,
        len(branches),
        processed_branches,
        skipped_branches,
        validation_failed,
        total_offers,
        total_records,
        cleaned_archives,
        elapsed,
    )


if __name__ == "__main__":
    asyncio.run(run_service("777", "catalog"))

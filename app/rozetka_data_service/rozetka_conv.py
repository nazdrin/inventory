import asyncio
import json
import logging
import os
import tempfile
import time
import xml.etree.ElementTree as ET

import requests
from dotenv import load_dotenv
from sqlalchemy.future import select

from app.database import EnterpriseSettings, MappingBranch, get_async_db
from app.services.database_service import process_database_service

load_dotenv()

DEFAULT_VAT = 20
REQUEST_TIMEOUT_SEC = 30

logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
logger.propagate = False


async def fetch_feed_url(enterprise_code: str) -> str | None:
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        feed_url = result.scalars().first()
        return feed_url if feed_url else None


async def fetch_branch_id(enterprise_code: str) -> str:
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


def download_xml(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Cache-Control": "no-cache",
    }
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
    if response.status_code == 200:
        return response.text
    raise RuntimeError(f"Ошибка загрузки: {response.status_code}")


def _safe_float(value: str | None) -> float:
    try:
        return float((value or "0").replace(",", "."))
    except Exception:
        return 0.0


def _safe_int(value: str | None) -> int:
    try:
        return int(float((value or "0").replace(",", ".")))
    except Exception:
        return 0


def parse_catalog_xml(xml_string: str) -> list[dict]:
    root = ET.fromstring(xml_string)
    offers: list[dict] = []

    for offer in root.findall(".//offer"):
        code = offer.findtext("code")
        name = offer.findtext("name")
        barcode = offer.findtext("barcode", "")
        producer = offer.findtext("brand", "N/A")

        if not code or not name:
            continue

        offers.append(
            {
                "code": code,
                "name": name.strip() if name else "no_name",
                "vat": DEFAULT_VAT,
                "producer": producer.strip() if producer else "",
                "barcode": barcode.strip() if barcode else "",
            }
        )

    return offers


def parse_stock_xml(xml_string: str, enterprise_code: str) -> list[dict]:
    root = ET.fromstring(xml_string)
    stock_data: list[dict] = []
    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    debug_file_path = os.path.join(temp_dir, f"{enterprise_code}_debug_stock_data.json")

    for offer in root.findall(".//offer"):
        code = offer.findtext("code")
        if not code:
            continue

        price = _safe_float(offer.findtext("newprice") or offer.findtext("price", "0"))
        stock_quantity = _safe_int(offer.findtext("quantity", "0"))

        stock_data.append(
            {
                "code": code,
                "price": price,
                "qty": stock_quantity,
                "price_reserve": price,
            }
        )

    with open(debug_file_path, "w", encoding="utf-8") as debug_file:
        json.dump(stock_data, debug_file, ensure_ascii=False, indent=4)

    logger.info("Rozetka stock debug JSON saved: path=%s records=%s", debug_file_path, len(stock_data))
    return stock_data


def save_to_json(data: list[dict], enterprise_code: str, file_type: str) -> str | None:
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logger.info("Rozetka %s JSON saved: path=%s records=%s", file_type, json_file_path, len(data))
        return json_file_path
    except IOError as e:
        logger.error("Ошибка при сохранении JSON-файла: %s", e)
        return None


async def run_service(enterprise_code: str, file_type: str) -> None:
    if file_type not in {"catalog", "stock"}:
        raise ValueError("Тип файла должен быть 'catalog' или 'stock'")

    run_started_at = time.monotonic()
    feed_url = await fetch_feed_url(enterprise_code)
    if not feed_url:
        logger.error("Не найден URL фида для enterprise_code=%s", enterprise_code)
        return

    branch_id = None
    if file_type == "stock":
        branch_id = await fetch_branch_id(enterprise_code)
        logger.info(
            "Rozetka stock run start: enterprise_code=%s branch=%s source_url=%s",
            enterprise_code,
            branch_id,
            feed_url,
        )
    else:
        logger.info("Rozetka catalog run start: enterprise_code=%s source_url=%s", enterprise_code, feed_url)

    download_started_at = time.monotonic()
    xml_data = download_xml(feed_url)
    logger.info(
        "Rozetka %s download summary: enterprise_code=%s bytes=%s elapsed=%.2fs",
        file_type,
        enterprise_code,
        len(xml_data.encode("utf-8")),
        time.monotonic() - download_started_at,
    )

    parse_started_at = time.monotonic()
    if file_type == "catalog":
        parsed_data = parse_catalog_xml(xml_data)
        logger.info(
            "Rozetka catalog parse summary: enterprise_code=%s records=%s elapsed=%.2fs",
            enterprise_code,
            len(parsed_data),
            time.monotonic() - parse_started_at,
        )
    else:
        parsed_data = parse_stock_xml(xml_data, enterprise_code)
        for item in parsed_data:
            item["branch"] = branch_id
        logger.info(
            "Rozetka stock parse summary: enterprise_code=%s branch=%s records=%s elapsed=%.2fs",
            enterprise_code,
            branch_id,
            len(parsed_data),
            time.monotonic() - parse_started_at,
        )

    json_file_path = save_to_json(parsed_data, enterprise_code, file_type)
    if not json_file_path:
        return

    await process_database_service(json_file_path, file_type, enterprise_code)
    if file_type == "catalog":
        logger.info(
            "Rozetka catalog run summary: enterprise_code=%s records=%s elapsed=%.2fs",
            enterprise_code,
            len(parsed_data),
            time.monotonic() - run_started_at,
        )
    else:
        logger.info(
            "Rozetka stock run summary: enterprise_code=%s branch=%s records=%s elapsed=%.2fs",
            enterprise_code,
            branch_id,
            len(parsed_data),
            time.monotonic() - run_started_at,
        )


if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE, "catalog"))

import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests
from sqlalchemy.future import select

from app.database import EnterpriseSettings, get_async_db
from app.models import MappingBranch
from app.services.database_service import process_database_service

REQUEST_TIMEOUT_SEC = 30
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_SEC = 0.5
DEFAULT_VAT = 20.0


def get_logger() -> logging.Logger:
    logger = logging.getLogger("hprofit")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = get_logger()


async def fetch_feed_url(enterprise_code: str) -> str | None:
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        value = result.scalars().first()
        return value.strip() if isinstance(value, str) and value.strip() else None


async def fetch_branch_by_enterprise_code(enterprise_code: str) -> str | None:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        branch = result.scalars().first()
        if branch is None:
            return None
        branch_value = str(branch).strip()
        return branch_value or None


def _should_retry_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def download_feed(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    started_at = time.monotonic()

    for attempt in range(1, HTTP_RETRY_ATTEMPTS + 1):
        logger.info("HProfit HTTP GET %s attempt=%s/%s", url, attempt, HTTP_RETRY_ATTEMPTS)
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
        except requests.RequestException as exc:
            logger.warning(
                "HProfit request exception attempt=%s/%s error=%s",
                attempt,
                HTTP_RETRY_ATTEMPTS,
                exc,
            )
            if attempt >= HTTP_RETRY_ATTEMPTS:
                raise
            time.sleep(HTTP_RETRY_BACKOFF_SEC * attempt)
            continue

        logger.info("HProfit HTTP response status=%s attempt=%s/%s", response.status_code, attempt, HTTP_RETRY_ATTEMPTS)
        if _should_retry_status(response.status_code) and attempt < HTTP_RETRY_ATTEMPTS:
            time.sleep(HTTP_RETRY_BACKOFF_SEC * attempt)
            continue
        if response.status_code != 200:
            raise RuntimeError(f"Ошибка загрузки: HTTP {response.status_code}")

        logger.info("HProfit download summary: bytes=%s elapsed=%.2fs", len(response.text), time.monotonic() - started_at)
        return response.text

    raise RuntimeError("Unexpected HProfit retry fallthrough")


def parse_xml_feed(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    offers = root.findall(".//offer")
    result = []
    for offer in offers:
        result.append(
            {
                "productId": offer.get("id"),
                "productName": offer.findtext("name"),
                "brand": offer.findtext("brand"),
                "barcode": offer.findtext("barcode"),
                "price": float(offer.findtext("price") or 0),
                "quantity": float(offer.findtext("quantity_in_stock") or 0),
                "reserve": 0,
            }
        )
    logger.info("HProfit parse summary: offers=%s", len(result))
    return result


def transform_catalog(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = [
        {
            "code": item.get("productId"),
            "name": item.get("productName"),
            "producer": "",
            "barcode": item.get("barcode"),
            "vat": DEFAULT_VAT,
        }
        for item in data
    ]
    logger.info("HProfit catalog transform summary: incoming=%s transformed=%s", len(data), len(result))
    return result


def transform_stock(data: list[dict[str, Any]], branch: str) -> list[dict[str, Any]]:
    result = []
    for item in data:
        try:
            qty = int(item.get("quantity", 0)) - int(item.get("reserve", 0))
        except (ValueError, TypeError):
            qty = 0

        result.append(
            {
                "branch": branch,
                "code": item.get("productId"),
                "price": item.get("price"),
                "qty": max(qty, 0),
                "price_reserve": item.get("price"),
            }
        )
    logger.info("HProfit stock transform summary: branch=%s incoming=%s transformed=%s", branch, len(data), len(result))
    return result


def save_to_json(data: list[dict[str, Any]], enterprise_code: str, file_type: str) -> str:
    temp_root = os.getenv("TEMP_FILE_PATH", "temp")
    dir_path = os.path.join(temp_root, str(enterprise_code))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{file_type}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    logger.info("HProfit JSON saved: path=%s records=%s", file_path, len(data))
    return file_path


async def run_service(enterprise_code: str, file_type: str):
    run_started_at = time.monotonic()

    url = await fetch_feed_url(enterprise_code)
    if not url:
        raise ValueError(f"HProfit feed URL not found for enterprise_code={enterprise_code}")

    feed_text = download_feed(url)
    if not feed_text.strip():
        raise ValueError("Получен пустой фид")

    try:
        raw_data = parse_xml_feed(feed_text)
    except Exception as exc:
        raise ValueError(f"Ошибка разбора XML: {exc}") from exc

    if file_type == "catalog":
        data = transform_catalog(raw_data)
        path = save_to_json(data, enterprise_code, "catalog")
        logger.info(
            "HProfit catalog run summary: enterprise_code=%s records=%s elapsed=%.2fs",
            enterprise_code,
            len(data),
            time.monotonic() - run_started_at,
        )
        await process_database_service(path, "catalog", enterprise_code)
    elif file_type == "stock":
        branch = await fetch_branch_by_enterprise_code(enterprise_code)
        if not branch:
            raise ValueError(f"HProfit branch not found for enterprise_code={enterprise_code}")
        data = transform_stock(raw_data, branch)
        path = save_to_json(data, enterprise_code, "stock")
        logger.info(
            "HProfit stock run summary: enterprise_code=%s branch=%s records=%s elapsed=%.2fs",
            enterprise_code,
            branch,
            len(data),
            time.monotonic() - run_started_at,
        )
        await process_database_service(path, "stock", enterprise_code)
    else:
        raise ValueError("Тип файла должен быть 'catalog' или 'stock'")

import json
import logging
import os
import tempfile
import time
from typing import Any, Dict, Tuple

import requests
from sqlalchemy.future import select

from app.database import EnterpriseSettings, get_async_db

PROM_API_URL = "https://my.prom.ua/api/v1/products/list"
LIMIT = 100000
REQUEST_TIMEOUT_SEC = 30
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_SEC = 0.5


class PromFetchError(RuntimeError):
    pass


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"prom.{name}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def _should_retry_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def fetch_products(api_key: str, logger: logging.Logger, limit: int = LIMIT) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    params = {"limit": limit}
    started_at = time.monotonic()

    for attempt in range(1, HTTP_RETRY_ATTEMPTS + 1):
        logger.info("Prom HTTP GET %s attempt=%s/%s limit=%s", PROM_API_URL, attempt, HTTP_RETRY_ATTEMPTS, limit)
        try:
            response = requests.get(PROM_API_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SEC)
        except requests.RequestException as exc:
            logger.warning(
                "Prom request exception attempt=%s/%s error=%s",
                attempt,
                HTTP_RETRY_ATTEMPTS,
                exc,
            )
            if attempt >= HTTP_RETRY_ATTEMPTS:
                raise
            time.sleep(HTTP_RETRY_BACKOFF_SEC * attempt)
            continue

        logger.info("Prom HTTP response status=%s attempt=%s/%s", response.status_code, attempt, HTTP_RETRY_ATTEMPTS)
        if _should_retry_status(response.status_code) and attempt < HTTP_RETRY_ATTEMPTS:
            time.sleep(HTTP_RETRY_BACKOFF_SEC * attempt)
            continue
        if response.status_code != 200:
            raise PromFetchError(f"Prom request failed: status={response.status_code} body={response.text[:200]}")
        payload = response.json()
        products = payload.get("products", []) or []
        summary = {
            "fetched_records": len(products),
            "limit": limit,
            "elapsed": time.monotonic() - started_at,
        }
        logger.info(
            "Prom fetch summary: fetched=%s limit=%s elapsed=%.2fs",
            summary["fetched_records"],
            summary["limit"],
            summary["elapsed"],
        )
        if len(products) >= limit:
            logger.warning("Prom fetch reached configured limit=%s; pagination support may be needed", limit)
        return payload, summary

    raise RuntimeError("Unexpected Prom retry fallthrough")


async def fetch_enterprise_settings(enterprise_code: str) -> EnterpriseSettings | None:
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()


def save_to_json(data: list[dict], enterprise_code: str, file_type: str, logger: logging.Logger) -> str | None:
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logger.info("Prom JSON saved: path=%s records=%s", json_file_path, len(data))
        return json_file_path
    except IOError as exc:
        logger.error("Ошибка при сохранении JSON-файла: %s", exc)
        return None

import json
import logging
import os
import tempfile
import time
from typing import Optional

import requests
from sqlalchemy.future import select

from app.database import EnterpriseSettings, MappingBranch, get_async_db

REQUEST_TIMEOUT_SEC = 30
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_SEC = 0.5
DEBUG_JSON_ENABLED = os.getenv("DSN_DEBUG_JSON", "0") == "1"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"dsn.{name}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def _should_retry_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


async def fetch_feed_url(enterprise_code: str) -> Optional[str]:
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        feed_url = result.scalars().first()
        return feed_url.strip() if isinstance(feed_url, str) and feed_url.strip() else None


async def fetch_branch_id(enterprise_code: str) -> Optional[str]:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        branch = result.scalars().first()
        if branch is None:
            return None
        branch_str = str(branch).strip()
        return branch_str or None


def download_xml(url: str, logger: logging.Logger) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    started_at = time.monotonic()

    for attempt in range(1, HTTP_RETRY_ATTEMPTS + 1):
        logger.info("DSN HTTP GET %s attempt=%s/%s", url, attempt, HTTP_RETRY_ATTEMPTS)
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
        except requests.RequestException as exc:
            logger.warning(
                "DSN request exception attempt=%s/%s error=%s",
                attempt,
                HTTP_RETRY_ATTEMPTS,
                exc,
            )
            if attempt >= HTTP_RETRY_ATTEMPTS:
                raise
            time.sleep(HTTP_RETRY_BACKOFF_SEC * attempt)
            continue

        logger.info("DSN HTTP response status=%s attempt=%s/%s", response.status_code, attempt, HTTP_RETRY_ATTEMPTS)
        if _should_retry_status(response.status_code) and attempt < HTTP_RETRY_ATTEMPTS:
            time.sleep(HTTP_RETRY_BACKOFF_SEC * attempt)
            continue
        if response.status_code != 200:
            raise RuntimeError(f"Ошибка загрузки XML: HTTP {response.status_code}")

        logger.info("DSN download summary: bytes=%s elapsed=%.2fs", len(response.text), time.monotonic() - started_at)
        return response.text

    raise RuntimeError("Unexpected DSN retry fallthrough")


def maybe_save_debug_json(data: list[dict], enterprise_code: str, suffix: str, logger: logging.Logger) -> None:
    if not DEBUG_JSON_ENABLED:
        return

    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    os.makedirs(temp_dir, exist_ok=True)
    debug_file_path = os.path.join(temp_dir, f"{enterprise_code}_debug_{suffix}.json")

    with open(debug_file_path, "w", encoding="utf-8") as debug_file:
        json.dump(data, debug_file, ensure_ascii=False, indent=4)

    logger.info("DSN debug JSON saved: path=%s records=%s", debug_file_path, len(data))


def save_to_json(data: list[dict], enterprise_code: str, file_type: str, logger: logging.Logger) -> str | None:
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logger.info("DSN JSON saved: path=%s records=%s", json_file_path, len(data))
        return json_file_path
    except IOError as exc:
        logger.error("Ошибка при сохранении JSON-файла: %s", exc)
        return None

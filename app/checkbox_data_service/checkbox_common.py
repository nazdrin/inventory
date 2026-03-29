import json
import logging
import os
import tempfile
import time
from typing import Any, Dict, List, Tuple

import requests
from sqlalchemy.future import select

from app.database import EnterpriseSettings, MappingBranch, get_async_db

LIMIT = 1000
API_URL = "https://api.checkbox.ua/api/v1/goods"
AUTH_URL = os.getenv("CHECKBOX_AUTH_URL", "https://api.checkbox.in.ua/api/v1/cashier/signin")
REQUEST_TIMEOUT_SEC = 30
HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_SEC = 0.5


class CheckboxFetchError(RuntimeError):
    pass


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"checkbox.{name}")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def _should_retry_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def _request_with_retry(
    method: str,
    url: str,
    *,
    logger: logging.Logger,
    headers: Dict[str, str] | None = None,
    json_payload: Dict[str, Any] | None = None,
    params: Dict[str, Any] | None = None,
) -> requests.Response:
    for attempt in range(1, HTTP_RETRY_ATTEMPTS + 1):
        logger.info("Checkbox HTTP %s %s attempt=%s/%s", method.upper(), url, attempt, HTTP_RETRY_ATTEMPTS)
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                json=json_payload,
                params=params,
                timeout=REQUEST_TIMEOUT_SEC,
            )
        except requests.RequestException as exc:
            logger.warning(
                "Checkbox HTTP exception method=%s url=%s attempt=%s/%s error=%s",
                method.upper(),
                url,
                attempt,
                HTTP_RETRY_ATTEMPTS,
                exc,
            )
            if attempt >= HTTP_RETRY_ATTEMPTS:
                raise
            time.sleep(HTTP_RETRY_BACKOFF_SEC * attempt)
            continue

        logger.info(
            "Checkbox HTTP response method=%s url=%s status=%s attempt=%s/%s",
            method.upper(),
            url,
            response.status_code,
            attempt,
            HTTP_RETRY_ATTEMPTS,
        )
        if _should_retry_status(response.status_code) and attempt < HTTP_RETRY_ATTEMPTS:
            time.sleep(HTTP_RETRY_BACKOFF_SEC * attempt)
            continue
        return response

    raise RuntimeError(f"Unexpected retry fallthrough for Checkbox request: {method.upper()} {url}")


def parse_login_password(raw: str) -> Tuple[str, str]:
    if not raw or "," not in raw:
        raise ValueError("В EnterpriseSettings.token ожидается строка 'login,password'.")
    login, password = [part.strip() for part in raw.split(",", 1)]
    if not login or not password:
        raise ValueError("Пустые login/password в EnterpriseSettings.token.")
    return login, password


async def fetch_enterprise_settings(enterprise_code: str) -> EnterpriseSettings | None:
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()


async def fetch_branch_by_enterprise(enterprise_code: str) -> str:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        branch = result.scalars().first()
    if not branch:
        raise ValueError(f"Checkbox stock misconfiguration: branch not found for enterprise_code={enterprise_code}")
    return str(branch)


def signin_get_api_key(login: str, password: str, logger: logging.Logger) -> str:
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    payload = {"login": login, "password": password}
    response = _request_with_retry(
        "post",
        AUTH_URL,
        logger=logger,
        headers=headers,
        json_payload=payload,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Checkbox signin error: {response.status_code} {response.text[:200]}")
    data = response.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Ответ авторизации Checkbox без 'access_token'.")
    return token


async def resolve_api_key(enterprise_code: str, logger: logging.Logger) -> str:
    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        raise ValueError(f"EnterpriseSettings не найден для enterprise_code={enterprise_code}")
    login, password = parse_login_password(enterprise_settings.token or "")
    logger.info(
        "Checkbox auth context: enterprise_code=%s auth_url=%s login=%s",
        enterprise_code,
        AUTH_URL,
        login,
    )
    started_at = time.monotonic()
    token = signin_get_api_key(login, password, logger)
    logger.info("Checkbox auth summary: enterprise_code=%s elapsed=%.2fs", enterprise_code, time.monotonic() - started_at)
    return token


def fetch_products_page(api_key: str, offset: int, logger: logging.Logger, limit: int = LIMIT) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    params = {
        "without_group_only": "false",
        "load_children": "false",
        "load_group": "false",
        "offset": offset,
        "limit": limit,
    }
    response = _request_with_retry("get", API_URL, logger=logger, headers=headers, params=params)
    if response.status_code != 200:
        raise CheckboxFetchError(f"Checkbox goods request failed: offset={offset} status={response.status_code}")
    return response.json()


def fetch_all_products(api_key: str, logger: logging.Logger, limit: int = LIMIT) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    all_products: List[Dict[str, Any]] = []
    offset = 0
    pages_fetched = 0
    started_at = time.monotonic()

    while True:
        response = fetch_products_page(api_key, offset=offset, logger=logger, limit=limit)
        products = response.get("results", []) or []
        if not products:
            break
        pages_fetched += 1
        all_products.extend(products)
        logger.info("Checkbox fetch progress: offset=%s fetched_page=%s total=%s", offset, len(products), len(all_products))
        if len(products) < limit:
            break
        offset += limit

    elapsed = time.monotonic() - started_at
    logger.info(
        "Checkbox fetch summary: pages=%s fetched=%s limit=%s elapsed=%.2fs",
        pages_fetched,
        len(all_products),
        limit,
        elapsed,
    )
    return all_products, {
        "pages_fetched": pages_fetched,
        "fetched_records": len(all_products),
        "elapsed": elapsed,
    }


def save_to_json(data: List[Dict[str, Any]], enterprise_code: str, file_type: str, logger: logging.Logger) -> str | None:
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")
        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        logger.info("Checkbox JSON saved: path=%s records=%s", json_file_path, len(data))
        return json_file_path
    except IOError as exc:
        logger.error("Ошибка при сохранении JSON-файла: %s", exc)
        return None

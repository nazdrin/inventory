import asyncio
import logging
import os
from time import perf_counter
from typing import Any, Dict, Optional

import aiohttp

DNTRADE_PRODUCTS_URL = "https://api.dntrade.com.ua/products/list"
DEFAULT_LIMIT = 100
REQUEST_TIMEOUT_SECONDS = int(os.getenv("DNTRADE_REQUEST_TIMEOUT_SEC", "30"))
MAX_RETRIES = int(os.getenv("DNTRADE_MAX_RETRIES", "3"))
RETRY_BASE_DELAY_SECONDS = float(os.getenv("DNTRADE_RETRY_BASE_DELAY_SEC", "1.2"))

logger = logging.getLogger(__name__)


async def fetch_products_page(
    session: aiohttp.ClientSession,
    api_key: str,
    offset: int = 0,
    limit: int = DEFAULT_LIMIT,
    store_id: Optional[str] = None,
    modified_from: Optional[str] = None,
    modified_to: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Fetch one DNTrade products page."""
    headers = {
        "ApiKey": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    params: Dict[str, Any] = {
        "limit": limit,
        "offset": offset,
    }
    if store_id is not None:
        params["store_id"] = store_id
    if modified_from is not None:
        params["modified_from"] = modified_from
    if modified_to is not None:
        params["modified_to"] = modified_to

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    for attempt in range(1, MAX_RETRIES + 1):
        request_started = perf_counter()
        try:
            if attempt == 1 and offset == 0 and (modified_from is not None or modified_to is not None):
                logger.info(
                    "DNTrade delta request params: offset=%s limit=%s store_id=%s modified_from=%s modified_to=%s",
                    offset,
                    limit,
                    store_id,
                    modified_from,
                    modified_to,
                )
            async with session.post(
                DNTRADE_PRODUCTS_URL,
                params=params,
                headers=headers,
                json={},
                timeout=timeout,
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    logger.warning(
                        "DNTrade API non-200: status=%s offset=%s store_id=%s modified_from=%s modified_to=%s attempt=%s body=%s",
                        response.status,
                        offset,
                        store_id,
                        modified_from,
                        modified_to,
                        attempt,
                        body[:300],
                    )
                    if response.status in {429, 500, 502, 503, 504} and attempt < MAX_RETRIES:
                        await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)
                        continue
                    return None

                data = await response.json()
                if not isinstance(data, dict):
                    logger.warning(
                        "DNTrade payload is not object: type=%s offset=%s store_id=%s modified_from=%s modified_to=%s",
                        type(data),
                        offset,
                        store_id,
                        modified_from,
                        modified_to,
                    )
                    return None
                logger.debug(
                    "DNTrade request success: offset=%s store_id=%s modified_from=%s modified_to=%s attempt=%s elapsed=%.3fs",
                    offset,
                    store_id,
                    modified_from,
                    modified_to,
                    attempt,
                    perf_counter() - request_started,
                )
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning(
                "DNTrade request error: offset=%s store_id=%s modified_from=%s modified_to=%s attempt=%s elapsed=%.3fs error=%s",
                offset,
                store_id,
                modified_from,
                modified_to,
                attempt,
                perf_counter() - request_started,
                exc,
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)
                continue
            return None

    return None

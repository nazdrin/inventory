import asyncio
import logging
from typing import Any, Dict, Optional

import aiohttp

DNTRADE_PRODUCTS_URL = "https://api.dntrade.com.ua/products/list"
DEFAULT_LIMIT = 100
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3

logger = logging.getLogger(__name__)


async def fetch_products_page(
    session: aiohttp.ClientSession,
    api_key: str,
    offset: int = 0,
    limit: int = DEFAULT_LIMIT,
    store_id: Optional[str] = None,
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

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
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
                        "DNTrade API non-200: status=%s offset=%s store_id=%s attempt=%s body=%s",
                        response.status,
                        offset,
                        store_id,
                        attempt,
                        body[:300],
                    )
                    if response.status in {429, 500, 502, 503, 504} and attempt < MAX_RETRIES:
                        await asyncio.sleep(1.2 * attempt)
                        continue
                    return None

                data = await response.json()
                if not isinstance(data, dict):
                    logger.warning(
                        "DNTrade payload is not object: type=%s offset=%s store_id=%s",
                        type(data),
                        offset,
                        store_id,
                    )
                    return None
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning(
                "DNTrade request error: offset=%s store_id=%s attempt=%s error=%s",
                offset,
                store_id,
                attempt,
                exc,
            )
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1.2 * attempt)
                continue
            return None

    return None

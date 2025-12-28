from __future__ import annotations

from datetime import datetime
from typing import Any
import os

import httpx

SALES_DRIVE_URL = os.getenv("SALESDRIVE_BASE_URL")
SALES_DRIVE_KEY = os.getenv("SALESDRIVE_API_KEY")


async def fetch_orders_for_segment(
    city: str,
    supplier_aliases: list[str],
    start_dt: datetime,
    end_dt: datetime,
) -> list[dict[str, Any]]:
    """
    Тянем список заявок за период по orderTime[from/to], затем фильтруем по city и supplier (по списку алиасов).
    Пока без пагинации (limit=100) — на первом запуске проверяем корректность.
    """

    if not SALES_DRIVE_URL:
        raise RuntimeError("SALESDRIVE_BASE_URL is not set")
    if not SALES_DRIVE_KEY:
        raise RuntimeError("SALESDRIVE_API_KEY is not set")

    params = {
        "limit": 100,
        "filter[orderTime][from]": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "filter[orderTime][to]": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }

    headers = {"X-Api-Key": SALES_DRIVE_KEY}

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{SALES_DRIVE_URL}/api/order/list/",
            params=params,
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()

    orders: list[dict[str, Any]] = []
    for row in data.get("data", []):
        if row.get("city") != city:
            continue
        if row.get("supplier") not in supplier_aliases:
            continue
        orders.append(row)

    return orders
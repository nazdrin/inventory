from __future__ import annotations

import logging
import os
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import EnterpriseSettings


logger = logging.getLogger("salesdrive.client")

DEFAULT_SALESDRIVE_BASE_URL = "https://petrenko.salesdrive.me"


async def get_salesdrive_api_key(session: AsyncSession, enterprise_code: str) -> str | None:
    result = await session.execute(
        select(EnterpriseSettings.token)
        .where(EnterpriseSettings.enterprise_code == str(enterprise_code))
        .limit(1)
    )
    token = result.scalar_one_or_none()
    return str(token).strip() if token else None


async def update_order_field(
    *,
    api_key: str,
    order_id: str | None,
    external_id: str | None,
    field_name: str,
    value: Any,
) -> bool:
    base_url = os.getenv("SALESDRIVE_BASE_URL", DEFAULT_SALESDRIVE_BASE_URL).rstrip("/")
    url = f"{base_url}/api/order/update/"
    payload: dict[str, Any] = {"data": {field_name: value}}
    if external_id:
        payload["externalId"] = external_id
    elif order_id:
        payload["id"] = order_id
    else:
        raise ValueError("SalesDrive update requires order_id or external_id")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(url, headers=headers, json=payload)
    if 200 <= response.status_code < 300:
        return True
    logger.warning(
        "SalesDrive order field update failed: status=%s body=%s",
        response.status_code,
        response.text[:1000],
    )
    return False

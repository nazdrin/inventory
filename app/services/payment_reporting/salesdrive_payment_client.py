from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import httpx

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


logger = logging.getLogger(__name__)

PaymentType = Literal["incoming", "outcoming"]


@dataclass(frozen=True)
class SalesDrivePaymentClientConfig:
    base_url: str
    api_key: str
    timeout_seconds: int = 30
    page_limit: int = 100
    rate_limit_retry_seconds: int = 65
    rate_limit_max_retries: int = 2


def _int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be an integer")


def load_salesdrive_payment_client_config() -> SalesDrivePaymentClientConfig:
    base_url = str(os.getenv("SALESDRIVE_PAYMENTS_BASE_URL") or os.getenv("SALESDRIVE_BASE_URL") or "").strip()
    api_key = str(os.getenv("SALESDRIVE_PAYMENTS_API_KEY") or "").strip()
    if not base_url:
        raise RuntimeError("SALESDRIVE_PAYMENTS_BASE_URL is not set")
    if not api_key:
        raise RuntimeError("SALESDRIVE_PAYMENTS_API_KEY is not set")

    page_limit = _int_env("SALESDRIVE_PAYMENTS_PAGE_LIMIT", 100)
    if page_limit <= 0 or page_limit > 100:
        raise RuntimeError("SALESDRIVE_PAYMENTS_PAGE_LIMIT must be between 1 and 100")

    return SalesDrivePaymentClientConfig(
        base_url=base_url.rstrip("/"),
        api_key=api_key,
        timeout_seconds=_int_env("SALESDRIVE_PAYMENTS_TIMEOUT_SECONDS", 30),
        page_limit=page_limit,
        rate_limit_retry_seconds=_int_env("SALESDRIVE_PAYMENTS_RATE_LIMIT_RETRY_SECONDS", 65),
        rate_limit_max_retries=_int_env("SALESDRIVE_PAYMENTS_RATE_LIMIT_MAX_RETRIES", 2),
    )


def _format_salesdrive_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _extract_total_pages(payload: dict[str, Any]) -> int | None:
    pagination = payload.get("pagination")
    if not isinstance(pagination, dict):
        return None
    for key in ("totalPages", "total_pages", "pages", "lastPage", "last_page"):
        value = pagination.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    total = pagination.get("total") or pagination.get("count")
    limit = pagination.get("limit") or pagination.get("perPage") or pagination.get("per_page")
    try:
        total_int = int(total)
        limit_int = int(limit)
    except (TypeError, ValueError):
        return None
    if limit_int <= 0:
        return None
    return (total_int + limit_int - 1) // limit_int


class SalesDrivePaymentClient:
    def __init__(self, config: SalesDrivePaymentClientConfig | None = None):
        self.config = config or load_salesdrive_payment_client_config()

    async def fetch_payments(
        self,
        *,
        payment_type: PaymentType,
        period_from: datetime,
        period_to: datetime,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1

        async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as client:
            while True:
                payload = await self._fetch_page(
                    client,
                    payment_type=payment_type,
                    period_from=period_from,
                    period_to=period_to,
                    page=page,
                )
                data = payload.get("data") or []
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected SalesDrive payment response data type: {type(data).__name__}")

                rows.extend([item for item in data if isinstance(item, dict)])
                total_pages = _extract_total_pages(payload)
                if total_pages is not None:
                    if page >= total_pages:
                        break
                elif len(data) < self.config.page_limit:
                    break
                page += 1

        return rows

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        *,
        payment_type: PaymentType,
        period_from: datetime,
        period_to: datetime,
        page: int,
    ) -> dict[str, Any]:
        params = {
            "type": payment_type,
            "filter[date][from]": _format_salesdrive_datetime(period_from),
            "filter[date][to]": _format_salesdrive_datetime(period_to),
            "page": page,
            "limit": self.config.page_limit,
        }
        headers = {"X-Api-Key": self.config.api_key}
        url = f"{self.config.base_url}/api/payment/list/"

        attempts = self.config.rate_limit_max_retries + 1
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = await client.get(url, params=params, headers=headers)
                if response.status_code == 429 and attempt < attempts:
                    await asyncio.sleep(self.config.rate_limit_retry_seconds)
                    continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError("Unexpected SalesDrive payment response: root is not an object")
                return payload
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                logger.warning(
                    "SalesDrive payment page fetch failed; retrying. type=%s page=%s attempt=%s/%s error=%s",
                    payment_type,
                    page,
                    attempt,
                    attempts,
                    exc,
                )
                await asyncio.sleep(self.config.rate_limit_retry_seconds)

        raise RuntimeError(f"SalesDrive payment fetch failed for type={payment_type} page={page}: {last_error}")

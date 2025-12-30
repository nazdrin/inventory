from __future__ import annotations

from datetime import datetime
from typing import Any
import os
import json
import logging
import httpx

# Ensure local `.env` is loaded when running ad-hoc `python -c ...` commands.
# In the main app, env vars may already be present; load_dotenv is safe to call.
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass


def _get_salesdrive_url() -> str | None:
    return os.getenv("SALESDRIVE_BASE_URL")


def _get_salesdrive_key() -> str | None:
    return os.getenv("SALESDRIVE_API_KEY")


logger = logging.getLogger(__name__)


def _norm(v: Any) -> str:
    """Normalize values coming from SalesDrive for comparisons."""
    if v is None:
        return ""
    # Some fields can arrive as dicts/objects; stringify safely.
    if isinstance(v, (dict, list)):
        try:
            v = json.dumps(v, ensure_ascii=False, sort_keys=True)
        except Exception:
            v = str(v)
    s = str(v)
    return " ".join(s.strip().split()).lower()


def _extract_supplier_value(row: dict[str, Any]) -> Any:
    """Try to extract supplier identifier/name from various possible fields."""
    # Primary expected field
    if "supplier" in row:
        return row.get("supplier")

    # Sometimes the supplier is stored in nested objects or custom structures.
    # Try a few common patterns without assuming schema.
    for k in ("supplierName", "supplier_name", "enterprise", "enterpriseName", "partner", "company"):
        if k in row:
            return row.get(k)

    # If products exist, supplier may be stored per-line.
    products = row.get("products") or row.get("items")
    if isinstance(products, list) and products:
        first = products[0]
        if isinstance(first, dict):
            for k in ("supplier", "supplierName", "supplier_name", "enterprise", "enterpriseName"):
                if k in first:
                    return first.get(k)

    # Some APIs keep supplier in custom fields.
    cf = row.get("customFields") or row.get("custom_fields")
    if isinstance(cf, dict):
        for k in ("supplier", "supplier_name", "supplierName"):
            if k in cf:
                return cf.get(k)

    return None


def _extract_city_value(row: dict[str, Any]) -> Any:
    if "city" in row:
        return row.get("city")
    for k in ("deliveryCity", "delivery_city", "shippingCity", "shipping_city"):
        if k in row:
            return row.get(k)
    return None


async def fetch_orders_for_segment(
    city: str | None,
    supplier_aliases: list[str],
    start_dt: datetime,
    end_dt: datetime,
    *,
    city_aliases: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Тянем список заявок за период по orderTime[from/to], затем фильтруем по city и supplier (по списку алиасов).
    Пока без пагинации (limit=100) — на первом запуске проверяем корректность.
    """

    # City can appear in different languages/spellings in SalesDrive.
    # If `city` is None/empty -> do not filter by city (useful for debugging).
    norm_cities: set[str] = set()
    if city:
        norm_cities.add(_norm(city))
    if city_aliases:
        norm_cities.update({_norm(c) for c in city_aliases if c})

    # Supplier can be stored as a readable name (e.g. "Biotus") while our code is "D1".
    # If `supplier_aliases` is empty -> do not filter by supplier (useful for debugging).
    norm_aliases = {_norm(a) for a in supplier_aliases if a}

    sales_drive_url = _get_salesdrive_url()
    sales_drive_key = _get_salesdrive_key()

    if not sales_drive_url:
        raise RuntimeError("SALESDRIVE_BASE_URL is not set")
    if not sales_drive_key:
        raise RuntimeError("SALESDRIVE_API_KEY is not set")

    params = {
        "limit": 100,
        "filter[orderTime][from]": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "filter[orderTime][to]": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
    }

    headers = {"X-Api-Key": sales_drive_key}

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{sales_drive_url}/api/order/list/",
            params=params,
            headers=headers,
        )
        resp.raise_for_status()
        data = resp.json()

    orders: list[dict[str, Any]] = []
    debug = os.getenv("BALANCER_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}

    if debug:
        logger.info(
            "SalesDrive fetched %s rows for period %s..%s",
            len(data.get("data", []) or []),
            params["filter[orderTime][from]"],
            params["filter[orderTime][to]"],
        )

    for row in data.get("data", []):
        row_city = _norm(_extract_city_value(row))
        if norm_cities and row_city not in norm_cities:
            if debug:
                logger.info(
                    "SalesDrive row filtered by city. row_city=%s expected=%s order_id=%s",
                    row_city,
                    sorted(norm_cities),
                    row.get("id") or row.get("orderId") or row.get("order_id"),
                )
            continue

        row_supplier = _norm(_extract_supplier_value(row))
        if norm_aliases and row_supplier not in norm_aliases:
            if debug:
                logger.info(
                    "SalesDrive row filtered by supplier. city=%s supplier=%s aliases=%s order_id=%s",
                    row_city,
                    row_supplier,
                    sorted(norm_aliases),
                    row.get("id") or row.get("orderId") or row.get("order_id"),
                )
            continue

        orders.append(row)

    return orders
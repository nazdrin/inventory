# feed_suziria.py
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Literal

import httpx
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SUZIRIA_API_URL = "https://b2b.suziria.ua/rest/"
WAREHOUSE_MAIN = "main"


async def _get_token_from_db(*, code: str) -> Optional[str]:
    """
    Для Suziria токен хранится в dropship_enterprises.feed_url.
    code = 'D8'
    """
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        token = res.scalar_one_or_none()
        return (token or "").strip() or None


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(v))
    except (TypeError, ValueError):
        return default


async def _fetch_suziria_catalog_json(*, token: str, timeout: int = 30) -> Dict[str, Any]:
    """
    GET https://b2b.suziria.ua/rest/?type=catalog&token=...
    Ожидаем JSON вида: { "status": true/false, "type": "catalog", "count": N, "data": [...] }
    """
    params = {"type": "catalog", "token": token}

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.get(SUZIRIA_API_URL, params=params)
        resp.raise_for_status()
        return resp.json()


def _extract_main_qty(product: Dict[str, Any]) -> int:
    """
    stock: [
      {"warehouse": "main", "quantity": 1},
      {"warehouse": "kyiv", "available": 1}
    ]
    Берем именно warehouse=main, поле quantity (на всякий случай поддержим available).
    """
    for row in product.get("stock", []) or []:
        if (row or {}).get("warehouse") == WAREHOUSE_MAIN:
            if "quantity" in row:
                return _to_int(row.get("quantity"), 0)
            if "available" in row:
                return _to_int(row.get("available"), 0)
            return 0
    return 0


def _extract_price(product: Dict[str, Any], price_type: str) -> float:
    """
    prices: [
      {"type":"ppc", "value": 6129.5},
      {"type":"opt", "value": 4715.04}
    ]
    """
    for p in product.get("prices", []) or []:
        if (p or {}).get("type") == price_type:
            return _to_float(p.get("value"), 0.0)
    return 0.0


async def _parse_catalog(*, token: str, timeout: int) -> str:
    payload = await _fetch_suziria_catalog_json(token=token, timeout=timeout)

    if not payload.get("status", False):
        errors = payload.get("errors")
        msg = f"Suziria API returned status=false. errors={errors}"
        logger.error(msg)
        await send_notification(msg)
        return "[]"

    data = payload.get("data") or []
    items: List[Dict[str, str]] = []

    for product in data:
        if not isinstance(product, dict):
            continue

        if product.get("active") is not True:
            continue

        article = (product.get("article") or "").strip()
        name = (product.get("name") or "").strip()
        barcode = (product.get("barcode") or "").strip()

        if not (article and name):
            continue

        items.append(
            {
                "id": article,          # id -> article
                "name": name,           # name -> name
                "barcode": barcode,     # barcode -> barcode
            }
        )

    logger.info("Suziria каталог: зібрано позицій: %d", len(items))
    return json.dumps(items, ensure_ascii=False, indent=2)


async def _parse_stock(*, token: str, timeout: int) -> str:
    payload = await _fetch_suziria_catalog_json(token=token, timeout=timeout)

    if not payload.get("status", False):
        errors = payload.get("errors")
        msg = f"Suziria API returned status=false. errors={errors}"
        logger.error(msg)
        await send_notification(msg)
        return "[]"

    data = payload.get("data") or []
    rows: List[Dict[str, Any]] = []

    for product in data:
        if not isinstance(product, dict):
            continue

        # Берем только активные? В ТЗ активность указана для каталога,
        # но логично не тащить мертвые позиции и в сток.
        if product.get("active") is not True:
            continue

        article = (product.get("article") or "").strip()
        if not article:
            continue

        qty = _extract_main_qty(product)
        if qty <= 0:
            continue  # для стока берем только qty > 0

        price_retail = _extract_price(product, "ppc")
        price_opt = _extract_price(product, "opt")

        rows.append(
            {
                "code_sup": article,         # code_sup -> article
                "qty": qty,                  # qty -> stock[warehouse=main].quantity
                "price_retail": price_retail, # prices[type=ppc].value
                "price_opt": price_opt,       # prices[type=opt].value
            }
        )

    logger.info("Suziria сток: зібрано позицій: %d", len(rows))
    return json.dumps(rows, ensure_ascii=False, indent=2)


async def parse_suziria_catalog_to_json(*, code: str = "D8", timeout: int = 30) -> str:
    """Повертає каталог Suziria у форматі, який очікує наш сервіс каталогу."""
    return await parse_suziria_feed_to_json(mode="catalog", code=code, timeout=timeout)


async def parse_suziria_stock_to_json(*, code: str = "D8", timeout: int = 30) -> str:
    """Повертає залишки+ціни Suziria у форматі, який очікує наш сервіс стока."""
    return await parse_suziria_feed_to_json(mode="stock", code=code, timeout=timeout)


async def parse_suziria_feed_to_json(
    *,
    mode: Literal["catalog", "stock"] = "catalog",
    code: str = "D8",
    timeout: int = 30,
) -> str:
    """
    Точка входа:
      - берет token из dropship_enterprises.feed_url по code='D8'
      - тянет /rest/?type=catalog&token=... (API повертає і каталог, і сток-поля в одному списку)
      - возвращает JSON-строку нужного формата
    """
    token = await _get_token_from_db(code=code)
    if not token:
        msg = f"Suziria: token not found in dropship_enterprises.feed_url for code={code}"
        logger.error(msg)
        await send_notification(msg)
        return "[]"

    try:
        if mode == "catalog":
            return await _parse_catalog(token=token, timeout=timeout)
        if mode == "stock":
            return await _parse_stock(token=token, timeout=timeout)

        raise ValueError("mode must be 'catalog' or 'stock'")

    except httpx.HTTPError as e:
        msg = f"Suziria HTTP error (mode={mode}, code={code}): {e}"
        logger.exception(msg)
        await send_notification(msg)
        return "[]"
    except Exception as e:
        msg = f"Suziria unexpected error (mode={mode}, code={code}): {e}"
        logger.exception(msg)
        await send_notification(msg)
        return "[]"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Парсер Suziria API: режими 'catalog' (каталог) і 'stock' (залишки/ціни). "
                    "Token береться з dropship_enterprises.feed_url по code (за замовчуванням D8)."
    )
    parser.add_argument(
        "--mode",
        choices=["catalog", "stock"],
        default="catalog",
        help="Режим: catalog | stock (за замовчуванням catalog)",
    )
    parser.add_argument(
        "--code",
        default="D8",
        help="значення поля code у dropship_enterprises (за замовчуванням D8)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="таймаут HTTP-запиту, сек.",
    )

    args = parser.parse_args()
    if args.mode == "catalog":
        out = asyncio.run(parse_suziria_catalog_to_json(code=args.code, timeout=args.timeout))
    else:
        out = asyncio.run(parse_suziria_stock_to_json(code=args.code, timeout=args.timeout))

    print(out)
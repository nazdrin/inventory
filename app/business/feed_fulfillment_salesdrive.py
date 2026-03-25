from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional

import httpx
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

logger = logging.getLogger(__name__)


def _to_int(val: Optional[str]) -> int:
    if not val:
        return 0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return max(int(float(s)), 0)
    except Exception:
        return 0


def _to_float(val: Optional[str]) -> float:
    if not val:
        return 0.0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _round_money(val: float) -> float:
    try:
        return float(Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        return 0.0


async def _get_feed_url_by_code(code: str) -> Optional[str]:
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


async def _get_profit_percent_by_code(code: str) -> float:
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT profit_percent FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        raw = res.scalar_one_or_none()

    try:
        val = float(raw)
    except Exception:
        val = 0.0

    if val > 1:
        val = val / 100.0
    if val < 0:
        val = 0.0
    if val > 1:
        val = 1.0
    return val


async def _load_feed_root(*, code: str, timeout: int) -> Optional[ET.Element]:
    feed_url = await _get_feed_url_by_code(code)
    if not feed_url:
        msg = f"Fulfillment SalesDrive: feed_url not found for code='{code}'"
        logger.error(msg)
        send_notification(msg, "Розробник")
        return None

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
            resp = await client.get(feed_url)
            resp.raise_for_status()
            xml_text = resp.text
    except Exception as exc:
        msg = f"Fulfillment SalesDrive: feed download failed for code='{code}' url='{feed_url}': {exc}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None

    try:
        return ET.fromstring(xml_text)
    except Exception as exc:
        msg = f"Fulfillment SalesDrive: XML parse failed for code='{code}' url='{feed_url}': {exc}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None


def _collect_offer_nodes(root: ET.Element) -> List[ET.Element]:
    return root.findall(".//offer")


def _extract_offer_id(offer: ET.Element) -> Optional[str]:
    offer_id = offer.get("id")
    if offer_id and str(offer_id).strip():
        return str(offer_id).strip()

    id_node = offer.find("id")
    if id_node is not None and id_node.text and id_node.text.strip():
        return id_node.text.strip()
    return None


def _extract_child_text(offer: ET.Element, tag: str) -> Optional[str]:
    node = offer.find(tag)
    if node is not None and node.text and node.text.strip():
        return node.text.strip()
    return None


async def parse_fulfillment_salesdrive_stock_to_json(*, code: str = "D14", timeout: int = 30, **kwargs) -> str:
    root = await _load_feed_root(code=code, timeout=timeout)
    if root is None:
        return "[]"

    profit_percent = await _get_profit_percent_by_code(code)
    offers = _collect_offer_nodes(root)
    logger.info("Fulfillment SalesDrive %s: offers read=%d", code, len(offers))

    rows: List[Dict[str, object]] = []
    for offer in offers:
        offer_id = _extract_offer_id(offer)
        if not offer_id:
            continue

        qty = _to_int(_extract_child_text(offer, "quantity_in_stock"))
        if qty <= 0:
            continue

        price_opt = _to_float(_extract_child_text(offer, "vendorprice"))
        if price_opt <= 0:
            continue

        price_opt = _round_money(price_opt)
        price_retail = _round_money(price_opt * (1.0 + profit_percent))
        if price_retail <= 0:
            continue

        rows.append(
            {
                "code_sup": offer_id,
                "qty": qty,
                "price_retail": price_retail,
                "price_opt": price_opt,
            }
        )

    logger.info("Fulfillment SalesDrive %s: parser output=%d", code, len(rows))
    return json.dumps(rows, ensure_ascii=False, indent=2)

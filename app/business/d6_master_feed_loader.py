from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import select, text

from app.business.supplier_identity import resolve_supplier_id_by_code
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d6_master_feed_loader")

D6_CODE = "D6"


@dataclass
class LoaderStats:
    items_read: int = 0
    inserted: int = 0
    updated: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "items_read": self.items_read,
            "inserted": self.inserted,
            "updated": self.updated,
            "warnings_count": self.warnings_count,
        }


def _warn(stats: LoaderStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


async def _extract_supplier_id() -> int:
    async with get_async_db(commit_on_exit=False) as session:
        supplier_id = await resolve_supplier_id_by_code(session, D6_CODE)
    if supplier_id is None:
        raise RuntimeError(f"Не найден supplier_id для {D6_CODE}")
    return supplier_id


async def _get_feed_url_by_code(code: str) -> Optional[str]:
    async with get_async_db() as session:
        result = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return result.scalar_one_or_none()


async def _load_feed_root(code: str = D6_CODE, timeout: int = 30) -> ET.Element:
    feed_url = await _get_feed_url_by_code(code)
    if not feed_url:
        raise RuntimeError(f"Не найден feed_url в dropship_enterprises для code='{code}'")

    headers = {"User-Agent": "Mozilla/5.0"}
    async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
        response = await client.get(feed_url)
        response.raise_for_status()

    try:
        return ET.fromstring(response.text)
    except ET.ParseError as exc:
        raise RuntimeError(f"Ошибка парсинга XML фида D6: {exc}") from exc


def _get_text(item: ET.Element, tag: str) -> Optional[str]:
    child = item.find(tag)
    if child is None:
        return None
    return _normalize_string(child.text)


def _parse_decimal(value: Optional[str], field_name: str, supplier_code: Optional[str], stats: LoaderStats) -> Optional[Decimal]:
    normalized = _normalize_string(value)
    if normalized is None:
        return None

    candidate = normalized.replace(" ", "").replace("\u00A0", "").replace(",", ".")
    if candidate.count(".") > 1:
        _warn(stats, "Некорректное числовое значение для %s (supplier_code=%s): %r", field_name, supplier_code, normalized)
        return None

    try:
        return Decimal(candidate)
    except InvalidOperation:
        _warn(stats, "Не удалось распарсить число для %s (supplier_code=%s): %r", field_name, supplier_code, normalized)
        return None


def _scaled_decimal(
    value: Optional[str],
    multiplier: str,
    field_name: str,
    supplier_code: Optional[str],
    stats: LoaderStats,
) -> Optional[Decimal]:
    parsed = _parse_decimal(value, field_name, supplier_code, stats)
    if parsed is None:
        return None
    return parsed * Decimal(multiplier)


def _parse_weight_g(value: Optional[str], supplier_code: Optional[str], stats: LoaderStats) -> Optional[Decimal]:
    parsed = _parse_decimal(value, "weight_g", supplier_code, stats)
    if parsed is None:
        return None
    if parsed <= 0:
        return None
    # Для D6 net_weight приходит в килограммах, в master-контуре храним граммы.
    return parsed * Decimal("1000")


def _extract_barcode(item: ET.Element, supplier_code: Optional[str], stats: LoaderStats) -> Optional[str]:
    for barcode_node in item.findall("./Barcodes/Barcode"):
        barcode = _normalize_string(barcode_node.text)
        if barcode:
            return barcode

    barcode = _get_text(item, "Barcode")
    if barcode:
        return barcode

    _warn(stats, "Отсутствует barcode для supplier_code=%s", supplier_code)
    return None


def _build_source_payload(item: ET.Element, barcode: Optional[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for child in item:
        if child.tag == "Barcodes":
            payload["Barcodes"] = [
                barcode_text
                for barcode_text in (_normalize_string(node.text) for node in child.findall("./Barcode"))
                if barcode_text
            ]
            continue
        payload[child.tag] = _normalize_string(child.text)

    if barcode and not payload.get("Barcodes"):
        payload["Barcodes"] = [barcode]
    return payload


def _build_source_hash(data: Dict[str, Any]) -> str:
    serialized = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _collect_items(root: ET.Element, limit: int = 0) -> List[ET.Element]:
    items = root.findall(".//item")
    if limit and limit > 0:
        return items[:limit]
    return items


def _parse_item(item: ET.Element, supplier_id: int, stats: LoaderStats) -> Optional[Dict[str, Any]]:
    feed_product_id = _get_text(item, "code")
    supplier_code = _get_text(item, "art")

    if not feed_product_id or not supplier_code:
        _warn(
            stats,
            "Пропущен item из-за отсутствия code/art: code=%r, art=%r",
            feed_product_id,
            supplier_code,
        )
        return None

    barcode = _extract_barcode(item, supplier_code, stats)
    source_payload = _build_source_payload(item, barcode)

    parsed = {
        "supplier_id": supplier_id,
        "feed_product_id": feed_product_id,
        "supplier_code": supplier_code,
        "name_raw": _get_text(item, "full_name"),
        "manufacturer_raw": _get_text(item, "brand"),
        "barcode": barcode,
        "description_raw": None,
        "weight_g": _parse_weight_g(_get_text(item, "net_weight"), supplier_code, stats),
        "length_mm": _scaled_decimal(_get_text(item, "length"), "10", "length_mm", supplier_code, stats),
        "width_mm": _scaled_decimal(_get_text(item, "width"), "10", "width_mm", supplier_code, stats),
        "height_mm": _scaled_decimal(_get_text(item, "height"), "10", "height_mm", supplier_code, stats),
        "volume_ml": None,
        "category_raw": _get_text(item, "category"),
        "source_payload": source_payload,
    }
    parsed["source_hash"] = _build_source_hash(
        {
            "feed_product_id": parsed["feed_product_id"],
            "supplier_code": parsed["supplier_code"],
            "name_raw": parsed["name_raw"],
            "manufacturer_raw": parsed["manufacturer_raw"],
            "barcode": parsed["barcode"],
            "weight_g": str(parsed["weight_g"]) if parsed["weight_g"] is not None else None,
            "length_mm": str(parsed["length_mm"]) if parsed["length_mm"] is not None else None,
            "width_mm": str(parsed["width_mm"]) if parsed["width_mm"] is not None else None,
            "height_mm": str(parsed["height_mm"]) if parsed["height_mm"] is not None else None,
            "category_raw": parsed["category_raw"],
            "source_payload": parsed["source_payload"],
        }
    )
    return parsed


async def load_d6_raw_supplier_feed(limit: int = 0) -> Dict[str, Any]:
    stats = LoaderStats()
    supplier_id = await _extract_supplier_id()
    logger.info("Запуск D6 master feed loader, supplier_id=%s", supplier_id)

    root = await _load_feed_root(code=D6_CODE)
    items = _collect_items(root, limit=limit)
    stats.items_read = len(items)

    async with get_async_db() as session:
        for item in items:
            parsed = _parse_item(item, supplier_id, stats)
            if parsed is None:
                continue

            stmt = select(RawSupplierFeedProduct).where(
                RawSupplierFeedProduct.supplier_id == supplier_id,
                RawSupplierFeedProduct.supplier_code == parsed["supplier_code"],
            )
            existing = (await session.execute(stmt)).scalar_one_or_none()

            if existing is None:
                session.add(
                    RawSupplierFeedProduct(
                        supplier_id=parsed["supplier_id"],
                        feed_product_id=parsed["feed_product_id"],
                        supplier_code=parsed["supplier_code"],
                        name_raw=parsed["name_raw"],
                        manufacturer_raw=parsed["manufacturer_raw"],
                        barcode=parsed["barcode"],
                        description_raw=None,
                        weight_g=parsed["weight_g"],
                        length_mm=parsed["length_mm"],
                        width_mm=parsed["width_mm"],
                        height_mm=parsed["height_mm"],
                        volume_ml=None,
                        category_raw=parsed["category_raw"],
                        source_payload=parsed["source_payload"],
                        source_hash=parsed["source_hash"],
                    )
                )
                stats.inserted += 1
                continue

            existing.feed_product_id = parsed["feed_product_id"]
            existing.name_raw = parsed["name_raw"]
            existing.manufacturer_raw = parsed["manufacturer_raw"]
            existing.barcode = parsed["barcode"]
            existing.description_raw = None
            existing.weight_g = parsed["weight_g"]
            existing.length_mm = parsed["length_mm"]
            existing.width_mm = parsed["width_mm"]
            existing.height_mm = parsed["height_mm"]
            existing.volume_ml = None
            existing.category_raw = parsed["category_raw"]
            existing.source_payload = parsed["source_payload"]
            existing.source_hash = parsed["source_hash"]
            stats.updated += 1

    logger.info(
        "D6 master feed loader завершён: items_read=%d, inserted=%d, updated=%d",
        stats.items_read,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка D6 (SportAtlet) XML фида в raw_supplier_feed_products"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N item (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_d6_raw_supplier_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

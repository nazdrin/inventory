from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
from sqlalchemy import select, text

from app.business.order_sender import SUPPLIERLIST_MAP
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d2_master_feed_loader")

D2_CODE = "D2"
BARCODE_PARAM_NAMES = {
    "Штрихкод",
    "Штрих-код",
    "Штрих код",
    "EAN",
    "EAN-13",
    "UPC",
    "GTIN",
    "Barcode",
    "barcode",
}
BARCODE_REGEX = re.compile(r"Штрихкод\s*:\s*([0-9A-Za-z\-]+)", re.IGNORECASE)


@dataclass
class LoaderStats:
    supplier_id: int
    items_read: int = 0
    inserted: int = 0
    updated: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
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


def _extract_supplier_id() -> int:
    supplier_token = SUPPLIERLIST_MAP.get(D2_CODE)
    if not supplier_token:
        raise RuntimeError(f"Не найден supplier mapping для {D2_CODE}")

    match = re.search(r"(\d+)$", supplier_token)
    if not match:
        raise RuntimeError(f"Не удалось извлечь supplier_id из значения {supplier_token!r} для {D2_CODE}")
    return int(match.group(1))


async def _get_feed_url_by_code(code: str = D2_CODE) -> Optional[str]:
    async with get_async_db() as session:
        result = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return result.scalar_one_or_none()


async def _load_feed_root(code: str = D2_CODE, timeout: int = 30) -> ET.Element:
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
        raise RuntimeError(f"Ошибка парсинга XML фида D2: {exc}") from exc


def _get_text(node: ET.Element, tag: str) -> Optional[str]:
    child = node.find(tag)
    if child is None:
        return None
    return _normalize_string(child.text)


def _extract_images(offer: ET.Element) -> List[str]:
    images: List[str] = []
    seen = set()
    for child in offer.findall("picture"):
        image_url = _normalize_string(child.text)
        if not image_url or image_url in seen:
            continue
        seen.add(image_url)
        images.append(image_url)
    return images


def _extract_params(offer: ET.Element) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for param in offer.findall("param"):
        name = _normalize_string(param.get("name") or param.get("Name"))
        value = _normalize_string(param.text)
        if not name or not value:
            continue
        if name not in result:
            result[name] = value
    return result


def _extract_barcode_from_text(value: Optional[str]) -> Optional[str]:
    text_value = _normalize_string(value)
    if not text_value:
        return None
    match = BARCODE_REGEX.search(text_value)
    if not match:
        return None
    return _normalize_string(match.group(1))


def _extract_barcode(
    *,
    params: Dict[str, str],
    description_ua: Optional[str],
    description_ru: Optional[str],
) -> (Optional[str], Optional[str]):
    for key in BARCODE_PARAM_NAMES:
        value = _normalize_string(params.get(key))
        if value:
            return value, "param"

    description_barcode = _extract_barcode_from_text(description_ua) or _extract_barcode_from_text(description_ru)
    if description_barcode:
        return description_barcode, "description_extracted"

    return None, None


def _build_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _collect_offers(root: ET.Element, limit: int = 0) -> List[ET.Element]:
    offers = root.findall(".//offer")
    if limit and limit > 0:
        return offers[:limit]
    return offers


def _parse_offer(offer: ET.Element, supplier_id: int, stats: LoaderStats) -> Optional[Dict[str, Any]]:
    supplier_code = _get_text(offer, "vendorCode")
    feed_product_id = _normalize_string(offer.get("id")) or _get_text(offer, "id")
    if not supplier_code:
        _warn(stats, "Пропущен D2 offer без vendorCode: offer_id=%r", feed_product_id)
        return None

    params = _extract_params(offer)
    description_ua = _get_text(offer, "description_ua")
    description_ru = _get_text(offer, "description")
    barcode, barcode_source = _extract_barcode(
        params=params,
        description_ua=description_ua,
        description_ru=description_ru,
    )
    if barcode is None:
        _warn(stats, "Не найден barcode для D2 supplier_code=%s", supplier_code)

    name_ua = _get_text(offer, "name_ua")
    name_ru = _get_text(offer, "name")
    images = _extract_images(offer)
    category_raw = _normalize_string(params.get("Категории")) or _get_text(offer, "categoryId")
    description_raw = description_ua or description_ru

    source_payload = {
        "name_ua": name_ua,
        "name_ru": name_ru,
        "description_ua": description_ua,
        "description_ru": description_ru,
        "images": images,
        "barcode_source": barcode_source,
        "params": params,
        "offer_id": feed_product_id,
        "category_id": _get_text(offer, "categoryId"),
        "url": _get_text(offer, "url"),
        "price": _get_text(offer, "price"),
        "quantity_in_stock": _get_text(offer, "quantity_in_stock"),
        "available": _normalize_string(offer.get("available")),
        "group_id": _normalize_string(offer.get("group_id")),
    }

    parsed = {
        "supplier_id": supplier_id,
        "feed_product_id": feed_product_id,
        "supplier_code": supplier_code,
        "name_raw": name_ua or name_ru,
        "manufacturer_raw": _get_text(offer, "vendor"),
        "barcode": barcode,
        "description_raw": description_raw,
        "category_raw": category_raw,
        "source_payload": source_payload,
        "source_hash": _build_source_hash(
            {
                "supplier_code": supplier_code,
                "barcode": barcode,
                "name_raw": name_ua or name_ru,
                "manufacturer_raw": _get_text(offer, "vendor"),
                "description_raw": description_raw,
                "category_raw": category_raw,
                "images": images,
            }
        ),
    }
    return parsed


async def load_d2_raw_supplier_feed(limit: int = 0) -> Dict[str, Any]:
    supplier_id = _extract_supplier_id()
    stats = LoaderStats(supplier_id=supplier_id)
    logger.info("Запуск D2 master feed loader, supplier_id=%s", supplier_id)

    root = await _load_feed_root(code=D2_CODE)
    offers = _collect_offers(root, limit=limit)
    stats.items_read = len(offers)

    async with get_async_db() as session:
        for offer in offers:
            parsed = _parse_offer(offer, supplier_id, stats)
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
                        supplier_id=supplier_id,
                        feed_product_id=parsed["feed_product_id"],
                        supplier_code=parsed["supplier_code"],
                        name_raw=parsed["name_raw"],
                        manufacturer_raw=parsed["manufacturer_raw"],
                        barcode=parsed["barcode"],
                        description_raw=parsed["description_raw"],
                        weight_g=None,
                        length_mm=None,
                        width_mm=None,
                        height_mm=None,
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
            existing.description_raw = parsed["description_raw"]
            existing.category_raw = parsed["category_raw"]
            existing.source_payload = parsed["source_payload"]
            existing.source_hash = parsed["source_hash"]
            stats.updated += 1

    logger.info(
        "D2 master feed loader завершён: items_read=%d, inserted=%d, updated=%d",
        stats.items_read,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка D2 (DSN) XML фида в raw_supplier_feed_products"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N offer (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_d2_raw_supplier_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

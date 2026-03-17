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

from sqlalchemy import select

from app.business.feed_zoohub import (
    D10_CODE_DEFAULT,
    _collect_item_nodes,
    _download_xml_feed,
    _find_child_text,
    _get_feed_url_by_code,
    _load_catalog_items_from_excel_url,
)
from app.business.order_sender import SUPPLIERLIST_MAP
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d10_master_feed_loader")


@dataclass
class LoaderStats:
    supplier_id: int
    items_read: int = 0
    barcode_matches_found: int = 0
    inserted: int = 0
    updated: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "items_read": self.items_read,
            "barcode_matches_found": self.barcode_matches_found,
            "inserted": self.inserted,
            "updated": self.updated,
            "warnings_count": self.warnings_count,
        }


def _warn(stats: LoaderStats, message: str, *args: Any) -> None:
    logger.warning(message, *args)
    stats.warnings_count += 1


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _extract_supplier_id() -> int:
    supplier_token = SUPPLIERLIST_MAP.get(D10_CODE_DEFAULT)
    if not supplier_token:
        raise RuntimeError("Не найден supplier mapping для D10")

    match = re.search(r"(\d+)$", supplier_token)
    if not match:
        raise RuntimeError(f"Не удалось извлечь supplier_id из значения {supplier_token!r} для D10")
    return int(match.group(1))


def _normalize_barcode(value: Any) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip().replace("\u00A0", "").replace(" ", "")
    if not raw:
        return None
    if raw.lower() in {"nan", "none"}:
        return None
    return raw


def _build_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _parse_weight_g(value: Any, stats: LoaderStats, supplier_code: str) -> Optional[Decimal]:
    weight_raw = _normalize_string(value)
    if not weight_raw:
        return None

    match = re.search(r"(-?\d+(?:[.,]\d+)?)\s*([a-zA-Z]+)", weight_raw)
    if not match:
        _warn(stats, "Не удалось распарсить вес D10: supplier_code=%s, product_weight=%s", supplier_code, weight_raw)
        return None

    number_raw = match.group(1).replace(",", ".")
    unit = match.group(2).lower()
    try:
        numeric = Decimal(number_raw)
    except InvalidOperation:
        _warn(stats, "Некорректный вес D10: supplier_code=%s, product_weight=%s", supplier_code, weight_raw)
        return None

    if unit == "kg":
        grams = numeric * Decimal("1000")
    elif unit == "g":
        grams = numeric
    else:
        _warn(stats, "Неизвестная единица веса D10: supplier_code=%s, product_weight=%s", supplier_code, weight_raw)
        return None

    if grams <= 0:
        return None
    return grams.quantize(Decimal("0.01"))


def _extract_images(item: ET.Element) -> tuple[Optional[str], List[str]]:
    main_image = _normalize_string(_find_child_text(item, ["image_link"]))
    additional_images: List[str] = []
    seen = set()

    if main_image:
        seen.add(main_image)

    for child in list(item):
        if child is None:
            continue
        if child.tag.split("}")[-1] != "additional_image_link":
            continue
        image_url = _normalize_string(child.text)
        if not image_url or image_url in seen:
            continue
        seen.add(image_url)
        additional_images.append(image_url)

    return main_image, additional_images


def _extract_product_details(item: ET.Element) -> Dict[str, str]:
    details: Dict[str, str] = {}
    for child in list(item):
        if child.tag.split("}")[-1] != "product_detail":
            continue

        attr_name = ""
        attr_value = ""
        for nested in list(child):
            local_name = nested.tag.split("}")[-1]
            if local_name == "attribute_name":
                attr_name = (nested.text or "").strip()
            elif local_name == "attribute_value":
                attr_value = (nested.text or "").strip()

        if attr_name:
            details[attr_name] = attr_value
    return details


def _build_excel_barcode_map(items: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    barcode_by_id: Dict[str, Optional[str]] = {}
    for item in items:
        item_id = _normalize_string(item.get("id"))
        if not item_id:
            continue
        barcode_by_id[item_id] = _normalize_barcode(item.get("barcode"))
    return barcode_by_id


def _normalize_item(
    *,
    item: ET.Element,
    supplier_id: int,
    barcode_by_external_id: Dict[str, Optional[str]],
    stats: LoaderStats,
) -> Optional[Dict[str, Any]]:
    feed_product_id = _normalize_string(_find_child_text(item, ["id"]))
    mpn = _normalize_string(_find_child_text(item, ["mpn"]))
    supplier_code = mpn or feed_product_id
    if not supplier_code:
        _warn(stats, "Пропущен D10 item без mpn и id")
        return None

    barcode = barcode_by_external_id.get(mpn or "")
    if barcode:
        stats.barcode_matches_found += 1
    else:
        _warn(stats, "Не найден barcode во внешнем Excel-источнике для D10 supplier_code=%s", supplier_code)

    title = _normalize_string(_find_child_text(item, ["title"]))
    description = _normalize_string(_find_child_text(item, ["description"]))
    brand = _normalize_string(_find_child_text(item, ["brand"]))
    product_weight = _normalize_string(_find_child_text(item, ["product_weight"]))
    product_type = _normalize_string(_find_child_text(item, ["product_type"]))
    main_image, additional_images = _extract_images(item)
    product_details = _extract_product_details(item)
    weight_g = _parse_weight_g(product_weight, stats, supplier_code)

    source_payload = {
        "title": title,
        "description": description,
        "brand": brand,
        "product_weight": product_weight,
        "product_type": product_type,
        "image_link": main_image,
        "additional_images": additional_images,
        "barcode_source": "excel_price_list",
        "barcode_external": barcode,
        "product_details": product_details,
        "price": _normalize_string(_find_child_text(item, ["price"])),
        "availability": _normalize_string(_find_child_text(item, ["availability"])),
        "link": _normalize_string(_find_child_text(item, ["link"])),
        "condition": _normalize_string(_find_child_text(item, ["condition"])),
        "adult": _normalize_string(_find_child_text(item, ["adult"])),
    }

    return {
        "supplier_id": supplier_id,
        "feed_product_id": feed_product_id,
        "supplier_code": supplier_code,
        "name_raw": title,
        "manufacturer_raw": brand,
        "barcode": barcode,
        "description_raw": description,
        "category_raw": product_type,
        "weight_g": weight_g,
        "source_payload": source_payload,
        "source_hash": _build_source_hash(
            {
                "supplier_code": supplier_code,
                "barcode": barcode,
                "name_raw": title,
                "manufacturer_raw": brand,
                "description_raw": description,
                "category_raw": product_type,
                "weight_g": str(weight_g) if weight_g is not None else None,
                "image_link": main_image,
                "additional_images": additional_images,
            }
        ),
    }


async def load_d10_raw_supplier_feed(limit: int = 0) -> Dict[str, Any]:
    supplier_id = _extract_supplier_id()
    stats = LoaderStats(supplier_id=supplier_id)
    logger.info("Запуск D10 master feed loader для supplier_id=%s", supplier_id)

    feed_url = await _get_feed_url_by_code(D10_CODE_DEFAULT)
    if not feed_url:
        raise RuntimeError("Не найден feed_url в dropship_enterprises для D10")

    xml_bytes = await _download_xml_feed(url=feed_url, timeout=60)
    if not xml_bytes:
        return stats.to_dict()

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as exc:
        raise RuntimeError(f"Ошибка парсинга XML D10: {exc}") from exc

    excel_items = await _load_catalog_items_from_excel_url(timeout=60)
    barcode_by_external_id = _build_excel_barcode_map(excel_items)

    items = _collect_item_nodes(root)
    if limit and limit > 0:
        items = items[:limit]
    stats.items_read = len(items)

    async with get_async_db() as session:
        for item in items:
            parsed = _normalize_item(
                item=item,
                supplier_id=supplier_id,
                barcode_by_external_id=barcode_by_external_id,
                stats=stats,
            )
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
                        weight_g=parsed["weight_g"],
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
            existing.weight_g = parsed["weight_g"]
            existing.category_raw = parsed["category_raw"]
            existing.source_payload = parsed["source_payload"]
            existing.source_hash = parsed["source_hash"]
            stats.updated += 1

    logger.info(
        "Завершён D10 master feed loader: items=%d, barcode_matches=%d, inserted=%d, updated=%d",
        stats.items_read,
        stats.barcode_matches_found,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка D10 (ZooHub) XML фида в raw_supplier_feed_products"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N XML items D10 (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_d10_raw_supplier_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET

from sqlalchemy import select

from app.business.feed_zoocomplex import (
    ZOOCOMPLEX_CODE_DEFAULT,
    _collect_offer_nodes,
    _extract_barcode as _extract_feed_barcode,
    _extract_name as _extract_feed_name,
    _extract_offer_id,
    _get_text,
    _load_feed_root,
)
from app.business.supplier_identity import resolve_supplier_id_by_code
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d13_master_feed_loader")

D13_CODE = ZOOCOMPLEX_CODE_DEFAULT
WEIGHT_PARAM_NAMES = {"Вага", "Вес", "Weight"}


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


def _normalize_barcode(value: Any) -> Optional[str]:
    text_value = _normalize_string(value)
    if not text_value:
        return None
    barcode = text_value.replace("\u00A0", "").replace(" ", "")
    return barcode or None


async def _extract_supplier_id() -> int:
    async with get_async_db(commit_on_exit=False) as session:
        supplier_id = await resolve_supplier_id_by_code(session, D13_CODE)
    if supplier_id is None:
        raise RuntimeError(f"Не найден supplier_id для {D13_CODE}")
    return supplier_id


def _local_name(tag: str) -> str:
    return tag.split("}")[-1]


def _extract_pictures(offer: ET.Element) -> List[str]:
    images: List[str] = []
    seen = set()
    for child in list(offer):
        if _local_name(child.tag) != "picture":
            continue
        image_url = _normalize_string(child.text)
        if not image_url or image_url in seen:
            continue
        seen.add(image_url)
        images.append(image_url)
    return images


def _extract_category_ids(offer: ET.Element) -> tuple[List[str], Optional[str]]:
    category_ids: List[str] = []
    seen = set()
    parent_category_id: Optional[str] = None

    for child in list(offer):
        if _local_name(child.tag) != "categoryId":
            continue
        category_id = _normalize_string(child.text)
        if not category_id:
            continue
        if category_id not in seen:
            seen.add(category_id)
            category_ids.append(category_id)

        parent_flag = _normalize_string(child.get("parent"))
        if parent_category_id is None and parent_flag and parent_flag.lower() in {"true", "1", "yes"}:
            parent_category_id = category_id

    return category_ids, parent_category_id


def _extract_params(offer: ET.Element) -> List[Dict[str, str]]:
    params: List[Dict[str, str]] = []
    for child in list(offer):
        if _local_name(child.tag) != "param":
            continue
        name = _normalize_string(child.get("name") or child.get("Name"))
        value = _normalize_string(child.text)
        if not name or not value:
            continue
        params.append({"name": name, "value": value})
    return params


def _params_to_dict(params: List[Dict[str, str]]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in params:
        name = _normalize_string(item.get("name"))
        value = _normalize_string(item.get("value"))
        if not name or not value or name in result:
            continue
        result[name] = value
    return result


def _parse_weight_g(raw_value: Optional[str]) -> Optional[Decimal]:
    if not raw_value:
        return None

    normalized = raw_value.strip().lower().replace(",", ".")
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*([^\d\s]+)?", normalized)
    if not match:
        return None

    try:
        numeric = Decimal(match.group(1))
    except InvalidOperation:
        return None

    if numeric <= 0:
        return None

    unit = (match.group(2) or "").strip()
    if unit in {"кг", "kg"}:
        grams = numeric * Decimal("1000")
    elif unit in {"г", "гр", "g", ""}:
        grams = numeric
    else:
        return None

    return grams.quantize(Decimal("0.01"))


def _build_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _parse_offer(offer: ET.Element, supplier_id: int, stats: LoaderStats) -> Optional[Dict[str, Any]]:
    offer_id = _extract_offer_id(offer)
    if not offer_id:
        _warn(stats, "Пропущен D13 offer без id")
        return None

    name = _extract_feed_name(offer)
    if not name:
        _warn(stats, "Пропущен D13 offer без name: offer_id=%s", offer_id)
        return None

    supplier_code = offer_id
    feed_product_id = offer_id
    manufacturer = _normalize_string(_get_text(offer, ["vendor"]))
    description = _normalize_string(_get_text(offer, ["description"]))
    model = _normalize_string(_get_text(offer, ["model"]))
    barcode = _normalize_barcode(_extract_feed_barcode(offer))
    vendor_code = _normalize_string(_get_text(offer, ["vendorCode"]))
    url = _normalize_string(_get_text(offer, ["url"]))
    price = _normalize_string(_get_text(offer, ["price"]))
    available = _normalize_string(offer.get("available"))
    pictures = _extract_pictures(offer)
    category_ids, parent_category_id = _extract_category_ids(offer)
    params = _extract_params(offer)
    params_dict = _params_to_dict(params)

    weight_raw = None
    for key in WEIGHT_PARAM_NAMES:
        weight_raw = _normalize_string(params_dict.get(key))
        if weight_raw:
            break
    weight_g = _parse_weight_g(weight_raw)

    category_raw = parent_category_id or (category_ids[0] if category_ids else None)
    source_payload = {
        "offer_id": offer_id,
        "url": url,
        "price": price,
        "available": available,
        "name": name,
        "description": description,
        "vendor": manufacturer,
        "model": model,
        "barcode": barcode,
        "vendorCode": vendor_code,
        "picture": pictures,
        "images": pictures,
        "category_ids": category_ids,
        "parent_category_id": parent_category_id,
        "params": params,
    }

    source_hash = _build_source_hash(
        {
            "supplier_code": supplier_code,
            "name_raw": name,
            "manufacturer_raw": manufacturer,
            "barcode": barcode,
            "description_raw": description,
            "category_raw": category_raw,
            "picture": pictures,
        }
    )

    return {
        "supplier_id": supplier_id,
        "feed_product_id": feed_product_id,
        "supplier_code": supplier_code,
        "name_raw": name,
        "manufacturer_raw": manufacturer,
        "barcode": barcode,
        "description_raw": description,
        "weight_g": weight_g,
        "category_raw": category_raw,
        "source_payload": source_payload,
        "source_hash": source_hash,
    }


async def load_d13_raw_supplier_feed(limit: int = 0) -> Dict[str, Any]:
    supplier_id = await _extract_supplier_id()
    stats = LoaderStats(supplier_id=supplier_id)
    logger.info("Запуск D13 master feed loader для supplier_id=%s", supplier_id)

    root = await _load_feed_root(code=D13_CODE, timeout=60)
    if root is None:
        return stats.to_dict()

    offers = _collect_offer_nodes(root)
    if limit and limit > 0:
        offers = offers[:limit]
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
        "Завершён D13 master feed loader: items=%d inserted=%d updated=%d",
        stats.items_read,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Загрузка raw supplier feed для D13 Zoocomplex")
    parser.add_argument("--limit", type=int, default=0, help="обработать только первые N offer (0 = без лимита)")
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_d13_raw_supplier_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

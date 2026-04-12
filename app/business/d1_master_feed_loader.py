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
from sqlalchemy import select

from app.business.supplier_identity import resolve_supplier_id_by_code
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d1_master_feed_loader")

D1_CODE = "D1"
D1_MAIN_FEED_URL = "https://static-opt.biotus.ua/media/amasty/feed/biotus_partner.xml"
D1_EXTRA_FEED_URL = "https://static-opt.biotus.ua/media/amasty/feed/prom_biotus_partner_ua.xml"


@dataclass
class LoaderStats:
    supplier_id: int
    main_feed_items_read: int = 0
    extra_feed_items_read: int = 0
    extra_feed_available: bool = False
    inserted: int = 0
    updated: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "main_feed_items_read": self.main_feed_items_read,
            "extra_feed_items_read": self.extra_feed_items_read,
            "extra_feed_available": self.extra_feed_available,
            "inserted": self.inserted,
            "updated": self.updated,
            "warnings_count": self.warnings_count,
        }


def _warn(stats: LoaderStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


async def _extract_supplier_id() -> int:
    async with get_async_db(commit_on_exit=False) as session:
        supplier_id = await resolve_supplier_id_by_code(session, D1_CODE)
    if supplier_id is None:
        raise RuntimeError(f"Не найден supplier_id для {D1_CODE}")
    return supplier_id


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _get_child_text(node: ET.Element, tag: str) -> Optional[str]:
    child = node.find(tag)
    if child is None:
        return None
    return _normalize_string(child.text)


async def _fetch_xml_root(url: str, *, required: bool, timeout: int = 60) -> Optional[ET.Element]:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
            response = await client.get(url)
            response.raise_for_status()
    except Exception as exc:
        if required:
            raise RuntimeError(f"Ошибка загрузки обязательного фида {url}: {exc}") from exc
        return None

    try:
        return ET.fromstring(response.text)
    except ET.ParseError as exc:
        if required:
            raise RuntimeError(f"Ошибка парсинга обязательного фида {url}: {exc}") from exc
        return None


def _collect_main_items(root: ET.Element) -> List[ET.Element]:
    items = root.findall(".//item")
    if items:
        return items
    return root.findall(".//offer")


def _collect_extra_offers(root: ET.Element) -> List[ET.Element]:
    offers = root.findall(".//offer")
    if offers:
        return offers
    return root.findall(".//item")


def _extract_images(node: ET.Element, tag_name: str) -> List[str]:
    images: List[str] = []
    for child in node.findall(tag_name):
        value = _normalize_string(child.text)
        if value:
            images.append(value)
    return images


def _merge_images(*groups: List[str]) -> List[str]:
    merged: List[str] = []
    seen = set()
    for group in groups:
        for image in group:
            normalized = _normalize_string(image)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
    return merged


def _build_extra_map(root: ET.Element) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for offer in _collect_extra_offers(root):
        vendor_code = _get_child_text(offer, "vendorCode")
        if not vendor_code:
            continue
        result[vendor_code] = {
            "vendorCode": vendor_code,
            "name": _get_child_text(offer, "name"),
            "name_ua": _get_child_text(offer, "name_ua"),
            "original_name": _get_child_text(offer, "original_name"),
            "description": _get_child_text(offer, "description"),
            "description_ua": _get_child_text(offer, "description_ua"),
            "pictures": _extract_images(offer, "picture"),
        }
    return result


def _build_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _build_merged_record(
    item: ET.Element,
    extra_map: Dict[str, Dict[str, Any]],
    stats: LoaderStats,
) -> Optional[Dict[str, Any]]:
    sku = _get_child_text(item, "sku")
    if not sku:
        _warn(stats, "Пропущен Biotus item без sku")
        return None

    barcode = _get_child_text(item, "barcode")
    if not barcode:
        _warn(stats, "У Biotus товара нет barcode: sku=%s", sku)

    extra = extra_map.get(sku)
    main_description = _get_child_text(item, "description")
    extra_description_ua = extra.get("description_ua") if extra else None
    extra_description_ru = extra.get("description") if extra else None
    description_ua = extra_description_ua or main_description
    description_ru = extra_description_ru or None
    description_raw = description_ua or main_description
    main_images = _extract_images(item, "image")
    extra_images = extra.get("pictures", []) if extra else []
    images = _merge_images(main_images, extra_images)
    name_ua = extra.get("name_ua") if extra else None
    name_ru = (extra.get("name") if extra else None) or _get_child_text(item, "name")

    extra_fields_used: List[str] = []
    if extra_description_ua:
        extra_fields_used.append("description_ua")
    if extra_description_ru:
        extra_fields_used.append("description")
    if name_ua:
        extra_fields_used.append("name_ua")
    if extra and extra.get("name"):
        extra_fields_used.append("name")
    if extra_images:
        extra_fields_used.append("pictures")

    source_payload = {
        "source_main": {
            "sku": sku,
            "barcode": barcode,
            "name": _get_child_text(item, "name"),
            "name_eng": _get_child_text(item, "name_eng"),
            "description": main_description,
            "vendor": _get_child_text(item, "vendor"),
            "images": main_images,
        },
        "source_extra_available": extra is not None,
        "extra_fields_used": extra_fields_used,
        "images": images,
        "description_ua": description_ua,
        "description_ru": description_ru,
        "name_ua": name_ua,
        "name_ru": name_ru,
        "source_extra": extra,
    }

    record = {
        "supplier_code": sku,
        "feed_product_id": sku,
        "name_raw": _get_child_text(item, "name"),
        "manufacturer_raw": _get_child_text(item, "vendor"),
        "barcode": barcode,
        "description_raw": description_raw,
        "category_raw": None,
        "source_payload": source_payload,
    }
    record["source_hash"] = _build_source_hash(
        {
            "supplier_code": record["supplier_code"],
            "barcode": record["barcode"],
            "name_raw": record["name_raw"],
            "manufacturer_raw": record["manufacturer_raw"],
            "description_raw": record["description_raw"],
            "name_ua": name_ua,
            "name_ru": name_ru,
        }
    )
    return record


async def load_d1_raw_supplier_feed(limit: int = 0) -> Dict[str, Any]:
    supplier_id = await _extract_supplier_id()
    stats = LoaderStats(supplier_id=supplier_id)
    logger.info("Запуск D1 master feed loader, supplier_id=%s", supplier_id)

    main_root = await _fetch_xml_root(D1_MAIN_FEED_URL, required=True)

    extra_root = await _fetch_xml_root(D1_EXTRA_FEED_URL, required=False)
    extra_map: Dict[str, Dict[str, Any]] = {}
    if extra_root is None:
        _warn(stats, "Дополнительный Biotus фид недоступен, продолжаем только с основным")
    else:
        stats.extra_feed_available = True
        extra_map = _build_extra_map(extra_root)
        stats.extra_feed_items_read = len(extra_map)

    items = _collect_main_items(main_root)
    if limit and limit > 0:
        items = items[:limit]
    stats.main_feed_items_read = len(items)

    async with get_async_db() as session:
        for item in items:
            record = _build_merged_record(item, extra_map, stats)
            if record is None:
                continue

            stmt = select(RawSupplierFeedProduct).where(
                RawSupplierFeedProduct.supplier_id == supplier_id,
                RawSupplierFeedProduct.supplier_code == record["supplier_code"],
            )
            existing = (await session.execute(stmt)).scalar_one_or_none()

            if existing is None:
                session.add(
                    RawSupplierFeedProduct(
                        supplier_id=supplier_id,
                        feed_product_id=record["feed_product_id"],
                        supplier_code=record["supplier_code"],
                        name_raw=record["name_raw"],
                        manufacturer_raw=record["manufacturer_raw"],
                        barcode=record["barcode"],
                        description_raw=record["description_raw"],
                        weight_g=None,
                        length_mm=None,
                        width_mm=None,
                        height_mm=None,
                        volume_ml=None,
                        category_raw=None,
                        source_payload=record["source_payload"],
                        source_hash=record["source_hash"],
                    )
                )
                stats.inserted += 1
                continue

            existing.feed_product_id = record["feed_product_id"]
            existing.name_raw = record["name_raw"]
            existing.manufacturer_raw = record["manufacturer_raw"]
            existing.barcode = record["barcode"]
            existing.description_raw = record["description_raw"]
            existing.category_raw = None
            existing.source_payload = record["source_payload"]
            existing.source_hash = record["source_hash"]
            stats.updated += 1

    logger.info(
        "Завершён D1 master feed loader: main=%d, extra=%d, inserted=%d, updated=%d",
        stats.main_feed_items_read,
        stats.extra_feed_items_read,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка Biotus D1 в raw_supplier_feed_products для master-контура"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N товаров из основного фида (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_d1_raw_supplier_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

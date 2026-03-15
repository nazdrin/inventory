from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlalchemy import select

from app.business.feed_vetstar import (
    D12_CODE_DEFAULT,
    _download_excel_bytes,
    _get_feed_url_by_code,
    _parse_catalog_from_rows,
    _read_xls_first_sheet_rows,
)
from app.business.order_sender import SUPPLIERLIST_MAP
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d12_master_feed_loader")


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
    logger.warning(message, *args)
    stats.warnings_count += 1


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value or text_value.lower() == "none":
        return None
    return text_value


def _extract_supplier_id() -> int:
    supplier_token = SUPPLIERLIST_MAP.get(D12_CODE_DEFAULT)
    if not supplier_token:
        raise RuntimeError("Не найден supplier mapping для D12")

    match = re.search(r"(\d+)$", supplier_token)
    if not match:
        raise RuntimeError(f"Не удалось извлечь supplier_id из значения {supplier_token!r} для D12")
    return int(match.group(1))


def _build_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _normalize_item(row: Dict[str, Any], stats: LoaderStats) -> Optional[Dict[str, Any]]:
    supplier_code = _normalize_string(row.get("id"))
    if not supplier_code:
        _warn(stats, "Пропущена D12 запись без id")
        return None

    name_raw = _normalize_string(row.get("name"))
    if not name_raw:
        _warn(stats, "Пропущена D12 запись без name: id=%s", supplier_code)
        return None

    barcode = _normalize_string(row.get("barcode"))
    if not barcode:
        _warn(stats, "Пустой barcode у D12 для id=%s", supplier_code)

    source_payload = {
        "id": supplier_code,
        "name": name_raw,
        "barcode": barcode,
    }

    return {
        "feed_product_id": supplier_code,
        "supplier_code": supplier_code,
        "name_raw": name_raw,
        "barcode": barcode,
        "source_payload": source_payload,
        "source_hash": _build_source_hash(
            {
                "supplier_code": supplier_code,
                "barcode": barcode,
                "name_raw": name_raw,
            }
        ),
    }


async def load_d12_raw_supplier_feed(limit: int = 0) -> Dict[str, Any]:
    supplier_id = _extract_supplier_id()
    stats = LoaderStats(supplier_id=supplier_id)
    logger.info("Запуск D12 master feed loader для supplier_id=%s", supplier_id)

    feed_url = await _get_feed_url_by_code(D12_CODE_DEFAULT)
    if not feed_url:
        raise RuntimeError("Не найден feed_url в dropship_enterprises для D12")

    file_bytes = await _download_excel_bytes(url=feed_url, timeout=60)
    if not file_bytes:
        raise RuntimeError("Не удалось загрузить catalog xls для D12")

    rows = _read_xls_first_sheet_rows(file_bytes)
    items = _parse_catalog_from_rows(rows)

    if limit and limit > 0:
        items = items[:limit]

    async with get_async_db() as session:
        for row in items:
            if not isinstance(row, dict):
                _warn(stats, "Пропущена некорректная D12 запись: %r", row)
                continue

            parsed = _normalize_item(row, stats)
            if parsed is None:
                continue

            stats.items_read += 1

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
                        manufacturer_raw=None,
                        barcode=parsed["barcode"],
                        description_raw=None,
                        weight_g=None,
                        length_mm=None,
                        width_mm=None,
                        height_mm=None,
                        volume_ml=None,
                        category_raw=None,
                        source_payload=parsed["source_payload"],
                        source_hash=parsed["source_hash"],
                    )
                )
                stats.inserted += 1
                continue

            existing.feed_product_id = parsed["feed_product_id"]
            existing.name_raw = parsed["name_raw"]
            existing.manufacturer_raw = None
            existing.barcode = parsed["barcode"]
            existing.description_raw = None
            existing.category_raw = None
            existing.source_payload = parsed["source_payload"]
            existing.source_hash = parsed["source_hash"]
            stats.updated += 1

    logger.info(
        "Завершён D12 master feed loader: items=%d, inserted=%d, updated=%d",
        stats.items_read,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка D12 (VetStar) catalog xls в raw_supplier_feed_products"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N записей каталога D12 (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_d12_raw_supplier_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

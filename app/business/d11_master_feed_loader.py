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

from app.business.feed_toros import (
    D11_CODE_DEFAULT,
    _connect_to_google_drive,
    _download_file_bytes,
    _fetch_latest_file_metadata,
    _get_gdrive_folder_by_code,
    _parse_d11_catalog_excel_xlsx,
)
from app.business.order_sender import SUPPLIERLIST_MAP
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d11_master_feed_loader")


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
    return text_value or None


def _extract_supplier_id() -> int:
    supplier_token = SUPPLIERLIST_MAP.get(D11_CODE_DEFAULT)
    if not supplier_token:
        raise RuntimeError("Не найден supplier mapping для D11")

    match = re.search(r"(\d+)$", supplier_token)
    if not match:
        raise RuntimeError(f"Не удалось извлечь supplier_id из значения {supplier_token!r} для D11")
    return int(match.group(1))


def _build_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _normalize_item(row: Dict[str, Any], stats: LoaderStats) -> Optional[Dict[str, Any]]:
    supplier_code = _normalize_string(row.get("id"))
    if not supplier_code:
        _warn(stats, "Пропущена D11 запись без id")
        return None

    name_raw = _normalize_string(row.get("name"))
    if not name_raw:
        _warn(stats, "Пропущена D11 запись без name: id=%s", supplier_code)
        return None

    barcode = _normalize_string(row.get("barcode"))
    if not barcode:
        _warn(stats, "Пустой barcode у D11 для id=%s", supplier_code)

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


async def load_d11_raw_supplier_feed(limit: int = 0) -> Dict[str, Any]:
    supplier_id = _extract_supplier_id()
    stats = LoaderStats(supplier_id=supplier_id)
    logger.info("Запуск D11 master feed loader для supplier_id=%s", supplier_id)

    folder_id = await _get_gdrive_folder_by_code(D11_CODE_DEFAULT)
    if not folder_id:
        raise RuntimeError("Не найден gdrive_folder в dropship_enterprises для D11")

    drive_service = await _connect_to_google_drive()
    file_meta = await _fetch_latest_file_metadata(drive_service, folder_id)
    file_id = file_meta["id"]
    file_name = file_meta.get("name") or "catalog.xlsx"
    logger.info("D11 catalog loader: найден файл %s (%s)", file_name, file_id)

    file_bytes = await _download_file_bytes(drive_service, file_id)
    rows = _parse_d11_catalog_excel_xlsx(file_bytes)

    if limit and limit > 0:
        rows = rows[:limit]

    async with get_async_db() as session:
        for row in rows:
            if not isinstance(row, dict):
                _warn(stats, "Пропущена некорректная D11 запись: %r", row)
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
        "Завершён D11 master feed loader: items=%d, inserted=%d, updated=%d",
        stats.items_read,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка D11 (Toros) catalog xlsx в raw_supplier_feed_products"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N записей каталога D11 (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_d11_raw_supplier_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

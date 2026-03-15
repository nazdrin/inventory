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

from app.business.feed_ortomedika import parse_feed_catalog_to_json
from app.business.order_sender import SUPPLIERLIST_MAP
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d9_master_feed_loader")

D9_CODE = "D9"


@dataclass
class LoaderStats:
    supplier_id: int
    raw_rows_read: int = 0
    inserted: int = 0
    updated: int = 0
    skipped_no_code: int = 0
    skipped_invalid: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "raw_rows_read": self.raw_rows_read,
            "inserted": self.inserted,
            "updated": self.updated,
            "skipped_no_code": self.skipped_no_code,
            "skipped_invalid": self.skipped_invalid,
        }


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _extract_supplier_id() -> int:
    supplier_token = SUPPLIERLIST_MAP.get(D9_CODE)
    if not supplier_token:
        raise RuntimeError("Не найден supplier mapping для D9")

    match = re.search(r"(\d+)$", supplier_token)
    if not match:
        raise RuntimeError(f"Не удалось извлечь supplier_id из значения {supplier_token!r} для D9")
    return int(match.group(1))


def _build_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _normalize_item(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    supplier_code = _normalize_string(row.get("id"))
    if not supplier_code:
        return None

    name_raw = _normalize_string(row.get("name"))
    if not name_raw:
        return None

    barcode = _normalize_string(row.get("barcode"))
    source_payload = {
        "article": supplier_code,
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


async def sync_d9_master_feed(limit: int = 0) -> Dict[str, Any]:
    supplier_id = _extract_supplier_id()
    stats = LoaderStats(supplier_id=supplier_id)
    logger.info("Запуск D9 master feed loader для supplier_id=%s", supplier_id)

    payload_json = await parse_feed_catalog_to_json(code=D9_CODE)
    rows = json.loads(payload_json or "[]")
    if not isinstance(rows, list):
        raise RuntimeError("D9 catalog parser вернул неожиданную структуру")

    if limit and limit > 0:
        rows = rows[:limit]

    async with get_async_db() as session:
        for row in rows:
            if not isinstance(row, dict):
                stats.skipped_invalid += 1
                continue

            parsed = _normalize_item(row)
            if parsed is None:
                if not _normalize_string(row.get("id")):
                    stats.skipped_no_code += 1
                else:
                    stats.skipped_invalid += 1
                continue

            stats.raw_rows_read += 1

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

            existing.name_raw = parsed["name_raw"]
            existing.barcode = parsed["barcode"]
            existing.source_payload = parsed["source_payload"]
            existing.source_hash = parsed["source_hash"]
            stats.updated += 1

    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка D9 (Ortomedika) каталога в raw_supplier_feed_products"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N записей каталога D9 (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await sync_d9_master_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

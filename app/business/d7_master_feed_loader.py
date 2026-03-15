from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from app.business.feed_pediakid import CATALOG_COLS, _download_gsheet_csv, _parse_csv_rows
from app.business.order_sender import SUPPLIERLIST_MAP
from app.database import get_async_db
from app.models import RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d7_master_feed_loader")

D7_CODE = "D7"


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
    supplier_token = SUPPLIERLIST_MAP.get(D7_CODE)
    if not supplier_token:
        raise RuntimeError("Не найден supplier mapping для D7")

    match = re.search(r"(\d+)$", supplier_token)
    if not match:
        raise RuntimeError(f"Не удалось извлечь supplier_id из значения {supplier_token!r} для D7")
    return int(match.group(1))


def _build_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _normalize_row(row: Dict[str, Any], supplier_id: int, stats: LoaderStats) -> Optional[Dict[str, Any]]:
    supplier_code = _normalize_string(row.get(CATALOG_COLS["id"]))
    if not supplier_code:
        _warn(stats, "Пропущена D7 запись без Артикул")
        return None

    name_raw = _normalize_string(row.get(CATALOG_COLS["name"]))
    if not name_raw:
        _warn(stats, "У D7 пустое Назва для Артикул=%s", supplier_code)

    barcode = _normalize_string(row.get(CATALOG_COLS["barcode"]))
    if not barcode:
        _warn(stats, "У D7 отсутствует Штрих-код для Артикул=%s", supplier_code)

    source_payload = {
        "articul": supplier_code,
        "name": name_raw,
        "barcode": barcode,
    }

    return {
        "supplier_id": supplier_id,
        "feed_product_id": supplier_code,
        "supplier_code": supplier_code,
        "name_raw": name_raw,
        "manufacturer_raw": None,
        "barcode": barcode,
        "description_raw": None,
        "category_raw": None,
        "source_payload": source_payload,
        "source_hash": _build_source_hash(
            {
                "supplier_code": supplier_code,
                "barcode": barcode,
                "name_raw": name_raw,
            }
        ),
    }


async def load_d7_raw_supplier_feed(limit: int = 0) -> Dict[str, Any]:
    supplier_id = _extract_supplier_id()
    stats = LoaderStats(supplier_id=supplier_id)
    logger.info("Запуск D7 master feed loader для supplier_id=%s", supplier_id)

    csv_text = await _download_gsheet_csv(code=D7_CODE)
    if not csv_text:
        return stats.to_dict()

    rows: List[Dict[str, str]] = _parse_csv_rows(csv_text)
    if limit and limit > 0:
        rows = rows[:limit]
    stats.items_read = len(rows)

    async with get_async_db() as session:
        for row in rows:
            if not isinstance(row, dict):
                _warn(stats, "Пропущена некорректная D7 строка: %r", row)
                continue

            parsed = _normalize_row(row, supplier_id, stats)
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
        "Завершён D7 master feed loader: items=%d, inserted=%d, updated=%d",
        stats.items_read,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка D7 (PEDIAKID) CSV каталога в raw_supplier_feed_products"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N строк каталога D7 (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_d7_raw_supplier_feed(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

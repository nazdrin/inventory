import argparse
import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import CatalogSupplierMapping, MasterCatalog, RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d9_barcode_mapping_sync")

D9_SUPPLIER_ID = 46
MAX_LOGGED_CONFLICTS = 20


@dataclass
class SyncStats:
    supplier_id: int = D9_SUPPLIER_ID
    raw_rows_read: int = 0
    unique_supplier_barcodes: int = 0
    unique_master_barcodes: int = 0
    matched_unique_pairs: int = 0
    inserted: int = 0
    updated: int = 0
    conflicts_master: int = 0
    conflicts_supplier: int = 0
    conflict_existing_mapping: int = 0
    skipped_no_barcode: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "raw_rows_read": self.raw_rows_read,
            "unique_supplier_barcodes": self.unique_supplier_barcodes,
            "unique_master_barcodes": self.unique_master_barcodes,
            "matched_unique_pairs": self.matched_unique_pairs,
            "inserted": self.inserted,
            "updated": self.updated,
            "conflicts_master": self.conflicts_master,
            "conflicts_supplier": self.conflicts_supplier,
            "conflict_existing_mapping": self.conflict_existing_mapping,
            "skipped_no_barcode": self.skipped_no_barcode,
        }


def _normalize_barcode(value: Any) -> Optional[str]:
    if value is None:
        return None
    barcode = str(value).strip().replace(" ", "")
    if not barcode:
        return None
    if barcode.lower() in {"nan", "none"}:
        return None
    return barcode


def _warn(message: str, *args: Any) -> None:
    logger.warning(message, *args)


async def sync_d9_supplier_mapping_by_barcode(limit: int = 0) -> Dict[str, Any]:
    stats = SyncStats()
    logger.info("Запуск sync D9 barcode mapping для supplier_id=%s", D9_SUPPLIER_ID)

    async with get_async_db() as session:
        master_rows = (await session.execute(select(MasterCatalog).order_by(MasterCatalog.id.asc()))).scalars().all()

        raw_stmt = (
            select(RawSupplierFeedProduct)
            .where(RawSupplierFeedProduct.supplier_id == D9_SUPPLIER_ID)
            .order_by(RawSupplierFeedProduct.id.asc())
        )
        if limit and limit > 0:
            raw_stmt = raw_stmt.limit(limit)
        raw_rows = (await session.execute(raw_stmt)).scalars().all()
        stats.raw_rows_read = len(raw_rows)

        master_by_barcode: Dict[str, List[MasterCatalog]] = defaultdict(list)
        supplier_by_barcode: Dict[str, List[RawSupplierFeedProduct]] = defaultdict(list)

        for master in master_rows:
            barcode = _normalize_barcode(master.barcode)
            if not barcode:
                stats.skipped_no_barcode += 1
                continue
            master_by_barcode[barcode].append(master)

        for raw in raw_rows:
            barcode = _normalize_barcode(raw.barcode)
            if not barcode:
                stats.skipped_no_barcode += 1
                if stats.skipped_no_barcode <= MAX_LOGGED_CONFLICTS:
                    _warn("Пропущена D9 запись без barcode: supplier_code=%s", raw.supplier_code)
                continue
            supplier_by_barcode[barcode].append(raw)

        stats.unique_master_barcodes = sum(1 for rows in master_by_barcode.values() if len(rows) == 1)
        stats.unique_supplier_barcodes = sum(1 for rows in supplier_by_barcode.values() if len(rows) == 1)

        conflicting_master_barcodes = {barcode for barcode, rows in master_by_barcode.items() if len(rows) > 1}
        conflicting_supplier_barcodes = {barcode for barcode, rows in supplier_by_barcode.items() if len(rows) > 1}
        stats.conflicts_master = len(conflicting_master_barcodes)
        stats.conflicts_supplier = len(conflicting_supplier_barcodes)

        for index, barcode in enumerate(sorted(conflicting_master_barcodes), start=1):
            if index <= MAX_LOGGED_CONFLICTS:
                _warn(
                    "barcode не уникален в master_catalog: barcode=%s, sku_count=%d",
                    barcode,
                    len(master_by_barcode[barcode]),
                )

        for index, barcode in enumerate(sorted(conflicting_supplier_barcodes), start=1):
            if index <= MAX_LOGGED_CONFLICTS:
                _warn(
                    "barcode не уникален у D9 в raw_supplier_feed_products: barcode=%s, item_count=%d",
                    barcode,
                    len(supplier_by_barcode[barcode]),
                )

        now = datetime.now(timezone.utc)
        for barcode, supplier_rows in supplier_by_barcode.items():
            master_matches = master_by_barcode.get(barcode)
            if not master_matches:
                continue
            if len(supplier_rows) != 1 or len(master_matches) != 1:
                continue

            master_row = master_matches[0]
            supplier_row = supplier_rows[0]
            stats.matched_unique_pairs += 1

            existing = (
                await session.execute(
                    select(CatalogSupplierMapping).where(
                        CatalogSupplierMapping.supplier_id == D9_SUPPLIER_ID,
                        CatalogSupplierMapping.supplier_code == supplier_row.supplier_code,
                    )
                )
            ).scalar_one_or_none()

            if existing is None:
                session.add(
                    CatalogSupplierMapping(
                        sku=master_row.sku,
                        supplier_id=D9_SUPPLIER_ID,
                        supplier_code=supplier_row.supplier_code,
                        supplier_product_id=supplier_row.feed_product_id,
                        supplier_product_name_raw=supplier_row.name_raw,
                        barcode=supplier_row.barcode,
                        is_confirmed=True,
                        is_active=True,
                        match_source="barcode",
                        first_seen_at=now,
                        last_seen_at=now,
                    )
                )
                stats.inserted += 1
                continue

            existing_sku = (existing.sku or "").strip()
            if existing_sku and existing_sku != master_row.sku:
                stats.conflict_existing_mapping += 1
                if stats.conflict_existing_mapping <= MAX_LOGGED_CONFLICTS:
                    _warn(
                        "Существующий mapping конфликтует и не будет перезаписан: supplier_code=%s, old_sku=%s, new_sku=%s",
                        supplier_row.supplier_code,
                        existing_sku,
                        master_row.sku,
                    )
                continue

            existing.sku = master_row.sku
            existing.supplier_product_id = supplier_row.feed_product_id
            existing.supplier_product_name_raw = supplier_row.name_raw
            existing.barcode = supplier_row.barcode
            existing.is_confirmed = True
            existing.is_active = True
            existing.match_source = "barcode"
            existing.last_seen_at = now
            stats.updated += 1

    logger.info(
        "Завершён sync D9 barcode mapping: matched=%d, inserted=%d, updated=%d",
        stats.matched_unique_pairs,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Синхронизация catalog_supplier_mapping для D9 по barcode")
    parser.add_argument("--limit", type=int, default=0, help="ограничить количество raw D9 записей (0 = без лимита)")
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await sync_d9_supplier_mapping_by_barcode(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

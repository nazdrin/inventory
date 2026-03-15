import argparse
import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import CatalogSupplierMapping, MasterCatalog, RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d11_barcode_mapping_sync")

D11_SUPPLIER_ID = 48
MAX_LOGGED_CONFLICTS = 20
MAX_SAMPLES = 20


@dataclass
class SyncStats:
    supplier_id: int = D11_SUPPLIER_ID
    raw_rows_read: int = 0
    master_rows_read: int = 0
    unique_master_barcodes: int = 0
    unique_supplier_barcodes: int = 0
    matched_unique_pairs: int = 0
    inserted: int = 0
    updated: int = 0
    conflicts_master: int = 0
    conflicts_supplier: int = 0
    conflict_existing_mapping: int = 0
    skipped_no_barcode: int = 0
    warnings_count: int = 0
    sample_matched_pairs: List[Dict[str, str]] = field(default_factory=list)
    sample_conflicts_master: List[Dict[str, Any]] = field(default_factory=list)
    sample_conflicts_supplier: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "raw_rows_read": self.raw_rows_read,
            "master_rows_read": self.master_rows_read,
            "unique_master_barcodes": self.unique_master_barcodes,
            "unique_supplier_barcodes": self.unique_supplier_barcodes,
            "matched_unique_pairs": self.matched_unique_pairs,
            "inserted": self.inserted,
            "updated": self.updated,
            "conflicts_master": self.conflicts_master,
            "conflicts_supplier": self.conflicts_supplier,
            "conflict_existing_mapping": self.conflict_existing_mapping,
            "skipped_no_barcode": self.skipped_no_barcode,
            "warnings_count": self.warnings_count,
            "sample_matched_pairs": self.sample_matched_pairs,
            "sample_conflicts_master": self.sample_conflicts_master,
            "sample_conflicts_supplier": self.sample_conflicts_supplier,
        }


def _warn(stats: SyncStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _normalize_barcode(value: Any) -> Optional[str]:
    if value is None:
        return None
    barcode = str(value).strip().replace(" ", "")
    if not barcode:
        return None
    if barcode.lower() in {"nan", "none"}:
        return None
    return barcode


def _append_sample(target: List[Dict[str, Any]], item: Dict[str, Any]) -> None:
    if len(target) < MAX_SAMPLES:
        target.append(item)


async def sync_d11_supplier_mapping_by_barcode(limit: int = 0) -> Dict[str, Any]:
    stats = SyncStats()
    logger.info("Запуск sync D11 barcode mapping для supplier_id=%s", D11_SUPPLIER_ID)

    async with get_async_db() as session:
        master_stmt = select(MasterCatalog).order_by(MasterCatalog.id.asc())
        raw_stmt = (
            select(RawSupplierFeedProduct)
            .where(RawSupplierFeedProduct.supplier_id == D11_SUPPLIER_ID)
            .order_by(RawSupplierFeedProduct.id.asc())
        )
        if limit and limit > 0:
            raw_stmt = raw_stmt.limit(limit)

        master_rows = (await session.execute(master_stmt)).scalars().all()
        raw_rows = (await session.execute(raw_stmt)).scalars().all()

        stats.master_rows_read = len(master_rows)
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
                    _warn(stats, "Пропущена D11 запись без barcode: supplier_code=%s", raw.supplier_code)
                continue
            supplier_by_barcode[barcode].append(raw)

        stats.unique_master_barcodes = sum(1 for rows in master_by_barcode.values() if len(rows) == 1)
        stats.unique_supplier_barcodes = sum(1 for rows in supplier_by_barcode.values() if len(rows) == 1)

        conflicting_master_barcodes = {barcode for barcode, rows in master_by_barcode.items() if len(rows) > 1}
        conflicting_supplier_barcodes = {barcode for barcode, rows in supplier_by_barcode.items() if len(rows) > 1}

        stats.conflicts_master = len(conflicting_master_barcodes)
        stats.conflicts_supplier = len(conflicting_supplier_barcodes)

        for index, barcode in enumerate(sorted(conflicting_master_barcodes), start=1):
            sample = {
                "barcode": barcode,
                "sku_count": len(master_by_barcode[barcode]),
            }
            _append_sample(stats.sample_conflicts_master, sample)
            if index <= MAX_LOGGED_CONFLICTS:
                _warn(
                    stats,
                    "barcode не уникален в master_catalog: barcode=%s, sku_count=%d",
                    barcode,
                    len(master_by_barcode[barcode]),
                )

        for index, barcode in enumerate(sorted(conflicting_supplier_barcodes), start=1):
            sample = {
                "barcode": barcode,
                "item_count": len(supplier_by_barcode[barcode]),
            }
            _append_sample(stats.sample_conflicts_supplier, sample)
            if index <= MAX_LOGGED_CONFLICTS:
                _warn(
                    stats,
                    "barcode не уникален у D11 в raw_supplier_feed_products: barcode=%s, item_count=%d",
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
            _append_sample(
                stats.sample_matched_pairs,
                {
                    "barcode": barcode,
                    "sku": master_row.sku,
                    "supplier_code": supplier_row.supplier_code,
                },
            )

            mapping_stmt = select(CatalogSupplierMapping).where(
                CatalogSupplierMapping.supplier_id == D11_SUPPLIER_ID,
                CatalogSupplierMapping.supplier_code == supplier_row.supplier_code,
            )
            existing = (await session.execute(mapping_stmt)).scalar_one_or_none()

            if existing is None:
                session.add(
                    CatalogSupplierMapping(
                        sku=master_row.sku,
                        supplier_id=D11_SUPPLIER_ID,
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
                        stats,
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
        "Завершён sync D11 barcode mapping: matched=%d, inserted=%d, updated=%d",
        stats.matched_unique_pairs,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Синхронизация catalog_supplier_mapping для D11 по barcode"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="ограничить количество raw D11 записей (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await sync_d11_supplier_mapping_by_barcode(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

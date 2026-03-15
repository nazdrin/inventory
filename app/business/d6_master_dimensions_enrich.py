import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import CatalogSupplierMapping, MasterCatalog, RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d6_master_dimensions_enrich")

D6_SUPPLIER_ID = 43


@dataclass
class EnrichStats:
    supplier_id: int = D6_SUPPLIER_ID
    mapped_rows_read: int = 0
    raw_rows_joined: int = 0
    master_rows_checked: int = 0
    weight_filled: int = 0
    length_filled: int = 0
    width_filled: int = 0
    height_filled: int = 0
    master_rows_updated: int = 0
    skipped_no_raw: int = 0
    skipped_no_dimensions: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "mapped_rows_read": self.mapped_rows_read,
            "raw_rows_joined": self.raw_rows_joined,
            "master_rows_checked": self.master_rows_checked,
            "weight_filled": self.weight_filled,
            "length_filled": self.length_filled,
            "width_filled": self.width_filled,
            "height_filled": self.height_filled,
            "master_rows_updated": self.master_rows_updated,
            "skipped_no_raw": self.skipped_no_raw,
            "skipped_no_dimensions": self.skipped_no_dimensions,
            "warnings_count": self.warnings_count,
        }


def _warn(stats: EnrichStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _is_positive_dimension(value: Optional[Decimal]) -> bool:
    return value is not None and value > 0


async def enrich_master_dimensions_from_d6(limit: int = 0) -> Dict[str, Any]:
    stats = EnrichStats()
    logger.info("Запуск D6 enrich master dimensions для supplier_id=%s", D6_SUPPLIER_ID)

    async with get_async_db() as session:
        mapping_stmt = (
            select(CatalogSupplierMapping)
            .where(CatalogSupplierMapping.supplier_id == D6_SUPPLIER_ID)
            .order_by(CatalogSupplierMapping.id.asc())
        )
        if limit and limit > 0:
            mapping_stmt = mapping_stmt.limit(limit)

        mapping_rows = (await session.execute(mapping_stmt)).scalars().all()
        stats.mapped_rows_read = len(mapping_rows)

        for mapping in mapping_rows:
            master = (
                await session.execute(
                    select(MasterCatalog).where(MasterCatalog.sku == mapping.sku)
                )
            ).scalar_one_or_none()
            if master is None:
                _warn(
                    stats,
                    "Не найден master_catalog для mapping: sku=%s, supplier_code=%s",
                    mapping.sku,
                    mapping.supplier_code,
                )
                continue

            stats.master_rows_checked += 1

            if (
                master.weight_g is not None
                and master.length_mm is not None
                and master.width_mm is not None
                and master.height_mm is not None
            ):
                continue

            raw = (
                await session.execute(
                    select(RawSupplierFeedProduct).where(
                        RawSupplierFeedProduct.supplier_id == D6_SUPPLIER_ID,
                        RawSupplierFeedProduct.supplier_code == mapping.supplier_code,
                    )
                )
            ).scalar_one_or_none()

            if raw is None:
                stats.skipped_no_raw += 1
                _warn(
                    stats,
                    "Не найдена raw D6 запись для mapping: sku=%s, supplier_code=%s",
                    mapping.sku,
                    mapping.supplier_code,
                )
                continue

            stats.raw_rows_joined += 1

            useful_dimensions = any(
                (
                    _is_positive_dimension(raw.weight_g),
                    _is_positive_dimension(raw.length_mm),
                    _is_positive_dimension(raw.width_mm),
                    _is_positive_dimension(raw.height_mm),
                )
            )
            if not useful_dimensions:
                stats.skipped_no_dimensions += 1
                _warn(
                    stats,
                    "У D6 нет полезных ВГХ: sku=%s, supplier_code=%s",
                    mapping.sku,
                    mapping.supplier_code,
                )
                continue

            changed = False

            if master.weight_g is None and _is_positive_dimension(raw.weight_g):
                master.weight_g = raw.weight_g
                stats.weight_filled += 1
                changed = True

            if master.length_mm is None and _is_positive_dimension(raw.length_mm):
                master.length_mm = raw.length_mm
                stats.length_filled += 1
                changed = True

            if master.width_mm is None and _is_positive_dimension(raw.width_mm):
                master.width_mm = raw.width_mm
                stats.width_filled += 1
                changed = True

            if master.height_mm is None and _is_positive_dimension(raw.height_mm):
                master.height_mm = raw.height_mm
                stats.height_filled += 1
                changed = True

            if changed:
                stats.master_rows_updated += 1

    logger.info(
        "Завершён D6 enrich master dimensions: checked=%d, updated=%d",
        stats.master_rows_checked,
        stats.master_rows_updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Обогащение master_catalog ВГХ из D6 по catalog_supplier_mapping"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N mapping-строк D6 (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await enrich_master_dimensions_from_d6(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

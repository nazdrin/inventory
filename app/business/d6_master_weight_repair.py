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
logger = logging.getLogger("d6_master_weight_repair")

D6_SUPPLIER_ID = 43


@dataclass
class RepairStats:
    supplier_id: int = D6_SUPPLIER_ID
    mapped_rows_read: int = 0
    raw_rows_joined: int = 0
    repaired: int = 0
    skipped_no_raw: int = 0
    skipped_no_weight: int = 0
    skipped_already_ok: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "mapped_rows_read": self.mapped_rows_read,
            "raw_rows_joined": self.raw_rows_joined,
            "repaired": self.repaired,
            "skipped_no_raw": self.skipped_no_raw,
            "skipped_no_weight": self.skipped_no_weight,
            "skipped_already_ok": self.skipped_already_ok,
            "warnings_count": self.warnings_count,
        }


def _warn(stats: RepairStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _is_positive_weight(value: Optional[Decimal]) -> bool:
    return value is not None and value > 0


async def repair_d6_master_weight(limit: int = 0) -> Dict[str, Any]:
    stats = RepairStats()
    logger.info("Запуск D6 master weight repair для supplier_id=%s", D6_SUPPLIER_ID)

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
                    "Не найдена raw D6 запись для repair: sku=%s, supplier_code=%s",
                    mapping.sku,
                    mapping.supplier_code,
                )
                continue

            stats.raw_rows_joined += 1

            if not _is_positive_weight(raw.weight_g) or master.weight_g is None:
                stats.skipped_no_weight += 1
                continue

            if master.weight_g >= Decimal("20") or raw.weight_g < Decimal("100"):
                stats.skipped_already_ok += 1
                continue

            if master.weight_g == raw.weight_g:
                stats.skipped_already_ok += 1
                continue

            master.weight_g = raw.weight_g
            stats.repaired += 1

    logger.info(
        "Завершён D6 master weight repair: mapped=%d, repaired=%d",
        stats.mapped_rows_read,
        stats.repaired,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Безопасная починка ошибочно сохранённого веса D6 в master_catalog"
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
    result = await repair_d6_master_weight(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

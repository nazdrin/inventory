import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import CatalogCategory, RawTabletkiCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("catalog_categories_sync")


@dataclass
class SyncStats:
    raw_rows_read: int = 0
    categories_l1_found: int = 0
    categories_l2_found: int = 0
    total_unique_categories: int = 0
    inserted: int = 0
    updated: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "raw_rows_read": self.raw_rows_read,
            "categories_l1_found": self.categories_l1_found,
            "categories_l2_found": self.categories_l2_found,
            "total_unique_categories": self.total_unique_categories,
            "inserted": self.inserted,
            "updated": self.updated,
            "warnings_count": self.warnings_count,
        }


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _warn(stats: SyncStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _register_category(
    categories: Dict[str, Dict[str, Any]],
    *,
    category_code: Optional[str],
    parent_category_code: Optional[str],
    name_ua: Optional[str],
    level_no: int,
    stats: SyncStats,
    source_label: str,
) -> bool:
    code = _normalize_string(category_code)
    name = _normalize_string(name_ua)
    parent_code = _normalize_string(parent_category_code)

    if not code and not name:
        return False

    if not code or not name:
        _warn(
            stats,
            "Пропущена категория %s из-за пустых данных: code=%r, name_ua=%r",
            source_label,
            code,
            name,
        )
        return False

    if level_no == 2 and not parent_code:
        _warn(
            stats,
            "У категории 2 уровня нет parent category code: code=%s, name_ua=%s",
            code,
            name,
        )

    categories[code] = {
        "category_code": code,
        "parent_category_code": parent_code,
        "name_ua": name,
        "name_ru": None,
        "level_no": level_no,
        "is_active": True,
    }
    return True


async def sync_catalog_categories_from_raw(limit: int = 0) -> Dict[str, Any]:
    stats = SyncStats()
    logger.info("Запуск синхронизации catalog_categories из raw_tabletki_catalog")

    async with get_async_db() as session:
        stmt = select(RawTabletkiCatalog).order_by(RawTabletkiCatalog.id.asc())
        if limit and limit > 0:
            stmt = stmt.limit(limit)

        raw_rows = (await session.execute(stmt)).scalars().all()
        stats.raw_rows_read = len(raw_rows)

        categories: Dict[str, Dict[str, Any]] = {}

        for raw in raw_rows:
            if _register_category(
                categories,
                category_code=raw.category_l1_code,
                parent_category_code=None,
                name_ua=raw.category_l1_name,
                level_no=1,
                stats=stats,
                source_label="L1",
            ):
                stats.categories_l1_found += 1

            if _register_category(
                categories,
                category_code=raw.category_l2_code,
                parent_category_code=raw.category_l1_code,
                name_ua=raw.category_l2_name,
                level_no=2,
                stats=stats,
                source_label="L2",
            ):
                stats.categories_l2_found += 1

        stats.total_unique_categories = len(categories)

        for item in categories.values():
            stmt = select(CatalogCategory).where(
                CatalogCategory.category_code == item["category_code"]
            )
            existing = (await session.execute(stmt)).scalar_one_or_none()

            if existing is None:
                session.add(
                    CatalogCategory(
                        category_code=item["category_code"],
                        parent_category_code=item["parent_category_code"],
                        name_ua=item["name_ua"],
                        name_ru=None,
                        level_no=item["level_no"],
                        is_active=True,
                    )
                )
                stats.inserted += 1
                continue

            existing.parent_category_code = item["parent_category_code"]
            existing.name_ua = item["name_ua"]
            existing.level_no = item["level_no"]
            existing.is_active = True
            stats.updated += 1

    logger.info(
        "Синхронизация завершена: raw_rows=%d, unique_categories=%d, inserted=%d, updated=%d",
        stats.raw_rows_read,
        stats.total_unique_categories,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Синхронизация catalog_categories из raw_tabletki_catalog"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N записей raw_tabletki_catalog (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await sync_catalog_categories_from_raw(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import CatalogImage, MasterCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_main_image_select")

D1_SUPPLIER_ID = 38
MAX_LOGGED_WARNINGS = 20


@dataclass
class SelectStats:
    supplier_id: int = D1_SUPPLIER_ID
    candidate_skus: int = 0
    selected_images: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped_no_images: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "candidate_skus": self.candidate_skus,
            "selected_images": self.selected_images,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "skipped_no_images": self.skipped_no_images,
            "warnings_count": self.warnings_count,
        }


def _warn(stats: SelectStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


async def select_master_main_images(limit: int = 0) -> Dict[str, Any]:
    stats = SelectStats()
    logger.info("Запуск выбора main_image_url из D1 catalog_images, supplier_id=%s", D1_SUPPLIER_ID)

    async with get_async_db() as session:
        image_stmt = (
            select(CatalogImage)
            .where(
                CatalogImage.supplier_id == D1_SUPPLIER_ID,
                CatalogImage.is_active.is_(True),
            )
            .order_by(CatalogImage.sku.asc(), CatalogImage.sort_order.asc(), CatalogImage.id.asc())
        )
        image_rows = (await session.execute(image_stmt)).scalars().all()

        images_by_sku: Dict[str, List[CatalogImage]] = {}
        for image in image_rows:
            sku = _normalize_string(image.sku)
            if not sku:
                if stats.warnings_count < MAX_LOGGED_WARNINGS:
                    _warn(stats, "Найдена запись catalog_images без sku, id=%s", image.id)
                continue
            images_by_sku.setdefault(sku, []).append(image)

        candidate_skus = list(images_by_sku.keys())
        if limit and limit > 0:
            candidate_skus = candidate_skus[:limit]
        stats.candidate_skus = len(candidate_skus)

        for sku in candidate_skus:
            image_rows = images_by_sku.get(sku, [])
            if not image_rows:
                stats.skipped_no_images += 1
                continue

            master = (
                await session.execute(
                    select(MasterCatalog).where(MasterCatalog.sku == sku)
                )
            ).scalar_one_or_none()
            if master is None:
                if stats.warnings_count < MAX_LOGGED_WARNINGS:
                    _warn(stats, "Не найден master_catalog для sku=%s", sku)
                continue

            selected_url = None
            for image in image_rows:
                image_url = _normalize_string(image.image_url)
                if image_url:
                    selected_url = image_url
                    break

            if not selected_url:
                stats.skipped_no_images += 1
                if stats.skipped_no_images <= MAX_LOGGED_WARNINGS:
                    _warn(stats, "Не найден валидный image_url для sku=%s", sku)
                continue

            stats.selected_images += 1

            if _normalize_string(master.main_image_url) == selected_url:
                stats.unchanged += 1
                continue

            master.main_image_url = selected_url
            stats.updated += 1

    logger.info(
        "Завершён выбор main_image_url: candidate_skus=%d, selected=%d, updated=%d, unchanged=%d",
        stats.candidate_skus,
        stats.selected_images,
        stats.updated,
        stats.unchanged,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Выбор главной картинки для master_catalog из catalog_images (D1)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N sku из master_catalog (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await select_master_main_images(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

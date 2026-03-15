import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import CatalogImage, CatalogSupplierMapping, RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d3_images_sync")

D3_SUPPLIER_ID = 40
SOURCE_TYPE = "supplier_feed"
MAX_LOGGED_WARNINGS = 20


@dataclass
class SyncStats:
    supplier_id: int = D3_SUPPLIER_ID
    mapped_rows_read: int = 0
    raw_rows_joined: int = 0
    images_found: int = 0
    inserted: int = 0
    updated: int = 0
    skipped_no_raw: int = 0
    skipped_no_images: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "mapped_rows_read": self.mapped_rows_read,
            "raw_rows_joined": self.raw_rows_joined,
            "images_found": self.images_found,
            "inserted": self.inserted,
            "updated": self.updated,
            "skipped_no_raw": self.skipped_no_raw,
            "skipped_no_images": self.skipped_no_images,
            "warnings_count": self.warnings_count,
        }


def _warn(stats: SyncStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _extract_images(source_payload: Any) -> List[str]:
    if not isinstance(source_payload, dict):
        return []
    raw_images = source_payload.get("images")
    if not isinstance(raw_images, list):
        return []

    result: List[str] = []
    seen = set()
    for value in raw_images:
        image_url = _normalize_string(value)
        if not image_url or image_url in seen:
            continue
        seen.add(image_url)
        result.append(image_url)
    return result


def _looks_strange_url(url: str) -> bool:
    normalized = url.lower()
    return not (normalized.startswith("http://") or normalized.startswith("https://"))


async def sync_d3_images(limit: int = 0) -> Dict[str, Any]:
    stats = SyncStats()
    logger.info("Запуск D3 images sync для supplier_id=%s", D3_SUPPLIER_ID)

    async with get_async_db() as session:
        mapping_stmt = (
            select(CatalogSupplierMapping)
            .where(CatalogSupplierMapping.supplier_id == D3_SUPPLIER_ID)
            .order_by(CatalogSupplierMapping.id.asc())
        )
        if limit and limit > 0:
            mapping_stmt = mapping_stmt.limit(limit)

        mapping_rows = (await session.execute(mapping_stmt)).scalars().all()
        stats.mapped_rows_read = len(mapping_rows)

        for mapping in mapping_rows:
            raw = (
                await session.execute(
                    select(RawSupplierFeedProduct).where(
                        RawSupplierFeedProduct.supplier_id == D3_SUPPLIER_ID,
                        RawSupplierFeedProduct.supplier_code == mapping.supplier_code,
                    )
                )
            ).scalar_one_or_none()

            if raw is None:
                stats.skipped_no_raw += 1
                if stats.skipped_no_raw <= MAX_LOGGED_WARNINGS:
                    _warn(
                        stats,
                        "Не найдена raw D3 запись для images sync: sku=%s, supplier_code=%s",
                        mapping.sku,
                        mapping.supplier_code,
                    )
                continue

            stats.raw_rows_joined += 1

            images = _extract_images(raw.source_payload)
            if not images:
                stats.skipped_no_images += 1
                if stats.skipped_no_images <= MAX_LOGGED_WARNINGS:
                    _warn(
                        stats,
                        "У D3 нет картинок в source_payload: sku=%s, supplier_code=%s",
                        mapping.sku,
                        mapping.supplier_code,
                    )
                continue

            stats.images_found += len(images)

            for sort_order, image_url in enumerate(images):
                if _looks_strange_url(image_url) and stats.warnings_count < MAX_LOGGED_WARNINGS:
                    _warn(
                        stats,
                        "Подозрительный image URL для D3: sku=%s, supplier_code=%s, url=%s",
                        mapping.sku,
                        mapping.supplier_code,
                        image_url,
                    )

                image_stmt = select(CatalogImage).where(
                    CatalogImage.sku == mapping.sku,
                    CatalogImage.supplier_id == D3_SUPPLIER_ID,
                    CatalogImage.image_url == image_url,
                )
                existing = (await session.execute(image_stmt)).scalar_one_or_none()

                if existing is None:
                    session.add(
                        CatalogImage(
                            sku=mapping.sku,
                            supplier_id=D3_SUPPLIER_ID,
                            source_type=SOURCE_TYPE,
                            image_url=image_url,
                            sort_order=sort_order,
                            is_main=False,
                            is_active=True,
                        )
                    )
                    stats.inserted += 1
                    continue

                existing.sort_order = sort_order
                existing.is_active = True
                existing.is_main = False
                existing.source_type = SOURCE_TYPE
                stats.updated += 1

    logger.info(
        "Завершён D3 images sync: mapped=%d, joined=%d, inserted=%d, updated=%d",
        stats.mapped_rows_read,
        stats.raw_rows_joined,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Синхронизация картинок D3 из raw_supplier_feed_products в catalog_images"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N mapping-строк D3 (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await sync_d3_images(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

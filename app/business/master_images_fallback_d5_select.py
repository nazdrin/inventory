import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Dict

from sqlalchemy import and_, func, or_, select

from app.database import get_async_db
from app.models import CatalogImage, MasterCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_images_fallback_d5_select")

D5_SUPPLIER_ID = 42


@dataclass
class FallbackStats:
    total_products_without_image: int = 0
    d5_images_found: int = 0
    updated_products: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "total_products_without_image": self.total_products_without_image,
            "d5_images_found": self.d5_images_found,
            "updated_products": self.updated_products,
        }


async def select_d5_fallback_main_images() -> Dict[str, int]:
    stats = FallbackStats()

    async with get_async_db() as session:
        empty_main_image_condition = or_(
            MasterCatalog.main_image_url.is_(None),
            MasterCatalog.main_image_url == "",
        )

        stats.total_products_without_image = len(
            (
                await session.execute(
                    select(MasterCatalog.id).where(empty_main_image_condition)
                )
            ).all()
        )
        logger.info("Товаров без главной картинки: %d", stats.total_products_without_image)

        if not stats.total_products_without_image:
            return stats.to_dict()

        joined_rows = (
            await session.execute(
                select(MasterCatalog, CatalogImage)
                .join(
                    CatalogImage,
                    and_(
                        CatalogImage.sku == MasterCatalog.sku,
                        CatalogImage.supplier_id == D5_SUPPLIER_ID,
                        CatalogImage.image_url.is_not(None),
                        CatalogImage.image_url != "",
                    ),
                )
                .where(empty_main_image_condition)
                .order_by(
                    MasterCatalog.id.asc(),
                    func.coalesce(CatalogImage.sort_order, 0).asc(),
                    CatalogImage.id.asc(),
                )
            )
        ).all()

        selected_by_sku = {}
        for master, image in joined_rows:
            if master.sku and master.sku not in selected_by_sku:
                selected_by_sku[master.sku] = (master, image)

        stats.d5_images_found = len(selected_by_sku)
        logger.info("Найдено fallback-картинок D5: %d", stats.d5_images_found)

        for master, image in selected_by_sku.values():
            if master.main_image_url is not None and str(master.main_image_url).strip():
                continue
            master.main_image_url = image.image_url
            stats.updated_products += 1

    logger.info("Обновлено товаров fallback-картинками D5: %d", stats.updated_products)
    return stats.to_dict()


async def _amain() -> None:
    result = await select_d5_fallback_main_images()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

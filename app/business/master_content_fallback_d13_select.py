import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Dict

from sqlalchemy import and_, case, or_, select

from app.database import get_async_db
from app.models import CatalogContent, MasterCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_content_fallback_d13_select")

D13_SUPPLIER_ID = 51


@dataclass
class FallbackStats:
    total_products_missing_ua: int = 0
    d13_ua_found: int = 0
    updated_ua: int = 0
    updated_products: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "total_products_missing_ua": self.total_products_missing_ua,
            "d13_ua_found": self.d13_ua_found,
            "updated_ua": self.updated_ua,
            "updated_products": self.updated_products,
        }


def _has_text_column(column):
    return and_(column.is_not(None), column != "")


async def select_d13_fallback_content() -> Dict[str, int]:
    stats = FallbackStats()

    async with get_async_db() as session:
        missing_ua_condition = or_(
            MasterCatalog.description_ua.is_(None),
            MasterCatalog.description_ua == "",
        )

        stats.total_products_missing_ua = len((await session.execute(select(MasterCatalog.id).where(missing_ua_condition))).all())
        logger.info("Товаров без description_ua: %d", stats.total_products_missing_ua)
        if not stats.total_products_missing_ua:
            return stats.to_dict()

        joined_rows = (
            await session.execute(
                select(MasterCatalog, CatalogContent)
                .join(
                    CatalogContent,
                    and_(
                        CatalogContent.sku == MasterCatalog.sku,
                        CatalogContent.supplier_id == D13_SUPPLIER_ID,
                        CatalogContent.is_active.is_(True),
                        CatalogContent.language_code == "ua",
                        _has_text_column(CatalogContent.description),
                    ),
                )
                .where(missing_ua_condition)
                .order_by(
                    MasterCatalog.id.asc(),
                    case((CatalogContent.is_selected.is_(True), 0), else_=1).asc(),
                    CatalogContent.id.asc(),
                )
            )
        ).all()

        selected_by_sku = {}
        for master, content in joined_rows:
            if master.sku and master.sku not in selected_by_sku:
                selected_by_sku[master.sku] = (master, content)

        stats.d13_ua_found = len(selected_by_sku)
        logger.info("Найдено D13 fallback descriptions ua: %d", stats.d13_ua_found)

        for sku, (master, content) in selected_by_sku.items():
            if master.description_ua is not None and str(master.description_ua).strip():
                continue
            master.description_ua = content.description
            stats.updated_ua += 1

        stats.updated_products = stats.updated_ua

    logger.info("Обновлено description_ua D13 fallback: %d", stats.updated_ua)
    return stats.to_dict()


async def _amain() -> None:
    result = await select_d13_fallback_content()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

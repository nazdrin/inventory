import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Dict, Tuple

from sqlalchemy import and_, case, or_, select

from app.database import get_async_db
from app.models import CatalogContent, MasterCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_content_fallback_d2_select")

D2_SUPPLIER_ID = 39


@dataclass
class FallbackStats:
    total_products_missing_ua: int = 0
    total_products_missing_ru: int = 0
    d2_ua_found: int = 0
    d2_ru_found: int = 0
    updated_ua: int = 0
    updated_ru: int = 0
    updated_products: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "total_products_missing_ua": self.total_products_missing_ua,
            "total_products_missing_ru": self.total_products_missing_ru,
            "d2_ua_found": self.d2_ua_found,
            "d2_ru_found": self.d2_ru_found,
            "updated_ua": self.updated_ua,
            "updated_ru": self.updated_ru,
            "updated_products": self.updated_products,
        }


def _has_text_column(column):
    return and_(column.is_not(None), column != "")


async def select_d2_fallback_content() -> Dict[str, int]:
    stats = FallbackStats()

    async with get_async_db() as session:
        missing_ua_condition = or_(
            MasterCatalog.description_ua.is_(None),
            MasterCatalog.description_ua == "",
        )
        missing_ru_condition = or_(
            MasterCatalog.description_ru.is_(None),
            MasterCatalog.description_ru == "",
        )

        stats.total_products_missing_ua = len(
            (await session.execute(select(MasterCatalog.id).where(missing_ua_condition))).all()
        )
        stats.total_products_missing_ru = len(
            (await session.execute(select(MasterCatalog.id).where(missing_ru_condition))).all()
        )

        logger.info("Товаров без description_ua: %d", stats.total_products_missing_ua)
        logger.info("Товаров без description_ru: %d", stats.total_products_missing_ru)

        joined_rows = (
            await session.execute(
                select(MasterCatalog, CatalogContent)
                .join(
                    CatalogContent,
                    and_(
                        CatalogContent.sku == MasterCatalog.sku,
                        CatalogContent.supplier_id == D2_SUPPLIER_ID,
                        CatalogContent.is_active.is_(True),
                        _has_text_column(CatalogContent.description),
                    ),
                )
                .where(
                    or_(missing_ua_condition, missing_ru_condition),
                    CatalogContent.language_code.in_(("ua", "ru")),
                )
                .order_by(
                    MasterCatalog.id.asc(),
                    CatalogContent.language_code.asc(),
                    case((CatalogContent.is_selected.is_(True), 0), else_=1).asc(),
                    CatalogContent.id.asc(),
                )
            )
        ).all()

        selected_by_key: Dict[Tuple[str, str], Tuple[MasterCatalog, CatalogContent]] = {}
        for master, content in joined_rows:
            if not master.sku:
                continue
            key = (master.sku, content.language_code)
            if key not in selected_by_key:
                selected_by_key[key] = (master, content)

        stats.d2_ua_found = sum(1 for _, language in selected_by_key.keys() if language == "ua")
        stats.d2_ru_found = sum(1 for _, language in selected_by_key.keys() if language == "ru")

        logger.info("Найдено D2 fallback descriptions ua: %d", stats.d2_ua_found)
        logger.info("Найдено D2 fallback descriptions ru: %d", stats.d2_ru_found)

        updated_skus = set()
        for (sku, language_code), (master, content) in selected_by_key.items():
            if language_code == "ua":
                if master.description_ua is not None and str(master.description_ua).strip():
                    continue
                master.description_ua = content.description
                stats.updated_ua += 1
                updated_skus.add(sku)
                continue

            if language_code == "ru":
                if master.description_ru is not None and str(master.description_ru).strip():
                    continue
                master.description_ru = content.description
                stats.updated_ru += 1
                updated_skus.add(sku)

        stats.updated_products = len(updated_skus)

    logger.info("Обновлено description_ua: %d", stats.updated_ua)
    logger.info("Обновлено description_ru: %d", stats.updated_ru)
    logger.info("Обновлено уникальных товаров: %d", stats.updated_products)
    return stats.to_dict()


async def _amain() -> None:
    result = await select_d2_fallback_content()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

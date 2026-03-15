import argparse
import asyncio
import json
from typing import Any, Dict, Iterable, List

from sqlalchemy import and_, distinct, func, or_, select

from app.database import get_async_db
from app.models import (
    CatalogCategory,
    CatalogContent,
    CatalogImage,
    CatalogSupplierMapping,
    MasterCatalog,
    RawSupplierFeedProduct,
    RawTabletkiCatalog,
)


SUPPLIER_IDS = (38, 39, 40, 43, 42, 44, 41, 47, 45, 46, 48, 49)


def _has_text_column(column):
    return and_(column.is_not(None), func.length(func.trim(column)) > 0)


def _has_positive_column(column):
    return and_(column.is_not(None), column > 0)


def _payload_images_count(rows: Iterable[RawSupplierFeedProduct]) -> int:
    total = 0
    for row in rows:
        payload = row.source_payload
        if not isinstance(payload, dict):
            continue
        images = payload.get("images")
        if isinstance(images, list) and any(str(item).strip() for item in images if item is not None):
            total += 1
    return total


async def _scalar_count(session, stmt) -> int:
    return int((await session.execute(stmt)).scalar() or 0)


async def _build_master_catalog_block(session) -> Dict[str, int]:
    return {
        "total_products": await _scalar_count(session, select(func.count()).select_from(MasterCatalog)),
        "with_barcode": await _scalar_count(session, select(func.count()).select_from(MasterCatalog).where(_has_text_column(MasterCatalog.barcode))),
        "with_weight": await _scalar_count(session, select(func.count()).select_from(MasterCatalog).where(_has_positive_column(MasterCatalog.weight_g))),
        "with_dimensions_complete": await _scalar_count(
            session,
            select(func.count()).select_from(MasterCatalog).where(
                _has_positive_column(MasterCatalog.length_mm),
                _has_positive_column(MasterCatalog.width_mm),
                _has_positive_column(MasterCatalog.height_mm),
            ),
        ),
        "with_main_image": await _scalar_count(session, select(func.count()).select_from(MasterCatalog).where(_has_text_column(MasterCatalog.main_image_url))),
        "with_description_ua": await _scalar_count(session, select(func.count()).select_from(MasterCatalog).where(_has_text_column(MasterCatalog.description_ua))),
        "with_description_ru": await _scalar_count(session, select(func.count()).select_from(MasterCatalog).where(_has_text_column(MasterCatalog.description_ru))),
        "archived_count": await _scalar_count(session, select(func.count()).select_from(MasterCatalog).where(MasterCatalog.is_archived.is_(True))),
    }


async def _build_categories_block(session) -> Dict[str, int]:
    return {
        "total_categories": await _scalar_count(session, select(func.count()).select_from(CatalogCategory)),
        "level_1_count": await _scalar_count(session, select(func.count()).select_from(CatalogCategory).where(CatalogCategory.level_no == 1)),
        "level_2_count": await _scalar_count(session, select(func.count()).select_from(CatalogCategory).where(CatalogCategory.level_no == 2)),
        "active_categories": await _scalar_count(session, select(func.count()).select_from(CatalogCategory).where(CatalogCategory.is_active.is_(True))),
    }


async def _build_raw_tabletki_block(session) -> Dict[str, int]:
    return {
        "total_raw_tabletki": await _scalar_count(session, select(func.count()).select_from(RawTabletkiCatalog)),
        "distinct_tabletki_sku": await _scalar_count(session, select(func.count(distinct(RawTabletkiCatalog.sku))).select_from(RawTabletkiCatalog)),
        "with_barcode": await _scalar_count(session, select(func.count()).select_from(RawTabletkiCatalog).where(_has_text_column(RawTabletkiCatalog.barcode))),
        "with_weight": await _scalar_count(session, select(func.count()).select_from(RawTabletkiCatalog).where(_has_positive_column(RawTabletkiCatalog.weight_g))),
        "with_dimensions_complete": await _scalar_count(
            session,
            select(func.count()).select_from(RawTabletkiCatalog).where(
                _has_positive_column(RawTabletkiCatalog.length_mm),
                _has_positive_column(RawTabletkiCatalog.width_mm),
                _has_positive_column(RawTabletkiCatalog.height_mm),
            ),
        ),
    }


async def _build_supplier_block(session, supplier_id: int) -> Dict[str, Any]:
    raw_rows = (
        await session.execute(
            select(RawSupplierFeedProduct).where(RawSupplierFeedProduct.supplier_id == supplier_id)
        )
    ).scalars().all()

    raw_count = len(raw_rows)
    raw_distinct_supplier_code = len(
        {
            row.supplier_code.strip()
            for row in raw_rows
            if row.supplier_code is not None and str(row.supplier_code).strip()
        }
    )
    raw_with_barcode = sum(1 for row in raw_rows if row.barcode is not None and str(row.barcode).strip())
    raw_with_description = sum(1 for row in raw_rows if row.description_raw is not None and str(row.description_raw).strip())
    raw_with_images = _payload_images_count(raw_rows)

    mapped_count = await _scalar_count(
        session,
        select(func.count()).select_from(CatalogSupplierMapping).where(CatalogSupplierMapping.supplier_id == supplier_id),
    )
    mapped_distinct_sku = await _scalar_count(
        session,
        select(func.count(distinct(CatalogSupplierMapping.sku))).select_from(CatalogSupplierMapping).where(CatalogSupplierMapping.supplier_id == supplier_id),
    )

    catalog_images_count = await _scalar_count(
        session,
        select(func.count()).select_from(CatalogImage).where(CatalogImage.supplier_id == supplier_id),
    )
    catalog_images_distinct_sku = await _scalar_count(
        session,
        select(func.count(distinct(CatalogImage.sku))).select_from(CatalogImage).where(CatalogImage.supplier_id == supplier_id),
    )

    catalog_content_count = await _scalar_count(
        session,
        select(func.count()).select_from(CatalogContent).where(CatalogContent.supplier_id == supplier_id),
    )
    catalog_content_distinct_sku = await _scalar_count(
        session,
        select(func.count(distinct(CatalogContent.sku))).select_from(CatalogContent).where(CatalogContent.supplier_id == supplier_id),
    )
    catalog_content_ua_count = await _scalar_count(
        session,
        select(func.count()).select_from(CatalogContent).where(
            CatalogContent.supplier_id == supplier_id,
            CatalogContent.language_code == "ua",
        ),
    )
    catalog_content_ru_count = await _scalar_count(
        session,
        select(func.count()).select_from(CatalogContent).where(
            CatalogContent.supplier_id == supplier_id,
            CatalogContent.language_code == "ru",
        ),
    )

    selected_as_main_image_count = await _scalar_count(
        session,
        select(func.count(distinct(MasterCatalog.sku)))
        .select_from(MasterCatalog)
        .join(
            CatalogImage,
            and_(
                CatalogImage.sku == MasterCatalog.sku,
                CatalogImage.supplier_id == supplier_id,
                CatalogImage.image_url == MasterCatalog.main_image_url,
            ),
        )
        .where(_has_text_column(MasterCatalog.main_image_url)),
    )
    selected_description_ua_count = await _scalar_count(
        session,
        select(func.count(distinct(MasterCatalog.sku)))
        .select_from(MasterCatalog)
        .join(
            CatalogContent,
            and_(
                CatalogContent.sku == MasterCatalog.sku,
                CatalogContent.supplier_id == supplier_id,
                CatalogContent.language_code == "ua",
                CatalogContent.description == MasterCatalog.description_ua,
            ),
        )
        .where(_has_text_column(MasterCatalog.description_ua)),
    )
    selected_description_ru_count = await _scalar_count(
        session,
        select(func.count(distinct(MasterCatalog.sku)))
        .select_from(MasterCatalog)
        .join(
            CatalogContent,
            and_(
                CatalogContent.sku == MasterCatalog.sku,
                CatalogContent.supplier_id == supplier_id,
                CatalogContent.language_code == "ru",
                CatalogContent.description == MasterCatalog.description_ru,
            ),
        )
        .where(_has_text_column(MasterCatalog.description_ru)),
    )

    return {
        "raw": {
            "raw_count": raw_count,
            "raw_distinct_supplier_code": raw_distinct_supplier_code,
            "raw_with_barcode": raw_with_barcode,
            "raw_with_description": raw_with_description,
            "raw_with_images": raw_with_images,
        },
        "mapping": {
            "mapped_count": mapped_count,
            "mapped_distinct_sku": mapped_distinct_sku,
        },
        "images": {
            "catalog_images_count": catalog_images_count,
            "catalog_images_distinct_sku": catalog_images_distinct_sku,
        },
        "content": {
            "catalog_content_count": catalog_content_count,
            "catalog_content_distinct_sku": catalog_content_distinct_sku,
            "catalog_content_ua_count": catalog_content_ua_count,
            "catalog_content_ru_count": catalog_content_ru_count,
        },
        "selection_impact": {
            "selected_as_main_image_count": selected_as_main_image_count,
            "selected_description_ua_count": selected_description_ua_count,
            "selected_description_ru_count": selected_description_ru_count,
        },
    }


async def _build_data_quality_block(session) -> Dict[str, int]:
    products_with_any_supplier_mapping = await _scalar_count(
        session,
        select(func.count(distinct(CatalogSupplierMapping.sku))).select_from(CatalogSupplierMapping),
    )
    return {
        "products_ready_for_publish": await _scalar_count(
            session,
            select(func.count()).select_from(MasterCatalog).where(
                _has_text_column(MasterCatalog.sku),
                _has_text_column(MasterCatalog.name_ua),
                or_(_has_text_column(MasterCatalog.category_l1_code), _has_text_column(MasterCatalog.category_l2_code)),
            ),
        ),
        "products_with_full_media_and_content": await _scalar_count(
            session,
            select(func.count()).select_from(MasterCatalog).where(
                _has_text_column(MasterCatalog.main_image_url),
                _has_text_column(MasterCatalog.description_ua),
            ),
        ),
        "products_with_barcode_and_category": await _scalar_count(
            session,
            select(func.count()).select_from(MasterCatalog).where(
                _has_text_column(MasterCatalog.barcode),
                or_(_has_text_column(MasterCatalog.category_l1_code), _has_text_column(MasterCatalog.category_l2_code)),
            ),
        ),
        "products_with_any_supplier_mapping": products_with_any_supplier_mapping,
    }


async def build_master_catalog_coverage_report() -> Dict[str, Any]:
    async with get_async_db() as session:
        return {
            "master_catalog": await _build_master_catalog_block(session),
            "catalog_categories": await _build_categories_block(session),
            "raw_tabletki_catalog": await _build_raw_tabletki_block(session),
            "suppliers": {
                str(supplier_id): await _build_supplier_block(session, supplier_id)
                for supplier_id in SUPPLIER_IDS
            },
            "data_quality": await _build_data_quality_block(session),
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Диагностический отчёт покрытия master-каталога и supplier-контуров"
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="печатать JSON в красивом формате",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    report = await build_master_catalog_coverage_report()
    if args.pretty:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(_amain())

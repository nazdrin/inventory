import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import CatalogContent, CatalogSupplierMapping, RawSupplierFeedProduct


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("d5_content_sync")

D5_SUPPLIER_ID = 42
SOURCE_TYPE = "supplier_feed"
MAX_LOGGED_WARNINGS = 20


@dataclass
class SyncStats:
    supplier_id: int = D5_SUPPLIER_ID
    mapped_rows_read: int = 0
    raw_rows_joined: int = 0
    ua_found: int = 0
    ru_found: int = 0
    inserted: int = 0
    updated: int = 0
    skipped_no_raw: int = 0
    skipped_no_content: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "mapped_rows_read": self.mapped_rows_read,
            "raw_rows_joined": self.raw_rows_joined,
            "ua_found": self.ua_found,
            "ru_found": self.ru_found,
            "inserted": self.inserted,
            "updated": self.updated,
            "skipped_no_raw": self.skipped_no_raw,
            "skipped_no_content": self.skipped_no_content,
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


def _extract_payload_value(payload: Any, key: str) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    return _normalize_string(payload.get(key))


async def _upsert_content(
    *,
    session,
    sku: str,
    supplier_id: int,
    language_code: str,
    title: Optional[str],
    description: str,
    stats: SyncStats,
) -> None:
    stmt = select(CatalogContent).where(
        CatalogContent.sku == sku,
        CatalogContent.supplier_id == supplier_id,
        CatalogContent.language_code == language_code,
        CatalogContent.source_type == SOURCE_TYPE,
    )
    existing = (await session.execute(stmt)).scalar_one_or_none()

    if existing is None:
        session.add(
            CatalogContent(
                sku=sku,
                language_code=language_code,
                source_type=SOURCE_TYPE,
                supplier_id=supplier_id,
                title=title,
                description=description,
                is_selected=False,
                is_active=True,
            )
        )
        stats.inserted += 1
        return

    existing.title = title
    existing.description = description
    existing.is_active = True
    stats.updated += 1


async def sync_d5_content(limit: int = 0) -> Dict[str, Any]:
    stats = SyncStats()
    logger.info("Запуск D5 content sync для supplier_id=%s", D5_SUPPLIER_ID)

    async with get_async_db() as session:
        mapping_stmt = (
            select(CatalogSupplierMapping)
            .where(CatalogSupplierMapping.supplier_id == D5_SUPPLIER_ID)
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
                        RawSupplierFeedProduct.supplier_id == D5_SUPPLIER_ID,
                        RawSupplierFeedProduct.supplier_code == mapping.supplier_code,
                    )
                )
            ).scalar_one_or_none()

            if raw is None:
                stats.skipped_no_raw += 1
                if stats.skipped_no_raw <= MAX_LOGGED_WARNINGS:
                    _warn(
                        stats,
                        "Не найдена raw D5 запись для content sync: sku=%s, supplier_code=%s",
                        mapping.sku,
                        mapping.supplier_code,
                    )
                continue

            stats.raw_rows_joined += 1

            payload = raw.source_payload
            if payload is not None and not isinstance(payload, dict):
                stats.skipped_no_content += 1
                if stats.skipped_no_content <= MAX_LOGGED_WARNINGS:
                    _warn(
                        stats,
                        "Неожиданная структура source_payload у D5: sku=%s, supplier_code=%s",
                        mapping.sku,
                        mapping.supplier_code,
                    )
                continue

            fallback_title = _normalize_string(raw.name_raw) or _extract_payload_value(payload, "name")
            fallback_description = _normalize_string(raw.description_raw) or _extract_payload_value(payload, "description")

            description_ua = _extract_payload_value(payload, "description_ua")
            description_ru = _extract_payload_value(payload, "description_ru")
            name_ua = _extract_payload_value(payload, "name_ua")
            name_ru = _extract_payload_value(payload, "name_ru")

            has_split_languages = bool(description_ua or description_ru or name_ua or name_ru)
            has_content = False

            if description_ua:
                has_content = True
                stats.ua_found += 1
                await _upsert_content(
                    session=session,
                    sku=mapping.sku,
                    supplier_id=D5_SUPPLIER_ID,
                    language_code="ua",
                    title=name_ua or fallback_title,
                    description=description_ua,
                    stats=stats,
                )
            elif not has_split_languages and fallback_description:
                has_content = True
                stats.ua_found += 1
                await _upsert_content(
                    session=session,
                    sku=mapping.sku,
                    supplier_id=D5_SUPPLIER_ID,
                    language_code="ua",
                    title=fallback_title,
                    description=fallback_description,
                    stats=stats,
                )

            if description_ru:
                has_content = True
                stats.ru_found += 1
                await _upsert_content(
                    session=session,
                    sku=mapping.sku,
                    supplier_id=D5_SUPPLIER_ID,
                    language_code="ru",
                    title=name_ru or fallback_title,
                    description=description_ru,
                    stats=stats,
                )

            if not has_content:
                stats.skipped_no_content += 1
                if stats.skipped_no_content <= MAX_LOGGED_WARNINGS:
                    _warn(
                        stats,
                        "У D5 нет описаний для content sync: sku=%s, supplier_code=%s",
                        mapping.sku,
                        mapping.supplier_code,
                    )

    logger.info(
        "Завершён D5 content sync: mapped=%d, joined=%d, inserted=%d, updated=%d",
        stats.mapped_rows_read,
        stats.raw_rows_joined,
        stats.inserted,
        stats.updated,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Синхронизация контента D5 из raw_supplier_feed_products в catalog_content"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N mapping-строк D5 (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await sync_d5_content(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

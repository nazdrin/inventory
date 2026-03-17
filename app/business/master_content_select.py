import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import CatalogContent, MasterCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_content_select")

D1_SUPPLIER_ID = 38
MAX_LOGGED_WARNINGS = 20


@dataclass
class SelectStats:
    supplier_id: int = D1_SUPPLIER_ID
    candidate_skus: int = 0
    ua_selected: int = 0
    ru_selected: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped_no_content: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "supplier_id": self.supplier_id,
            "candidate_skus": self.candidate_skus,
            "ua_selected": self.ua_selected,
            "ru_selected": self.ru_selected,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "skipped_no_content": self.skipped_no_content,
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


def _pick_best_content(rows: List[CatalogContent]) -> Optional[CatalogContent]:
    valid_rows = [row for row in rows if _normalize_string(row.description)]
    if not valid_rows:
        return None
    valid_rows.sort(key=lambda row: (not bool(row.is_selected), row.id))
    return valid_rows[0]


async def select_master_content(limit: int = 0) -> Dict[str, Any]:
    stats = SelectStats()
    logger.info("Запуск выбора описаний master_catalog из D1 catalog_content, supplier_id=%s", D1_SUPPLIER_ID)

    async with get_async_db() as session:
        content_stmt = (
            select(CatalogContent)
            .where(
                CatalogContent.supplier_id == D1_SUPPLIER_ID,
                CatalogContent.is_active.is_(True),
            )
            .order_by(CatalogContent.sku.asc(), CatalogContent.language_code.asc(), CatalogContent.id.asc())
        )
        content_rows = (await session.execute(content_stmt)).scalars().all()

        content_by_sku: Dict[str, List[CatalogContent]] = {}
        for row in content_rows:
            sku = _normalize_string(row.sku)
            if not sku:
                if stats.warnings_count < MAX_LOGGED_WARNINGS:
                    _warn(stats, "Найдена запись catalog_content без sku, id=%s", row.id)
                continue
            content_by_sku.setdefault(sku, []).append(row)

        candidate_skus = list(content_by_sku.keys())
        if limit and limit > 0:
            candidate_skus = candidate_skus[:limit]
        stats.candidate_skus = len(candidate_skus)

        for sku in candidate_skus:
            master = (
                await session.execute(
                    select(MasterCatalog).where(MasterCatalog.sku == sku)
                )
            ).scalar_one_or_none()
            if master is None:
                if stats.warnings_count < MAX_LOGGED_WARNINGS:
                    _warn(stats, "Не найден master_catalog для sku=%s", sku)
                continue

            grouped: Dict[str, List[CatalogContent]] = {"ua": [], "ru": []}
            for row in content_by_sku.get(sku, []):
                language_code = _normalize_string(row.language_code)
                if language_code in grouped:
                    grouped[language_code].append(row)
                elif language_code and stats.warnings_count < MAX_LOGGED_WARNINGS:
                    _warn(stats, "Неожиданный language_code=%s для sku=%s", language_code, sku)

            changed = False
            has_any_content = False

            ua_row = _pick_best_content(grouped["ua"])
            if grouped["ua"] and ua_row is None and stats.skipped_no_content < MAX_LOGGED_WARNINGS:
                _warn(stats, "Пустое ua-описание для sku=%s", sku)
            if ua_row is not None:
                has_any_content = True
                stats.ua_selected += 1
                description_ua = _normalize_string(ua_row.description)
                if description_ua != _normalize_string(master.description_ua):
                    master.description_ua = description_ua
                    changed = True

            ru_row = _pick_best_content(grouped["ru"])
            if grouped["ru"] and ru_row is None and stats.skipped_no_content < MAX_LOGGED_WARNINGS:
                _warn(stats, "Пустое ru-описание для sku=%s", sku)
            if ru_row is not None:
                has_any_content = True
                stats.ru_selected += 1
                description_ru = _normalize_string(ru_row.description)
                if description_ru != _normalize_string(master.description_ru):
                    master.description_ru = description_ru
                    changed = True

            if not has_any_content:
                stats.skipped_no_content += 1
                continue

            if changed:
                stats.updated += 1
            else:
                stats.unchanged += 1

    logger.info(
        "Завершён выбор описаний: candidate_skus=%d, ua_selected=%d, ru_selected=%d, updated=%d, unchanged=%d",
        stats.candidate_skus,
        stats.ua_selected,
        stats.ru_selected,
        stats.updated,
        stats.unchanged,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Выбор описаний для master_catalog из catalog_content (D1)"
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
    result = await select_master_content(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

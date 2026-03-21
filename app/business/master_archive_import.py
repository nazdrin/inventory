import argparse
import asyncio
import io
import json
import logging
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Set

import httpx
from dotenv import load_dotenv
from sqlalchemy import select, update

from app.database import get_async_db
from app.models import MasterCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_archive_import")

DEFAULT_MASTER_ARCHIVE_YML_URL = (
    "https://petrenko.salesdrive.me/export/yml/export.yml"
    "?publicKey=Eexu43HSgYJ9ehHcfZo_fYYuNI_wmJMnnyR0OywlcSb5v38ZCtlmuKSNOOVrrScmm1QUf"
)
ARCHIVE_REASON = "salesdrive_yml_archive"
ARCHIVE_SOURCE = "salesdrive_yml"


@dataclass
class ArchiveStats:
    source: str = ARCHIVE_SOURCE
    feed_rows: int = 0
    matched_in_master: int = 0
    updated_to_archived: int = 0
    already_archived: int = 0
    not_found_in_master: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "feed_rows": self.feed_rows,
            "matched_in_master": self.matched_in_master,
            "updated_to_archived": self.updated_to_archived,
            "already_archived": self.already_archived,
            "not_found_in_master": self.not_found_in_master,
        }


def _get_archive_yml_url() -> str:
    value = (os.getenv("MASTER_ARCHIVE_YML_URL") or "").strip()
    return value or DEFAULT_MASTER_ARCHIVE_YML_URL


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


async def _download_archive_feed(url: str) -> bytes:
    timeout = httpx.Timeout(60.0, connect=20.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.content


def _read_archive_skus(yml_bytes: bytes, stats: ArchiveStats, limit: int = 0) -> List[str]:
    seen: Set[str] = set()
    result: List[str] = []

    for _, elem in ET.iterparse(io.BytesIO(yml_bytes), events=("end",)):
        if _xml_local_name(elem.tag) != "offer":
            continue

        sku = (elem.attrib.get("id") or "").strip()
        if sku and sku not in seen:
            seen.add(sku)
            result.append(sku)
            if limit and len(result) >= limit:
                elem.clear()
                break

        elem.clear()

    stats.feed_rows = len(result)
    return result


async def import_master_archive(limit: int = 0) -> Dict[str, Any]:
    load_dotenv()
    stats = ArchiveStats()
    archive_url = _get_archive_yml_url()

    logger.info("Загружаем архив master_catalog из SalesDrive YML")
    logger.info("archive_url=%s", archive_url)

    yml_bytes = await _download_archive_feed(archive_url)
    archive_skus = _read_archive_skus(yml_bytes, stats, limit=limit)
    logger.info("SalesDrive YML offer ids received: %d", stats.feed_rows)

    if not archive_skus:
        logger.info(
            "Archive sync summary: feed_rows=%d matched_in_master=%d updated_to_archived=%d already_archived=%d not_found_in_master=%d",
            stats.feed_rows,
            stats.matched_in_master,
            stats.updated_to_archived,
            stats.already_archived,
            stats.not_found_in_master,
        )
        return stats.to_dict()

    async with get_async_db() as session:
        rows = (
            await session.execute(
                select(
                    MasterCatalog.sku,
                    MasterCatalog.is_archived,
                    MasterCatalog.archived_reason,
                ).where(MasterCatalog.sku.in_(archive_skus))
            )
        ).all()

        matched_by_sku = {
            str(sku): {
                "is_archived": is_archived,
                "archived_reason": archived_reason,
            }
            for sku, is_archived, archived_reason in rows
        }

        stats.matched_in_master = len(matched_by_sku)
        stats.not_found_in_master = len(archive_skus) - stats.matched_in_master

        skus_to_archive: List[str] = []
        for sku in archive_skus:
            row = matched_by_sku.get(sku)
            if row is None:
                continue
            if row["is_archived"] is True:
                stats.already_archived += 1
                continue
            skus_to_archive.append(sku)

        if skus_to_archive:
            result = await session.execute(
                update(MasterCatalog)
                .where(MasterCatalog.sku.in_(skus_to_archive))
                .values(
                    is_archived=True,
                    archived_reason=ARCHIVE_REASON,
                )
            )
            stats.updated_to_archived = result.rowcount or len(skus_to_archive)

    logger.info(
        "Archive sync summary: feed_rows=%d matched_in_master=%d updated_to_archived=%d already_archived=%d not_found_in_master=%d",
        stats.feed_rows,
        stats.matched_in_master,
        stats.updated_to_archived,
        stats.already_archived,
        stats.not_found_in_master,
    )
    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Импорт архивных SKU из SalesDrive YML в master_catalog")
    parser.add_argument("--limit", type=int, default=0, help="обработать только первые N sku (0 = без лимита)")
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await import_master_archive(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

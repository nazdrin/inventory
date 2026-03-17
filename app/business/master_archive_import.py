import argparse
import asyncio
import io
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy import select

from app.database import get_async_db
from app.models import MasterCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_archive_import")

ARCHIVE_REASON = "google_drive_archive_file"
ARCHIVE_COL_SKU = "ID товара/услуги"


@dataclass
class ArchiveStats:
    file: str = ""
    archive_rows_read: int = 0
    matched_master_rows: int = 0
    archived_updated: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file": self.file,
            "archive_rows_read": self.archive_rows_read,
            "matched_master_rows": self.matched_master_rows,
            "archived_updated": self.archived_updated,
            "warnings_count": self.warnings_count,
        }


def _warn(stats: ArchiveStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _env_required(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Не задано обязательное окружение: {name}")
    return value


def _get_archive_folder_id() -> str:
    return _env_required("MASTER_ARCHIVE_FOLDER_ID")


def _get_archive_file_id() -> Optional[str]:
    value = (os.getenv("MASTER_ARCHIVE_FILE_ID") or "").strip()
    return value or None


def _normalize_header(value: Any) -> str:
    if value is None:
        return ""
    normalized = str(value).replace("\r", " ").replace("\n", " ").strip()
    return " ".join(normalized.split())


def _format_columns(columns: List[str], max_items: int = 30) -> str:
    visible = columns[:max_items]
    suffix = "" if len(columns) <= max_items else f" ... +{len(columns) - max_items} more"
    return ", ".join(repr(column) for column in visible) + suffix


async def _connect_drive():
    creds_path = _env_required("GOOGLE_DRIVE_CREDENTIALS_PATH")
    if not os.path.exists(creds_path):
        raise FileNotFoundError(f"GOOGLE_DRIVE_CREDENTIALS_PATH не найден: {creds_path}")
    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=credentials)


async def _fetch_single_file_metadata(drive_service, folder_id: str) -> Dict[str, Any]:
    try:
        results = (
            drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id, name, mimeType, modifiedTime)",
                pageSize=10,
                orderBy="modifiedTime desc",
            )
            .execute()
        )
        files = results.get("files", []) or []
        if not files:
            raise FileNotFoundError("В папке Google Drive нет файлов")
        return files[0]
    except HttpError as exc:
        raise RuntimeError(f"Ошибка Drive API при получении списка файлов: {exc}") from exc


async def _fetch_file_metadata_by_id(drive_service, file_id: str) -> Dict[str, Any]:
    try:
        return (
            drive_service.files()
            .get(fileId=file_id, fields="id, name, mimeType, modifiedTime")
            .execute()
        )
    except HttpError as exc:
        raise RuntimeError(
            f"Не удалось получить файл архива из Google Drive по MASTER_ARCHIVE_FILE_ID={file_id}: {exc}"
        ) from exc


async def _download_file_bytes(drive_service, file_id: str) -> bytes:
    try:
        request = drive_service.files().get_media(fileId=file_id)
        handle = io.BytesIO()
        downloader = MediaIoBaseDownload(handle, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        handle.seek(0)
        return handle.read()
    except HttpError as exc:
        raise RuntimeError(f"Ошибка Drive API при загрузке файла {file_id}: {exc}") from exc


def _read_archive_skus(xlsx_bytes: bytes, stats: ArchiveStats, limit: int = 0) -> List[str]:
    df = pd.read_excel(io.BytesIO(xlsx_bytes), dtype=str)
    raw_columns = [_normalize_header(column) for column in df.columns.tolist()]
    logger.info("Колонки Excel: %s", _format_columns(raw_columns))

    normalized_to_original: Dict[str, str] = {}
    for original_column in df.columns.tolist():
        normalized_column = _normalize_header(original_column)
        if normalized_column and normalized_column not in normalized_to_original:
            normalized_to_original[normalized_column] = original_column

    archive_column = normalized_to_original.get(_normalize_header(ARCHIVE_COL_SKU))
    if archive_column is None:
        raise RuntimeError(
            "В Excel отсутствует обязательная колонка: "
            f"{ARCHIVE_COL_SKU}. Найдены колонки: {_format_columns(raw_columns)}"
        )

    seen: Set[str] = set()
    result: List[str] = []
    for value in df[archive_column].tolist():
        sku = str(value).strip() if value is not None and str(value).strip().lower() != "nan" else ""
        if not sku or sku in seen:
            continue
        seen.add(sku)
        result.append(sku)
        if limit and len(result) >= limit:
            break

    stats.archive_rows_read = len(result)
    return result


async def import_master_archive(limit: int = 0) -> Dict[str, Any]:
    load_dotenv()
    stats = ArchiveStats()
    drive = await _connect_drive()
    archive_file_id = _get_archive_file_id()
    logger.info("Загружаем архив master_catalog из Google Drive")
    if archive_file_id:
        meta = await _fetch_file_metadata_by_id(drive, archive_file_id)
    else:
        meta = await _fetch_single_file_metadata(drive, _get_archive_folder_id())

    stats.file = meta.get("name", "")
    logger.info("file_id=%s", meta.get("id"))
    logger.info("file_name=%s", meta.get("name"))
    logger.info("mimeType=%s", meta.get("mimeType"))
    xlsx_bytes = await _download_file_bytes(drive, meta["id"])
    archive_skus = _read_archive_skus(xlsx_bytes, stats, limit=limit)

    if not archive_skus:
        return stats.to_dict()

    async with get_async_db() as session:
        rows = (
            await session.execute(
                select(MasterCatalog).where(MasterCatalog.sku.in_(archive_skus))
            )
        ).scalars().all()
        stats.matched_master_rows = len(rows)

        for row in rows:
            if row.is_archived is True and row.archived_reason == ARCHIVE_REASON:
                continue
            row.is_archived = True
            row.archived_reason = ARCHIVE_REASON
            stats.archived_updated += 1

    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Импорт архивных SKU из Google Drive в master_catalog")
    parser.add_argument("--limit", type=int, default=0, help="обработать только первые N sku (0 = без лимита)")
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await import_master_archive(limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

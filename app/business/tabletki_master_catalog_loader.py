import argparse
import asyncio
import hashlib
import io
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy import select

from app.database import get_async_db
from app.models import MasterCatalog, RawTabletkiCatalog


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("tabletki_master_catalog_loader")

COL_GUID = "ГУИД"
COL_SKU = "Товар.Код"
COL_NAME_UA = "Товар.Наименование (укр.)"
COL_NAME_RU = "Товар"
COL_MANUFACTURER = "Товар.Производитель.Наименование (укр)"
COL_BARCODE = "Код ШК"
COL_CATEGORY_L2_CODE = "Товар.Товарная иерархия.Код"
COL_CATEGORY_L1_CODE = "Товар.Товарная иерархия.Родитель.Код"
COL_CATEGORY_L1_NAME = "Товар.Товарная иерархия.Родитель.Наименование (укр)"
COL_CATEGORY_L2_NAME = "Товар.Товарная иерархия.Наименование (укр)"
COL_VOLUME = "Товар.Объем"
COL_WEIGHT = "Товар.Вес"
COL_LENGTH = "Товар.Глубина"
COL_HEIGHT = "Товар.Высота"
COL_WIDTH = "Товар.Ширина"

REQUIRED_COLUMNS = [COL_GUID, COL_SKU, COL_NAME_UA, COL_NAME_RU]
HEADER_MIN_COLUMNS = [COL_GUID, COL_SKU]
OPTIONAL_COLUMNS = [
    COL_MANUFACTURER,
    COL_BARCODE,
    COL_CATEGORY_L2_CODE,
    COL_CATEGORY_L1_CODE,
    COL_CATEGORY_L1_NAME,
    COL_CATEGORY_L2_NAME,
    COL_VOLUME,
    COL_WEIGHT,
    COL_LENGTH,
    COL_HEIGHT,
    COL_WIDTH,
]


@dataclass
class LoaderStats:
    file: str = ""
    rows_read: int = 0
    raw_inserted: int = 0
    raw_updated: int = 0
    master_inserted: int = 0
    master_updated: int = 0
    warnings_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file": self.file,
            "rows_read": self.rows_read,
            "raw_inserted": self.raw_inserted,
            "raw_updated": self.raw_updated,
            "master_inserted": self.master_inserted,
            "master_updated": self.master_updated,
            "warnings_count": self.warnings_count,
        }


def _env_required(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"Не задано обязательное окружение: {name}")
    return value


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
                fields="files(id, name, modifiedTime)",
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


def _warn(stats: LoaderStats, message: str, *args: Any) -> None:
    stats.warnings_count += 1
    logger.warning(message, *args)


def _normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text_value = str(value).strip()
    return text_value or None


def _normalize_numeric(value: Any, field_name: str, sku: Optional[str], stats: LoaderStats) -> Optional[Decimal]:
    normalized = _normalize_string(value)
    if normalized is None:
        return None

    candidate = normalized.replace(" ", "").replace(",", ".")
    if candidate.count(".") > 1:
        _warn(stats, "Некорректное числовое значение для %s (sku=%s): %r", field_name, sku, normalized)
        return None

    try:
        return Decimal(candidate)
    except InvalidOperation:
        _warn(stats, "Не удалось распарсить число для %s (sku=%s): %r", field_name, sku, normalized)
        return None


def _find_header_row(df: pd.DataFrame) -> int:
    for idx, row in df.iterrows():
        values = {_normalize_string(value) for value in row.tolist()}
        if all(column in values for column in HEADER_MIN_COLUMNS):
            return int(idx)
    raise RuntimeError(
        "Не удалось найти строку заголовка в Excel. Ожидались минимум колонки "
        f"{', '.join(HEADER_MIN_COLUMNS)}."
    )


def _prepare_dataframe(xlsx_bytes: bytes, stats: LoaderStats) -> pd.DataFrame:
    raw_df = pd.read_excel(io.BytesIO(xlsx_bytes), header=None, dtype=object)
    header_row_idx = _find_header_row(raw_df)

    header_values = [
        _normalize_string(value) or f"unnamed_{idx}"
        for idx, value in enumerate(raw_df.iloc[header_row_idx].tolist())
    ]
    data_df = raw_df.iloc[header_row_idx + 1 :].copy()
    data_df.columns = header_values
    data_df = data_df.reset_index(drop=True)
    data_df = data_df.loc[:, ~data_df.columns.duplicated()]
    data_df = data_df.dropna(how="all")

    missing_required = [column for column in REQUIRED_COLUMNS if column not in data_df.columns]
    if missing_required:
        raise RuntimeError(
            "В Excel отсутствуют обязательные колонки: " + ", ".join(missing_required)
        )

    for column in OPTIONAL_COLUMNS:
        if column not in data_df.columns:
            _warn(stats, "В Excel отсутствует необязательная колонка: %s", column)

    return data_df


def _row_source_hash(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _build_row_payload(row: pd.Series, stats: LoaderStats) -> Optional[Dict[str, Any]]:
    payload = {str(column): _normalize_string(value) for column, value in row.items()}
    sku = payload.get(COL_SKU)
    if not sku:
        _warn(stats, "Пропущена строка без sku (колонка %s)", COL_SKU)
        return None

    item = {
        "tabletki_guid": payload.get(COL_GUID),
        "sku": sku,
        "barcode": payload.get(COL_BARCODE),
        "manufacturer": payload.get(COL_MANUFACTURER),
        "name_ua": payload.get(COL_NAME_UA),
        "name_ru": payload.get(COL_NAME_RU),
        "category_l1_code": payload.get(COL_CATEGORY_L1_CODE),
        "category_l1_name": payload.get(COL_CATEGORY_L1_NAME),
        "category_l2_code": payload.get(COL_CATEGORY_L2_CODE),
        "category_l2_name": payload.get(COL_CATEGORY_L2_NAME),
        "weight_g": _normalize_numeric(payload.get(COL_WEIGHT), "weight_g", sku, stats),
        "length_mm": _normalize_numeric(payload.get(COL_LENGTH), "length_mm", sku, stats),
        "width_mm": _normalize_numeric(payload.get(COL_WIDTH), "width_mm", sku, stats),
        "height_mm": _normalize_numeric(payload.get(COL_HEIGHT), "height_mm", sku, stats),
        "volume_ml": _normalize_numeric(payload.get(COL_VOLUME), "volume_ml", sku, stats),
        "description_ua": None,
        "description_ru": None,
        "source_payload": payload,
    }
    item["source_hash"] = _row_source_hash(payload)
    return item


def _read_tabletki_rows(xlsx_bytes: bytes, stats: LoaderStats, limit: int = 0) -> List[Dict[str, Any]]:
    df = _prepare_dataframe(xlsx_bytes, stats)

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        item = _build_row_payload(row, stats)
        if item is None:
            continue
        rows.append(item)
        if limit and len(rows) >= limit:
            break

    stats.rows_read = len(rows)
    return rows


async def load_tabletki_raw(rows: List[Dict[str, Any]], stats: LoaderStats) -> None:
    now = datetime.now(timezone.utc)

    async with get_async_db() as session:
        for item in rows:
            stmt = select(RawTabletkiCatalog).where(RawTabletkiCatalog.sku == item["sku"])
            existing = (await session.execute(stmt)).scalar_one_or_none()

            if existing is None:
                obj = RawTabletkiCatalog(
                    tabletki_guid=item["tabletki_guid"],
                    sku=item["sku"],
                    barcode=item["barcode"],
                    manufacturer=item["manufacturer"],
                    name_ua=item["name_ua"],
                    name_ru=item["name_ru"],
                    category_l1_code=item["category_l1_code"],
                    category_l1_name=item["category_l1_name"],
                    category_l2_code=item["category_l2_code"],
                    category_l2_name=item["category_l2_name"],
                    weight_g=item["weight_g"],
                    length_mm=item["length_mm"],
                    width_mm=item["width_mm"],
                    height_mm=item["height_mm"],
                    volume_ml=item["volume_ml"],
                    description_ua=None,
                    description_ru=None,
                    source_payload=item["source_payload"],
                    source_hash=item["source_hash"],
                    loaded_at=now,
                )
                session.add(obj)
                stats.raw_inserted += 1
                continue

            existing.tabletki_guid = item["tabletki_guid"]
            existing.barcode = item["barcode"]
            existing.manufacturer = item["manufacturer"]
            existing.name_ua = item["name_ua"]
            existing.name_ru = item["name_ru"]
            existing.category_l1_code = item["category_l1_code"]
            existing.category_l1_name = item["category_l1_name"]
            existing.category_l2_code = item["category_l2_code"]
            existing.category_l2_name = item["category_l2_name"]
            existing.weight_g = item["weight_g"]
            existing.length_mm = item["length_mm"]
            existing.width_mm = item["width_mm"]
            existing.height_mm = item["height_mm"]
            existing.volume_ml = item["volume_ml"]
            existing.description_ua = None
            existing.description_ru = None
            existing.source_payload = item["source_payload"]
            existing.source_hash = item["source_hash"]
            existing.loaded_at = now
            stats.raw_updated += 1


async def sync_tabletki_raw_to_master(stats: LoaderStats, limit: int = 0) -> None:
    async with get_async_db() as session:
        stmt = select(RawTabletkiCatalog).order_by(RawTabletkiCatalog.id.asc())
        if limit and limit > 0:
            stmt = stmt.limit(limit)

        raw_rows = (await session.execute(stmt)).scalars().all()

        for raw in raw_rows:
            master_stmt = select(MasterCatalog).where(MasterCatalog.sku == raw.sku)
            existing = (await session.execute(master_stmt)).scalar_one_or_none()

            if existing is None:
                obj = MasterCatalog(
                    sku=raw.sku,
                    tabletki_guid=raw.tabletki_guid,
                    barcode=raw.barcode,
                    manufacturer=raw.manufacturer,
                    name_ua=raw.name_ua,
                    name_ru=raw.name_ru,
                    category_l1_code=raw.category_l1_code,
                    category_l2_code=raw.category_l2_code,
                    weight_g=raw.weight_g,
                    length_mm=raw.length_mm,
                    width_mm=raw.width_mm,
                    height_mm=raw.height_mm,
                    volume_ml=raw.volume_ml,
                )
                session.add(obj)
                stats.master_inserted += 1
                continue

            existing.tabletki_guid = raw.tabletki_guid
            existing.barcode = raw.barcode
            existing.manufacturer = raw.manufacturer
            existing.name_ua = raw.name_ua
            existing.name_ru = raw.name_ru
            existing.category_l1_code = raw.category_l1_code
            existing.category_l2_code = raw.category_l2_code
            existing.weight_g = raw.weight_g
            existing.length_mm = raw.length_mm
            existing.width_mm = raw.width_mm
            existing.height_mm = raw.height_mm
            existing.volume_ml = raw.volume_ml
            stats.master_updated += 1


async def load_tabletki_master_catalog(mode: str, limit: int = 0) -> Dict[str, Any]:
    load_dotenv()
    stats = LoaderStats()

    if mode in {"raw", "full"}:
        folder_id = _env_required("GOOGLE_DRIVE_FOLDER_ID")
        drive = await _connect_drive()
        metadata = await _fetch_single_file_metadata(drive, folder_id)
        stats.file = metadata.get("name", "")
        logger.info("Drive файл: %s (%s)", stats.file, metadata.get("id"))

        xlsx_bytes = await _download_file_bytes(drive, metadata["id"])
        rows = _read_tabletki_rows(xlsx_bytes, stats, limit=limit)
        logger.info("Подготовлено строк из Excel: %d", len(rows))
        await load_tabletki_raw(rows, stats)

    if mode in {"master", "full"}:
        await sync_tabletki_raw_to_master(stats, limit=limit)

    return stats.to_dict()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка master-каталога Tabletki из Excel на Google Drive в PostgreSQL"
    )
    parser.add_argument(
        "--mode",
        choices=["raw", "master", "full"],
        default="full",
        help="raw = только raw_tabletki_catalog, master = только sync raw->master, full = оба шага",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="обработать только первые N строк (0 = без лимита)",
    )
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await load_tabletki_master_catalog(mode=args.mode, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(_amain())

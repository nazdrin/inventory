# app/business/competitor_price_loader.py
import os
import io
import logging
import asyncio
from decimal import Decimal, InvalidOperation
from typing import Optional, Tuple, List

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, delete

# === ВАШИ ИМПОРТЫ И ИНФРА ===
from app.database import get_async_db  # должен отдавать AsyncSession
from app.models import CompetitorPrice  # таблица с полями: code (str), city (str), competitor_price (Numeric)

logger = logging.getLogger("competitor_loader")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

# -------- Google Drive --------
def _connect_to_google_drive():
    """Создаёт клиент Drive API через сервисный аккаунт (GOOGLE_DRIVE_CREDENTIALS_PATH)."""
    creds_path = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH")
    if not creds_path or not os.path.exists(creds_path):
        raise FileNotFoundError(f"Неверный путь к учетным данным Google Drive: {creds_path}")

    credentials = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build("drive", "v3", credentials=credentials)
    logger.info("Подключено к Google Drive")
    return service

def _list_files_in_folder(service, folder_id: str) -> List[dict]:
    """Возвращает список файлов в папке Google Drive."""
    files = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"
    while True:
        resp = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token,
            pageSize=1000,
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def _download_file_as_bytes(service, file_id: str) -> bytes:
    """Скачивает файл по file_id в память (bytes)."""
    request = service.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()

# -------- Парсинг таблиц --------
CODE_CANDIDATES = ["Код товара Tabletki.ua", "code", "Код", "Артикул", "productId"]
PRICE_CANDIDATES = ["Цена", "price", "Price"]

def _normalize_code(code_raw) -> str:
    """
    Нормализует код товара:
    - убирает .0 в конце (как из float 1000161.0, так и из строки "1000161.0")
    - возвращает пустую строку, если кода нет или он NaN.
    """
    if code_raw is None:
        return ""
    # если пришло как float из Excel
    if isinstance(code_raw, float):
        if pd.isna(code_raw):
            return ""
        # целые значения типа 1000161.0 превращаем в "1000161"
        if code_raw.is_integer():
            return str(int(code_raw))
        # на всякий случай убираем хвост ".0" или лишние нули
        return str(code_raw).rstrip("0").rstrip(".")

    s = str(code_raw).strip()
    # если код пришёл строкой вида "1000161.0" — обрезаем ".0"
    if s.endswith(".0"):
        s = s[:-2]
    return s

def _pick_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        lc = cand.lower()
        if lc in cols:
            return cols[lc]
    # fallback: частичный поиск
    for c in df.columns:
        lc_name = c.lower()
        if any(lc_name == cand.lower() for cand in candidates):
            return c
    return None

def _as_decimal(x) -> Optional[Decimal]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        # заменим возможные запятые и пробелы
        if isinstance(x, str):
            x = x.replace(" ", "").replace(",", ".")
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None

def _parse_table(b: bytes, filename: str) -> List[Tuple[str, str, Decimal]]:
    """
    Возвращает список (code, city, competitor_price).
    Город берём из имени файла без расширения.
    """
    city = os.path.splitext(os.path.basename(filename))[0].strip()
    # Определяем парсер по расширению
    ext = os.path.splitext(filename)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(io.BytesIO(b))
    elif ext in (".csv",):
        # пробуем ; затем ,
        try:
            df = pd.read_csv(io.BytesIO(b), encoding="utf-8", sep=";")
        except Exception:
            df = pd.read_csv(io.BytesIO(b), encoding="utf-8", sep=",")
    else:
        logger.warning(f"Пропуск файла {filename}: не поддерживаемый тип")
        return []

    if df.empty:
        return []

    code_col = _pick_column(df, CODE_CANDIDATES)
    price_col = _pick_column(df, PRICE_CANDIDATES)
    if not code_col or not price_col:
        logger.warning(f"Файл {filename}: не найдены колонки кода/цены. Найдено: {df.columns.tolist()}")
        return []

    out: List[Tuple[str, str, Decimal]] = []
    for _, row in df.iterrows():
        code_raw = row.get(code_col)
        price_raw = row.get(price_col)

        if code_raw is None or (isinstance(code_raw, float) and pd.isna(code_raw)):
            continue

        code = _normalize_code(code_raw)
        if not code:
            continue

        price = _as_decimal(price_raw)
        if not price:
            continue

        out.append((code, city, price))
    seen = set()
    for _, row in df.iterrows():
        code = _normalize_code(row.get(code_col))
        price = _as_decimal(row.get(price_col))
        if not code or price is None:
            continue
        key = (code, city)
        if key in seen:
            logger.debug("Duplicate in file %s: (%s, %s)", filename, code, city)
        seen.add(key)
        out.append((code, city, price))
    return out
    # return out
from collections import OrderedDict
from sqlalchemy.dialects.postgresql import insert as pg_insert
from decimal import Decimal

BATCH_SIZE = 1000

async def upsert_competitor_prices(session, rows):
    """
    rows: [(code:str, city:str, price:Decimal), ...]
    1) де-дуп по (code, city) — последняя цена выигрывает
    2) батчи по 1000
    """
    if not rows:
        return 0

    # --- 1) де-дуп ---
    uniq: "OrderedDict[tuple[str,str], Decimal]" = OrderedDict()
    for code, city, price in rows:
        # нормализация (заодно убираем мусорные пробелы)
        c = str(code).strip()
        ct = str(city).strip()
        p = Decimal(price).quantize(Decimal("0.01"))
        uniq[(c, ct)] = p  # последняя встреченная цена остаётся

    payload_all = [
        {"code": c, "city": ct, "competitor_price": p}
        for (c, ct), p in uniq.items()
    ]

    total = 0
    # --- 2) батчи ---
    for i in range(0, len(payload_all), BATCH_SIZE):
        chunk = payload_all[i:i+BATCH_SIZE]
        stmt = pg_insert(CompetitorPrice).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=["code", "city"],
            set_={"competitor_price": stmt.excluded.competitor_price}
        )
        try:
            await session.execute(stmt)
            await session.commit()
            total += len(chunk)
        except Exception as e:
            # логируем, чтобы быстро увидеть, если вдруг снова дубль или другой кейс
            logger.exception("DB upsert failed on batch [%s:%s]", i, i+len(chunk))
            # чтобы не терять остальные батчи — продолжаем (по желанию можно сделать raise)
            await session.rollback()
    return total

# -------- Точка входа --------
async def run():
    folder_id = os.getenv("COMPETITOR_GDRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("Не задан COMPETITOR_GDRIVE_FOLDER_ID в .env")

    service = _connect_to_google_drive()
    files = _list_files_in_folder(service, folder_id)
    supported = [f for f in files if os.path.splitext(f["name"])[1].lower() in (".xlsx", ".xls", ".csv")]
    logger.info(f"Найдено файлов в папке: всего={len(files)}, поддерживаемых={len(supported)}")

    total_rows = 0
    async with get_async_db() as session:
        # Города, которые уже есть в БД
        result = await session.execute(select(CompetitorPrice.city).distinct())
        existing_cities = {row[0] for row in result if row[0] is not None}
        processed_cities = set()

        for f in supported:
            city = os.path.splitext(os.path.basename(f["name"]))[0].strip()
            processed_cities.add(city)

            try:
                b = _download_file_as_bytes(service, f["id"])
                rows = _parse_table(b, f["name"])

                # Перед загрузкой новых данных полностью очищаем город
                await session.execute(
                    delete(CompetitorPrice).where(CompetitorPrice.city == city)
                )
                await session.commit()

                if not rows:
                    logger.info(f"{f['name']}: нет валидных строк, данные по городу {city} очищены")
                    continue

                cnt = await upsert_competitor_prices(session, rows)
                total_rows += cnt
                logger.info(f"{f['name']}: загружено {cnt} записей")
            except Exception as e:
                logger.exception("DB upsert failed")  # оставь как есть
                logger.error("Exc type=%s; msg=%s", type(e).__name__, str(e))

        # Удаляем данные по городам, для которых раньше были записи, но сейчас файлов нет
        cities_to_clear = existing_cities - processed_cities
        if cities_to_clear:
            await session.execute(
                delete(CompetitorPrice).where(CompetitorPrice.city.in_(list(cities_to_clear)))
            )
            await session.commit()
            logger.info(
                "Удалены данные по городам без файлов: %s",
                ", ".join(sorted(cities_to_clear)),
            )

    logger.info(f"Готово. Всего загружено записей: {total_rows}")

if __name__ == "__main__":
    asyncio.run(run())

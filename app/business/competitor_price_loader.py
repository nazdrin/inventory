# app/business/competitor_price_loader.py
import os
import io
import json
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

# -------- Парсинг входящего JSON --------
# Ожидаемый формат: список объектов
# [
#   {"code": "1077608", "city": "Kyiv", "delivery_price": 470.0},
#   {"code": "1077608", "city": "Lviv", "delivery_price": 470.0}
# ]

CODE_FIELD = "code"
CITY_FIELD = "city"
PRICE_FIELD = "delivery_price"


def _normalize_code(code_raw) -> str:
    """Нормализует код товара и убирает хвост `.0`, если он пришёл из числовых типов."""
    if code_raw is None:
        return ""

    # если пришло как float
    if isinstance(code_raw, float):
        if pd.isna(code_raw):
            return ""
        if code_raw.is_integer():
            return str(int(code_raw))
        return str(code_raw).rstrip("0").rstrip(".")

    s = str(code_raw).strip()
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _as_decimal(x) -> Optional[Decimal]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        if isinstance(x, str):
            x = x.replace(" ", "").replace(",", ".")
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def _parse_delivery_json(b: bytes, filename: str) -> List[Tuple[str, str, Decimal]]:
    """Возвращает список (code, city, competitor_price) из JSON."""
    try:
        payload = json.loads(b.decode("utf-8"))
    except Exception:
        # иногда сервисы сохраняют json в UTF-8 with BOM
        payload = json.loads(b.decode("utf-8-sig"))

    if not isinstance(payload, list):
        logger.warning("Файл %s: ожидался JSON-массив (list), получили %s", filename, type(payload).__name__)
        return []

    out: List[Tuple[str, str, Decimal]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue

        code = _normalize_code(item.get(CODE_FIELD))
        city = str(item.get(CITY_FIELD) or "").strip()
        price = _as_decimal(item.get(PRICE_FIELD))

        if not code or not city or price is None:
            continue

        out.append((code, city, price))

    return out

from collections import OrderedDict

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

    # Имя файла в папке Google Drive (по умолчанию competitors_delivery_total.json)
    delivery_filename = os.getenv("COMPETITOR_DELIVERY_JSON_NAME", "competitors_delivery_total.json")

    service = _connect_to_google_drive()
    files = _list_files_in_folder(service, folder_id)

    # Ищем JSON по имени, если не нашли — берём первый попавшийся .json
    json_files = [f for f in files if os.path.splitext(f["name"])[1].lower() == ".json"]
    if not json_files:
        raise FileNotFoundError("В папке Google Drive не найден ни один .json файл")

    target = next((f for f in json_files if f["name"] == delivery_filename), None)
    if target is None:
        target = json_files[0]
        logger.warning(
            "Не найден файл %s — использую первый JSON из папки: %s",
            delivery_filename,
            target["name"],
        )

    logger.info("Читаю JSON файл: %s", target["name"])

    b = _download_file_as_bytes(service, target["id"])
    rows = _parse_delivery_json(b, target["name"])

    if not rows:
        logger.warning("%s: нет валидных строк — загрузка в БД пропущена", target["name"])
        return

    total_rows = 0

    # Список городов, присутствующих в JSON
    processed_cities = {city for _, city, _ in rows}

    async with get_async_db() as session:
        # Города, которые уже есть в БД
        result = await session.execute(select(CompetitorPrice.city).distinct())
        existing_cities = {row[0] for row in result if row[0] is not None}

        # 1) Для каждого города из JSON — полностью очищаем город и загружаем его данные
        for city in sorted(processed_cities):
            city_rows = [(code, ct, price) for (code, ct, price) in rows if ct == city]

            await session.execute(delete(CompetitorPrice).where(CompetitorPrice.city == city))
            await session.commit()

            cnt = await upsert_competitor_prices(session, city_rows)
            total_rows += cnt
            logger.info("%s: загружено %s записей", city, cnt)

        # 2) Удаляем данные по городам, которые были в БД, но отсутствуют в новом JSON
        cities_to_clear = existing_cities - processed_cities
        if cities_to_clear:
            await session.execute(delete(CompetitorPrice).where(CompetitorPrice.city.in_(list(cities_to_clear))))
            await session.commit()
            logger.info(
                "Удалены данные по городам без строк в JSON: %s",
                ", ".join(sorted(cities_to_clear)),
            )

    logger.info("Готово. Всего загружено записей: %s", total_rows)

if __name__ == "__main__":
    asyncio.run(run())

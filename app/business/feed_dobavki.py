from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from typing import Optional, List, Dict, Literal
from urllib.parse import urlparse, parse_qs

import math
import httpx
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Колонки в Google Sheet для поставщика D4 ---
COL_ARTIKUL = "Артикул"
COL_NAME = "Назва"
COL_BARCODE = "Штрихкод"
COL_AVAILABLE = "Наявність"
COL_QTY = "Кількість на складі"
COL_PRICE_USD = "Ціна зі знижкою"   # как на скрине, без звёздочки


# ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================

def _convert_gsheet_link_to_csv_url(feed_url: str) -> str:
    """
    Преобразует обычную ссылку вида:
      https://docs.google.com/spreadsheets/d/<ID>/edit?gid=<GID>
    в CSV-export:
      https://docs.google.com/spreadsheets/d/<ID>/export?format=csv&gid=<GID>

    Если формат другой, возвращаем исходный feed_url (на случай, если уже CSV).
    """
    try:
        parsed = urlparse(feed_url)
        path_parts = parsed.path.split("/")
        if "d" not in path_parts:
            return feed_url

        idx = path_parts.index("d")
        file_id = path_parts[idx + 1]

        qs = parse_qs(parsed.query)
        gid = qs.get("gid", ["0"])[0]

        csv_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}"
        return csv_url
    except Exception as e:
        logger.warning("Не удалось преобразовать ссылку Google Sheet в CSV: %s", e)
        return feed_url


async def _get_feed_settings_by_code(code: str) -> tuple[Optional[str], Optional[float], Optional[float]]:
    """
    Достаёт из dropship_enterprises:
      - feed_url
      - gdrive_folder (используем как курс USD→UAH)
      - retail_markup (наценка в процентах, например 25 = +25%)
    """
    async with get_async_db() as session:
        res = await session.execute(
            text(
                """
                SELECT feed_url, gdrive_folder, retail_markup
                FROM dropship_enterprises
                WHERE code = :code
                LIMIT 1
                """
            ),
            {"code": code},
        )
        row = res.first()
        if not row:
            return None, None, None

        feed_url, rate, markup = row

        try:
            rate_val = float(rate) if rate is not None else None
        except (TypeError, ValueError):
            rate_val = None

        try:
            # В БД retail_markup хранится как процент (например 25),
            # здесь конвертируем в долю (0.25)
            markup_val = float(markup) / 100.0 if markup is not None else None
        except (TypeError, ValueError):
            markup_val = None

        return feed_url, rate_val, markup_val


def _parse_csv_text(csv_text: str) -> List[Dict[str, str]]:
    """
    Парсинг CSV-текста:
      - пропускаем первую строку (надпись "Система знижок ...");
      - со второй строки берём заголовки и данные.
    """
    lines = csv_text.splitlines()
    if len(lines) <= 1:
        return []

    # отрезаем первую строку с надписью "Система знижок..."
    body = "\n".join(lines[1:])

    sample = body[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(body), dialect=dialect)
    return list(reader)


async def _load_sheet_rows(*, code: str, timeout: int) -> Optional[List[Dict[str, str]]]:
    """
    Рабочий режим:
        1) берём feed_url, gdrive_folder, retail_markup по code из dropship_enterprises;
        2) конвертируем ссылку в CSV-export;
        3) скачиваем CSV по HTTP;
        4) возвращаем список строк (dict по именам колонок).
    """
    feed_url, _, _ = await _get_feed_settings_by_code(code)
    if not feed_url:
        msg = f"Не найден feed_url в dropship_enterprises для code='{code}'"
        logger.error(msg)
        send_notification(msg, "Розробник")
        return None

    csv_url = _convert_gsheet_link_to_csv_url(feed_url)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with httpx.AsyncClient(
            headers=headers,
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            resp = await client.get(csv_url)
            resp.raise_for_status()
            csv_text = resp.text
    except Exception as e:
        msg = f"Ошибка загрузки CSV по ссылке {csv_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None

    rows = _parse_csv_text(csv_text)
    if not rows:
        msg = f"CSV для code='{code}' пустой или без данных"
        logger.error(msg)
        send_notification(msg, "Розробник")
        return None

    logger.info("D4 CSV: прочитано строк (code=%s): %d", code, len(rows))
    return rows


def _is_available(row: Dict[str, str]) -> bool:
    """
    Фильтр по колонке 'Наявність'.
    TRUE: 'TRUE', 'True', 'true', '1', 'так', 'yes'.
    """
    val = (row.get(COL_AVAILABLE) or "").strip().lower()
    return val in {"true", "1", "так", "yes"}


def _parse_qty(value: Optional[str]) -> int:
    """
    Из 'Кількість на складі' вида '30' / '30+' / '10 шт' берём только цифровую часть.
    """
    if not value:
        return 0
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return 0
    try:
        return int(digits)
    except ValueError:
        return 0


def _to_float(val: Optional[str]) -> float:
    if val is None:
        return 0.0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


# ===================== ПАРСЕР CATALOG =====================

async def parse_d4_catalog_to_json(
    *,
    code: str = "D4",
    timeout: int = 30,
) -> str:
    """
    Каталог (D4 Google Sheet) → JSON:
    [
      {"id": "<Артикул>", "name": "<Назва>", "barcode": "<Штрихкод>"}
    ]

    Только строки с Наявність == TRUE.
    """
    rows = await _load_sheet_rows(code=code, timeout=timeout)
    if rows is None:
        return "[]"

    items: List[Dict[str, str]] = []
    for row in rows:
        if not _is_available(row):
            continue

        sku = (row.get(COL_ARTIKUL) or "").strip()
        name = (row.get(COL_NAME) or "").strip()
        barcode = (row.get(COL_BARCODE) or "").strip()

        if not (sku and name):
            continue

        items.append({
            "id": sku,
            "name": " ".join(name.split()),
            "barcode": barcode,
        })

    logger.info("D4 каталог: собрано позиций (code=%s): %d", code, len(items))
    return json.dumps(items, ensure_ascii=False, indent=2)


# ===================== ПАРСЕР STOCK =====================

async def parse_d4_stock_to_json(
    *,
    code: str = "D4",
    timeout: int = 30,
) -> str:
    """
    Сток (D4 Google Sheet) → JSON:
    [
      {
        "code_sup": "<Артикул>",
        "qty": <int>,
        "price_opt": Ціна зі знижкою (оптова ціна з фіду, грн),
        "price_retail": price_opt * (1 + retail_markup)
      }
    ]

    price_opt = Ціна зі знижкою (оптова ціна з фіду, грн)
    price_retail = price_opt * (1 + retail_markup)
    """
    rows = await _load_sheet_rows(code=code, timeout=timeout)
    if rows is None:
        return "[]"

    # наценку берём из БД (курс для D4 больше не используется)
    _, _, markup = await _get_feed_settings_by_code(code)
    if markup is None:
        msg = (
            f"Для code='{code}' не задан корректный retail_markup в dropship_enterprises"
        )
        logger.error(msg)
        send_notification(msg, "Розробник")
        return "[]"

    rows_out: List[Dict[str, object]] = []
    for row in rows:
        if not _is_available(row):
            continue

        sku = (row.get(COL_ARTIKUL) or "").strip()
        if not sku:
            continue

        qty = _parse_qty(row.get(COL_QTY))
        wholesale_raw = _to_float(row.get(COL_PRICE_USD))

        # COL_PRICE_USD ("Ціна зі знижкою") — це оптова ціна з фіду (грн)
        price_opt = round(wholesale_raw, 2)
        if price_opt < 0:
            price_opt = 0.0

        # Роздріб рахуємо від опту + націнка
        price_retail_raw = price_opt * (1.0 + markup)
        price_retail = round(price_retail_raw, 2)

        rows_out.append({
            "code_sup": sku,
            "qty": qty,
            "price_retail": price_retail,
            "price_opt": price_opt,
        })

    logger.info("D4 сток: собрано позиций (code=%s): %d", code, len(rows_out))
    return json.dumps(rows_out, ensure_ascii=False, indent=2)


# ===================== ОБЩАЯ ОБГОРТКА =====================

async def parse_d4_feed_to_json(
    *,
    mode: Literal["catalog", "stock"] = "catalog",
    code: str = "D4",
    timeout: int = 30,
) -> str:
    """
    Унифицированная обёртка для поставщика D4 (Google Sheet):
      mode = "catalog" → каталог
      mode = "stock"   → сток
    """
    if mode == "catalog":
        return await parse_d4_catalog_to_json(code=code, timeout=timeout)
    elif mode == "stock":
        return await parse_d4_stock_to_json(code=code, timeout=timeout)
    else:
        raise ValueError("mode must be 'catalog' or 'stock'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Парсер фида D4 (Google Sheet): "
            "режимы 'catalog' (каталог) и 'stock' (остатки/цены). "
            "URL и параметры берутся из БД по dropship_enterprises.code"
        )
    )
    parser.add_argument(
        "--mode",
        choices=["catalog", "stock"],
        default="catalog",
        help="Режим: catalog | stock (по умолчанию catalog)",
    )
    parser.add_argument(
        "--code",
        default="D4",
        help="значение поля code в dropship_enterprises (по умолчанию D4)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="таймаут HTTP-запроса, сек.",
    )

    args = parser.parse_args()
    out = asyncio.run(
        parse_d4_feed_to_json(
            mode=args.mode,
            code=args.code,
            timeout=args.timeout,
        )
    )
    print(out)
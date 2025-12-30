from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from typing import Optional, List, Dict, Literal
from urllib.parse import urlparse, parse_qs

import httpx
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# --- helpers ---
def _to_int(val: Optional[str]) -> int:
    """М'яко перетворює рядок у int (пробіли/коми), негативні -> 0."""
    if val is None:
        return 0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    if not s:
        return 0
    try:
        num = float(s)
        return max(int(num), 0)
    except Exception:
        return 0


def _to_float(val: Optional[str]) -> float:
    """М'яко перетворює рядок у float (пробіли/коми)."""
    if val is None:
        return 0.0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _norm(s: Optional[str]) -> str:
    return " ".join(str(s or "").strip().split())


def _make_gsheet_csv_url(url: str) -> str:
    """
    Превращает ссылку вида:
      https://docs.google.com/spreadsheets/d/<ID>/edit?gid=0#gid=0
    в CSV export:
      https://docs.google.com/spreadsheets/d/<ID>/export?format=csv&gid=0
    """
    u = url.strip()
    parsed = urlparse(u)
    if "docs.google.com" not in parsed.netloc or "/spreadsheets/d/" not in parsed.path:
        # Если вдруг уже отдают готовый CSV/публичный export — вернем как есть
        return u

    parts = parsed.path.split("/")
    # ['', 'spreadsheets', 'd', '<ID>', 'edit']
    try:
        doc_id = parts[3]
    except Exception:
        return u

    qs = parse_qs(parsed.query or "")
    gid = (qs.get("gid") or ["0"])[0]

    return f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}"


async def _get_feed_url_by_code(code: str) -> Optional[str]:
    """Дістає feed_url з dropship_enterprises за значенням code."""
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


async def _get_profit_percent_by_code(code: str) -> float:
    """profit_percent (%) із dropship_enterprises по code. 25 -> 0.25."""
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT profit_percent FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        raw = res.scalar_one_or_none()

    try:
        val = float(raw)
    except Exception:
        val = 0.0

    if val > 1:
        val = val / 100.0

    if val < 0:
        val = 0.0
    if val > 1:
        val = 1.0

    return val


async def _download_gsheet_csv(*, code: str, timeout: int = 30) -> Optional[str]:
    """
    1) беремо feed_url (google sheet link) з БД по code
    2) конвертуємо в export CSV URL
    3) завантажуємо CSV текст
    """
    feed_url = await _get_feed_url_by_code(code)
    if not feed_url:
        msg = f"Не знайдено feed_url у dropship_enterprises для code='{code}'"
        logger.error(msg)
        send_notification(msg, "Розробник")
        return None

    csv_url = _make_gsheet_csv_url(feed_url)

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(csv_url)
            resp.raise_for_status()
            return resp.text
    except Exception as e:
        msg = f"Помилка завантаження Google Sheet CSV (code={code}) url={csv_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None


def _parse_csv_rows(csv_text: str) -> List[Dict[str, str]]:
    """
    Возвращает строки как dict по заголовкам.
    Важно: Google CSV обычно с разделителем ','.
    """
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f)
    rows: List[Dict[str, str]] = []
    for r in reader:
        if not r:
            continue
        rows.append({(k or "").strip(): (v or "").strip() for k, v in r.items()})
    return rows


# --- PEDIAKID (D7) ---
CATALOG_COLS = {
    "id": "Артикул",
    "name": "Назва",
    "barcode": "Штрих-код",
}

STOCK_COLS = {
    "code_sup": "Артикул",
    "qty": "Доступно",
    "price_retail": "РРЦ",
}


async def parse_pediakid_catalog_to_json(*, code: str = "D7", timeout: int = 30) -> str:
    """
    Каталог (PEDIAKID/GoogleSheet) → JSON:
    [
      {"id": "<Артикул>", "name": "<Назва>", "barcode": "<Штрих-код>"}
    ]
    Пишем только строки где Штрих-код НЕ пустой.
    """
    csv_text = await _download_gsheet_csv(code=code, timeout=timeout)
    if not csv_text:
        return "[]"

    rows = _parse_csv_rows(csv_text)

    items: List[Dict[str, str]] = []
    for r in rows:
        sku = _norm(r.get(CATALOG_COLS["id"]))
        name = _norm(r.get(CATALOG_COLS["name"]))
        barcode = _norm(r.get(CATALOG_COLS["barcode"]))

        if not barcode:
            continue
        if not (sku and name):
            continue

        items.append({"id": sku, "name": name, "barcode": barcode})

    logger.info("PEDIAKID каталог: зібрано позицій (code=%s): %d", code, len(items))
    return json.dumps(items, ensure_ascii=False, indent=2)


async def parse_pediakid_stock_to_json(*, code: str = "D7", timeout: int = 30) -> str:
    """
    Сток (PEDIAKID/GoogleSheet) → JSON:
    [
      {"code_sup": "<Артикул>", "qty": <int>, "price_retail": <float>, "price_opt": <float>}
    ]
    Пишем только строки где Доступно > 0.
    price_opt = price_retail / (1 + profit_percent)
    """
    csv_text = await _download_gsheet_csv(code=code, timeout=timeout)
    if not csv_text:
        return "[]"

    profit_percent = await _get_profit_percent_by_code(code)

    rows_in = _parse_csv_rows(csv_text)

    rows_out: List[Dict[str, object]] = []
    for r in rows_in:
        code_sup = _norm(r.get(STOCK_COLS["code_sup"]))
        if not code_sup:
            continue

        qty = _to_int(r.get(STOCK_COLS["qty"]))
        if qty <= 0:
            continue

        price_retail = _to_float(r.get(STOCK_COLS["price_retail"]))
        price_opt = price_retail / (1.0 + profit_percent) if price_retail > 0 else 0.0
        if price_opt < 0:
            price_opt = 0.0

        rows_out.append(
            {
                "code_sup": code_sup,
                "qty": qty,
                "price_retail": price_retail,
                "price_opt": price_opt,
            }
        )

    logger.info("PEDIAKID сток: зібрано позицій (code=%s): %d", code, len(rows_out))
    return json.dumps(rows_out, ensure_ascii=False, indent=2)


async def parse_pediakid_feed_to_json(
    *, mode: Literal["catalog", "stock"] = "catalog", code: str = "D7", timeout: int = 30
) -> str:
    if mode == "catalog":
        return await parse_pediakid_catalog_to_json(code=code, timeout=timeout)
    if mode == "stock":
        return await parse_pediakid_stock_to_json(code=code, timeout=timeout)
    raise ValueError("mode must be 'catalog' or 'stock'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Парсер фіда PEDIAKID (Google Sheets): режими 'catalog' і 'stock'. "
                    "Посилання береться з БД dropship_enterprises.feed_url по dropship_enterprises.code"
    )
    parser.add_argument("--mode", choices=["catalog", "stock"], default="catalog")
    parser.add_argument("--code", default="D7", help="code у dropship_enterprises (за замовчуванням D7)")
    parser.add_argument("--timeout", type=int, default=30, help="таймаут HTTP-запиту, сек.")

    args = parser.parse_args()
    out = asyncio.run(parse_pediakid_feed_to_json(mode=args.mode, code=args.code, timeout=args.timeout))
    print(out)
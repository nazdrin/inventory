from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Literal

import httpx
import xml.etree.ElementTree as ET
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ===== Общие хелперы (файл автономный) =====

def _get_text(el: ET.Element, candidates: List[str]) -> Optional[str]:
    """Возвращает текст первого дочернего тега из списка candidates."""
    for tag in candidates:
        child = el.find(tag)
        if child is not None and child.text and child.text.strip():
            return child.text.strip()
    return None


def _to_int(val: Optional[str]) -> int:
    """Мягко конвертирует строку в int (убирает пробелы, запятую -> точку). Отрицательные → 0."""
    if not val:
        return 0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        num = float(s)
        return max(int(num), 0)
    except Exception:
        return 0


def _to_float(val: Optional[str]) -> float:
    """Мягко конвертирует строку в float (убирает пробелы, запятую -> точку)."""
    if not val:
        return 0.0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_delivery_date(val: Optional[str]) -> Optional[date]:
    """Парсит дату вида 'DD.MM.YYYY' в date. Если не удалось — None."""
    if not val:
        return None
    s = str(val).strip()
    try:
        return datetime.strptime(s, "%d.%m.%Y").date()
    except Exception:
        return None


def _next_business_day_cutoff(today: date) -> date:
    """Возвращает максимально допустимую дату отправки относительно today (Киев).

    - Пн–Чт: today + 1 день
    - Пт: следующий понедельник (today + 3)
    - Сб: следующий понедельник (today + 2)
    - Вс: следующий понедельник (today + 1)
    """
    wd = today.weekday()  # Mon=0 ... Sun=6
    if wd <= 3:  # Mon-Thu
        return today + timedelta(days=1)
    if wd == 4:  # Fri
        return today + timedelta(days=3)
    if wd == 5:  # Sat
        return today + timedelta(days=2)
    # Sun
    return today + timedelta(days=1)


def _is_allowed_delivery_date(delivery_dt: date, today: date) -> bool:
    """Правило:
    Дата отправки должна попадать в диапазон [today .. cutoff], где cutoff = next_business_day_cutoff(today).

    Пример:
    - Если сегодня Пт, то допускаются Пт, Сб, Вс, Пн (но не Вт)
    - Если сегодня Сб, то допускаются Сб, Вс, Пн (но не Вт)
    """
    cutoff = _next_business_day_cutoff(today)
    return today <= delivery_dt <= cutoff


async def _get_feed_url_by_code(code: str) -> Optional[str]:
    """Достаёт feed_url из dropship_enterprises по значению code."""
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


async def _load_feed_root(*, code: str, timeout: int) -> Optional[ET.Element]:
    """1) Берём feed_url из БД по code 2) Загружаем XML по HTTP 3) Возвращаем корень ElementTree"""
    feed_url = await _get_feed_url_by_code(code)
    if not feed_url:
        msg = f"Не найден feed_url в dropship_enterprises для code='{code}'"
        logger.error(msg)
        send_notification(msg, "Розробник")
        return None

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
            resp = await client.get(feed_url)
            resp.raise_for_status()
            xml_text = resp.text
    except Exception as e:
        msg = f"Ошибка загрузки фида {feed_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None

    try:
        return ET.fromstring(xml_text)
    except Exception as e:
        msg = f"Ошибка парсинга XML из {feed_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None


# ===== Вспомогательные функции для D5 =====

def _d6_collect_items(root: ET.Element) -> List[ET.Element]:
    """Товары в узлах <item> внутри <items>."""
    return root.findall(".//item")


def _d6_extract_art(item: ET.Element) -> Optional[str]:
    """id/code_sup для D5: <art>."""
    art = _get_text(item, ["art"])
    return art.strip() if art else None


def _d6_extract_full_name(item: ET.Element) -> Optional[str]:
    """Название для D5: <full_name>."""
    name = _get_text(item, ["full_name"])
    return " ".join(name.split()) if name else None


def _d6_extract_qty(item: ET.Element) -> int:
    """Количество: <pcs>."""
    pcs_raw = _get_text(item, ["pcs"])
    return _to_int(pcs_raw)


def _d6_extract_price_retail(item: ET.Element) -> float:
    """Розничная цена: <price_Roz>."""
    price_raw = _get_text(item, ["price_Roz", "price_roz"])
    return _to_float(price_raw)


def _d6_extract_price_opt(item: ET.Element) -> float:
    """Оптовая цена: <price_Opt> (учитываем возможную опечатку <price_0pt>)."""
    price_raw = _get_text(item, ["price_Opt", "price_0pt", "price_opt"])
    return _to_float(price_raw)


async def _d6_pick_barcode_for_item(session, item: ET.Element) -> str:
    """Выбор штрихкода по правилам Barcode/Barcodes."""
    barcodes: List[str] = []

    # 1) Вложенные <Barcodes><Barcode>
    for b in item.findall("./Barcodes/Barcode"):
        text_val = (b.text or "").strip()
        if text_val:
            barcodes.append(text_val)

    # 2) На всякий случай — одиночный тег <Barcode> прямо внутри <item>
    if not barcodes:
        single = item.find("Barcode")
        if single is not None and single.text and single.text.strip():
            barcodes.append(single.text.strip())

    if not barcodes:
        return ""

    if len(barcodes) == 1:
        return barcodes[0]

    # несколько штрихкодов — ищем первый, который есть в catalog_mapping."Barcode"
    for bc in barcodes:
        res = await session.execute(
            text('SELECT 1 FROM catalog_mapping WHERE "Barcode" = :bc LIMIT 1'),
            {"bc": bc},
        )
        if res.scalar_one_or_none() is not None:
            return bc

    # если ничего не нашли в таблице — возвращаем первый
    return barcodes[0]


# ===== Парсер каталога D5 =====

async def parse_d6_catalog_to_json(*, code: str = "D6", timeout: int = 30) -> str:
    """Каталог (D5) → JSON."""
    root = await _load_feed_root(code=code, timeout=timeout)
    if root is None:
        return "[]"

    items_json: List[Dict[str, str]] = []

    async with get_async_db() as session:
        for item in _d6_collect_items(root):
            art = _d6_extract_art(item)
            name = _d6_extract_full_name(item)

            if not (art and name):
                continue

            barcode = await _d6_pick_barcode_for_item(session, item)

            items_json.append({
                "id": art,
                "name": name,
                "barcode": barcode,
            })

    logger.info("D6 каталог: собрано позиций (code=%s): %d", code, len(items_json))
    return json.dumps(items_json, ensure_ascii=False, indent=2)


# ===== Парсер стока D6 =====

async def parse_d6_stock_to_json(*, code: str = "D6", timeout: int = 30) -> str:
    """Сток (D5) → JSON."""
    root = await _load_feed_root(code=code, timeout=timeout)
    if root is None:
        return "[]"

    rows: List[Dict[str, object]] = []

    for item in _d6_collect_items(root):
        # Фильтр по дате отправки (берём только ближайшие допустимые будни)
        kyiv_today = datetime.now(ZoneInfo("Europe/Kyiv")).date()
        delivery_raw = _get_text(item, ["delivery_date"])
        delivery_dt = _parse_delivery_date(delivery_raw)

        # Если даты нет или она некорректна — не грузим в сток
        if delivery_dt is None:
            continue

        # Дата отправки должна быть в допустимом окне: сегодня..cutoff (Пт включает Сб/Вс/Пн)
        if not _is_allowed_delivery_date(delivery_dt, kyiv_today):
            continue

        art = _d6_extract_art(item)
        if not art:
            continue

        qty = _d6_extract_qty(item)

        # Игнорируем позиции с нулевым или отрицательным остатком
        if qty <= 0:
            continue

        price_retail = _d6_extract_price_retail(item)
        price_opt = _d6_extract_price_opt(item)

        rows.append({
            "code_sup": art,
            "qty": qty,
            "price_retail": price_retail,
            "price_opt": price_opt,
        })

    logger.info("D6 сток: собрано позиций (code=%s): %d", code, len(rows))
    return json.dumps(rows, ensure_ascii=False, indent=2)


# ===== Унифицированная обёртка =====

async def parse_d6_feed_to_json(
    *,
    mode: Literal["catalog", "stock"] = "catalog",
    code: str = "D6",
    timeout: int = 30,
) -> str:
    """Унифицированная обёртка для поставщика D6."""
    if mode == "catalog":
        return await parse_d6_catalog_to_json(code=code, timeout=timeout)
    elif mode == "stock":
        return await parse_d6_stock_to_json(code=code, timeout=timeout)
    else:
        raise ValueError("mode must be 'catalog' or 'stock'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Парсер фида D5: режимы 'catalog' (каталог) и 'stock' (остатки/цены). "
                    "URL берётся из БД по dropship_enterprises.code"
    )
    parser.add_argument("--mode", choices=["catalog", "stock"], default="catalog",
                        help="Режим: catalog | stock (по умолчанию catalog)")
    parser.add_argument("--code", default="D6",
                        help="значение поля code в dropship_enterprises (по умолчанию D6)")
    parser.add_argument("--timeout", type=int, default=30, help="таймаут HTTP-запроса, сек.")

    args = parser.parse_args()
    out = asyncio.run(parse_d6_feed_to_json(mode=args.mode, code=args.code, timeout=args.timeout))
    print(out)
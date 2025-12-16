# feed_parser.py
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional, List, Dict, Literal

import httpx
import xml.etree.ElementTree as ET
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Типовые названия параметра штрихкода, встречающиеся в фидах
BARCODE_PARAM_NAMES = {
    "Штрихкод", "Штрих-код", "Штрих код",
    "EAN", "EAN-13", "UPC", "GTIN", "Barcode", "barcode"
}


def _get_text(el: ET.Element, candidates: List[str]) -> Optional[str]:
    """Возвращает текст первого дочернего тега из списка кандидатов."""
    for tag in candidates:
        child = el.find(tag)
        if child is not None and child.text and child.text.strip():
            return child.text.strip()
    return None


def _extract_barcode(el: ET.Element) -> Optional[str]:
    """Поиск штрихкода в типовых полях и в <param name='...'>."""
    # 1) Прямые поля
    barcode = _get_text(el, ["barcode", "ean", "gtin", "upc", "Barcode"])
    if barcode:
        return barcode
    # 2) Варианты через <param name="...">
    for p in el.findall(".//param"):
        name = (p.get("name") or p.get("Name") or "").strip()
        if name in BARCODE_PARAM_NAMES and p.text and p.text.strip():
            return p.text.strip()
    return None


def _extract_sku(el: ET.Element) -> Optional[str]:
    """Достаём SKU из распространённых тегов/атрибутов (в т.ч. vendorCode для стока)."""
    sku = (
        _get_text(el, ["sku", "productId", "code", "id", "vendorCode"])
        or el.get("sku")
        or el.get("id")
    )
    return str(sku).strip() if sku else None


def _to_int(val: Optional[str]) -> int:
    """Мягко преобразует строку в int (учитывая пробелы/запятые), отрицательные -> 0."""
    if not val:
        return 0
    s = str(val).strip()
    # Убираем пробелы-разделители тысяч
    s = s.replace(" ", "").replace("\u00A0", "")
    # Заменяем запятую на точку, затем берём целую часть
    s = s.replace(",", ".")
    try:
        num = float(s)
        return max(int(num), 0)
    except Exception:
        return 0


def _to_float(val: Optional[str]) -> float:
    """Мягко преобразует строку в float (учитывая пробелы/запятые)."""
    if not val:
        return 0.0
    s = str(val).strip()
    s = s.replace(" ", "").replace("\u00A0", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


async def _get_feed_url_by_code(code: str = "D1") -> Optional[str]:
    """Достаёт feed_url из dropship_enterprises по значению поля code."""
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


async def _get_gdrive_folder_by_code(code: str = "D1") -> Optional[str]:
    """Достаёт gdrive_folder из dropship_enterprises по значению поля code (для сток-фида)."""
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT gdrive_folder FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


async def _get_retail_markup_by_code(code: str = "D1") -> float:
    """Достаёт retail_markup (%) из dropship_enterprises по значению поля code.

    В БД значение хранится как проценты (например, 25), возвращаем долю (0.25).
    """
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT retail_markup FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        raw = res.scalar_one_or_none()

    # retail_markup может быть None / строкой / числом
    try:
        val = float(raw)
    except Exception:
        val = 0.0

    # 25 -> 0.25
    if val > 1:
        val = val / 100.0

    # Нормализуем границы
    if val < 0:
        val = 0.0
    if val > 1:
        val = 1.0

    return val


async def _load_feed_root_from_url(feed_url: str, timeout: int) -> Optional[ET.Element]:
    """Загружает и парсит XML по переданному URL."""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
            resp = await client.get(feed_url)
            resp.raise_for_status()
            xml_text = resp.text
    except Exception as e:
        msg = f"Ошибка загрузки фида {feed_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return None

    try:
        return ET.fromstring(xml_text)
    except Exception as e:
        msg = f"Ошибка парсинга XML из {feed_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return None


async def _load_feed_root(*, code: str, timeout: int) -> Optional[ET.Element]:
    """
    Единый метод получения данных каталога:
    1) берём feed_url из БД по code
    2) скачиваем XML
    3) возвращаем корень ElementTree
    """
    feed_url = await _get_feed_url_by_code(code)
    if not feed_url:
        msg = f"Не найден feed_url в dropship_enterprises для code='{code}'"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        return None

    return await _load_feed_root_from_url(feed_url, timeout)


async def _load_stock_feed_root(*, code: str, timeout: int) -> Optional[ET.Element]:
    """
    Загрузка фида остатков по значению gdrive_folder из dropship_enterprises.
    Используется для формирования стока.
    """
    feed_url = await _get_gdrive_folder_by_code(code)
    if not feed_url:
        msg = f"Не найден gdrive_folder в dropship_enterprises для code='{code}'"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        return None

    return await _load_feed_root_from_url(feed_url, timeout)


def _collect_product_nodes(root: ET.Element) -> List[ET.Element]:
    """Собираем узлы товаров (типовые: <offer> или <item>)."""
    product_nodes = root.findall(".//offer") + root.findall(".//item")
    if not product_nodes:
        # Фолбек: если структура нестандартная — берём все элементы, у которых есть дети
        product_nodes = [el for el in root.iter() if list(el)]
    return product_nodes


async def parse_feed_catalog_to_json(*, code: str = "D1", timeout: int = 30) -> str:
    """
    Каталог: возвращает JSON со списком
    [
      {"id": "<sku>", "name": "<name>", "barcode": "<barcode>"},
      ...
    ]
    """
    root = await _load_feed_root(code=code, timeout=timeout)
    if root is None:
        return "[]"

    items: List[Dict[str, str]] = []
    for node in _collect_product_nodes(root):
        sku = _extract_sku(node)
        name = _get_text(node, ["name", "title"])
        barcode = _extract_barcode(node)

        if not (sku and name):
            continue

        items.append({
            "id": sku,
            "name": name,
            "barcode": (barcode or "").strip()
        })

    logger.info("Каталог: собрано позиций (code=%s): %d", code, len(items))
    return json.dumps(items, ensure_ascii=False, indent=2)


async def parse_feed_stock_to_json(*, code: str = "D1", timeout: int = 30) -> str:
    """
    Сток: возвращает JSON со списком
    [
      {"code_sup": "<vendorCode>", "qty": <int>, "price_retail": <float>, "price_opt": 0},
      ...
    ]

    Маппинг для сток-фида (offers):
      - vendorCode          -> code_sup
      - quantity_in_stock   -> qty
      - price               -> price_retail
      - price_opt           -> 0 (по умолчанию)
    """
    root = await _load_stock_feed_root(code=code, timeout=timeout)
    if root is None:
        return "[]"

    retail_markup = await _get_retail_markup_by_code(code)

    rows: List[Dict[str, object]] = []
    for node in _collect_product_nodes(root):
        sku = _extract_sku(node)
        if not sku:
            continue

        # qty: только новый формат сток-фида
        qty_raw = _get_text(node, ["quantity_in_stock"]) or node.get("quantity_in_stock")

        # price: только новый формат сток-фида
        price_raw = _get_text(node, ["price"]) or node.get("price")

        qty = _to_int(qty_raw)
        # Игнорируем позиции с нулевым или отрицательным остатком
        if qty <= 0:
            continue

        price_retail = _to_float(price_raw)

        price_opt = price_retail /(1.0 + retail_markup)
        # На всякий случай не уходим в минус
        if price_opt < 0:
            price_opt = 0.0

        rows.append({
            "code_sup": sku,
            "qty": qty,
            "price_retail": price_retail,
            "price_opt": price_opt,
        })

    logger.info("Сток: собрано позиций (code=%s): %d", code, len(rows))
    return json.dumps(rows, ensure_ascii=False, indent=2)


async def parse_feed_to_json(*, mode: Literal["catalog", "stock"] = "catalog",
                             code: str = "D1", timeout: int = 30) -> str:
    """Унифицированная обёртка, если понадобится вызывать по одному входу."""
    if mode == "catalog":
        return await parse_feed_catalog_to_json(code=code, timeout=timeout)
    elif mode == "stock":
        return await parse_feed_stock_to_json(code=code, timeout=timeout)
    else:
        raise ValueError("mode must be 'catalog' or 'stock'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Парсер фида: режимы 'catalog' (каталог) и 'stock' (остатки/цены). "
                    "URL берётся из БД по dropship_enterprises.code"
    )
    parser.add_argument("--mode", choices=["catalog", "stock"], default="catalog",
                        help="Режим обработки: catalog | stock (по умолчанию catalog)")
    parser.add_argument("--code", default="D1",
                        help="значение поля code в dropship_enterprises (по умолчанию D1)")
    parser.add_argument("--timeout", type=int, default=30, help="таймаут HTTP-запроса, сек.")

    args = parser.parse_args()
    out = asyncio.run(parse_feed_to_json(mode=args.mode, code=args.code, timeout=args.timeout))
    print(out)
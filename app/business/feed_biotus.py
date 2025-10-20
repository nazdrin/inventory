# feed_parser.py
from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional, List, Dict

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
    """Возвращает текст первого дочернего тега из переданного списка названий."""
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


async def _get_feed_url_by_code(code: str = "D1") -> Optional[str]:
    """Достаёт feed_url из dropship_enterprises по значению поля code."""
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


async def parse_feed_to_json(*, code: str = "D1", timeout: int = 30) -> str:
    """
    Берёт feed_url из БД (dropship_enterprises.feed_url по code, по умолчанию 'D1'),
    загружает XML-фид, извлекает sku, name, barcode и возвращает JSON-строку:
    [
      {"id": "<sku>", "name": "<name>", "barcode": "<barcode>"},
      ...
    ]
    """
    feed_url = await _get_feed_url_by_code(code)
    if not feed_url:
        msg = f"Не найден feed_url в dropship_enterprises для code='{code}'"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        return "[]"

    # 1) Качаем XML (асинхронно)
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
        return "[]"

    # 2) Парсим XML
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        msg = f"Ошибка парсинга XML из {feed_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return "[]"

    # 3) Сбор элементов
    # Типовые контейнеры товаров: <offer> (YML) или <item>
    product_nodes = root.findall(".//offer") + root.findall(".//item")
    if not product_nodes:
        # Фолбек: если структура нестандартная — берём все элементы, у которых есть дети
        product_nodes = [el for el in root.iter() if list(el)]

    items: List[Dict[str, str]] = []

    for node in product_nodes:
        # sku: пробуем распространённые варианты
        sku = (
            _get_text(node, ["sku", "productId", "code", "id"])
            or node.get("sku")
            or node.get("id")
        )
        name = _get_text(node, ["name", "title"])
        barcode = _extract_barcode(node)

        # пропускаем, если ключевые поля отсутствуют
        if not (sku and name):
            continue

        items.append({
            "id": str(sku).strip(),
            "name": name,
            "barcode": (barcode or "").strip()
        })

    result_json = json.dumps(items, ensure_ascii=False, indent=2)
    logger.info("Собрано позиций из фида (%s, code=%s): %d", feed_url, code, len(items))
    return result_json


if __name__ == "__main__":
    # Ручной запуск: берёт URL из БД по code (по умолчанию D1)
    import argparse

    parser = argparse.ArgumentParser(
        description="Локальный запуск парсера фида (URL берётся из БД по dropship_enterprises.code)"
    )
    parser.add_argument("--code", default="D1", help="значение поля code в dropship_enterprises (по умолчанию D1)")
    parser.add_argument("--timeout", type=int, default=30, help="таймаут HTTP-запроса, сек.")

    args = parser.parse_args()

    out = asyncio.run(parse_feed_to_json(code=args.code, timeout=args.timeout))
    print(out)
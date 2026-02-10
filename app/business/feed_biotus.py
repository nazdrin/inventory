from __future__ import annotations

import asyncio
import json
import logging
import math
from typing import Optional, List, Dict, Literal, Tuple

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


# Математическое округление до целых UAH (.5 вверх)
def _round_uah(val: float) -> int:
    """Округление до целых по математическим правилам (.5 вверх)."""
    if val <= 0:
        return 0
    return int(math.floor(val + 0.5))


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


async def _get_profit_percent_by_code(code: str = "D1") -> float:
    """Достаёт profit_percent (%) из dropship_enterprises по значению поля code.

    В БД значение хранится как проценты (например, 25), возвращаем долю (0.25).
    """
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT profit_percent FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        raw = res.scalar_one_or_none()

    # profit_percent может быть None / строкой / числом
    try:
        val = float(raw)
    except Exception:
        val = 0.0

    # 25 -> 0.25
    if val > 1:
        val = val / 100.0

    # Нормализуем границы: допускаем отрицательное значение,
    # но по модулю ограничиваем 100%.
    if val > 1:
        val = 1.0
    if val < -1:
        val = -1.0

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


def _build_partner_price_usd_map(root: ET.Element) -> Dict[str, float]:
    """Строит словарь {sku: price_partner_usd} из фида по feed_url."""
    mp: Dict[str, float] = {}
    for node in _collect_product_nodes(root):
        sku = _extract_sku(node)
        if not sku:
            continue
        usd_raw = _get_text(node, ["price_partner_usd"]) or node.get("price_partner_usd")
        usd_val = _to_float(usd_raw)
        if usd_val > 0:
            mp[sku] = usd_val
    return mp


def _build_rsp_rate_map(root: ET.Element) -> Dict[str, float]:
    """Строит словарь {sku: price_rsp_uah / price_rsp_usd} из фида по feed_url."""
    mp: Dict[str, float] = {}
    for node in _collect_product_nodes(root):
        sku = _extract_sku(node)
        if not sku:
            continue
        uah_raw = _get_text(node, ["price_rsp_uah"]) or node.get("price_rsp_uah")
        usd_raw = _get_text(node, ["price_rsp_usd"]) or node.get("price_rsp_usd")
        uah_val = _to_float(uah_raw)
        usd_val = _to_float(usd_raw)
        if uah_val > 0 and usd_val > 0:
            mp[sku] = uah_val / usd_val
    return mp


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
      {"code_sup": "<vendorCode>", "qty": <int>, "price_retail": <float>, "price_opt": расчётная оптовая цена (см. формулу ниже)},
      ...
    ]

    Маппинг для сток-фида (offers):
      - vendorCode          -> code_sup
      - quantity_in_stock   -> qty
      - price               -> price_retail
      - price_opt           -> расчётная оптовая цена (см. формулу ниже)

    Формула price_opt:
      - если доступны price_partner_usd, price_rsp_uah и price_rsp_usd в фиде по feed_url:
          price_opt = price_partner_usd * (price_rsp_uah / price_rsp_usd) * (1 - profit_percent)
      - иначе:
          price_opt = price_retail / 1.25
    """
    # 1) Пытаемся взять основной сток-фид (URL берётся из dropship_enterprises.gdrive_folder)
    root = await _load_stock_feed_root(code=code, timeout=timeout)
    used_fallback = False

    # 2) Если основной сток-фид недоступен/падает — берём запасной фид из dropship_enterprises.feed_url
    #    (он же используется для каталога), но парсим из него поля: sku, in_stock, price_rsp_uah
    if root is None:
        used_fallback = True
        msg = (
            f"Сток-фид из gdrive_folder недоступен для code='{code}'. "
            f"Переключаемся на фид из feed_url (fallback)."
        )
        logger.warning(msg)
        # Нотификация разработчику, чтобы было видно факт деградации
        send_notification(msg, "Разработчик")

        root = await _load_feed_root(code=code, timeout=timeout)
        if root is None:
            # Если и запасной фид не загрузился — возвращаем пустой результат
            return "[]"

    profit_percent = await _get_profit_percent_by_code(code)
    # profit_percent задаётся как "на сколько снизить". В БД вводим положительное число (например 20),
    # которое превращается в 0.20 и даёт коэффициент 0.80.
    # Если в БД по ошибке задано отрицательное — это будет означать увеличение (1 - (-0.2) = 1.2).
    coef_profit = 1.0 - profit_percent  # 0.2 -> 0.8

    # safety: не даём уйти в отрицательные/нулевые коэффициенты
    if coef_profit <= 0:
        coef_profit = 0.0

    partner_root: Optional[ET.Element] = None
    partner_price_usd_map: Dict[str, float] = {}
    rate_map: Dict[str, float] = {}

    # Пытаемся загрузить фид по feed_url, чтобы достать price_partner_usd
    # (если мы уже в fallback-режиме, root = feed_url и достаточно использовать его)
    if used_fallback:
        partner_root = root
    else:
        partner_root = await _load_feed_root(code=code, timeout=timeout)

    if partner_root is not None:
        partner_price_usd_map = _build_partner_price_usd_map(partner_root)
        rate_map = _build_rsp_rate_map(partner_root)
    else:
        logger.warning(
            "Фид по feed_url недоступен для code=%s. price_partner_usd недоступен, используем fallback-формулу price_retail/1.25",
            code,
        )

    rows: List[Dict[str, object]] = []

    if not used_fallback:
        # ОСНОВНОЙ СТОК-ФИД (структура: quantity_in_stock / price)
        for node in _collect_product_nodes(root):
            sku = _extract_sku(node)
            if not sku:
                continue

            qty_raw = _get_text(node, ["quantity_in_stock"]) or node.get("quantity_in_stock")
            price_raw = _get_text(node, ["price"]) or node.get("price")

            qty = _to_int(qty_raw)
            if qty <= 0:
                continue

            price_retail = _to_float(price_raw)

            # Основная формула: partner_usd * (price_rsp_uah / price_rsp_usd) * coef_profit
            price_opt = 0.0
            partner_usd = partner_price_usd_map.get(sku)
            rate = rate_map.get(sku)

            if partner_usd and rate and rate > 0 and coef_profit > 0:
                price_opt = float(partner_usd) * float(rate) * float(coef_profit)
            else:
                # Фолбек: если partner_usd отсутствует или недоступен курс/фид — берём от розницы
                price_opt = price_retail / 1.25 if price_retail > 0 else 0.0

            if price_opt < 0:
                price_opt = 0.0

            price_opt_int = _round_uah(float(price_opt))

            rows.append({
                "code_sup": sku,
                "qty": qty,
                "price_retail": price_retail,
                "price_opt": price_opt_int,
            })
    else:
        # FALLBACK-ФИД (структура: in_stock / price_rsp_uah)
        for node in _collect_product_nodes(root):
            sku = _extract_sku(node)
            if not sku:
                continue

            qty_raw = _get_text(node, ["in_stock"]) or node.get("in_stock")
            price_raw = _get_text(node, ["price_rsp_uah"]) or node.get("price_rsp_uah")

            qty = _to_int(qty_raw)
            if qty <= 0:
                continue

            price_retail = _to_float(price_raw)

            # Основная формула: partner_usd * (price_rsp_uah / price_rsp_usd) * coef_profit
            price_opt = 0.0
            partner_usd = partner_price_usd_map.get(sku)
            rate = rate_map.get(sku)

            if partner_usd and rate and rate > 0 and coef_profit > 0:
                price_opt = float(partner_usd) * float(rate) * float(coef_profit)
            else:
                # Фолбек: если partner_usd отсутствует или недоступен курс/фид — берём от розницы
                price_opt = price_retail / 1.25 if price_retail > 0 else 0.0

            if price_opt < 0:
                price_opt = 0.0

            price_opt_int = _round_uah(float(price_opt))

            rows.append({
                "code_sup": sku,
                "qty": qty,
                "price_retail": price_retail,
                "price_opt": price_opt_int,
            })

    logger.info(
        "Сток: собрано позиций (code=%s): %d (fallback=%s)",
        code,
        len(rows),
        "yes" if used_fallback else "no",
    )
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

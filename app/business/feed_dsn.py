# feed_dsn.py
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

# Типові назви параметра штрихкоду (на випадок різних фідів)
BARCODE_PARAM_NAMES = {
    "Штрихкод", "Штрих-код", "Штрих код",
    "EAN", "EAN-13", "UPC", "GTIN", "Barcode", "barcode"
}


def _get_text(el: ET.Element, candidates: List[str]) -> Optional[str]:
    """Повертає текст першого дочірнього тегу зі списку кандидатів."""
    for tag in candidates:
        child = el.find(tag)
        if child is not None and child.text and child.text.strip():
            return child.text.strip()
    return None


def _to_int(val: Optional[str]) -> int:
    """М'яко перетворює рядок у int (пробіли/коми), негативні -> 0."""
    if not val:
        return 0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        num = float(s)
        return max(int(num), 0)
    except Exception:
        return 0


def _to_float(val: Optional[str]) -> float:
    """М'яко перетворює рядок у float (пробіли/коми)."""
    if not val:
        return 0.0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _extract_barcode(el: ET.Element) -> Optional[str]:
    """Пошук штрихкоду: спершу прямі поля, далі <param name='...'>."""
    # Прямі поля (на випадок інших фідів)
    direct = _get_text(el, ["barcode", "ean", "gtin", "upc", "Barcode"])
    if direct:
        return direct
    # Параметри
    for p in el.findall(".//param"):
        name = (p.get("name") or p.get("Name") or "").strip()
        if name in BARCODE_PARAM_NAMES and p.text and p.text.strip():
            return p.text.strip()
    return None


def _dsn_extract_sku(offer: ET.Element) -> Optional[str]:
    """DSN: SKU = атрибут offer@id."""
    sku = offer.get("id") or _get_text(offer, ["id", "vendorCode"])  # fallback
    return str(sku).strip() if sku else None


def _dsn_extract_name(offer: ET.Element) -> Optional[str]:
    """DSN: назва з <name_ua> (або <name> як запасний варіант)."""
    name = _get_text(offer, ["name_ua"]) or _get_text(offer, ["name", "title"])
    if name:
        # Прибираємо зайві переводи рядка/пробіли по краях
        return " ".join(name.split())
    return None


def _dsn_extract_qty(offer: ET.Element) -> int:
    """DSN: кількість з <quantity_in_stock>."""
    qty_raw = _get_text(offer, ["quantity_in_stock"]) or offer.get("quantity_in_stock")
    return _to_int(qty_raw)


def _dsn_extract_price_retail(offer: ET.Element) -> float:
    """DSN: роздрібна ціна з <price>."""
    price_raw = _get_text(offer, ["price"]) or offer.get("price")
    return _to_float(price_raw)


async def _get_feed_url_by_code(code: str) -> Optional[str]:
    """Дістає feed_url з dropship_enterprises за значенням code."""
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


async def _load_feed_root(*, code: str, timeout: int) -> Optional[ET.Element]:
    """
    1) беремо feed_url з БД по code
    2) завантажуємо XML
    3) повертаємо корінь ElementTree
    """
    feed_url = await _get_feed_url_by_code(code)
    if not feed_url:
        msg = f"Не знайдено feed_url у dropship_enterprises для code='{code}'"
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
        msg = f"Помилка завантаження фіду {feed_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None

    try:
        return ET.fromstring(xml_text)
    except Exception as e:
        msg = f"Помилка парсингу XML з {feed_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None


def _collect_offer_nodes(root: ET.Element) -> List[ET.Element]:
    """DSN: товари у вузлах <offer> (усередині <offers>)."""
    nodes = root.findall(".//offer")
    if not nodes:
        # Фолбек на випадок нетипової структури
        nodes = [el for el in root.iter() if el.tag.lower() == "offer" or list(el)]
    return nodes


async def parse_dsn_catalog_to_json(*, code: str = "D2", timeout: int = 30) -> str:
    """
    Каталог (DSN) → JSON:
    [
      {"id": "<offer@id>", "name": "<name_ua>", "barcode": "<Штрихкод|...>"}
    ]
    """
    root = await _load_feed_root(code=code, timeout=timeout)
    if root is None:
        return "[]"

    items: List[Dict[str, str]] = []
    for offer in _collect_offer_nodes(root):
        sku = _dsn_extract_sku(offer)
        name = _dsn_extract_name(offer)
        barcode = _extract_barcode(offer)

        if not (sku and name):
            continue

        items.append({
            "id": sku,
            "name": name,
            "barcode": (barcode or "").strip()
        })

    logger.info("DSN каталог: зібрано позицій (code=%s): %d", code, len(items))
    return json.dumps(items, ensure_ascii=False, indent=2)


async def parse_dsn_stock_to_json(*, code: str = "D2", timeout: int = 30) -> str:
    """
    Сток (DSN) → JSON:
    [
      {"code_sup": "<offer@id>", "qty": <int>, "price_retail": <float>, "price_opt": 0}
    ]

    Мапінг:
      - offer@id           -> code_sup
      - quantity_in_stock  -> qty
      - price              -> price_retail
      - price_opt          -> 0 (за замовчуванням)
    """
    root = await _load_feed_root(code=code, timeout=timeout)
    if root is None:
        return "[]"

    rows: List[Dict[str, object]] = []
    for offer in _collect_offer_nodes(root):
        sku = _dsn_extract_sku(offer)
        if not sku:
            continue

        qty = _dsn_extract_qty(offer)
        # Игнорируем позиции с нулевым або від'ємним залишком
        if qty <= 0:
            continue
        

        price_retail = _dsn_extract_price_retail(offer)

        rows.append({
            "code_sup": sku,
            "qty": qty,
            "price_retail": price_retail,
            "price_opt": 0
        })

    logger.info("DSN сток: зібрано позицій (code=%s): %d", code, len(rows))
    return json.dumps(rows, ensure_ascii=False, indent=2)


async def parse_dsn_feed_to_json(*, mode: Literal["catalog", "stock"] = "catalog",
                                 code: str = "D2", timeout: int = 30) -> str:
    """Уніфікована обгортка для DSN."""
    if mode == "catalog":
        return await parse_dsn_catalog_to_json(code=code, timeout=timeout)
    elif mode == "stock":
        return await parse_dsn_stock_to_json(code=code, timeout=timeout)
    else:
        raise ValueError("mode must be 'catalog' or 'stock'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Парсер фіда DSN: режими 'catalog' (каталог) і 'stock' (залишки/ціни). "
                    "URL береться з БД по dropship_enterprises.code"
    )
    parser.add_argument("--mode", choices=["catalog", "stock"], default="catalog",
                        help="Режим: catalog | stock (за замовчуванням catalog)")
    parser.add_argument("--code", default="D2",
                        help="значення поля code у dropship_enterprises (за замовчуванням D2)")
    parser.add_argument("--timeout", type=int, default=30, help="таймаут HTTP-запиту, сек.")

    args = parser.parse_args()
    out = asyncio.run(parse_dsn_feed_to_json(mode=args.mode, code=args.code, timeout=args.timeout))
    print(out)
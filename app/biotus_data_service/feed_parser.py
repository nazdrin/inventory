# feed_parser.py
import json
import logging
import requests
import xml.etree.ElementTree as ET
from typing import Optional, List, Dict

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Типові назви параметра штрихкоду, які зустрічаються у фідах
BARCODE_PARAM_NAMES = {
    "Штрихкод", "Штрих-код", "Штрих код",
    "EAN", "EAN-13", "UPC", "GTIN", "Barcode", "barcode"
}

def _get_text(el: ET.Element, candidates: List[str]) -> Optional[str]:
    """Повертає текст першого наявного підвузла з переліку назв."""
    for tag in candidates:
        child = el.find(tag)
        if child is not None and child.text and child.text.strip():
            return child.text.strip()
    return None

def _extract_barcode(el: ET.Element) -> Optional[str]:
    """Пошук штрихкоду в типових полях та в <param name='...'>."""
    # 1) Прямі поля
    barcode = _get_text(el, ["barcode", "ean", "gtin", "upc", "Barcode"])
    if barcode:
        return barcode
    # 2) Варіанти через <param name="...">
    for p in el.findall(".//param"):
        name = (p.get("name") or p.get("Name") or "").strip()
        if name in BARCODE_PARAM_NAMES and p.text and p.text.strip():
            return p.text.strip()
    return None

def parse_feed_to_json(url: str, *, timeout: int = 30) -> str:
    """
    Завантажує XML-фід, дістає sku, name, barcode і повертає JSON-рядок:
    [
      {"id": "<sku>", "name": "<name>", "barcode": "<barcode>"},
      ...
    ]
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)

    # Типові контейнери товарів: <offer> (YML) або <item>
    product_nodes = root.findall(".//offer") + root.findall(".//item")
    if not product_nodes:
        # Фолбек: якщо структура нестандартна, спробуємо всі елементи другого рівня
        product_nodes = [el for el in root.iter() if list(el)]

    items: List[Dict[str, str]] = []

    for node in product_nodes:
        # sku: пробуємо поширені варіанти
        sku = (
            _get_text(node, ["sku", "productId", "code", "id"])
            or node.get("sku")
            or node.get("id")
        )
        name = _get_text(node, ["name", "title"])
        barcode = _extract_barcode(node)

        # Пропускаємо, якщо ключові поля відсутні
        if not (sku and name):
            continue

        items.append({
            "id": str(sku).strip(),
            "name": name,
            "barcode": (barcode or "").strip()
        })

    result_json = json.dumps(items, ensure_ascii=False, indent=2)
    logging.info("Зібрано позицій: %s", len(items))
    return result_json

if __name__ == "__main__":
    # 🔧 Вкажи URL фіда тут для швидкого тесту
    FEED_URL = "https://static-opt.biotus.ua/media/amasty/feed/biotus_partner.xml"
    print(parse_feed_to_json(FEED_URL))
# feed_converter.py
import os
import json
import logging
import requests
import xml.etree.ElementTree as ET

from sqlalchemy.future import select
from app.database import get_async_db
from app.models import EnterpriseSettings, MappingBranch
from app.services.database_service import process_database_service


logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)


# ---------- БД ----------
async def fetch_feed_url(enterprise_code: str) -> str | None:
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(
                EnterpriseSettings.enterprise_code == enterprise_code
            )
        )
        return result.scalars().first()


async def fetch_branch_by_enterprise_code(enterprise_code: str) -> str:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(
                MappingBranch.enterprise_code == enterprise_code
            )
        )
        branch = result.scalars().first()
    if not branch:
        raise ValueError(f"Branch не найден для enterprise_code={enterprise_code}")
    return str(branch)


# ---------- Загрузка и парсинг ----------
def download_feed(url: str) -> str:
    headers = {"User-Agent": "TabletkiFeedConverter/1.0"}
    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Ошибка загрузки: HTTP {resp.status_code}")
    text = resp.text or ""
    if not text.strip():
        raise RuntimeError("Получен пустой фид")
    return text


def _safe_float(x, default: float = 0.0) -> float:
    try:
        s = str(x).replace(",", ".")
        return float(s)
    except Exception:
        return default


def _safe_int_ge0(x, default: int = 0) -> int:
    try:
        s = str(x).replace(",", ".")
        v = int(float(s))
        return max(0, v)
    except Exception:
        return default


def parse_xml_feed(xml_text: str) -> list[dict]:
    """
    Читаем <offer>, оставляем поля «как есть» (без нормализации пробелов),
    дубликаты НЕ удаляем.
    """
    root = ET.fromstring(xml_text)
    offers = root.findall(".//offer")
    result: list[dict] = []
    for offer in offers:
        item = {
            # согласовано
            "@id": offer.get("id") or "",                         # code позже
            "name": (offer.findtext("name") or ""),               # name
            "vendor": (offer.findtext("vendor") or ""),           # producer
            "barcode": (offer.findtext("barcode") or ""),         # barcode
            "price": _safe_float(offer.findtext("price")),        # price
            "quantity_in_stock": _safe_int_ge0(
                offer.findtext("quantity_in_stock")
            ),                                                    # qty
        }
        result.append(item)
    return result


# ---------- Трансформация ----------
def transform_catalog(data: list[dict]) -> list[dict]:
    """
    [{ code, name, producer, barcode, vat }]
    """
    out: list[dict] = []
    for o in data:
        out.append({
            "code": o.get("@id", "") or "",
            "name": o.get("name", "") or "",
            "producer": o.get("vendor", "") or "",
            "barcode": o.get("barcode", "") or "",
            "vat": 20.0,
        })
    return out


def transform_stock(data: list[dict], branch: str) -> list[dict]:
    """
    [{ branch, code, price, qty, price_reserve }]
    qty — строго из quantity_in_stock; reserve не учитываем.
    price_reserve = price.
    """
    out: list[dict] = []
    for o in data:
        price = _safe_float(o.get("price", 0.0))
        qty = _safe_int_ge0(o.get("quantity_in_stock", 0))
        out.append({
            "branch": branch,
            "code": o.get("@id", "") or "",
            "price": price,
            "qty": qty,
            "price_reserve": price,
        })
    return out


# ---------- Сохранение и отправка ----------
def _base_temp_dir() -> str:
    return os.getenv("TEMP_FILE_PATH", "temp")


def save_to_json(data: list[dict], enterprise_code: str, file_type: str) -> str:
    dir_path = os.path.join(_base_temp_dir(), str(enterprise_code))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{file_type}.json")

    # компактно; если нужен человекочитаемый — indent=2
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    logger.info("✅ Данные сохранены: %s (records=%d)", file_path, len(data))
    return file_path


async def send_catalog_data(file_path: str, enterprise_code: str):
    await process_database_service(file_path, "catalog", enterprise_code)


async def send_stock_data(file_path: str, enterprise_code: str):
    await process_database_service(file_path, "stock", enterprise_code)


# ---------- Точка входа ----------
async def run_service(enterprise_code: str, file_type: str) -> str:
    if file_type not in ("catalog", "stock"):
        raise ValueError("Тип файла должен быть 'catalog' или 'stock'")

    url = await fetch_feed_url(enterprise_code)
    if not url:
        raise ValueError(f"URL фида не найден для enterprise_code={enterprise_code}")

    feed_text = download_feed(url)
    raw = parse_xml_feed(feed_text)

    if file_type == "catalog":
        data = transform_catalog(raw)
        path = save_to_json(data, enterprise_code, "catalog")
        await send_catalog_data(path, enterprise_code)
        return path

    # stock
    branch = await fetch_branch_by_enterprise_code(enterprise_code)
    data = transform_stock(raw, branch)
    path = save_to_json(data, enterprise_code, "stock")
    await send_stock_data(path, enterprise_code)
    return path


# Локальный тест
if __name__ == "__main__":  # pragma: no cover
    import asyncio
    import argparse
    p = argparse.ArgumentParser(description="Tabletki Feed Converter (requests+ET)")
    p.add_argument("--enterprise_code", required=True)
    p.add_argument("--file_type", required=True, choices=["catalog", "stock"])
    args = p.parse_args()

    async def _main():
        sp = await run_service(args.enterprise_code, args.file_type)
        print("Saved:", sp)

    asyncio.run(_main())
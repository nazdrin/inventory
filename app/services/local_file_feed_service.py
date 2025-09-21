# app/services/local_file_feed_service.py
import os
import json
import logging
import argparse
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Union, Optional

from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings  # noqa: F401  # (может понадобиться позже)
from app.models import MappingBranch
from app.services.database_service import process_database_service

# ---------------------------
# Логирование
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ---------------------------
# 1) Получение branch из MappingBranch
# ---------------------------
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

# ---------------------------
# 2) Загрузка локального файла
#    Поддерживает:
#    - JSON-объект
#    - JSON-массив
#    - NDJSON (по строкам)
# ---------------------------
def load_local_json(file_path: Union[str, Path]) -> List[Dict[str, Any]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    # Попытка: обычный JSON
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return [parsed]
        if isinstance(parsed, list):
            # Убедимся, что список словарей
            return [x for x in parsed if isinstance(x, dict)]
    except json.JSONDecodeError:
        pass

    # Попытка: NDJSON
    data: List[Dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                data.append(obj)
        except json.JSONDecodeError:
            logging.warning("Строка пропущена (не JSON): %s", line[:200])
    if data:
        return data

    raise ValueError("Не удалось распарсить файл как JSON/NDJSON")

# ---------------------------
# 3) Преобразование каталога
# Маппинг по ТЗ:
#   Id       → code
#   Name     → name
#   producer → "" (пусто)
#   Barcode  → barcode
#   vat      → 20.0 (константа)
#
# Примечание: если Id отсутствует, пробуем вытащить из PropertiesList, где Name == "Код товару"
# ---------------------------
def _extract_code(item: Dict[str, Any]) -> Optional[str]:
    code = item.get("Id")
    if code is not None:
        return str(code)

    # fallback: PropertiesList → "Код товару"
    props = item.get("PropertiesList")
    if isinstance(props, list):
        for p in props:
            try:
                if p.get("Name") == "Код товару" and p.get("Value") is not None:
                    return str(p.get("Value"))
            except AttributeError:
                continue
    return None

def transform_catalog(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in data:
        code = _extract_code(item)
        if not code:
            logging.warning("Пропуск записи без code (нет Id и нет PropertiesList['Код товару']): %s", str(item)[:200])
            continue

        name = item.get("Name", "")
        barcode = item.get("Barcode", "")

        result.append({
            "code": str(code),
            "name": str(name) if name is not None else "",
            "producer": "",            # по ТЗ — пусто
            "barcode": str(barcode) if barcode is not None else "",
            "vat": 20.0,               # по ТЗ — константа
        })
    return result

# ---------------------------
# 4) Преобразование стока
# Маппинг по ТЗ:
#   Id         → code
#   MaxPrice   → price, price_reserve
#   TotalStock → qty
#   branch     → отдельный параметр (по enterprise_code)
# ---------------------------
def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default

def _to_int_nonneg(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        n = int(float(v))
        return max(n, 0)
    except (TypeError, ValueError):
        return default

def transform_stock(data: List[Dict[str, Any]], branch: str) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in data:
        code = _extract_code(item)
        if not code:
            logging.warning("Пропуск записи без code (нет Id и нет PropertiesList['Код товару']): %s", str(item)[:200])
            continue

        price = _to_float(item.get("MaxPrice"), 0.0)
        qty = _to_int_nonneg(item.get("TotalStock"), 0)

        result.append({
            "branch": str(branch),
            "code": str(code),
            "price": price,
            "qty": qty,
            "price_reserve": price,  # по ТЗ = price
        })
    return result

# ---------------------------
# 5) Сохранение в файл (как в ТЗ)
# ---------------------------
def save_to_json(data, enterprise_code: Union[str, int], file_type: str) -> str:
    dir_path = os.path.join("temp", str(enterprise_code))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{file_type}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    logging.info(f"✅ Данные сохранены: {file_path}")
    return file_path

# ---------------------------
# 6) Отправка данных
# ---------------------------
async def send_catalog_data(file_path: str, enterprise_code: str):
    await process_database_service(file_path, "catalog", enterprise_code)

async def send_stock_data(file_path: str, enterprise_code: str):
    await process_database_service(file_path, "stock", enterprise_code)

# ---------------------------
# 7) Точка входа (доработано: добавлен file_path)
# ---------------------------
async def run_service(enterprise_code: str, file_type: str, file_path: Union[str, Path]):
    # Загрузка «сырого» файла локально
    raw_data = load_local_json(file_path)
    if not raw_data:
        logging.warning("Входные данные пустые. Нечего обрабатывать.")
        return

    if file_type == "catalog":
        data = transform_catalog(raw_data)
        path = save_to_json(data, enterprise_code, "catalog")
        await send_catalog_data(path, enterprise_code)

    elif file_type == "stock":
        branch = await fetch_branch_by_enterprise_code(enterprise_code)
        data = transform_stock(raw_data, branch)
        path = save_to_json(data, enterprise_code, "stock")
        await send_stock_data(path, enterprise_code)

    else:
        raise ValueError("Тип файла должен быть 'catalog' или 'stock'")

# ---------------------------
# CLI для удобного запуска локально
# ---------------------------
def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Локальная конвертация фида (без FTP)")
    p.add_argument("--enterprise", "-e", required=True, help="enterprise_code (строка)")
    p.add_argument("--file-type", "-t", required=True, choices=["catalog", "stock"], help="Тип входного файла")
    p.add_argument("--path", "-p", required=True, help="Путь к локальному JSON-файлу")
    return p

def main_cli():
    parser = _build_arg_parser()
    args = parser.parse_args()
    asyncio.run(run_service(args.enterprise, args.file_type, args.path))

if __name__ == "__main__":
    main_cli()

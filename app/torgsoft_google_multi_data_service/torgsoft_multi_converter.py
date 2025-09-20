# app/torgsoft_google/torgsoft_converter.py

import asyncio
import json
import logging
import math
import os
import tempfile
from typing import Any, Dict, List, Optional

import pandas as pd

from sqlalchemy.future import select
from app.database import get_async_db
from app.database import get_async_db, MappingBranch

async def fetch_branch(enterprise_code: str, store_id: str) -> str:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(
                MappingBranch.enterprise_code == enterprise_code,
                MappingBranch.store_id == store_id
            )
        )
        branch = result.scalars().first()
        if not branch:
            raise ValueError(f"❌ Branch не найден для enterprise_code={enterprise_code}, store_id={store_id}")
        return str(branch)


# 👉 Замени на актуальный путь в твоём проекте, если отличается
try:
    from app.services.database_service import process_database_service  # type: ignore
except Exception:
    try:
        from app.services.stock_scheduler_service import process_database_service  # type: ignore
    except Exception:
        process_database_service = None  # будет лог и сохранение JSON-файла без отправки

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =========================
# ВНУТРЕННИЕ УТИЛИТЫ
# =========================

UA_TO_STD_MAP = {
    # ОДИНСТВЕННЫЙ источник кода:
    "код фото": "code",

    # Название товара
    "назва товару": "name",
    "найменування товару": "name",

    # Штрихкод
    "штрих-код": "barcode",
    "штрихкод": "barcode",

    # Цены (для стока)
    "ціна роздрібна": "price",
    "цена розничная": "price",
    "ціна зі знижкою": "price_reserve",
    "цена со скидкой": "price_reserve",

    # Количество (для стока)
    "кількість": "qty",
    "количество": "qty",

    # Больше НЕ используем "№" как код — намеренно исключено
    # "№": "code",
    # "номер": "code",
    # "№ з/п": "code",
}

DEFAULT_PRODUCER = "N/A"
DEFAULT_VAT = 20.0


def _get_temp_dir() -> str:
    tmp = os.getenv("TEMP_DIR", tempfile.gettempdir())
    os.makedirs(tmp, exist_ok=True)
    return tmp


def _norm(s: Any) -> str:
    return str(s).strip().lower() if s is not None else ""


def _coerce_str(v: Any) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    s = str(v).strip()
    # Сохраним ведущие нули для штрихкодов
    return s


def _coerce_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return default
        s = str(v).replace(",", ".").strip()
        return float(s) if s else default
    except Exception:
        return default


def _coerce_int_nonneg(v: Any, default: int = 0) -> int:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return default
        if isinstance(v, str) and not v.strip():
            return default
        val = int(float(str(v).replace(",", ".").strip()))
        return max(val, 0)
    except Exception:
        return default


def _read_table(file_path: str) -> pd.DataFrame:
    """
    Читает Excel/CSV и АВТОМАТИЧЕСКИ определяет строку заголовков.
    Работает, если заголовки находятся на 1–10 строке (включая 3-ю строку).
    """
    def _normalize_cols(cols):
        out = []
        for c in cols:
            s = _norm(c)
            out.append(" ".join(s.split()))
        return out

    def _detect_header_row(df_probe: pd.DataFrame) -> int:
        # ключевые фразы, по которым узнаём заголовок
        header_hints = [
            "код фото",           # 👈 добавили
            "назва товару", "найменування товару",
            "штрих-код", "штрихкод",
            "кількість", "количество",
            "ціна роздрібна", "цена розничная",
            "ціна зі знижкою", "цена со скидкой",
            # (можно оставить "№" в хинтах как вторичный признак строки заголовка,
            # это не повлияет на выбор колонки для code)
            "№", "номер", "№ з/п",
        ]
        max_rows_to_check = min(len(df_probe), 10)
        for i in range(max_rows_to_check):
            row_vals = _normalize_cols(list(df_probe.iloc[i].values))
            joined = " ".join(row_vals)
            # эвристика: в строке должны встречаться хотя бы 2 подсказки
            hits = sum(1 for h in header_hints if h in joined)
            if hits >= 2:
                return i
        # если не нашли — предполагаем первую строку
        return 0

    # --- читаем "черновик" без заголовков, чтобы определить строку шапки
    if file_path.lower().endswith((".xlsx", ".xls")):
        probe = pd.read_excel(file_path, header=None, nrows=10, dtype=object)
        header_row = _detect_header_row(probe)
        df = pd.read_excel(file_path, header=header_row, dtype=object)
    elif file_path.lower().endswith(".csv"):
        try:
            probe = pd.read_csv(file_path, header=None, nrows=10, dtype=object)
        except UnicodeDecodeError:
            probe = pd.read_csv(file_path, header=None, nrows=10, dtype=object, encoding="cp1251")
        header_row = _detect_header_row(probe)
        try:
            df = pd.read_csv(file_path, header=header_row, dtype=object)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, header=header_row, dtype=object, encoding="cp1251")
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {file_path}")

    # нормализуем имена колонок
    df.columns = [" ".join(_norm(c).split()) for c in df.columns]

    # лог для дебага — увидим, какие колонки реально получились
    logging.info("Загружены колонки: %s", list(df.columns))
    return df



def _map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Приводим столбцы к стандартным именам согласно UA_TO_STD_MAP.
    Неизвестные столбцы остаются как есть.
    """
    rename_map: Dict[str, str] = {}
    for col in df.columns:
        std = UA_TO_STD_MAP.get(col, None)
        if std:
            rename_map[col] = std
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _save_json_temp(data: List[Dict[str, Any]], prefix: str) -> str:
    out_path = os.path.join(_get_temp_dir(), f"{prefix}_torgsoft_{next(tempfile._get_candidate_names())}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return out_path


async def _send_downstream(data: List[Dict[str, Any]], data_type: str, enterprise_code: str) -> None:
    """
    Отправка дальше по проекту:
    - Если доступен process_database_service -> вызываем его.
    - Иначе сохраняем JSON во временный файл и пишем предупреждение в лог.
    """
    if process_database_service is None:
        path = _save_json_temp(data, prefix=data_type)
        logging.warning(
            "process_database_service не найден. Данные сохранены во временный JSON: %s", path
        )
        return

    # Сервис ожидает путь к JSON-файлу
    path = _save_json_temp(data, prefix=data_type)
    logging.info("Отправка %s данных в process_database_service: %s", data_type, path)
    try:
        if asyncio.iscoroutinefunction(process_database_service):
            await process_database_service(path, data_type, enterprise_code)
        else:
            # На случай синхронной реализации
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, process_database_service, path, data_type, enterprise_code)
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


# =========================
# ПУБЛИЧНЫЕ ФУНКЦИИ КОНВЕРТАЦИИ
# =========================

async def process_torgsoft_catalog(
    enterprise_code: str,
    file_path: str,
    file_type: str = "catalog",
) -> None:
    """
    Конвертация каталога из файла формата «Стан склад».
    Если встречаются дубли по code (Код фото) — в итоговом файле
    остаётся только последняя запись для каждого кода.
    """
    df = _read_table(file_path)
    df = _map_columns(df)

    # Проверка обязательных колонок
    missing = [c for c in ("code", "name") if c not in df.columns]
    if missing:
        raise ValueError(f"В каталоге отсутствуют обязательные колонки: {', '.join(missing)}")

    items_by_code: Dict[str, Dict[str, Any]] = {}

    for _, row in df.iterrows():
        item_code = _coerce_str(row.get("code"))
        item_name = _coerce_str(row.get("name"))

        if not item_code or not item_name:
            continue  # пропускаем пустые строки

        # Каждый новый дубль перезаписывает предыдущий
        items_by_code[item_code] = {
            "code": item_code,
            "name": item_name,
            "producer": DEFAULT_PRODUCER,
            "vat": DEFAULT_VAT,
            "barcode": _coerce_str(row.get("barcode")),
        }

    # Преобразуем словарь обратно в список
    converted: List[Dict[str, Any]] = list(items_by_code.values())

    logging.info(
        "Каталог сконвертирован: %s позиций (уникальные коды, дубли удалены)",
        len(converted),
    )
    await _send_downstream(converted, data_type="catalog", enterprise_code=enterprise_code)




from sqlalchemy.future import select
from app.database import get_async_db, MappingBranch

def _norm_store_id(s: str) -> str:
    s = (s or "").strip().lower()
    # унифицируем дефисы и пробелы
    s = s.replace("—", "-").replace("–", "-")
    s = " ".join(s.split())           # сжать кратные пробелы
    s = s.replace(" - ", "-").replace(" -", "-").replace("- ", "-")
    return s

async def _build_branch_map(enterprise_code: str) -> dict[str, str]:
    """Читаем все сопоставления склад->branch для данного enterprise_code и кэшируем."""
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.store_id, MappingBranch.branch)
            .where(MappingBranch.enterprise_code == enterprise_code)
        )
        mapping = {}
        for store_id, branch in result.all():
            key = _norm_store_id(str(store_id))
            if key:
                mapping[key] = str(branch)
        return mapping

async def process_torgsoft_stock(
    enterprise_code: str,
    file_path: str,
    file_type: str = "stock",
) -> None:
    """
    Сток из «Стан.xlsx/18.xlsx» с несколькими складами.
    branch подбирается по enterprise_code + store_id (колонка «Склад»).
    """
    df = _read_table(file_path)
    df = _map_columns(df)

    # Обязательные поля
    required = ["code", "qty", "склад"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f"В стоке отсутствуют обязательные колонки: {', '.join(miss)}")

    # Кэш соответствий склад->branch
    store_to_branch = await _build_branch_map(enterprise_code)

    converted = []
    skipped_rows = 0

    for _, row in df.iterrows():
        item_code = _coerce_str(row.get("code"))
        store_id_raw = _coerce_str(row.get("склад"))

        # 1) Пропуск пустых/служебных строк и «вторых шапок»
        if not item_code or not store_id_raw:
            skipped_rows += 1
            continue
        if item_code.lower() == "код фото" or store_id_raw.lower() == "склад":
            skipped_rows += 1
            continue

        qty = _coerce_int_nonneg(row.get("qty"))
        price = _coerce_float(row.get("price"))
        price_reserve = _coerce_float(row.get("price_reserve"))

        key = _norm_store_id(store_id_raw)
        branch = store_to_branch.get(key)

        if not branch:
            # Доп. попытка: иногда в файле могут быть «30421» — прямые коды склада
            branch = store_to_branch.get(_norm_store_id(str(int(float(store_id_raw)))) if store_id_raw.replace(".", "", 1).isdigit() else "")
        if not branch:
            raise ValueError(f"❌ Branch не найден для enterprise_code={enterprise_code}, store_id={store_id_raw}")

        converted.append({
            "branch": branch,
            "code": item_code,
            "price": price,
            "price_reserve": price_reserve,
            "qty": qty,
        })

    logging.info(
        "Сток сконвертирован: %s позиций (enterprise=%s). Пропущено строк: %s",
        len(converted), enterprise_code, skipped_rows
    )
    await _send_downstream(converted, data_type="stock", enterprise_code=enterprise_code)

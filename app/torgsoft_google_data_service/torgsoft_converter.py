# app/torgsoft_google/torgsoft_converter.py

import asyncio
import json
import logging
import math
import os
import tempfile
from typing import Any, Dict, List, Optional

import pandas as pd

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
    Маппинг полей:
      code = «№»
      name = «Назва товару»
      barcode = «Штрих-код»
      producer = 'N/A'
      vat = 20
    (Цены/количество в каталоге можно игнорировать — это часть стока)
    """
    df = _read_table(file_path)
    df = _map_columns(df)

    # Проверим обязательные для каталога поля
    missing = []
    for req in ["code", "name"]:
        if req not in df.columns:
            missing.append(req)
    if missing:
        raise ValueError(f"В каталоге отсутствуют обязательные колонки: {', '.join(missing)}")

    converted: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        item_code = _coerce_str(row.get("code"))
        item_name = _coerce_str(row.get("name"))

        if not item_code or not item_name:
            # пропустим пустые строки
            continue

        converted.append(
            {
                "code": item_code,
                "name": item_name,
                "producer": DEFAULT_PRODUCER,
                "vat": DEFAULT_VAT,
                "barcode": _coerce_str(row.get("barcode")),
            }
        )

    logging.info("Каталог сконвертирован: %s позиций", len(converted))
    await _send_downstream(converted, data_type="catalog", enterprise_code=enterprise_code)


async def process_torgsoft_stock(
    enterprise_code: str,
    file_path: str,
    file_type: str = "stock",
    *,
    branch: str,
    single_store: Optional[bool] = None,
    store_serial: Optional[str] = None,
) -> None:
    """
    Конвертация стока из файла формата «Стан склад».
    Маппинг полів:
      code = «№»
      barcode = «Штрих-код»
      price = «Ціна роздрібна»
      price_reserve = «Ціна зі знижкою»
      qty = «Кількість» (неотрицательная)
      branch = приходит из MappingBranch по коду предприятия
    Примечания:
      - Если price_reserve пустой -> используем price и наоборот? НЕТ. Мы передаём оба поля как есть.
      - qty < 0 приводим к 0.
    """
    df = _read_table(file_path)
    df = _map_columns(df)

    missing = []
    for req in ["code", "qty"]:
        if req not in df.columns:
            missing.append(req)
    if missing:
        raise ValueError(f"В стоке отсутствуют обязательные колонки: {', '.join(missing)}")

    converted: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        item_code = _coerce_str(row.get("code"))
        if not item_code:
            continue

        qty = _coerce_int_nonneg(row.get("qty"))
        price = _coerce_float(row.get("price"))
        price_reserve = _coerce_float(row.get("price_reserve"))

        converted.append(
            {
                "branch": str(branch),
                "code": item_code,
                # "barcode": _coerce_str(row.get("barcode")),
                "price": price,
                "price_reserve": price_reserve,
                "qty": qty,
            }
        )

    logging.info(
        "Сток сконвертирован: %s позиций (enterprise=%s, branch=%s)",
        len(converted),
        enterprise_code,
        branch,
    )
    await _send_downstream(converted, data_type="stock", enterprise_code=enterprise_code)

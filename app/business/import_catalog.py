import logging
import os
import io
from typing import List, Dict, Any
import json  # ← добавь наверху файла
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import get_async_db, EnterpriseSettings, MappingBranch, CatalogMapping
from app.services.notification_service import send_notification

from sqlalchemy import update, text  # ← было только update
# Реестр парсеров: code -> async функция parse(code=...) -> JSON-строка
# Для D1 используем ваш парсер, который берёт URL из БД по code
try:
    from app.business.feed_biotus import parse_feed_to_json as parse_feed_D1
except Exception:
    parse_feed_D1 = None  # если модуля нет — пропустим D1

PARSER_REGISTRY: Dict[str, Any] = {
    "D1": parse_feed_D1,
    # Добавите сюда остальные, например:
    # "D2": parse_feed_D2,
    # "D3": parse_feed_D3,
}


load_dotenv()
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Константы
# ──────────────────────────────────────────────────────────────────────────────
# Базовые поля, которые мы импортируем/обновляем
BASE_FIELDS = ["ID", "Name", "Producer", "Guid", "Barcode", "Code_Tabletki"]
# Размер чанка для массовой вставки
UPSERT_CHUNK_SIZE = 5000

# Допустимые колонки для записи данных поставщика
NAME_D_COLUMNS = [f"Name_D{i}" for i in range(1, 11)]
CODE_D_COLUMNS = [f"Code_D{i}" for i in range(1, 11)]


async def _get_active_supplier_codes() -> List[str]:
    """
    Возвращает список code из dropship_enterprises, где is_active = TRUE.
    """
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT code FROM dropship_enterprises WHERE is_active = TRUE")
        )
        codes = res.scalars().all()
    # Чистим пустые/дубли, сохраняем порядок
    seen = set()
    result = []
    for c in codes:
        if not c:
            continue
        c = str(c).strip()
        if c and c not in seen:
            seen.add(c)
            result.append(c)
    return result

async def write_supplier_data_by_barcode(
    payload: Dict[str, Any],
    name_column: str,
    code_column: str,
) -> Dict[str, Any]:
    """
    Обновляет строку catalog_mapping по штрихкоду: записывает name -> name_column и id -> code_column.
    Пример payload:
        {
          "id": "NWY-06690",
          "name": "Глід, Hawthorn, Nature's Way, ...",
          "barcode": "033674066904"
        }

    Требования:
      - name_column ∈ {Name_D1..Name_D10}
      - code_column ∈ {Code_D1..Code_D10}
      - В таблице есть строка с таким Barcode.

    Возврат:
      {"updated": <int>, "barcode": <str>, "name_column": <str>, "code_column": <str>}
    """
    # Валидация колонок (защитимся от опечаток/SQL-инъекций в именах полей)
    if name_column not in NAME_D_COLUMNS:
        raise ValueError(f"Недопустимая колонка для имени: {name_column}. Разрешены: {', '.join(NAME_D_COLUMNS)}")
    if code_column not in CODE_D_COLUMNS:
        raise ValueError(f"Недопустимая колонка для кода: {code_column}. Разрешены: {', '.join(CODE_D_COLUMNS)}")

    # Достаём значения из входных данных
    ext_id = (payload.get("id") or "").strip()
    ext_name = (payload.get("name") or "").strip()
    barcode = (payload.get("barcode") or "").strip()

    if not barcode:
        raise ValueError("Пустой 'barcode' во входных данных")
    if not ext_id and not ext_name:
        # Разрешаем обновлять только одно поле, но хотя бы одно должно быть
        raise ValueError("Во входных данных нет значений 'id' и 'name' для записи")

    # Сборка динамического UPDATE
    values_to_set = {}
    if ext_name:
        values_to_set[name_column] = ext_name
    if ext_id:
        values_to_set[code_column] = ext_id

    # Если нечего обновлять — выходим
    if not values_to_set:
        return {"updated": 0, "barcode": barcode, "name_column": name_column, "code_column": code_column}

    # Построим выражение через модельные атрибуты, чтобы избежать ручного квотирования
    name_attr = getattr(CatalogMapping, name_column)
    code_attr = getattr(CatalogMapping, code_column)

    stmt = (
        update(CatalogMapping)
        .where(CatalogMapping.Barcode == barcode)
        .values({name_attr.key: values_to_set.get(name_column, None),
                 code_attr.key: values_to_set.get(code_column, None)})
        .execution_options(synchronize_session=False)
    )

    async with get_async_db() as session:
        result = await session.execute(stmt)
        await session.commit()

    updated = result.rowcount or 0
    # logger.info(
        # "Поставщик по barcode %s: обновлено %d строк (%s, %s)",
        # barcode, updated, name_column, code_column
    # )

    if updated == 0:
        pass
        # Можно отправить нотификацию, если критично
        # send_notification(f"catalog_mapping: не найден штрихкод {barcode} для обновления", "Разработчик")

    return {"updated": updated, "barcode": barcode, "name_column": name_column, "code_column": code_column}



# ──────────────────────────────────────────────────────────────────────────────
# Google Drive
# ──────────────────────────────────────────────────────────────────────────────
async def _connect_to_google_drive():
    """
    Создает клиент Drive API через сервисный аккаунт.
    Использует GOOGLE_DRIVE_CREDENTIALS_PATH.
    """
    creds_path = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH")
    if not creds_path or not os.path.exists(creds_path):
        msg = f"Неверный путь к учетным данным Google Drive: {creds_path}"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        raise FileNotFoundError(msg)

    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)
    logger.info("Подключено к Google Drive")
    return service


async def _fetch_single_file_metadata(drive_service, folder_id: str) -> Dict[str, Any]:
    """
    Возвращает metadata ОДНОГО файла (id, name) из папки.
    По условию задачи в папке всегда один файл.
    """
    try:
        results = (
            drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id, name)",
                pageSize=10,
            )
            .execute()
        )
        files = results.get("files", []) or []
        if not files:
            raise FileNotFoundError("В папке нет файлов")
        # Так как файл один — берем первый
        return files[0]
    except HttpError as e:
        msg = f"HTTP ошибка при получении файлов из папки {folder_id}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        raise


async def _download_file_bytes(drive_service, file_id: str) -> bytes:
    """
    Загружает файл с Google Drive в bytes.
    """
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()
    except HttpError as e:
        msg = f"HTTP ошибка при загрузке файла {file_id}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# Парсинг .xlsx → унифицированные dict
# ──────────────────────────────────────────────────────────────────────────────
def parse_catalog_xlsx(file_bytes: bytes, filename: str) -> List[Dict[str, Any]]:
    """
    Ожидаемые колонки листа Excel (без нормализации названий):
      - 'Товар.Код'
      - 'Товар.Наименование (укр.)'
      - 'Товар.Производитель.Наименование'
      - 'ГУИД'
      - 'Код ШК'
    Выходные ключи для БД:
      ID, Name, Producer, Guid, Barcode, Code_Tabletki
    """
    df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        id_val = (r.get("Товар.Код") or "").strip()
        if not id_val:
            # пропускаем строку без ключа
            continue

        row = {
            "ID": id_val,
            "Name": (r.get("Товар.Наименование (укр.)") or "").strip(),
            "Producer": (r.get("Товар.Производитель.Наименование") or "").strip(),
            "Guid": (r.get("ГУИД") or "").strip(),
            "Barcode": (r.get("Код ШК") or "").strip(),
            "Code_Tabletki": id_val,  # дублируем
        }
        rows.append(row)

    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Upsert в БД: CatalogMapping (только базовые поля + чанки)
# ──────────────────────────────────────────────────────────────────────────────
def _only_base_fields(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Оставляет в каждой записи только базовые колонки.
    Пустые значения приводим к пустой строке, чтобы избежать None.
    """
    cleaned: List[Dict[str, Any]] = []
    for r in rows:
        rec = {k: (r.get(k, "") or "") for k in BASE_FIELDS}
        cleaned.append(rec)
    return cleaned


async def upsert_catalog_mapping(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Пакетный upsert в CatalogMapping по ключу 'ID', с разбиением на чанки.
    Обновляются ТОЛЬКО базовые поля (D-поля не трогаем и не вставляем).
    ⚠️ Требование к БД: на колонках Name_D1…Name_D10 и Code_D1…Code_D10 должен быть DEFAULT '' (или они nullable).
    """
    if not rows:
        return {"affected": 0}

    rows = _only_base_fields(rows)
    table = CatalogMapping.__table__
    affected = 0

    for i in range(0, len(rows), UPSERT_CHUNK_SIZE):
        chunk = rows[i : i + UPSERT_CHUNK_SIZE]
        insert_stmt = pg_insert(table).values(chunk)

        update_cols = {
            "Name": insert_stmt.excluded.Name,
            "Producer": insert_stmt.excluded.Producer,
            "Guid": insert_stmt.excluded.Guid,
            "Barcode": insert_stmt.excluded.Barcode,
            "Code_Tabletki": insert_stmt.excluded.Code_Tabletki,
        }

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[table.c.ID],
            set_=update_cols,
        )

        async with get_async_db() as session:
            await session.execute(upsert_stmt)
            await session.commit()

        affected += len(chunk)

    return {"affected": affected}


# ──────────────────────────────────────────────────────────────────────────────
# Оркестрация
# ──────────────────────────────────────────────────────────────────────────────
async def run_service(enterprise_code: str, file_type: str) -> Dict[str, Any]:
    """
    Оркестрация:
      1) Импорт одного .xlsx из Google Drive → парсинг → upsert базовых полей в CatalogMapping
      2) Для всех активных поставщиков (dropship_enterprises.is_active=TRUE):
           - находим парсер в PARSER_REGISTRY по code
           - вызываем его (await parser(code=code)) → JSON
           - нормализуем, затем пишем name -> Name_{code}, id -> Code_{code} по barcode
    """
    if (file_type or "").strip().lower() != "catalog":
        logger.info("file_type != 'catalog' → сервис завершён без действий")
        return {
            "file_name": None,
            "rows": 0,
            "db": {"affected": 0},
            "feeds": {},  # ← несколько поставщиков
        }

    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        msg = "Не задан GOOGLE_DRIVE_FOLDER_ID в .env"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        return {
            "file_name": None,
            "rows": 0,
            "db": {"affected": 0},
            "feeds": {},
        }

    def _to_items(obj: Any) -> List[Dict[str, Any]]:
        """
        Нормализует результат парсера к списку словарей.
        Поддерживает:
          - list[dict]
          - dict с ключом items/products/data/result (список) или одиночный объект → [dict]
          - str/bytes с JSON → json.loads → рекурсивно
          - любой итерируемый (кроме str/bytes) → list(...)
        Иначе → [].
        """
        if obj is None:
            return []
        if isinstance(obj, (str, bytes, bytearray)):
            try:
                decoded = obj.decode("utf-8") if isinstance(obj, (bytes, bytearray)) else obj
                if decoded and decoded[0] == "\ufeff":
                    decoded = decoded.lstrip("\ufeff")
                parsed = json.loads(decoded)
                return _to_items(parsed)
            except Exception as e:
                logger.error("Не удалось распарсить строковый JSON из парсера: %s", e)
                return []
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            for k in ("items", "products", "data", "result"):
                v = obj.get(k)
                if isinstance(v, list):
                    return v
            return [obj]
        if hasattr(obj, "__iter__"):
            try:
                return list(obj)
            except Exception:
                return []
        return []

    try:
        # 1) Импорт из Google Drive → upsert базовых полей
        drive_service = await _connect_to_google_drive()
        meta = await _fetch_single_file_metadata(drive_service, folder_id)
        file_id, file_name = meta["id"], meta.get("name", "catalog.xlsx")
        logger.info("Выбран файл: %s (%s)", file_name, file_id)

        file_bytes = await _download_file_bytes(drive_service, file_id)
        rows = parse_catalog_xlsx(file_bytes, file_name)
        logger.info("Распарсено строк: %d", len(rows))

        db_stats = await upsert_catalog_mapping(rows)
        logger.info("Upsert в БД выполнен. Затронуто записей (вставлено/обновлено): %d", db_stats["affected"])

        # 2) Мульти-поставщики: читаем активные коды и запускаем соответствующие парсеры
        feeds_agg: Dict[str, Dict[str, int]] = {}
        active_codes = await _get_active_supplier_codes()
        if not active_codes:
            logger.info("Активные поставщики не найдены — этап обновления по фидам пропущен")
        else:
            logger.info("Активные коды поставщиков: %s", ", ".join(active_codes))

        for code in active_codes:
            parser = PARSER_REGISTRY.get(code)
            if not callable(parser):
                logger.warning("Нет парсера для code=%s — пропускаем", code)
                feeds_agg[code] = {"items": 0, "updated": 0, "errors": 0}
                continue

            name_col = f"Name_{code}"
            code_col = f"Code_{code}"

            # Парсинг фида (URL берётся внутри парсера по code)
            try:
                raw = await parser(code=code)
            except Exception as e:
                logger.exception("Ошибка выполнения парсера для code=%s: %s", code, e)
                send_notification(f"Ошибка парсера для {code}: {e}", "Разработчик")
                feeds_agg[code] = {"items": 0, "updated": 0, "errors": 1}
                continue

            items = _to_items(raw)
            updated = 0
            errors = 0

            for payload in items:
                try:
                    if not isinstance(payload, dict):
                        errors += 1
                        continue
                    norm = {
                        "id": str(payload.get("id", "") or "").strip(),
                        "name": str(payload.get("name", "") or "").strip(),
                        "barcode": str(payload.get("barcode", "") or "").strip(),
                    }
                    if not norm["barcode"]:
                        errors += 1
                        continue

                    res = await write_supplier_data_by_barcode(
                        norm, name_column=name_col, code_column=code_col
                    )
                    updated += int(res.get("updated", 0) or 0)
                except Exception as item_err:
                    errors += 1
                    logger.exception("Ошибка обновления позиции (code=%s): %s", code, item_err)

            feeds_agg[code] = {"items": len(items), "updated": updated, "errors": errors}
            logger.info("Feed %s обработан: items=%d, updated=%d, errors=%d",
                        code, len(items), updated, errors)

        return {
            "file_name": file_name,
            "rows": len(rows),
            "db": db_stats,
            "feeds": feeds_agg,  # сводка по всем поставщикам
        }

    except Exception as e:
        msg = f"[{enterprise_code}] Ошибка импорта каталога: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return {
            "file_name": None,
            "rows": 0,
            "db": {"affected": 0},
            "feeds": {},
        }



if __name__ == "__main__":
    import asyncio
    import argparse
    import json
    from dotenv import load_dotenv

    # Базовая настройка логов
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )

    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Ручной запуск импорта каталога из Google Drive и upsert в CatalogMapping"
    )
    parser.add_argument("--enterprise", required=True, help="enterprise_code (например, 342)")
    parser.add_argument("--type", default="catalog", help="file_type, по умолчанию 'catalog'")

    args = parser.parse_args()

    result = asyncio.run(run_service(args.enterprise, args.type))
    # Красивый вывод результата
    print(json.dumps(result, ensure_ascii=False, indent=2))

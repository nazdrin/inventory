import logging
import os
import io
from typing import List, Dict, Any, Optional
import json  # ← добавь наверху файла
import httpx
import xml.etree.ElementTree as ET
from sqlalchemy import and_, or_, case
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from pathlib import Path
from datetime import datetime
from sqlalchemy import select  # ← добавили
from sqlalchemy.ext.asyncio import AsyncSession  # type: ignore[import]

from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import get_async_db, EnterpriseSettings, MappingBranch, CatalogMapping
from app.services.notification_service import send_notification

from sqlalchemy import update, text  # ← было только update
# Реестр парсеров: code -> async функция parse(code=...) -> JSON-строка
# Для D1 используем ваш парсер, который берёт URL из БД по code
try:
    from app.business.feed_biotus import parse_feed_catalog_to_json as parse_feed_D1
    from app.business.feed_dsn import parse_dsn_catalog_to_json as parse_feed_D2
    from app.business.feed_proteinplus import parse_feed_catalog_to_json as parse_feed_D3
    from app.business.feed_dobavki import parse_d4_feed_to_json as parse_feed_D4
    from app.business.feed_monstr import parse_feed_to_json as parse_feed_D5
    from app.business.feed_sportatlet import parse_d6_feed_to_json as parse_feed_D6
    from app.business.feed_pediakid import parse_pediakid_feed_to_json as parse_feed_D7
    from app.business.feed_suziria import parse_suziria_feed_to_json as parse_feed_D8
    from app.business.feed_ortomedika import parse_feed_to_json as parse_feed_D9
    from app.business.feed_zoohub import parse_feed_to_json as parse_feed_D10
    from app.business.feed_toros import parse_feed_to_json as parse_feed_D11
except Exception:
    parse_feed_D1 = None  # если модуля нет — пропустим D1

PARSER_REGISTRY: Dict[str, Any] = {
    "D1": parse_feed_D1,
    # Добавите сюда остальные, например:
    "D2": parse_feed_D2,
    "D3": parse_feed_D3, 
    "D4": parse_feed_D4,
    "D5": parse_feed_D5,
    "D6": parse_feed_D6,
    "D7": parse_feed_D7,
    "D8": parse_feed_D8,
    "D9": parse_feed_D9,
    "D10": parse_feed_D10,
    "D11": parse_feed_D11,
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


# --- Варианты ШК для аккуратного матчинга с одним ведущим нулём ---
def _barcode_variants(code: Optional[str]) -> List[str]:
    """Возвращает варианты barcode для матчинга, учитывая ТОЛЬКО один ведущий '0'.

    Кейсы:
      - БД хранит '0' + barcode, поставщик прислал без '0'
      - БД хранит без '0', поставщик прислал с одним ведущим '0'

    Варианты генерируются ТОЛЬКО для цифровых строк, чтобы не ломать прочую логику.
    """
    if not code:
        return []
    s = str(code).strip()
    if not s:
        return []

    variants = [s]
    if s.isdigit():
        if s.startswith("0") and len(s) > 1:
            variants.append(s[1:])
        else:
            variants.append("0" + s)

    # уникализируем, сохраняя порядок
    seen = set()
    out: List[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


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
    session: "AsyncSession | None" = None,
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

    ⚠️ Если передан session, функция НЕ создаёт свою сессию и НЕ делает commit — это остаётся на вызывающем коде.
    """
    # Валидация колонок (защитимся от опечаток/SQL-инъекций в именах полей)
    if name_column not in NAME_D_COLUMNS:
        raise ValueError(
            f"Недопустимая колонка для имени: {name_column}. Разрешены: {', '.join(NAME_D_COLUMNS)}"
        )
    if code_column not in CODE_D_COLUMNS:
        raise ValueError(
            f"Недопустимая колонка для кода: {code_column}. Разрешены: {', '.join(CODE_D_COLUMNS)}"
        )

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
    values_to_set: Dict[str, Any] = {}
    if ext_name:
        values_to_set[name_column] = ext_name
    if ext_id:
        values_to_set[code_column] = ext_id

    # Если нечего обновлять — выходим
    if not values_to_set:
        return {
            "updated": 0,
            "barcode": barcode,
            "name_column": name_column,
            "code_column": code_column,
        }

    # Построим выражение через модельные атрибуты, чтобы избежать ручного квотирования
    name_attr = getattr(CatalogMapping, name_column)
    code_attr = getattr(CatalogMapping, code_column)

    # Матчим по barcode, учитывая вариант с одним ведущим нулём
    barcode_candidates = _barcode_variants(barcode)
    if not barcode_candidates:
        raise ValueError("Пустой 'barcode' после очистки")

    stmt = (
        update(CatalogMapping)
        .where(CatalogMapping.Barcode.in_(barcode_candidates))
        .values(
            {
                name_attr.key: values_to_set.get(name_column, None),
                code_attr.key: values_to_set.get(code_column, None),
            }
        )
        .execution_options(synchronize_session=False)
    )

    # Если сессию не передали — создаём и коммитим внутри (старое поведение, 1 товар = 1 транзакция)
    if session is None:
        async with get_async_db() as own_session:
            result = await own_session.execute(stmt)
            await own_session.commit()
            updated = result.rowcount or 0
    else:
        # Используем уже открытую сессию, коммит снаружи
        result = await session.execute(stmt)
        updated = result.rowcount or 0

    if updated == 0:
        # Можно добавить нотификацию при необходимости
        pass

    return {
        "updated": updated,
        "barcode": barcode,
        "name_column": name_column,
        "code_column": code_column,
    }



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
            "Code_Tabletki": ""
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

        excluded = insert_stmt.excluded

        # Предикаты «реального изменения»: новое значение не пустое и отличается от текущего
        name_changed = and_(excluded.Name != "", excluded.Name.is_distinct_from(table.c.Name))
        producer_changed = and_(excluded.Producer != "", excluded.Producer.is_distinct_from(table.c.Producer))
        barcode_changed = and_(excluded.Barcode != "", excluded.Barcode.is_distinct_from(table.c.Barcode))

        # Обновляем только непустыми значениями; пустые не затирают существующие
        update_cols = {
            "Name": case((name_changed, excluded.Name), else_=table.c.Name),
            "Producer": case((producer_changed, excluded.Producer), else_=table.c.Producer),
            "Barcode": case((barcode_changed, excluded.Barcode), else_=table.c.Barcode),
            # Guid / Code_Tabletki сейчас не обновляем (только при insert остаются как в values)
        }

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[table.c.ID],
            set_=update_cols,
            where=or_(name_changed, producer_changed, barcode_changed),
        )

        async with get_async_db() as session:
            await session.execute(upsert_stmt)
            await session.commit()

        affected += len(chunk)

    return {"affected": affected}
# ──────────────────────────────────────────────────────────────────────────────
# SalesDrive YML (каталог)
# ──────────────────────────────────────────────────────────────────────────────
SALESDRIVE_YML_URL_TEMPLATE = "https://petrenko.salesdrive.me/export/yml/export.yml?publicKey={public_key}"


def _strip_ns(tag: str) -> str:
    """Убирает namespace из XML тега."""
    if not tag:
        return tag
    return tag.split("}")[-1]


async def _get_salesdrive_public_key(enterprise_code: str) -> str:
    """Берёт publicKey из enterprise_settings.google_drive_folder_id_rest по enterprise_code."""
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT google_drive_folder_id_rest FROM enterprise_settings WHERE enterprise_code = :c LIMIT 1"),
            {"c": enterprise_code},
        )
        public_key = res.scalar_one_or_none()

    public_key = (public_key or "").strip()
    if not public_key:
        raise RuntimeError(
            f"Не найден publicKey (google_drive_folder_id_rest) для enterprise_code={enterprise_code}"
        )
    return public_key


async def _download_salesdrive_yml(public_key: str) -> bytes:
    url = SALESDRIVE_YML_URL_TEMPLATE.format(public_key=public_key)
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers={"accept": "application/xml"}, timeout=120)
        resp.raise_for_status()
        return resp.content


def parse_catalog_yml(file_bytes: bytes, filename: str = "export.yml") -> List[Dict[str, Any]]:
    """Парсит YML/XML и возвращает унифицированные dict для CatalogMapping.

    Маппинг:
      - ID        <- offer@id
      - Name      <- name_ua (если не пусто), иначе name
      - Producer  <- vendor
      - Barcode   <- barcode
      - Guid / Code_Tabletki: заглушки '' (не обновляем их при upsert)

    Важно: парсинг потоковый (iterparse) + elem.clear(), чтобы не росла память.
    """
    rows: List[Dict[str, Any]] = []

    # iterparse на BytesIO, освобождаем элементы по мере чтения
    context = ET.iterparse(io.BytesIO(file_bytes), events=("end",))
    for event, elem in context:
        if _strip_ns(elem.tag) != "offer":
            continue

        offer_id = (elem.attrib.get("id") or "").strip()
        if not offer_id:
            elem.clear()
            continue

        def _find_text(tag_name: str) -> str:
            for ch in list(elem):
                if _strip_ns(ch.tag) == tag_name:
                    return (ch.text or "").strip()
            return ""

        name_ua = _find_text("name_ua")
        name = name_ua if name_ua else _find_text("name")
        vendor = _find_text("vendor")
        barcode = _find_text("barcode")

        rows.append(
            {
                "ID": offer_id,
                "Name": name,
                "Producer": vendor,
                "Guid": "",
                "Barcode": barcode,
                "Code_Tabletki": "",
            }
        )

        # освобождаем память
        elem.clear()

    return rows


async def load_catalog_from_salesdrive_yml(enterprise_code: str) -> Dict[str, Any]:
    """Скачивает YML по publicKey из БД и парсит в rows."""
    public_key = await _get_salesdrive_public_key(enterprise_code)
    yml_bytes = await _download_salesdrive_yml(public_key)
    rows = parse_catalog_yml(yml_bytes, filename="export.yml")
    return {"file_name": "export.yml", "rows": rows}

async def export_catalog_mapping_to_json_and_process(
    enterprise_code: str,
    file_type: str = "catalog",
    export_dir_env: str = "CATALOG_EXPORT_DIR",
    default_dir: str = "exports"
) -> str:
    """
    Экспорт ВСЕЙ таблицы CatalogMapping в JSON и вызов process_database_service.
    Маппинг:
      code <- ID
      name <- Name
      producer <- Producer
      morion <- Guid
      tabletki <- Code_Tabletki
      barcode <- Barcode
      vat <- 20.0

    Файл **перезаписывается** каждый запуск (без версий).
    """
    # 1) Выборка из БД: берём только нужные колонки
    async with get_async_db() as session:
        result = await session.execute(
            select(
                CatalogMapping.ID,
                CatalogMapping.Name,
                CatalogMapping.Producer,
                CatalogMapping.Guid,
                CatalogMapping.Barcode,
                CatalogMapping.Code_Tabletki,
            )
        )
        rows = result.all()

    # 2) Преобразование к нужному формату
    payload = []
    for (ID, Name, Producer, Guid, Barcode, Code_Tabletki) in rows:
        payload.append({
            "code": (ID or "").strip(),
            "name": (Name or "").strip(),
            "vat": 20.0,
            "producer": (Producer or "").strip(),
            "tabletki": (Code_Tabletki or "").strip(),     # ← фикc: tabletki из Code_Tabletki
            "barcode": (Barcode or "").strip(),
        })

    # 3) Сохранение файла (перезапись, атомарно)
    export_root = Path(os.getenv(export_dir_env) or default_dir)
    export_root.mkdir(parents=True, exist_ok=True)

    # фиксированное имя — БЕЗ timestamp
    final_path = export_root / f"catalog_{enterprise_code}.json"
    tmp_path = export_root / f"catalog_{enterprise_code}.json.partial"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # атомарная замена (без "дырявых" файлов при чтении)
    os.replace(tmp_path, final_path)

    logger.info("Каталог выгружен в JSON: %s (записей: %d)", final_path, len(payload))

    # 4) Вызов process_database_service
    try:
        from app.business.process_database_service import process_database_service  # type: ignore
    except Exception:
        try:
            from app.services.database_service import process_database_service  # type: ignore
        except Exception as e:
            msg = f"Не удалось импортировать process_database_service: {e}"
            logger.exception(msg)
            send_notification(msg, "Разработчик")
            return str(final_path)

    try:
        await process_database_service(str(final_path), file_type, enterprise_code)
        logger.info("process_database_service: обработан файл %s", final_path)
    except Exception as e:
        msg = f"Ошибка при вызове process_database_service: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")

    return str(final_path)
# ──────────────────────────────────────────────────────────────────────────────
# Оркестрация
# ──────────────────────────────────────────────────────────────────────────────
async def run_service(enterprise_code: str, file_type: str) -> Dict[str, Any]:
    """
    Оркестрация:
      1) Импорт одного YML из SalesDrive → парсинг → upsert базовых полей в CatalogMapping
      2) Для всех активных поставщиков (dropship_enterprises.is_active=TRUE):
           - находим парсер в PARSER_REGISTRY по code
           - вызываем его (await parser(code=code)) → JSON/объект
           - нормализуем → записываем name -> Name_{code}, id -> Code_{code} по barcode
      3) Экспорт ВСЕЙ таблицы CatalogMapping в JSON нужного формата и вызов
         process_database_service(json_file_path, "catalog", enterprise_code)
    Возврат: сводка по шагам + путь к экспортированному JSON.
    """
    if (file_type or "").strip().lower() != "catalog":
        logger.info("file_type != 'catalog' → сервис завершён без действий")
        return {
            "file_name": None,
            "rows": 0,
            "db": {"affected": 0},
            "feeds": {},
            "export_file": None,
        }

    # Локальная утилита нормализации результатов парсера к списку dict
    def _to_items(obj: Any) -> List[Dict[str, Any]]:
        """
        Поддерживает:
          - list[dict]
          - dict с ключом items/products/data/result (list) или одиночный dict → [dict]
          - str/bytes → json.loads → рекурсивно
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
        # 1) Импорт из SalesDrive YML → парсинг → upsert базовых полей
        yml_result = await load_catalog_from_salesdrive_yml(enterprise_code)
        file_name = yml_result.get("file_name") or "export.yml"
        rows = yml_result.get("rows") or []
        logger.info("SalesDrive YML: распарсено строк: %d", len(rows))

        db_stats = await upsert_catalog_mapping(rows)
        logger.info("Upsert в БД выполнен. Затронуто записей (вставлено/обновлено): %d", db_stats.get("affected", 0))

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
            errors = 0

            # Готовим данные для bulk‑обновления по поставщику:
            bulk_rows = []
            for payload in items:
                try:
                    if not isinstance(payload, dict):
                        errors += 1
                        continue

                    ext_id = str(payload.get("id", "") or "").strip()
                    ext_name = str(payload.get("name", "") or "").strip()
                    barcode = str(payload.get("barcode", "") or "").strip()

                    if not barcode:
                        # Без штрихкода обновить нечего
                        errors += 1
                        continue

                    # Пустые строки превращаем в None, чтобы не перетирать существующие значения
                    name_val = ext_name or None
                    code_val = ext_id or None

                    if name_val is None and code_val is None:
                        # Нечего писать в D‑колонки
                        errors += 1
                        continue

                    bulk_rows.append(
                        {
                            "barcode": barcode,
                            "name": name_val,
                            "code": code_val,
                        }
                    )
                except Exception as norm_err:
                    errors += 1
                    logger.exception("Ошибка нормализации позиции (code=%s): %s", code, norm_err)

            # Если после фильтрации записей не осталось — просто фиксируем статистику
            if not bulk_rows:
                feeds_agg[code] = {"items": len(items), "updated": 0, "errors": errors}
                logger.info(
                    "Feed %s обработан (bulk): items=%d, updated=%d, errors=%d (нет валидных строк для обновления)",
                    code,
                    len(items),
                    0,
                    errors,
                )
                continue

            # Выполняем один bulk‑UPDATE по поставщику.
            # Используем json_array_elements для разворота массива JSON в CTE,
            # а имена колонок подставляем из заранее провалидированных name_col / code_col.
            updated = 0
            try:
                async with get_async_db() as session:
                    data_json = json.dumps(bulk_rows, ensure_ascii=False)

                    sql = f"""
                    WITH s AS (
                        SELECT
                            NULLIF(trim(elem->>'barcode'), '') AS barcode,
                            NULLIF(trim(elem->>'name'),    '') AS name,
                            NULLIF(trim(elem->>'code'),    '') AS code,
                            CASE
                                WHEN (elem->>'barcode') ~ '^[0-9]+$' THEN
                                    CASE
                                        WHEN left(elem->>'barcode', 1) = '0' AND length(elem->>'barcode') > 1
                                            THEN substr(elem->>'barcode', 2)
                                        ELSE '0' || (elem->>'barcode')
                                    END
                                ELSE NULL
                            END AS barcode_alt
                        FROM json_array_elements((:data)::json) AS elem
                    ),
                    m AS (
                        -- Выбираем строку CatalogMapping по точному совпадению ШК,
                        -- иначе пробуем вариант с одним ведущим нулём.
                        SELECT
                            s.barcode,
                            s.name,
                            s.code,
                            COALESCE(cm_exact."Barcode", cm_alt."Barcode") AS target_barcode
                        FROM s
                        LEFT JOIN {CatalogMapping.__tablename__} AS cm_exact
                            ON cm_exact."Barcode" = s.barcode
                        LEFT JOIN {CatalogMapping.__tablename__} AS cm_alt
                            ON cm_alt."Barcode" = s.barcode_alt
                           AND cm_exact."Barcode" IS NULL
                        WHERE s.barcode IS NOT NULL
                    )
                    UPDATE {CatalogMapping.__tablename__} AS cm
                    SET
                        "{name_col}" = COALESCE(m.name, cm."{name_col}"),
                        "{code_col}" = COALESCE(m.code, cm."{code_col}")
                    FROM m
                    WHERE cm."Barcode" = m.target_barcode
                    """
                    result = await session.execute(text(sql), {"data": data_json})
                    await session.commit()
                    updated = result.rowcount or 0
            except Exception as bulk_err:
                logger.exception("Bulk‑обновление для code=%s завершилось ошибкой: %s", code, bulk_err)
                # В случае ошибки считаем, что все подготовленные строки не обновились
                errors += len(bulk_rows)
                updated = 0

            feeds_agg[code] = {"items": len(items), "updated": updated, "errors": errors}
            logger.info(
                "Feed %s обработан (bulk): items=%d, updated=%d, errors=%d",
                code,
                len(items),
                updated,
                errors,
            )

        # 3) Экспорт готовой таблицы в JSON и запуск процессинга
        export_path = await export_catalog_mapping_to_json_and_process(
            enterprise_code=enterprise_code,
            file_type="catalog"
        )
        logger.info("Файл экспорта: %s", export_path)

        return {
            "file_name": file_name,
            "rows": len(rows),
            "db": db_stats,
            "feeds": feeds_agg,            # сводка по всем поставщикам
            "export_file": export_path,    # путь к созданному JSON
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
            "export_file": None,
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
        description="Ручной запуск импорта каталога из SalesDrive YML и upsert в CatalogMapping"
    )
    parser.add_argument("--enterprise", required=True, help="enterprise_code (например, 342)")
    parser.add_argument("--type", default="catalog", help="file_type, по умолчанию 'catalog'")

    args = parser.parse_args()

    result = asyncio.run(run_service(args.enterprise, args.type))
    # Красивый вывод результата
    print(json.dumps(result, ensure_ascii=False, indent=2))

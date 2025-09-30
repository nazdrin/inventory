import os
import json
import asyncio
import logging
from io import BytesIO
from datetime import datetime, timedelta
from ftplib import FTP, error_perm

from dotenv import load_dotenv
from sqlalchemy.future import select
from app.database import get_async_db
from app.models import MappingBranch
from app.services.database_service import process_database_service

# =========================
# Внутренние константы (по запросу — не в .env)
# =========================
ENTERPRISE_CODE = "2"           # код предприятия для маппинга ветки
FILE_TYPE = "both"              # 'catalog' | 'stock' | 'both'
DEFAULT_VAT = 20.0              # НДС для каталога
KEEP_LATEST = 3                 # сколько последних файлов оставлять во входящей папке
MAX_AGE_DAYS = 7                # удалять старше N дней
TEMP_DIR = "./temp"             # куда сохраняем временные JSON перед отправкой

# =========================
# Подключение к FTP — берём из .env только доступ и папки
# =========================
load_dotenv()

FTP_HOST = os.getenv("FTP_HOST", "127.0.0.1")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER", "zoomagazin")
FTP_PASS = os.getenv("FTP_PASS", "")

# ВАЖНО: FTP_DIR — корень клиента (если настроен local_root на upload, то здесь '/')
FTP_DIR = os.getenv("FTP_DIR", "/")
# Подпапки внутри корня (без начального '/')
FTP_ARCHIVE_DIR = os.getenv("FTP_ARCHIVE_DIR", "archive").lstrip("/")
FTP_FAILED_DIR = os.getenv("FTP_FAILED_DIR", "failed").lstrip("/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# =========================
# Утилиты для путей и FTP
# =========================
def _join_ftp(*parts: str) -> str:
    """Аккуратно склеивает части пути для FTP (нормализуем слэши)."""
    cleaned = [p.strip("/") for p in parts if p and p != "/"]
    return "/" + "/".join(cleaned) if cleaned else "/"

def _ensure_remote_dir(ftp: FTP, abs_path: str) -> None:
    """Рекурсивно создаёт абсолютную директорию на FTP, если её нет."""
    if not abs_path.startswith("/"):
        raise ValueError("Ожидался абсолютный путь")
    segs = [s for s in abs_path.strip("/").split("/") if s]
    cur = "/"
    for s in segs:
        cur = _join_ftp(cur, s)
        try:
            ftp.mkd(cur)
        except error_perm as e:
            if not str(e).startswith("550"):
                raise

def _list_json_files_with_mtime(ftp, path):
    ftp.encoding = 'latin1'  # 💡 Обход кодировки
    try:
        names = ftp.nlst(path)
    except UnicodeDecodeError as e:
        logging.warning(f"❗️ UnicodeDecodeError: {e}")
        names = ftp.nlst()  # Без пути — fallback
    except Exception as e:
        logging.error(f"Ошибка получения списка файлов: {e}")
        return []

    # Примитивная фильтрация .json
    json_files = []
    for name in names:
        if name.lower().endswith(".json"):
            try:
                mdtm = ftp.sendcmd(f"MDTM {name}")
                dt_str = mdtm.replace("213 ", "")
                mtime = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
                json_files.append((name, mtime))
            except Exception:
                continue

    return sorted(json_files, key=lambda x: x[1], reverse=True)


def _download_to_string(ftp, path, filename):
    from io import BytesIO
    buf = BytesIO()

    # Скачиваем бинарно
    ftp.retrbinary(f"RETR {filename}", buf.write)
    buf.seek(0)

    # Пробуем прочитать как Windows-1251 (чаще всего используется в таких случаях)
    try:
        return buf.read().decode("utf-8")
    except UnicodeDecodeError:
        return buf.read().decode("windows-1251")  # Или "latin1" — можно протестировать


def _move_into(ftp: FTP, src_dir_abs: str, filename: str, dst_dir_abs: str) -> str:
    """Переместить файл из src_dir в dst_dir, добавить timestamp к имени."""
    _ensure_remote_dir(ftp, dst_dir_abs)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    base, ext = os.path.splitext(filename)
    dst_name = f"{base}__{ts}{ext}"
    ftp.rename(_join_ftp(src_dir_abs, filename), _join_ftp(dst_dir_abs, dst_name))
    return _join_ftp(dst_dir_abs, dst_name)

def _cleanup_incoming(ftp: FTP, cwd_abs: str, keep_latest: int, max_age_days: int):
    now = datetime.now()
    files = _list_json_files_with_mtime(ftp, cwd_abs)
    if not files:
        return
    files.sort(key=lambda x: x[1], reverse=True)
    latest = set(name for name, _ in files[:max(0, keep_latest)])
    for name, mt in files:
        if name in latest:
            continue
        if (now - mt).days >= max_age_days:
            try:
                ftp.delete(_join_ftp(cwd_abs, name))
                logging.info(f"🧹 Удалён старый файл: {name}")
            except Exception as e:
                logging.warning(f"Не удалось удалить {name}: {e}")


# =========================
# Доступ к БД
# =========================
async def fetch_branch_by_enterprise_code(enterprise_code: str) -> str:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        branch = result.scalars().first()
        if not branch:
            raise ValueError(f"Branch не найден для enterprise_code={enterprise_code}")
        return str(branch)


# =========================
# Трансформации
# =========================
def _normalize_input(json_str: str) -> list[dict]:
    data = json.loads(json_str)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("Ожидался JSON-объект или массив объектов")

def transform_catalog(items: list[dict]) -> list[dict]:
    return [{
        "code": str(it.get("Id", "")),
        "name": str(it.get("Name", "") or ""),
        "producer": "",
        "barcode": str(it.get("Barcode", "") or ""),
        "vat": DEFAULT_VAT,
    } for it in items]

def transform_stock(items: list[dict], branch: str) -> list[dict]:
    out = []
    for it in items:
        price = max(float(it.get("MaxPrice", 0.0) or 0.0), 0.0)
        qty = max(float(it.get("TotalStock", 0.0) or 0.0), 0.0)
        out.append({
            "branch": branch,
            "code": str(it.get("Id", "")),
            "price": price,
            "qty": qty,
            "price_reserve": price
        })
    return out

def save_to_json(data, enterprise_code: str, file_type: str) -> str:
    out_dir = os.path.join(TEMP_DIR, str(enterprise_code))
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{file_type}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logging.info(f"✅ Сохранено: {path}")
    return path

async def send_catalog_data(path: str, enterprise_code: str):
    await process_database_service(path, "catalog", enterprise_code)

async def send_stock_data(path: str, enterprise_code: str):
    await process_database_service(path, "stock", enterprise_code)


# =========================
# Основной сценарий
# =========================
async def run_service(enterprise_code: str, file_type: str):
    # Абсолютные пути на FTP
    incoming_abs = FTP_DIR if FTP_DIR.startswith("/") else _join_ftp("/", FTP_DIR)
    archive_abs = _join_ftp(incoming_abs, FTP_ARCHIVE_DIR) if not FTP_ARCHIVE_DIR.startswith("/") else FTP_ARCHIVE_DIR
    failed_abs = _join_ftp(incoming_abs, FTP_FAILED_DIR) if not FTP_FAILED_DIR.startswith("/") else FTP_FAILED_DIR

    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.encoding = "utf-8"

    # Убеждаемся, что папки есть
    for d in (incoming_abs, archive_abs, failed_abs):
        _ensure_remote_dir(ftp, d)

    latest_name = None
    try:
        files = _list_json_files_with_mtime(ftp, incoming_abs)
        if not files:
            logging.info("Нет JSON-файлов во входящей папке.")
            return

        files.sort(key=lambda x: x[1], reverse=True)
        latest_name, latest_mtime = files[0]
        logging.info(f"Обработка файла: {latest_name} (mtime={latest_mtime})")

        raw = _download_to_string(ftp, incoming_abs, latest_name)
        items = _normalize_input(raw)

        ft = (file_type or "both").lower()
        if ft not in ("catalog", "stock", "both"):
            raise ValueError("file_type должен быть 'catalog', 'stock' или 'both'")

        if ft in ("catalog", "both"):
            catalog = transform_catalog(items)
            cat_path = save_to_json(catalog, enterprise_code, "catalog")
            await send_catalog_data(cat_path, enterprise_code)

        if ft in ("stock", "both"):
            branch = await fetch_branch_by_enterprise_code(enterprise_code)
            stock = transform_stock(items, branch)
            st_path = save_to_json(stock, enterprise_code, "stock")
            await send_stock_data(st_path, enterprise_code)

        moved = _move_into(ftp, incoming_abs, latest_name, archive_abs)
        logging.info(f"📦 Перемещён в архив: {moved}")

        _cleanup_incoming(ftp, incoming_abs, KEEP_LATEST, MAX_AGE_DAYS)

    except Exception as e:
        logging.exception(f"❌ Ошибка: {e}")
        try:
            if latest_name:
                failed = _move_into(ftp, incoming_abs, latest_name, failed_abs)
                logging.warning(f"Файл перемещён в failed: {failed}")
        except Exception as e2:
            logging.warning(f"Не удалось переместить в failed: {e2}")
    finally:
        try:
            ftp.quit()
        except Exception:
            pass


# Локальный запуск
if __name__ == "__main__":
    asyncio.run(run_service(ENTERPRISE_CODE, FILE_TYPE))
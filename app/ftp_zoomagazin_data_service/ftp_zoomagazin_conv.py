import os
import json
import asyncio
import logging
from io import BytesIO
from datetime import datetime, timedelta
from ftplib import FTP, error_perm

from dotenv import load_dotenv
from sqlalchemy.future import select

from app.database import get_async_db, EnterpriseSettings  # EnterpriseSettings не используется напрямую, но пусть будет для контекста
from app.models import MappingBranch
from app.services.database_service import process_database_service

# =========================
# Настройки и константы
# =========================
load_dotenv()

FTP_HOST = os.getenv("FTP_HOST", "127.0.0.1")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER", "zoomagazin")
FTP_PASS = os.getenv("FTP_PASS", "")

FTP_DIR = os.getenv("FTP_DIR", "/upload")
FTP_ARCHIVE_DIR = os.getenv("FTP_ARCHIVE_DIR", "/archive")
FTP_FAILED_DIR = os.getenv("FTP_FAILED_DIR", "/failed")

FTP_KEEP_LATEST = int(os.getenv("FTP_KEEP_LATEST", "3"))
FTP_MAX_AGE_DAYS = int(os.getenv("FTP_MAX_AGE_DAYS", "7"))

TEMP_FILE_PATH = os.getenv("TEMP_FILE_PATH", "./temp")
ENTERPRISE_CODE = os.getenv("ENTERPRISE_CODE", "2")  # Укажи реальный код предприятия
FILE_TYPE = os.getenv("FILE_TYPE", "both").lower()    # 'catalog' | 'stock' | 'both'

DEFAULT_VAT = float(os.getenv("DEFAULT_VAT", "20.0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =========================
# Вспомогательные функции
# =========================

def _ensure_remote_dir(ftp: FTP, path: str) -> None:
    """Рекурсивно создаёт директорию на FTP, если её нет."""
    if not path or path == "/":
        return
    # нормализуем и идём глубже
    parts = [p for p in path.strip("/").split("/") if p]
    cur = ""
    for p in parts:
        cur += f"/{p}"
        try:
            ftp.mkd(cur)
        except error_perm as e:
            # если уже существует — нормально
            if not str(e).startswith("550"):
                raise

def connect_ftp(cwd: str = FTP_DIR) -> FTP:
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    # важное — UTF-8 для имён файлов
    ftp.encoding = "utf-8"
    # застрахуемся, что директории существуют
    for d in (FTP_DIR, FTP_ARCHIVE_DIR, FTP_FAILED_DIR):
        _ensure_remote_dir(ftp, d)
    ftp.cwd(cwd)
    logging.info(f"FTP connected. CWD: {cwd}")
    return ftp

def _list_json_files_with_mtime(ftp: FTP):
    """Возвращает список (name, mtime: datetime) в текущей дире, только *.json."""
    files = []
    try:
        # Предпочтительно MLSD (возвращает modify=YYYYMMDDHHMMSS)
        for name, facts in ftp.mlsd():
            if facts.get("type") == "file" and name.lower().endswith(".json"):
                m = facts.get("modify")
                if m:
                    mt = datetime.strptime(m, "%Y%m%d%H%M%S")
                else:
                    # запасной путь — MDTM
                    try:
                        mt_raw = ftp.sendcmd(f"MDTM {name}")[4:].strip()
                        mt = datetime.strptime(mt_raw, "%Y%m%d%H%M%S")
                    except Exception:
                        mt = datetime.min
                files.append((name, mt))
    except (error_perm, AttributeError):
        # Если MLSD не поддержан — запасной вариант
        names = [n for n in ftp.nlst() if n.lower().endswith(".json")]
        for n in names:
            try:
                mt_raw = ftp.sendcmd(f"MDTM {n}")[4:].strip()
                mt = datetime.strptime(mt_raw, "%Y%m%d%H%M%S")
            except Exception:
                mt = datetime.min
            files.append((n, mt))
    return files

def _download_to_string(ftp: FTP, filename: str) -> str:
    buf = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buf.write)
    buf.seek(0)
    return buf.read().decode("utf-8")

def _move_remote(ftp: FTP, src_name: str, dst_dir: str) -> str:
    """Переместить файл в папку (archive/failed) с датой в имени."""
    _ensure_remote_dir(ftp, dst_dir)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    # добавим timestamp перед расширением
    base, ext = os.path.splitext(os.path.basename(src_name))
    dst_name = f"{base}__{ts}{ext or ''}"
    src_path = f"{FTP_DIR.rstrip('/')}/{src_name}"
    dst_path = f"{dst_dir.rstrip('/')}/{dst_name}"
    ftp.rename(src_path, dst_path)
    return dst_path

def _cleanup_incoming(ftp: FTP, keep_latest: int, max_age_days: int):
    """Оставляем N последних файлов, остальное старше max_age_days — удаляем."""
    now = datetime.now()
    files = _list_json_files_with_mtime(ftp)
    if not files:
        return
    # сортировка по времени убыв.
    files.sort(key=lambda x: x[1], reverse=True)
    latest_set = set(name for name, _ in files[:max(0, keep_latest)])
    for name, mtime in files:
        if name in latest_set:
            continue
        if (now - mtime).days >= max_age_days:
            try:
                ftp.delete(name)
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

def _normalize_input(json_content: str) -> list[dict]:
    data = json.loads(json_content)
    if isinstance(data, dict):
        data = [data]
    elif not isinstance(data, list):
        raise ValueError("Ожидался JSON-объект или массив объектов")
    return data

def transform_catalog(items: list[dict]) -> list[dict]:
    out = []
    for it in items:
        out.append({
            "code": str(it.get("Id", "")),
            "name": str(it.get("Name", "") or ""),
            "producer": "",
            "barcode": str(it.get("Barcode", "") or ""),
            "vat": DEFAULT_VAT,
        })
    return out

def transform_stock(items: list[dict], branch: str) -> list[dict]:
    out = []
    for it in items:
        price = float(it.get("MaxPrice", 0.0) or 0.0)
        stock = float(it.get("TotalStock", 0.0) or 0.0)
        # не допускаем отрицательных
        price = max(price, 0.0)
        stock = max(stock, 0.0)
        out.append({
            "branch": branch,
            "code": str(it.get("Id", "")),
            "price": price,
            "qty": stock,
            "price_reserve": price
        })
    return out

def save_to_json(data, enterprise_code: str, file_type: str) -> str:
    dir_path = os.path.join(TEMP_FILE_PATH, str(enterprise_code))
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"{file_type}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logging.info(f"✅ Данные сохранены: {file_path}")
    return file_path

async def send_catalog_data(file_path: str, enterprise_code: str):
    await process_database_service(file_path, "catalog", enterprise_code)

async def send_stock_data(file_path: str, enterprise_code: str):
    await process_database_service(file_path, "stock", enterprise_code)

# =========================
# Основной сценарий
# =========================

async def run_service(enterprise_code: str, file_type: str = FILE_TYPE):
    """
    file_type: 'catalog' | 'stock' | 'both'
    """
    ftp = None
    latest_name = None
    try:
        ftp = connect_ftp(FTP_DIR)
        files = _list_json_files_with_mtime(ftp)
        if not files:
            logging.info("Нет JSON-файлов во входящей папке.")
            return

        # берём самый свежий
        files.sort(key=lambda x: x[1], reverse=True)
        latest_name, latest_mtime = files[0]
        logging.info(f"Обработка файла: {latest_name} (mtime={latest_mtime})")

        # загружаем содержимое
        json_str = _download_to_string(ftp, latest_name)
        items = _normalize_input(json_str)

        # что конвертировать
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

        # в архив
        archived = _move_remote(ftp, latest_name, FTP_ARCHIVE_DIR)
        logging.info(f"📦 Перемещён в архив: {archived}")

        # уборка
        _cleanup_incoming(ftp, FTP_KEEP_LATEST, FTP_MAX_AGE_DAYS)

    except Exception as e:
        logging.exception(f"❌ Ошибка при обработке файла {latest_name or ''}: {e}")
        try:
            if ftp and latest_name:
                failed = _move_remote(ftp, latest_name, FTP_FAILED_DIR)
                logging.warning(f"Файл перемещён в failed: {failed}")
        except Exception as e2:
            logging.warning(f"Не удалось переместить в failed: {e2}")
        # можно не падать дальше, если это сервис-крон
    finally:
        try:
            if ftp:
                ftp.quit()
        except Exception:
            pass

# =========================
# Запуск локально
# =========================

if __name__ == "__main__":
    # Пример: для zoomagazin укажи ENTERPRISE_CODE в .env
    # FILE_TYPE можно оставить 'both', или задать 'catalog'/'stock'
    asyncio.run(run_service(ENTERPRISE_CODE, FILE_TYPE))

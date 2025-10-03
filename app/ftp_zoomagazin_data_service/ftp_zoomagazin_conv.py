import os
import json
import asyncio
import logging
from io import BytesIO
from datetime import datetime
from ftplib import FTP, error_perm

from dotenv import load_dotenv
from sqlalchemy.future import select
from app.database import get_async_db
from app.models import MappingBranch
from app.services.database_service import process_database_service

# =========================
# Константы
# =========================
ENTERPRISE_CODE = "2"
FILE_TYPE = "both"
DEFAULT_VAT = 20.0
TEMP_DIR = "./temp"

load_dotenv()

FTP_HOST = os.getenv("FTP_HOST", "127.0.0.1")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER", "zoomagazin")
FTP_PASS = os.getenv("FTP_PASS", "")
FTP_DIR = os.getenv("FTP_DIR", "/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# =========================
# Утилиты FTP
# =========================
def _join_ftp(*parts: str) -> str:
    cleaned = [p.strip("/") for p in parts if p and p != "/"]
    return "/" + "/".join(cleaned) if cleaned else "/"


def _ensure_remote_dir(ftp: FTP, abs_path: str) -> None:
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
    ftp.encoding = 'latin1'
    try:
        names = ftp.nlst(path)
    except Exception as e:
        logging.error(f"Ошибка получения списка файлов: {e}")
        return []

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
    buf = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buf.write)
    buf.seek(0)
    try:
        return buf.read().decode("utf-8")
    except UnicodeDecodeError:
        buf.seek(0)
        return buf.read().decode("windows-1251")


def _cleanup_except_latest(ftp: FTP, cwd_abs: str):
    """Удаляет все JSON-файлы кроме самого последнего"""
    files = _list_json_files_with_mtime(ftp, cwd_abs)
    if not files:
        return

    files.sort(key=lambda x: x[1], reverse=True)
    latest_file = files[0][0]

    for name, _ in files[1:]:
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


async def send_catalog_data(data: list[dict], enterprise_code: str):
    path = save_to_json(data, enterprise_code, "catalog")
    await process_database_service(path, "catalog", enterprise_code)


async def send_stock_data(data: list[dict], enterprise_code: str):
    path = save_to_json(data, enterprise_code, "stock")
    await process_database_service(path, "stock", enterprise_code)


# =========================
# Основной сценарий
# =========================
async def run_service(enterprise_code: str, file_type: str):
    incoming_abs = FTP_DIR if FTP_DIR.startswith("/") else _join_ftp("/", FTP_DIR)

    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.encoding = "utf-8"

    _ensure_remote_dir(ftp, incoming_abs)

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
            await send_catalog_data(catalog, enterprise_code)

        if ft in ("stock", "both"):
            branch = await fetch_branch_by_enterprise_code(enterprise_code)
            stock = transform_stock(items, branch)
            await send_stock_data(stock, enterprise_code)

        logging.info("📦 Перемещение отключено. Удаляем все кроме последнего файла.")
        _cleanup_except_latest(ftp, incoming_abs)

    except Exception as e:
        logging.exception(f"❌ Ошибка: {e}")
    finally:
        try:
            ftp.quit()
        except Exception:
            pass


# Локальный запуск
if __name__ == "__main__":
    asyncio.run(run_service(ENTERPRISE_CODE, FILE_TYPE))

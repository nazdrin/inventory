import os
import json
import asyncio
import logging
from io import BytesIO
from datetime import datetime
from ftplib import FTP, error_perm

from dotenv import load_dotenv
from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings
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
FTP_USER = os.getenv("FTP_USER_1", "zoomagazin")
FTP_PASS = os.getenv("FTP_PASS_1", "")
FTP_DIR = os.getenv("FTP_DIR", "/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


# =========================
# Утилиты FTP
# =========================
def _join_ftp(*parts: str) -> str:
    cleaned = [p.strip("/") for p in parts if p and p != "/"]
    return "/" + "/".join(cleaned) if cleaned else "/"


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


# =========================
# Доступ к БД
# =========================
async def fetch_store_branches(enterprise_code: str) -> list[dict]:
    """Возвращает список [{'store_id': 'Zoomagazin_1.json', 'branch': '111'}, ...]"""
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.store_id, MappingBranch.branch)
            .where(MappingBranch.enterprise_code == enterprise_code)
        )
        rows = result.all()
        if not rows:
            raise ValueError(f"Нет данных mapping_branch для enterprise_code={enterprise_code}")
        return [{"store_id": r[0], "branch": str(r[1])} for r in rows]


async def fetch_catalog_store_id(enterprise_code: str) -> str:
    """
    Возвращает имя файла каталога для FTP по enterprise_code.
    Алгоритм:
      1) В EnterpriseSettings находим branch_id для данного enterprise_code.
      2) В MappingBranch ищем запись с тем же enterprise_code и branch == branch_id.
      3) Возвращаем значение store_id (например, 'catalog-Zoomagazin_2sm.json').
    """
    async with get_async_db() as session:
        # 1) Получаем branch_id из EnterpriseSettings
        result_branch_id = await session.execute(
            select(EnterpriseSettings.branch_id).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        branch_id = result_branch_id.scalars().first()
        if branch_id is None:
            raise ValueError(f"branch_id не найден в EnterpriseSettings для enterprise_code={enterprise_code}")

        # 2) Находим store_id в MappingBranch по enterprise_code и branch == branch_id
        result_store = await session.execute(
            select(MappingBranch.store_id).where(
                (MappingBranch.enterprise_code == enterprise_code) & (MappingBranch.branch == str(branch_id))
            )
        )
        store_id = result_store.scalars().first()
        if not store_id:
            raise ValueError(
                f"store_id не найден в MappingBranch для enterprise_code={enterprise_code} и branch={branch_id}"
            )

        return str(store_id)


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
# Обработка каталога
# =========================
async def process_catalog(ftp: FTP, enterprise_code: str):
    """Обработка каталога — имя файла берётся из БД:
    EnterpriseSettings.branch_id -> MappingBranch.branch -> MappingBranch.store_id (например, 'catalog-Zoomagazin_2sm.json')
    """
    incoming_abs = FTP_DIR if FTP_DIR.startswith("/") else _join_ftp("/", FTP_DIR)

    # Получаем ожидаемое имя файла каталога из БД
    try:
        expected_filename = await fetch_catalog_store_id(enterprise_code)
    except Exception as e:
        logging.error(f"Не удалось получить имя файла каталога из БД: {e}")
        return

    # Получаем список файлов на FTP
    files = _list_json_files_with_mtime(ftp, incoming_abs)
    file_names = [name for name, _ in files]

    # Ищем точное совпадение (без учёта регистра)
    target_file = next(
        (fname for fname in file_names if fname.lower() == expected_filename.lower()),
        None
    )

    if not target_file:
        logging.warning(f"Файл каталога '{expected_filename}' не найден в каталоге FTP '{incoming_abs}'")
        return

    logging.info(f"📘 Обработка каталога: {target_file}")
    raw = _download_to_string(ftp, incoming_abs, target_file)
    items = _normalize_input(raw)
    catalog = transform_catalog(items)
    await send_catalog_data(catalog, enterprise_code)


# =========================
# Обработка стока
# =========================
async def process_stock(ftp: FTP, enterprise_code: str):
    """Обрабатывает все стоки по store_id из mapping_branch"""
    incoming_abs = FTP_DIR if FTP_DIR.startswith("/") else _join_ftp("/", FTP_DIR)
    store_branches = await fetch_store_branches(enterprise_code)
    all_stock = []

    files = _list_json_files_with_mtime(ftp, incoming_abs)
    file_names = [name for name, _ in files]

    for sb in store_branches:
        store_id = sb["store_id"]
        branch = sb["branch"]

        # ищем файл с таким именем
        # match = next((f for f in file_names if f == store_id), None)
        match = next((f for f in file_names if store_id.lower() in f.lower() and f.lower().endswith(".json")), None)

        if not match:
            logging.warning(f"❗ Файл {store_id} не найден на FTP, пропускаем.")
            continue

        logging.info(f"💾 Обработка стока: {store_id} → branch {branch}")
        raw = _download_to_string(ftp, incoming_abs, match)
        items = _normalize_input(raw)
        stock = transform_stock(items, branch)
        all_stock.extend(stock)

    if not all_stock:
        logging.warning("⚠️ Не найдено ни одного валидного файла стока.")
        return

    await send_stock_data(all_stock, enterprise_code)


# =========================
# Основной сценарий
# =========================
async def run_service(enterprise_code: str, file_type: str):
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.encoding = "utf-8"

    try:
        if file_type in ("catalog", "both"):
            await process_catalog(ftp, enterprise_code)

        if file_type in ("stock", "both"):
            await process_stock(ftp, enterprise_code)

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

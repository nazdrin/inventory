import os
import json
import asyncio
from io import BytesIO
from ftplib import FTP
from datetime import datetime, timedelta
from app.services.database_service import process_database_service
from app.database import get_async_db, MappingBranch
from sqlalchemy.future import select
from dotenv import load_dotenv
load_dotenv()

# Константы
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_DIR = os.getenv("FTP_DIR")
FILE_TYPE = "stock"

def connect_ftp():
    ftp = FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)
    return ftp

def get_latest_json_file(ftp):
    files = ftp.nlst()
    json_files = [f for f in files if f.endswith(".json")]
    if not json_files:
        return None

    latest_file = None
    latest_time = None

    for filename in json_files:
        try:
            modified_time = ftp.sendcmd(f"MDTM {filename}")[4:].strip()
            modified_datetime = datetime.strptime(modified_time, "%Y%m%d%H%M%S")
            if latest_time is None or modified_datetime > latest_time:
                latest_file = filename
                latest_time = modified_datetime
        except:
            continue

    return latest_file

def download_file_to_memory(ftp, filename):
    buffer = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    return buffer.read().decode("utf-8")

async def fetch_branch_id(enterprise_code):
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        return result.scalars().first() or "unknown"

def convert_file_from_string(json_content, enterprise_code, branch_id):
    data = json.loads(json_content)
    if isinstance(data, dict):
        data = [data]

    result = []
    for item in data:
        price = float(item.get("MaxPrice", 0.0) or 0.0)
        stock = float(item.get("TotalStock", 0.0) or 0.0)

        # Установка отрицательных значений в 0
        price = max(price, 0.0)
        stock = max(stock, 0.0)

        converted = {
            "code": str(item.get("Id", "")),
            "price": price,
            "qty": stock,
            "price_reserve": price,
            "branch": branch_id,
        }
        result.append(converted)

    temp_dir = os.getenv("TEMP_FILE_PATH", "/tmp")
    os.makedirs(temp_dir, exist_ok=True)

    output_path = os.path.join(temp_dir, f"{enterprise_code}_{FILE_TYPE}_converted.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

    return output_path


def delete_old_files(ftp):
    try:
        files = ftp.nlst()
        json_files = [f for f in files if f.endswith(".json")]
        if not json_files:
            return

        file_dates = {}
        for f in json_files:
            try:
                mdtm = ftp.sendcmd(f"MDTM {f}")[4:].strip()
                modified_datetime = datetime.strptime(mdtm, "%Y%m%d%H%M%S")
                file_dates[f] = modified_datetime
            except:
                continue

        if not file_dates:
            return

        latest_file = max(file_dates.items(), key=lambda x: x[1])[0]

        now = datetime.now()
        for filename, file_date in file_dates.items():
            if filename == latest_file:
                continue
            if now - file_date > timedelta(days=7):
                try:
                    ftp.delete(filename)
                    print(f"✅ Удалён старый файл: {filename}")
                except Exception as e:
                    print(f"⚠️ Ошибка при удалении {filename}: {e}")
    except Exception as e:
        print(f"Ошибка при очистке FTP: {e}")

# -------------------------------
# Основная функция запуска
# -------------------------------
async def run_service(enterprise_code, file_type):
    ftp = connect_ftp()
    latest_file = get_latest_json_file(ftp)

    if not latest_file:
        print("Нет новых JSON-файлов.")
        ftp.quit()
        return

    print(f"Обработка файла: {latest_file}")
    json_string = download_file_to_memory(ftp, latest_file)
    branch_id = await fetch_branch_id(enterprise_code)

    converted_path = convert_file_from_string(json_string, enterprise_code, branch_id)
    print(f"Сконвертирован в: {converted_path}")

    await process_database_service(converted_path, FILE_TYPE, enterprise_code)
    delete_old_files(ftp)

    ftp.quit()

# Для локального запуска
if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

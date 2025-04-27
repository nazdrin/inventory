import os
import json
import asyncio
from io import BytesIO
from ftplib import FTP
from datetime import datetime, timedelta
from app.services.database_service import process_database_service
from dotenv import load_dotenv
load_dotenv()

# Константы
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_DIR = os.getenv("FTP_DIR")
FILE_TYPE = "catalog"
DEFAULT_VAT = 20

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
    """Скачать файл с FTP в строку (без сохранения на диск)."""
    buffer = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)
    return buffer.read().decode("utf-8")

def convert_file_from_string(json_content, enterprise_code):
    """Конвертация из JSON-строки и сохранение результата на диск."""
    data = json.loads(json_content)

    if isinstance(data, dict):
        data = [data]

    result = []
    for item in data:
        converted = {
            "code": str(item.get("Id", "")),
            "name": item.get("Name", ""),
            "vat": DEFAULT_VAT,
            "producer": "N/A",
            "barcode": item.get("Barcode", "")
        }
        result.append(converted)

    output_path = os.path.join(os.getenv("TEMP_FILE_PATH", "/tmp"), f"{enterprise_code}_{FILE_TYPE}_converted.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

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

        # Находим последний файл
        latest_file = max(file_dates.items(), key=lambda x: x[1])[0]

        now = datetime.now()
        for filename, file_date in file_dates.items():
            if filename == latest_file:
                continue  # Пропускаем последний по дате
            if now - file_date > timedelta(seconds=30):
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
async def run_service(enterprise_code):
    ftp = connect_ftp()
    latest_file = get_latest_json_file(ftp)

    if not latest_file:
        print("Нет новых JSON-файлов.")
        ftp.quit()
        return

    print(f"Обработка файла: {latest_file}")
    json_string = download_file_to_memory(ftp, latest_file)

    converted_path = convert_file_from_string(json_string, enterprise_code)
    print(f"Сконвертирован в: {converted_path}")
    print("Текущие файлы после удаления:", ftp.nlst())

    await process_database_service(converted_path, FILE_TYPE, enterprise_code)
    delete_old_files(ftp)  # Просто вызываем

    ftp.quit()

# Для локального запуска
if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "2"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

import os
import ftplib
import json
import logging
from io import BytesIO
from typing import Optional
from app.services.database_service import process_database_service


# --- Конфигурация ---
FTP_HOST = os.getenv("FTP_HOST", "localhost")
FTP_PORT = int(os.getenv("FTP_PORT", 21))
FTP_USER = os.getenv("FTP_USER", "")
FTP_PASS = os.getenv("FTP_PASS", "")
FTP_DIR = os.getenv("FTP_DIR", "/")
TEMP_FILE_PATH = os.getenv("TEMP_FILE_PATH", "/root/temp")

DEFAULT_FILE_TYPE = "catalog"


# --- Декодирование имени файла только для логов ---
def _decode_filename(name: str) -> str:
    try:
        return name.encode("latin1").decode("utf-8")
    except UnicodeDecodeError:
        try:
            return name.encode("latin1").decode("cp1251")
        except UnicodeDecodeError:
            return name


# --- Подключение к FTP ---
def _connect_ftp() -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.encoding = "latin1"  # Ключевая настройка
    return ftp


# --- Поиск последнего файла ---
def _get_latest_file_name(ftp: ftplib.FTP, directory: str) -> Optional[str]:
    files = ftp.nlst(directory)
    json_files = []

    for f in files:
        if not f.lower().endswith(".json"):
            continue

        try:
            resp = ftp.sendcmd(f"MDTM {f}")
            mtime = resp[4:].strip()
            decoded_name = _decode_filename(f)
            json_files.append((f, decoded_name, mtime))  # оригинальное и декодированное имя
        except Exception:
            continue

    if not json_files:
        return None

    # Сортировка по дате
    latest = max(json_files, key=lambda x: x[2])
    ftp_name, decoded_name, _ = latest

    logging.info(f"📄 Найден последний файл: {decoded_name}")
    return ftp_name  # возвращаем оригинальное FTP-имя
        

# --- Загрузка файла с FTP и попытка декодировать ---
def _download_to_string(ftp: ftplib.FTP, directory: str, filename: str) -> str:
    buf = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buf.write)
    buf.seek(0)

    raw_bytes = buf.read()
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return raw_bytes.decode("cp1251")
        except UnicodeDecodeError as e:
            raise Exception(f"❌ Не удалось декодировать содержимое файла: {e}")


# --- Сохранение во временный файл ---
def _save_temp_json(content: str, enterprise_code: str, file_type: str = DEFAULT_FILE_TYPE) -> str:
    os.makedirs(TEMP_FILE_PATH, exist_ok=True)
    file_path = os.path.join(TEMP_FILE_PATH, f"{file_type}-{enterprise_code}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    logging.info(f"✅ Временный файл сохранён: {file_path}")
    return file_path


async def run_service(enterprise_code: str, file_type: str = DEFAULT_FILE_TYPE) -> None:
    logging.info(f"🚀 Запуск сервиса для enterprise_code={enterprise_code}, type={file_type}")
    ftp = _connect_ftp()

    try:
        latest_name = _get_latest_file_name(ftp, FTP_DIR)
        if not latest_name:
            raise FileNotFoundError("❌ Не найден ни один подходящий файл .json")

        log_name = _decode_filename(latest_name)
        logging.info(f"📥 Загрузка файла: {log_name}")

        # 1. Скачиваем файл
        raw_content = _download_to_string(ftp, FTP_DIR, latest_name)
        data_json = json.loads(raw_content)

        # 2. Преобразуем в целевой формат
        if file_type == "catalog":
            from app.services.data_converter import transform_catalog
            transformed = transform_catalog(data_json)

        elif file_type == "stock":
            from app.services.data_converter import transform_stock
            from app.services.database_service import fetch_branch_by_enterprise_code
            branch = await fetch_branch_by_enterprise_code(enterprise_code)
            transformed = transform_stock(data_json, branch)

        else:
            raise ValueError("Неверный тип файла (ожидается 'catalog' или 'stock')")

        # 3. Сохраняем уже ПРЕОБРАЗОВАННЫЕ данные
        temp_path = _save_temp_json(
            json.dumps(transformed, ensure_ascii=False, indent=4),
            enterprise_code,
            file_type
        )

        # 4. Отправляем в БД
        await process_database_service(temp_path, file_type, enterprise_code)

    except Exception as e:
        logging.error(f"🔥 Ошибка: {e}")
    finally:
        ftp.quit()
        logging.info("🔒 FTP-сессия завершена")



# --- Запуск вручную ---
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_service("342", "catalog"))

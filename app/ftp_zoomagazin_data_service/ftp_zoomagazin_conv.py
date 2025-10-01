import os
import ftplib
import json
import logging
from io import BytesIO
from typing import Optional
from app.services.database_service import process_database_service


FTP_HOST = os.getenv("FTP_HOST", "localhost")
FTP_PORT = int(os.getenv("FTP_PORT", 21))
FTP_USER = os.getenv("FTP_USER", "")
FTP_PASS = os.getenv("FTP_PASS", "")
FTP_DIR = os.getenv("FTP_DIR", "/")
TEMP_FILE_PATH = os.getenv("TEMP_FILE_PATH", "/root/temp")

DEFAULT_FILE_TYPE = "catalog"  # или "stock"


def _decode_filename(name: str) -> str:
    """Декодирует имя файла, если оно содержит кириллицу, закодированную как latin1."""
    try:
        return name.encode("latin1").decode("utf-8")
    except UnicodeDecodeError:
        try:
            return name.encode("latin1").decode("cp1251")
        except UnicodeDecodeError:
            return name


def _connect_ftp() -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.encoding = "latin1"  # важно!
    return ftp


def _get_latest_file_name(ftp: ftplib.FTP, directory: str) -> Optional[str]:
    files = ftp.nlst(directory)
    json_files = []

    for f in files:
        if not f.lower().endswith(".json"):
            continue

        try:
            # Пробуем получить время модификации
            resp = ftp.sendcmd(f"MDTM {f}")
            mtime = resp[4:].strip()
            name_decoded = _decode_filename(f)
            json_files.append((name_decoded, mtime, f))  # добавляем и оригинальное имя
        except Exception:
            continue

    if not json_files:
        return None

    # Сортируем по времени модификации
    latest = max(json_files, key=lambda x: x[1])
    logging.info(f"📄 Найден последний файл: {latest[0]}")
    return latest[2]  # возвращаем имя, пригодное для FTP


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

        logging.info(f"📥 Загрузка файла: {latest_name}")
        raw_content = _download_to_string(ftp, FTP_DIR, latest_name)
        temp_path = _save_temp_json(raw_content, enterprise_code, file_type)
        await process_database_service(temp_path, file_type, enterprise_code)

    except Exception as e:
        logging.error(f"🔥 Ошибка: {e}")
    finally:
        ftp.quit()
        logging.info("🔒 FTP-сессия завершена")


# Для локального теста
if __name__ == "__main__":
    import asyncio

    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_service("342", "catalog"))

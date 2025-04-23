import csv
import json
import logging
import os
import shutil
import tempfile
from dotenv import load_dotenv
from app.services.database_service import process_database_service
import chardet

load_dotenv()
DEFAULT_VAT = 20

def detect_encoding(file_path):
    """
    Определяет кодировку файла на основе первых 2КБ содержимого.
    Если не удалось определить — возвращает cp1251.
    """
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(2048)
            result = chardet.detect(raw_data)
            encoding = result.get("encoding")
            if encoding:
                return encoding
    except Exception as e:
        logging.warning(f"Ошибка при определении кодировки: {str(e)}")

    logging.warning("Кодировка не определена, используется cp1251 по умолчанию")
    return "cp1251"

async def process_jetvet_catalog(
    enterprise_code: str,
    file_path: str,
    file_type: str,
    single_store: bool,
    store_serial: str
):
    """
    Обработка CSV-файла JetVet каталога и сохранение в JSON.
    Также сохраняет исходный файл для отладки.
    """
    try:
        items = []
        encoding = detect_encoding(file_path)
        logging.info(f"Определена кодировка файла: {encoding}")

        # Сохраняем входной файл
        try:
            temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
            os.makedirs(temp_dir, exist_ok=True)
            saved_input_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_original.csv")
            shutil.copy(file_path, saved_input_path)
            logging.info(f"Исходный файл сохранён для отладки: {saved_input_path}")
        except Exception as e:
            logging.warning(f"Не удалось сохранить входной файл: {str(e)}")

        try:
            with open(file_path, mode="r", encoding=encoding, errors="replace") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=";")

                logging.info(f"Заголовки CSV: {reader.fieldnames}")

                for row in reader:
                    logging.debug(f"Обработка строки: {row}")

                    code = row.get("id") or row.get("code")
                    name = row.get("name", "").strip()
                    barcode = row.get("barcode", "").strip()

                    if not code or not name:
                        logging.warning(f"Пропущена строка: code={code}, name={name}, raw={row}")
                        continue

                    item = {
                        "code": code,
                        "name": name,
                        "vat": DEFAULT_VAT,
                        "producer": "N/A",
                        "barcode": barcode
                    }
                    items.append(item)

        except UnicodeDecodeError as e:
            logging.error(f"Ошибка декодирования файла {file_path} с кодировкой {encoding}: {e}")
            return

        if not items:
            logging.warning(f"Пустой результат при обработке каталога JetVet для {enterprise_code}")
            return

        # Сохранение в JSON и передача на обработку
        output_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)

        logging.info(f"Каталог JetVet успешно сконвертирован и сохранен: {output_path}")
        await process_database_service(output_path, file_type, enterprise_code)

    except Exception as e:
        logging.error(f"Ошибка при обработке JetVet каталога: {str(e)}")

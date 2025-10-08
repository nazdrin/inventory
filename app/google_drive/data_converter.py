import tempfile
import logging
import json
import os

from sqlalchemy.ext.asyncio import AsyncSession
from app.models import EnterpriseSettings
from sqlalchemy import select
from app.services.database_service import process_database_service  # Импорт функции Database_service
import xmltodict
from dotenv import load_dotenv
load_dotenv()
from app.services.notification_service import send_notification 
xml_data = "<root><element>value</element></root>"
parsed_data = xmltodict.parse(xml_data)

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def get_branch_id(enterprise_code: str, db_session: AsyncSession) -> str:
    """
    Получает branch_id для заданного enterprise_code из таблицы EnterpriseSettings.
    """
    logging.info(f"Попытка получения branch_id для enterprise_code: {enterprise_code}")
    query = select(EnterpriseSettings.branch_id).where(EnterpriseSettings.enterprise_code == enterprise_code)
    result = await db_session.execute(query)
    branch_id = result.scalar()
    if not branch_id:
        logging.error(f"branch_id отсутствует для enterprise_code {enterprise_code}")
        send_notification(f"branch_id отсутствует для предприятия в процессе конвертации {enterprise_code}",enterprise_code )
        raise ValueError(f"branch_id отсутствует для enterprise_code {enterprise_code}")
    logging.info(f"Получен branch_id: {branch_id}")
    return branch_id

async def convert_to_json(file_path: str, file_type: str,enterprise_code):
    """
    Конвертирует данные из файла в JSON.
    :param file_path: Путь к файлу
    :param file_type: Тип файла (catalog или stock)
    :return: Список словарей с данными
    """
    try:        
        if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
            import openpyxl
            wb = openpyxl.load_workbook(file_path, data_only=True)
            sheet = wb.active
            headers = [str(cell.value).strip().lower() for cell in sheet[1]]
            data = [
                {headers[col_idx]: row[col_idx].value for col_idx in range(len(headers))}
                for row in sheet.iter_rows(min_row=2)
            ]
            # Преобразование значений в целые числа, если это возможно
            for item in data:
                for key, value in item.items():
                    if isinstance(value, float) and value.is_integer():
                        item[key] = int(value)  # Преобразуем в целое число, если это возможно
                    elif value is None:
                        item[key] = ""  # Обработка None значений, если нужно
            logging.info(f"Успешно конвертирован файл {file_path} в JSON")
            return data
        
        elif file_path.endswith(".json"):
            with open(file_path, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
                logging.info(f"Успешно считан JSON файл {file_path}")
                
                return data
        
        elif file_path.endswith(".xml"):
            with open(file_path, "r", encoding="utf-8") as xml_file:
                xml_data = xml_file.read()                                

                # Парсинг XML в JSON-структуру
                json_data = xmltodict.parse(xml_data)               
                
                # Приведение ключей JSON к нижнему регистру
                def normalize_keys(d):
                    if isinstance(d, dict):
                        return {k.lower(): normalize_keys(v) for k, v in d.items()}
                    elif isinstance(d, list):
                        return [normalize_keys(i) for i in d]
                    else:
                        return d

                json_data = normalize_keys(json_data)                

                # Проверка структуры на основе типа файла
                if file_type == "catalog":
                    if not isinstance(json_data, dict) or "catalog" not in json_data:
                        logging.warning(f"Некорректная структура XML файла {file_path}. Ожидалась структура с 'Catalog'.")
                        return None
                    data = json_data.get("catalog", {}).get("item", [])
                elif file_type == "stock":
                    # Получаем данные из 'stock' -> 'item'
                    data = json_data.get("stock", {}).get("item", [])                   

                    # Приводим данные к списку, если они представлены как словарь (один элемент)
                    if isinstance(data, dict):
                        logging.warning("Данные из XML представлены в виде словаря, конвертация в список.")
                        data = [data]

                    # Проверка на корректный формат данных
                    if not isinstance(data, list):
                        logging.error(f"Неожиданный тип данных для 'stock': {type(data)}")
                        raise ValueError("Неверный формат данных для 'stock'. Ожидался список или словарь.")

                    # Нормализация данных: ключи приводим к нижнему регистру
                    normalized_data = []
                    for item in data:
                        if not isinstance(item, dict):
                            logging.error(f"Некорректный элемент данных: {item}")
                            continue
                        normalized_item = {k.lower(): v for k, v in item.items()}
                        normalized_data.append(normalized_item)
                    
                    # Возвращаем результат
                    return normalized_data
                else:
                    logging.error(f"Неизвестный тип файла: {file_type}")
                    return None
                # Если данные отсутствуют, логируем предупреждение
                if not data:
                    
                    return None

                # Нормализация полей внутри данных
                if isinstance(data, dict):  # Если только один элемент, превращаем в список
                    logging.warning("Данные из XML представлены в виде словаря, конвертация в список.")
                    data = [data]

                for idx, item in enumerate(data):
                    if not isinstance(item, dict):
                        logging.error(f"Элемент данных не является словарем: {item} (индекс {idx})")
                    data[idx] = {k.lower(): v for k, v in item.items()}

                return data
        
        elif file_path.endswith(".csv"):
            import csv
            with open(file_path, "r", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)
                data = [row for row in reader]
                logging.info(f"Успешно конвертирован CSV файл {file_path} в JSON")
                return data
        
        else:
            logging.error(f"Неподдерживаемый формат файла: {file_path}")
            send_notification(f"Неподдерживаемый формат файла: {file_path}для предприятия {enterprise_code}",enterprise_code)
            raise ValueError(f"Неподдерживаемый формат файла: {file_path}")
    
    except Exception as e:
        logging.error(f"Ошибка конвертации файла {file_path} в JSON: {str(e)}")
        send_notification(f"Ошибка конвертации файла {file_path}в JSON: {str(e)} для предприятия {enterprise_code}",enterprise_code)
        raise

def clean_record_keys_and_values(record):
    """
    Очищает ключи и значения записи от лишних пробелов.
    :param record: Словарь с данными записи
    :return: Очищенный словарь
    """
    return {k.strip(): v.strip() if isinstance(v, str) else v for k, v in record.items()}

def add_branch_information(data, single_store, store_serial, branch_id,enterprise_code):
    """
    Добавляет информацию о branch или branch_id в зависимости от условий.
    :param data: Список данных
    :param single_store: Используется ли один магазин
    :param store_serial: Серийный номер магазина
    :param branch_id: Идентификатор филиала
    :return: Обновленный список данных
    """
    if not data:
        logging.warning("Пустой список данных передан для добавления branch информации.")
        send_notification(f"Пустой список данных передан для добавления branch информации.для предприятия {enterprise_code}",enterprise_code)
        return data
    logging.info("Начало добавления информации о branch и branch_id.")
    for record in data:
        if "branch" not in record and single_store:
            record["branch"] = store_serial
        if "branchid" not in record and branch_id:
            record["branch_id"] = branch_id
    logging.info("Добавление информации о branch и branch_id завершено.")
    return data

async def transform_data_types(data, file_type,enterprise_code):
    """
    Преобразует типы данных в соответствии с моделями InventoryData и InventoryStock.
    :param data: Список словарей с данными
    :param file_type: Тип файла (catalog или stock)
    :return: Список словарей с преобразованными данными
    """
    
    try:
        transformed_data = []
        for item in data:
            # Очистка ключей и значений
            item = clean_record_keys_and_values(item)
            logging.debug(f"Обработка элемента: {item}")
            
            # Проверка и фильтрация записей
            if file_type == "catalog" and not item.get("code"):
                continue
            if file_type == "stock" and (not item.get("code") or not item.get("branch")):
                continue

            transformed_item = {}

            if file_type == "catalog":
                transformed_item = {
                    "code": str(item.get("code", "")).strip() if isinstance(item.get("code", ""), str) else str(int(item.get("code", 0))),
                    "name": str(item.get("name", "")).strip(),
                    "vat": float(item.get("vat", 0.0)),
                    "producer": str(item.get("producer", "")).strip(),
                    "morion": str(item.get("morion", "")).strip(),
                    "tabletki": str(item.get("tabletki", "")).strip(),
                    "barcode": str(item.get("barcode", "")).strip(),
                    "badm": str(item.get("badm", "")).strip(),
                    "optima": str(item.get("optima", "")).strip(),
                }
            elif file_type == "stock":
                transformed_item = {
                    "branch": str(item.get("branch", "")).strip(),
                    "code": str(item.get("code", "")).strip() if isinstance(item.get("code", ""), str) else str(int(item.get("code", 0))),  
                    "price": float(item.get("price", 0)),
                    "qty": int(item.get("qty", 0)),
                    "price_reserve": float(item.get("pricereserve", 0))
                }
            
            transformed_data.append(transformed_item)
        logging.info(f"Преобразование типов данных завершено")
        return transformed_data
    except Exception as e:
        logging.error(f"Ошибка преобразования типов данных: {str(e)}")
        send_notification(f"Ошибка преобразования типов данных: {str(e)}для предприятия {enterprise_code}",enterprise_code)
        raise

async def process_data_converter(
    enterprise_code, file_path, file_type, store_serial, single_store, db_session
):
    try:
        branch_id = None
        if file_type == "catalog":
            branch_id = await get_branch_id(enterprise_code, db_session)

        converted_data = await convert_to_json(file_path, file_type, enterprise_code)
        if not converted_data:
            logging.warning(f"Пустые данные после конвертации файла {file_path}")

        converted_data = add_branch_information(converted_data, single_store, store_serial, branch_id, enterprise_code)
        if not converted_data:
            logging.warning(f"Пустые данные после добавления branch информации для файла {file_path}")

        transformed_data = await transform_data_types(converted_data, file_type, enterprise_code)
        
        if not transformed_data:
            logging.warning(f"Пустые данные после преобразования типов для файла {file_path}")

        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")
        
        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(transformed_data, json_file, ensure_ascii=False, indent=4)
        logging.info(f"JSON записан в файл: {json_file_path}")

        # Передача enterprise_code в process_database_service
        await process_database_service(json_file_path, file_type, enterprise_code)
        
    except Exception as e:
        error_message = f"Ошибка обработки файла {file_path} для предприятия {enterprise_code}: {str(e)}"
        logging.error(error_message)
        send_notification(error_message, enterprise_code)
        raise
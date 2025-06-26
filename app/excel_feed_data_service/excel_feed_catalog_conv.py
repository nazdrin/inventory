import sys
import requests
import json
import asyncio
import pandas as pd
import tempfile
import os
import logging
from dotenv import load_dotenv
from app.services.database_service import process_database_service

load_dotenv()

DEFAULT_VAT = 20
FEED_URL = "http://34.72.22.88/price/26t73v5tng/vs_price_03.xls"

def download_excel(url):
    """Загрузка Excel-файла."""
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        temp_path = os.path.join(tempfile.gettempdir(), "feed_data.xls")
        with open(temp_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        return temp_path
    else:
        raise Exception(f"Ошибка загрузки файла: {response.status_code}")

def parse_excel(file_path):
    """Парсинг Excel и конвертация данных."""
    # Читаем 2 строки заголовков и объединяем их
    # Фильтруем нужные колонки по частичному совпадению
    
    df = pd.read_excel(file_path, dtype=str, header=[3, 4])
    df.columns = [' '.join(col).strip() if isinstance(col, tuple) else col.strip() for col in df.columns]

    # Очищаем названия, убирая мусорные суффиксы
    df.columns = df.columns.str.replace(r"Unnamed: \d+_level_\d+", "", regex=True).str.strip()
    columns_mapping = {
    "Артикул": [col for col in df.columns if "Артикул" in col][0],
    "Найменування": [col for col in df.columns if "Найменування" in col][0],
    "Штрихкод": [col for col in df.columns if "Штрихкод" in col][0],
    }

# Оставляем только нужные колонки и переименовываем
    df = df[[columns_mapping["Артикул"], columns_mapping["Найменування"], columns_mapping["Штрихкод"]]]
    df.columns = ["code", "name", "barcode"]

    # Выводим результат для проверки
    print(df.columns.tolist())  
    print(df.head())  # Вывод первых 5 строк таблицы
    required_columns = {"code", "name", "barcode"}
    if not required_columns.issubset(df.columns):
        raise Exception(f"В файле отсутствуют необходимые колонки: {required_columns - set(df.columns)}")
    df = df[list(required_columns)].rename(columns={"code": "code", "name": "name", "barcode": "barcode"})
    # Добавляем недостающие колонки
    df["vat"] = DEFAULT_VAT
    df["producer"] = "N/A"
    
    # Удаляем дубли по 'code'
    df = df.drop_duplicates(subset=["code"], keep=False)
    
    # Заполняем пустые значения, приводим к строке
    df = df.fillna("").astype(str)
    
    # Приводим vat к float
    df["vat"] = df["vat"].astype(float)
    
    return df.to_dict(orient="records")
    

def save_to_json(data, enterprise_code, file_type):
    """Сохранение данных в JSON-файл."""
    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    os.makedirs(temp_dir, exist_ok=True)
    json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")
    
    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    
    logging.info(f"JSON записан в файл: {json_file_path}")
    return json_file_path

async def run_service(enterprise_code, file_type):
    """Основной процесс загрузки и обработки Excel-фида."""
    try:
        excel_path = download_excel(FEED_URL)
        parsed_data = parse_excel(excel_path)
        
        file_type = "catalog"
        # Заполняем пустые значения, приводим к строке
        json_file_path = save_to_json(parsed_data, enterprise_code, file_type)
        
        if json_file_path:
            await process_database_service(json_file_path, file_type, enterprise_code)
    except Exception as e:
        logging.error(f"Ошибка обработки: {e}")

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "257"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))
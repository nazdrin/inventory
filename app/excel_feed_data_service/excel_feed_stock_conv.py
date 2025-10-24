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
from app.database import get_async_db, EnterpriseSettings, MappingBranch
from sqlalchemy.future import select

load_dotenv()

FEED_URL = "http://34.72.22.88/price/26t73v5tng/vs_price_03.xls"

async def fetch_branch_id(enterprise_code):
    """Получение branch из MappingBranch."""
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        branch = result.scalar()  # Получаем одно значение
        return branch if branch else "unknown"


def download_excel(url):
    """Загрузка Excel-файла."""
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        temp_path = os.path.join(tempfile.gettempdir(), "feed_stock_data.xls")
        with open(temp_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        return temp_path
    else:
        raise Exception(f"Ошибка загрузки файла: {response.status_code}")

def parse_excel(file_path):
    """Парсинг Excel и конвертация данных."""
    df = pd.read_excel(file_path, dtype=str, header=4)  # Начинаем с 6-й строки (индекс 5)

    # Убираем лишние пробелы в заголовках
    df.columns = df.columns.str.strip()

    print("Названия колонок:", df.columns.tolist())  # Отладка

    # Выбираем нужные колонки:
    df = df.iloc[:, [1, 3, 8]]  # code (2-я колонка), qty (4-я колонка), price (9-я колонка)
    df.columns = ["code", "qty", "price"]

    # Очистка `code`
    df["code"] = df["code"].str.strip()
    df = df.dropna(subset=["code"])  # Удаляем пустые строки
    df["code"] = df["code"].astype(str)  # Принудительно строковый тип

    # Обработка `qty`
    qty_map = {"+": 1, "++": 3, "+++": 5, "-": 0}
    df["qty"] = df["qty"].map(qty_map).fillna(0).astype(int)

    # Обработка `price`
    df["price"] = df["price"].str.replace(",", ".").str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")  # Преобразуем price в float, ошибки -> NaN
    df["price_reserve"] = df["price"]

    # Удаление строк, где `price` пустой (NaN)
    df = df.dropna(subset=["price"])
    df["price"] = df["price"].astype(float)
    df["price_reserve"] = df["price_reserve"].astype(float)

    # Дополнительная проверка перед возвратом
    if df.empty:
        print("❌ Ошибка: после обработки нет данных!")
    else:
        print("✅ Данные успешно обработаны!")

    print("Первые строки данных после обработки:")
    print(df.head(10))

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
        
        branch_mapping = await fetch_branch_id(enterprise_code)
        for item in parsed_data:
            item["branch"] = branch_mapping  # Просто присваиваем строку напрямую

        
        file_type = "stock"
        json_file_path = save_to_json(parsed_data, enterprise_code, file_type)
        
        if json_file_path:
            await process_database_service(json_file_path, file_type, enterprise_code)
    except Exception as e:
        logging.error(f"Ошибка обработки: {e}")

if __name__ == "__main__":
    TEST_ENTERPRISE_CODE = "257"
    asyncio.run(run_service(TEST_ENTERPRISE_CODE))

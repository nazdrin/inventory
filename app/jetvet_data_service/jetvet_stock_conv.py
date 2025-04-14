import csv
import json
import logging
import os
import tempfile
from dotenv import load_dotenv
from app.services.database_service import process_database_service
from app.database import get_async_db, MappingBranch
import chardet
import pandas as pd
from sqlalchemy.future import select

load_dotenv()

def detect_encoding(file_path):
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

async def fetch_branch_id(enterprise_code: str) -> str:
    async with get_async_db() as session:
        result = await session.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
        branch = result.scalars().first()
        return branch if branch else "unknown"

async def process_jetvet_stock(
    enterprise_code: str,
    file_path: str,
    file_type: str,
    single_store: bool,
    store_serial: str
):
    """
    Обработка CSV-файла JetVet остатков и сохранение в JSON.
    """
    try:
        encoding = detect_encoding(file_path)
        logging.info(f"Определена кодировка файла: {encoding}")

        df = pd.read_csv(file_path, sep=";", encoding=encoding, dtype=str)
        df = df.fillna("")

        logging.info(f"Загружено строк из файла: {len(df)}")

        branch_id = await fetch_branch_id(enterprise_code)
        items = []

        for _, row in df.iterrows():
            code = (row.get("code") or row.get("id", "")).strip()
            price = row.get("outprice", "0").replace(",", ".").strip()
            qty = row.get("stock", "0").replace(",", ".").strip()
            
            try:
                price_float = float(price)
                qty_float = float(qty)
                qty_int = int(round(qty_float))
            except ValueError:
                logging.warning(f"Пропущена строка из-за ошибки преобразования: {row.to_dict()}")
                continue
            
            if not code:
                logging.warning(f"Пропущена строка без кода: {row.to_dict()}")
                continue
            


            if not code:
                logging.warning(f"Пропущена строка без кода: {row.to_dict()}")
                continue

            item = {
                "branch": branch_id,
                "code": code,
                "price": price_float,
                "qty": qty_int,
                "price_reserve": price_float
            }
            items.append(item)

        if not items:
            logging.warning(f"Пустой результат при обработке остатков JetVet для {enterprise_code}")
            return

        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        output_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)

        logging.info(f"Остатки JetVet успешно сконвертированы и сохранены: {output_path}")
        await process_database_service(output_path, file_type, enterprise_code)

    except Exception as e:
        logging.error(f"Ошибка при обработке JetVet остатков: {str(e)}")

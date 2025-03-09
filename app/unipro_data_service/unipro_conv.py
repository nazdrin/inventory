import json
import os
import logging
import asyncio
from app.services.database_service import process_database_service
import tempfile
import os
import logging
from dotenv import load_dotenv
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import MappingBranch
from app.database import get_async_db  # Импортируем get_async_db
load_dotenv()
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Словарь соответствий для получения enterprise_code и branch
# mapping = {
    # "BCE73E60-A5C9-4FB4-9186-B6F88FDF3BDA": ("253", "30467
# }


def save_to_json(data, enterprise_code, file_type):
    """Сохранение данных в JSON-файл в указанную временную директорию из .env."""
    try:
        # Получаем временный путь из переменной окружения, иначе используем системный temp
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)

        # Формируем путь к JSON-файлу
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logging.info(f"JSON записан в файл: {json_file_path}")
        return json_file_path
        
    except IOError as e:
        logging.error(f"Ошибка при сохранении JSON-файла: {e}")
        return None
        
conversion_counter = 0  # Инициализация глобального счетчика

def get_conversion_type():
    """Определяет тип конвертации (catalog или stock) без файла, используя глобальную переменную."""
    global conversion_counter
    conversion_counter += 1
    return "catalog" if conversion_counter % 30 == 0 else "stock"


async def get_enterprise_info(source_id: str, db: AsyncSession):
    """
    Получает enterprise_code и branch из таблицы mapping_branch по storeID (source_id).
    
    :param source_id: store_id, который используется для поиска.
    :param db: Асинхронная сессия базы данных.
    :return: Кортеж (enterprise_code, branch) или (None, None), если не найдено.
    """
    result = await db.execute(
        select(MappingBranch.enterprise_code, MappingBranch.branch).where(MappingBranch.store_id == source_id)
    )
    mapping = result.first()  # Получаем первую найденную запись

    if mapping:
        return mapping.enterprise_code, mapping.branch
    return None, None

def convert_catalog(data, enterprise_code):
    """Конвертирует данные каталога в нужный формат."""
    goods = data.get("goods", [])
    converted = [
        {
            "code": item["guid"],
            "name": item["namefull"],
            "vat": 20.0,
            "producer": item.get("dopprop1") or "n/a",
            "barcode": item.get("barcode", ""),
            # "branch_id": enterprise_code
        }
        for item in goods
    ]
    return converted

def convert_stock(data, branch):
    """Конвертирует данные остатков (стока) в нужный формат."""
    goods = data.get("goods", [])
    converted = [
        {
            "branch": branch,
            "code": item["guid"],
            "price": item.get("p1", 0),
            "qty": max(item.get("qtty", 0), 0),  # Если qtty < 0, устанавливаем 0
            "price_reserve": item.get("p1", 0)
        }
        for item in goods
    ]
    return converted

async def unipro_convert(json_file_path):
    """Основная функция обработки конвертации."""
    try:
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        source_id = data.get("info", {}).get("sourceID")

        async with get_async_db() as db:  # Создаем сессию БД
            enterprise_code, branch = await get_enterprise_info(source_id, db)  # Теперь `db` передается корректно

        if not enterprise_code:
            logger.error("❌ Enterprise code не найден для sourceID")
            raise ValueError("Enterprise code not found for sourceID")
        
        conversion_type = get_conversion_type()
        
        if conversion_type == "catalog":
            converted_data = convert_catalog(data, enterprise_code)
        else:
            converted_data = convert_stock(data, branch)
        
        json_file_path = save_to_json(converted_data, enterprise_code, conversion_type)

        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(converted_data, f, ensure_ascii=False, indent=4)
        
        # Отправка в process_database_service (async call)
        result = await process_database_service(json_file_path, conversion_type, enterprise_code)
        
        return result
    
    except Exception as e:
        logger.error("❌ Ошибка в unipro_convert: %s", str(e), exc_info=True)
        raise

import json
import os
import logging
import asyncio
from app.services.database_service import process_database_service

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Словарь соответствий для получения enterprise_code и branch
mapping = {
    "BCE73E60-A5C9-4FB4-9186-B6F88FDF3BDA": ("121", "34564")
}

# Переменная счетчика запусков
COUNTER_FILE = "conversion_counter.txt"

def get_conversion_type():
    """Определяет тип конвертации (catalog или stock) на основе счетчика запусков."""
    if os.path.exists(COUNTER_FILE):
        with open(COUNTER_FILE, "r") as f:
            count = int(f.read().strip())
    else:
        count = 0
    
    count += 1
    with open(COUNTER_FILE, "w") as f:
        f.write(str(count % 2))
    
    return "catalog" if count % 2 == 0 else "stock"

def get_enterprise_info(source_id):
    """Получает enterprise_code и branch из mapping по sourceID."""
    return mapping.get(source_id, (None, None))

def convert_catalog(data, enterprise_code):
    """Конвертирует данные каталога в нужный формат."""
    goods = data.get("goods", [])
    converted = [
        {
            "code": item["guid"],
            "name": item["namefull"],
            "vat": 20.0,
            "producer": "n/a",
            "barcode": item.get("barcode", ""),
            "branch_id": enterprise_code
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
            "qty": item.get("qtty", 0),
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
        
        enterprise_code, branch = get_enterprise_info(source_id)
        if not enterprise_code:
            logger.error("❌ Enterprise code не найден для sourceID")
            raise ValueError("Enterprise code not found for sourceID")
        
        conversion_type = get_conversion_type()
        
        if conversion_type == "catalog":
            converted_data = convert_catalog(data, enterprise_code)
        else:
            converted_data = convert_stock(data, branch)
        
        output_file = f"converted_{conversion_type}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(converted_data, f, ensure_ascii=False, indent=4)
        
        # Отправка в process_database_service (async call)
        result = await process_database_service(output_file, conversion_type, enterprise_code)
        
        return result
    
    except Exception as e:
        logger.error("❌ Ошибка в unipro_convert: %s", str(e), exc_info=True)
        raise

if __name__ == "__main__":
    json_file = "input.json"  # Указать путь к входному JSON
    asyncio.run(unipro_convert(json_file))
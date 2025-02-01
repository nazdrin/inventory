import sys
import os
import logging
import json
import aiohttp
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from .database import get_async_db, DeveloperSettings, EnterpriseSettings
from app.notification_service import send_notification  # Импортируем функцию для отправки уведомлений
from datetime import datetime,timezone
import pytz
local_tz = pytz.timezone('Europe/Kiev')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Получаем путь к временному каталогу из переменной окружения
TEMP_FILE_PATH = os.getenv("TEMP_FILE_PATH", "./temp_logs")

async def save_catalog_log(enterprise_code: str, formatted_json: dict):
    """Сохраняет JSON-данные стока в файл в каталоге TEMP_FILE_PATH."""
    try:
        # Определяем папку для предприятия
        catalog_folder = os.path.join(TEMP_FILE_PATH, enterprise_code)
        os.makedirs(catalog_folder, exist_ok=True)  # Создаем папку, если её нет

        # Формируем имя файла: stock_{дата}.json
        file_name = f"catalog_{datetime.now(local_tz).strftime('%Y%m%d')}.json"
        file_path = os.path.join(catalog_folder, file_name)

        # Записываем JSON в файл (перезаписываем предыдущий)
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(formatted_json, file, ensure_ascii=False, indent=4)

        logging.info(f"Stock JSON log saved for enterprise_code={enterprise_code} at {file_path}")
    except Exception as e:
        logging.error(f"Failed to save catalog JSON log for enterprise_code={enterprise_code}: {str(e)}")
        
# Словарь для преобразования идентификаторов
SUPPLIER_MAPPING = {
    "morion": 1,
    "tabletki": 2,
    "barcode": 3,
    "badm": 4,
    "optima": 5
}

# Преобразование данных в нужный формат
async def transform_data(raw_data: list, developer_settings: DeveloperSettings,enterprise_code):
    try:
        suppliers = []
        offers = []

        # Формируем Suppliers
        for supplier_key, supplier_id in SUPPLIER_MAPPING.items():
            edrpo = getattr(developer_settings, supplier_key, None)
            suppliers.append({
                "ID": str(supplier_id),
                "Name": supplier_key.capitalize(),
                "Edrpo": edrpo
            })

        # Формируем Offers
        for item in raw_data:
            supplier_codes = [
                {"ID": str(SUPPLIER_MAPPING[key]), "Code": value}
                for key, value in item.items() if key in SUPPLIER_MAPPING
            ]
            # Фильтруем SupplierCodes, чтобы удалить элементы с пустым ID, ID == 'None' или ID == '0'
            supplier_codes = [
                supplier for supplier in supplier_codes 
                if supplier["ID"] not in ["", "0", "None"] and supplier["Code"] not in ["", "None"]
            ]
            offers.append({
                "Code": item.get("code", ""),
                "Name": item.get("name", ""),
                "Producer": item.get("producer", ""),
                "VAT": item.get("vat", 0.0),
                "SupplierCodes": supplier_codes
            })

        # Возвращаем данные в нужном формате
        return {
            "Suppliers": suppliers,
            "Offers": offers
        }
    except Exception as e:
        logging.error(f"Error transforming data: {str(e)}")
        send_notification(f"Ошибка трансформации данных каталога перед отправкой {str(e)} для предприятия {enterprise_code}",enterprise_code)
        raise

# Функция для отправки данных на эндпоинт
async def post_data_to_endpoint(endpoint: str, data: dict, login: str, password: str,enterprise_code):
    try:
        headers = {"Content-Type": "application/json"}
        auth = aiohttp.BasicAuth(login, password)

        async with aiohttp.ClientSession() as session:
            async with session.post(endpoint, json=data, headers=headers, auth=auth) as response:
                response_text = await response.text()
                logging.info(f"Response from endpoint: {response.status} - {response_text}")
                return response.status, response_text
    except Exception as e:
        logging.error(f"Error posting data to endpoint: {str(e)}")
        send_notification(f"Ошибка отпраки каталога на ендпоинт {str(e)} для предприятия {enterprise_code}",enterprise_code)
        raise

# Основная функция обработки данных
async def export_catalog(enterprise_code: str, raw_data: list):
    async with get_async_db() as db:
        try:
            # Получение настроек разработчика
            result = await db.execute(select(DeveloperSettings).limit(1))
            developer_settings = result.scalar_one_or_none()
            if not developer_settings:
                logging.error("DeveloperSettings not found.")
                send_notification(f"Ошибка нет данных разработчика для отправки каталога для предприятия {enterprise_code}",enterprise_code)
                return

            # Получение настроек предприятия
            result = await db.execute(
                select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise_settings = result.scalar_one_or_none()
            if not enterprise_settings:
                logging.error(f"EnterpriseSettings not found for enterprise_code: {enterprise_code}")
                send_notification(f"Ошибка нет данных кода предприятия для отправки каталога для предприятия {enterprise_code}",enterprise_code)
                return

            # Преобразование данных
            transformed_data = await transform_data(raw_data, developer_settings,enterprise_code)

            # Вывод данных в формате JSON в консоль
            #logging.info("Transformed Data (JSON):")
            #catalog_log_file=json.dumps(transformed_data, ensure_ascii=False, indent=4)
            await save_catalog_log(enterprise_code, transformed_data)
            # Формируем URL эндпоинта
            endpoint = f"{developer_settings.endpoint_catalog}/Import/Ref/{enterprise_settings.branch_id}"
            logging.info(f"Prepared endpoint URL: {endpoint}")

            


            # Отправка данных на реальный эндпоинт
            response = await post_data_to_endpoint(endpoint,transformed_data, enterprise_settings.tabletki_login, enterprise_settings.tabletki_password,enterprise_code )
            
            #logging.info(f"Real response: {response}")
            send_notification(f"Отправка каталога на эндпоинт для предприятия {enterprise_code} произошла",enterprise_code)

        except Exception as e:
            logging.error(f"Error exporting catalog for enterprise_code={enterprise_code}: {str(e)}")
            send_notification(f"Ошибка процесса отправки каталога {str(e)} для предприятия {enterprise_code}",enterprise_code)




import sys
import os
import logging
import json
import aiohttp
from sqlalchemy.future import select
import nest_asyncio
import asyncio
from datetime import datetime,timezone
import pytz

from app.database import get_async_db, DeveloperSettings, EnterpriseSettings
from app.services.stock_update_service import update_stock  # Импортируем функцию обновления остатков
from app.services.notification_service import send_notification 
import json
local_tz = pytz.timezone('Europe/Kiev')
# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Получаем путь к временному каталогу из переменной окружения
TEMP_FILE_PATH = os.getenv("TEMP_FILE_PATH", "./temp_logs")

async def save_stock_log(enterprise_code: str, formatted_json: dict):
    """Сохраняет JSON-данные стока в файл в каталоге TEMP_FILE_PATH."""
    try:
        # Определяем папку для предприятия
        stock_folder = os.path.join(TEMP_FILE_PATH, enterprise_code)
        os.makedirs(stock_folder, exist_ok=True)  # Создаем папку, если её нет

        # Формируем имя файла: stock_{дата}.json
        file_name = f"stock_{datetime.now(local_tz).strftime('%Y%m%d')}.json"
        file_path = os.path.join(stock_folder, file_name)

        # Записываем JSON в файл (перезаписываем предыдущий)
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(formatted_json, file, ensure_ascii=False, indent=4)

        logging.info(f"Stock JSON log saved for enterprise_code={enterprise_code} at {file_path}")
    except Exception as e:
        logging.error(f"Failed to save stock JSON log for enterprise_code={enterprise_code}: {str(e)}")
        
async def process_stock_file(enterprise_code: str, stock_file: list):
    

    if not stock_file:
        logging.warning(f"Empty stock_file for enterprise_code={enterprise_code}. Skipping processing.")
        return

    if not isinstance(enterprise_code, str):
        logging.error(f"Invalid type for enterprise_code: {type(enterprise_code)}. Converting to string.")
        enterprise_code = str(enterprise_code)

    async with get_async_db() as db:
        try:
            
            query = select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
            result = await db.execute(query)
            enterprise_settings = result.scalar_one_or_none()

            if not enterprise_settings:
                logging.error(f"No settings found for enterprise_code={enterprise_code}")
                send_notification(f"Нет настоек предприятия для отправки стока для предприятия {enterprise_code}",enterprise_code)
                return

            # Apply discount_rate
            discount_rate = enterprise_settings.discount_rate or 0
            logging.info(f"Applying discount_rate={discount_rate} for enterprise_code={enterprise_code}")
            if discount_rate > 0:
                for item in stock_file:
                    if 'price_reserve' in item:
                        item['price_reserve'] = round(item['price_reserve'] * (1 - discount_rate / 100), 2)

            # Process stock_correction
            if enterprise_settings.stock_correction:
                
                stock_file = await update_stock(stock_file, enterprise_code)

            # Преобразуем данные в нужный формат
            branches_data = {}
            for item in stock_file:
    # Преобразуем 'branch' в целое число, если это возможно
                if isinstance(item['branch'], float) and item['branch'].is_integer():
                    item['branch'] = str(int(item['branch']))
                elif isinstance(item['branch'], str) and item['branch'].replace('.', '', 1).isdigit():
                    item['branch'] = str(int(float(item['branch'])))
                else:
                    item['branch'] = None
                
                item['code'] = str(int(item['code'])) if isinstance(item['code'], float) and item['code'].is_integer() else item['code']
                # Проверяем, есть ли уже ветка в данных
                branch_code = str(item['branch'])
                if branch_code not in branches_data:
                    branches_data[branch_code] = {
                        "Code": branch_code,
                        "Rests": [],
                        "DateTime": datetime.now(timezone.utc).astimezone(local_tz).strftime("%d.%m.%Y %H:%M:%S")  # Добавляем дату и время
                    }
                # Добавляем остатки для каждой ветки
                branches_data[branch_code]['Rests'].append({
                    "Code": str(item['code']),
                    "Price": item['price'],
                    "Qty": item['qty'],
                    "PriceReserve": item['price_reserve']
                })

            # Форматируем данные как JSON
            formatted_json = {
                "Branches": list(branches_data.values())
            }
            # Форматируем и выводим данные JSON для удобства в терминале
            logging.info(f"Formatted JSON data for enterprise_code={enterprise_code}: {json.dumps(formatted_json, indent=4)}")
            # Сохраняем JSON вместо вывода в лог
            await save_stock_log(enterprise_code, formatted_json)

            # Отправляем данные на эндпоинт
            result = await db.execute(select(DeveloperSettings).limit(1))
            developer_settings = result.scalar_one_or_none()
            if not developer_settings:
                logging.error("No developer settings found.")
                send_notification(f"Нет настоек разработчика для отправки стока для предприятия {enterprise_code}",enterprise_code)
                return []

            endpoint = f"{developer_settings.endpoint_stock}/Import/Rests"
            logging.info(f"Prepared endpoint URL: {endpoint}")
                
            login = enterprise_settings.tabletki_login
            password = enterprise_settings.tabletki_password
            # Отправляем данные на указанный эндпоинт
            await send_to_endpoint(endpoint, formatted_json, login, password,enterprise_code)

            logging.info(f"Stock file for enterprise_code={enterprise_code} processed successfully.")
            send_notification(f"Отправка на ендпоинт стока прошла успешно для предприятия {enterprise_code}",enterprise_code)

        except Exception as e:
            logging.exception(f"Error processing stock file for enterprise_code={enterprise_code}: {str(e)}")
            send_notification(f"Ошибка процесса отправки стока{str(e)} для предприятия {enterprise_code}",enterprise_code)
async def send_to_endpoint(endpoint: str, data: list, login: str, password: str,enterprise_code):
    """
    Отправляет данные на указанный эндпоинт.
    """
    try:
        headers = {"Content-Type": "application/json"}
        auth = aiohttp.BasicAuth(login, password)

        async with aiohttp.ClientSession() as session:
            async with session.post(endpoint, json=data, headers=headers, auth=auth) as response:
                response_text = await response.text()
                logging.info(f"Response from endpoint: {response.status} - {response_text}")
                return response.status, response_text
    except Exception as e:
        logging.error(f"Error sending data to endpoint: {str(e)}")
        send_notification(f"Ошибка  отправки стока на ендпоинт {str(e)} для предприятия {enterprise_code}",enterprise_code)
        raise


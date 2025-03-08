import os
import logging
import json
import aiohttp
from sqlalchemy.future import select
import asyncio
from datetime import datetime, timezone
import pytz

from app.database import get_async_db, DeveloperSettings, EnterpriseSettings
from app.services.notification_service import send_notification 

local_tz = pytz.timezone('Europe/Kiev')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TEMP_FILE_PATH = os.getenv("TEMP_FILE_PATH", "./temp_logs")

async def save_stock_log(enterprise_code: str, formatted_json: dict):
    """Сохраняет JSON-данные стока в файл в каталоге TEMP_FILE_PATH."""
    try:
        stock_folder = os.path.join(TEMP_FILE_PATH, enterprise_code)
        os.makedirs(stock_folder, exist_ok=True)

        file_name = "stock.json"
        file_path = os.path.join(stock_folder, file_name)

        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(formatted_json, file, ensure_ascii=False, indent=4)

        logging.info(f"Stock JSON log saved for enterprise_code={enterprise_code} at {file_path}")
    except Exception as e:
        logging.error(f"Failed to save stock JSON log for enterprise_code={enterprise_code}: {str(e)}")

async def process_stock_file(enterprise_code: str, stock_file: list):
    """Форматирует данные и отправляет их на API."""

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
                send_notification(f"Нет настроек предприятия для отправки стока {enterprise_code}", enterprise_code)
                return

            # Преобразуем данные в нужный формат
            branches_data = {}
            for item in stock_file:
                item['branch'] = str(int(float(item['branch']))) if isinstance(item['branch'], float) else str(item['branch'])
                item['code'] = str(int(item['code'])) if isinstance(item['code'], float) else str(item['code'])

                branch_code = str(item['branch'])
                if branch_code not in branches_data:
                    branches_data[branch_code] = {
                        "Code": branch_code,
                        "Rests": [],
                        "DateTime": datetime.now(timezone.utc).astimezone(local_tz).strftime("%d.%m.%Y %H:%M:%S")
                    }

                branches_data[branch_code]['Rests'].append({
                    "Code": str(item['code']),
                    "Price": item['price'],
                    "Qty": item['qty'],
                    "PriceReserve": item['price_reserve']
                })

            formatted_json = {"Branches": list(branches_data.values())}
            logging.info(f"Formatted JSON data for enterprise_code={enterprise_code}: {json.dumps(formatted_json, indent=4)}")

            await save_stock_log(enterprise_code, formatted_json)

            result = await db.execute(select(DeveloperSettings).limit(1))
            developer_settings = result.scalar_one_or_none()
            if not developer_settings:
                logging.error("No developer settings found.")
                send_notification(f"Нет настроек разработчика для отправки стока {enterprise_code}", enterprise_code)
                return

            endpoint = f"{developer_settings.endpoint_stock}/Import/Rests"
            login = enterprise_settings.tabletki_login
            password = enterprise_settings.tabletki_password

            await send_to_endpoint(endpoint, formatted_json, login, password, enterprise_code)

            logging.info(f"Stock file for enterprise_code={enterprise_code} processed successfully.")
            send_notification(f"Отправка стока на API прошла успешно для {enterprise_code}", enterprise_code)

        except Exception as e:
            logging.exception(f"Error processing stock file for {enterprise_code}: {str(e)}")
            send_notification(f"Ошибка отправки стока для {enterprise_code}: {str(e)}", enterprise_code)

async def send_to_endpoint(endpoint: str, data: list, login: str, password: str, enterprise_code):
    """Отправляет данные на указанный API-эндпоинт."""
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
        send_notification(f"Ошибка отправки стока на API {enterprise_code}: {str(e)}", enterprise_code)
        raise
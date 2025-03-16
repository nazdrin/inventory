import asyncio
import json
import logging
import aiohttp  
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings 
from app.services.notification_service import send_notification 

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def api_call(branch: str, endpoint_orders: str, login: str, password: str):
    """
    Функция для реального вызова API.
    :param branch: Код филиала.
    :param endpoint_orders: URL для API.
    :param login: Логин для API.
    :param password: Пароль для API.
    :return: Ответ от API.
    """
    url = f"{endpoint_orders}/api/orders/{branch}/4"
    headers = {"Content-Type": "application/json"}
    auth = aiohttp.BasicAuth(login, password)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, auth=auth) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    logging.error(f"Error from API for branch {branch}: {response.status}")
                    return []
    except Exception as e:
        logging.error(f"Error calling API for branch {branch}: {e}")
        return []

async def update_stock(stock_data, enterprise_code):
    """
    Основная функция обработки стока. Вызывается из stock_export_service.py.
    :param stock_data: Входной JSON с данными стока.
    :param enterprise_code: Код предприятия.
    :return: Обновленный JSON файл.
    """
    logging.info(f"Запуск обработки стока для enterprise_code={enterprise_code}...")

    updated_data = []
    async with get_async_db() as db:
        try:
            # Получаем настройки предприятия
            logging.info(f"Fetching settings for enterprise_code={enterprise_code}")
            query = select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
            result = await db.execute(query)
            enterprise_settings = result.scalar_one_or_none()

            if not enterprise_settings:
                logging.error(f"No settings found for enterprise_code={enterprise_code}")
                send_notification(f"Нет настроек для получения заказов для предприятия {enterprise_code}", enterprise_code)
                return []

            # Получаем настройки разработчика для endpoint
            query = select(DeveloperSettings).limit(1)
            result = await db.execute(query)
            developer_settings = result.scalar_one_or_none()

            if not developer_settings:
                logging.error("No developer settings found.")
                send_notification(f"Нет настроек разработчика для получения заказов для предприятия {enterprise_code}", enterprise_code)
                return []

            # Получаем endpoint для API
            endpoint_orders = developer_settings.endpoint_orders
            login = enterprise_settings.tabletki_login
            password = enterprise_settings.tabletki_password

            # Собираем уникальные филиалы
            branches = {record["branch"] for record in stock_data}
            logging.info(f"Уникальные филиалы для обработки: {branches}")

            # Выполняем запрос для каждого уникального branch
            for branch in branches:
                logging.info(f"Запрос API для branch {branch}...")
                api_response = await api_call(branch, endpoint_orders, login, password)

                if api_response:
                    pass
                else:
                    logging.warning(f"Нет данных в ответе от API для branch {branch}")

                # Обрабатываем все записи для этого branch
                for record in stock_data:
                    if record["branch"] == branch:
                        code = record["code"]
                        qty = record["qty"]

                        # Ищем товар в ответе API
                        updated_qty = qty  # Начинаем с исходного количества
                        found_in_api = False  # Флаг для проверки наличия товара в ответе API

                        for api_record in api_response:
                            for row in api_record.get("rows", []):  # Итерируемся по списку товаров внутри 'rows'
                                goods_code = row.get("goodsCode")
                                
                                if str(goods_code) == str(code):  # Сравниваем товары по их кодам
                                    api_qty = float(row.get("qty", 0))
                                    updated_qty = max(qty - api_qty, 0)  # Количество не может быть отрицательным
                                    break 
                            else:
                                pass
                        if not found_in_api:
                            pass
                        updated_record = record.copy()
                        updated_record["qty"] = updated_qty  # Обновляем количество товара
                        updated_data.append(updated_record)

            logging.info("Обновление стока завершено.")
            return updated_data
        except Exception as e:
            logging.exception(f"Error updating stock for enterprise_code={enterprise_code}: {str(e)}")
            send_notification(f"Ошибка процесса получения заказов для предприятия - {enterprise_code}", enterprise_code)
            return []
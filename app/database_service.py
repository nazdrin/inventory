import logging
import os
from app.models import InventoryData, InventoryStock, EnterpriseSettings
from app.database import get_async_db
from sqlalchemy.ext.asyncio import AsyncSession
import json
from datetime import datetime
from sqlalchemy.future import select
from sqlalchemy.exc import NoResultFound
from app.cleanup_service import cleanup_old_data
from app.catalog_export_service import export_catalog  # Импорт функции экспорта
from app.stock_export_service import process_stock_file
from app.notification_service import send_notification  # Импортируем функцию для отправки уведомлений


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Лог в консоль
    ]
)

# Устанавливаем более высокий уровень логирования для консоли
console_handler = logging.StreamHandler()
#console_handler.setLevel(logging.WARNING)  # В консоли только WARNING и ERROR
logging.getLogger().addHandler(console_handler)# Настройка логирования
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def process_database_service(file_path: str, data_type: str, enterprise_code: str):
    """
    Обрабатывает данные из JSON и записывает их в базу данных.
    :param file_path: Путь к JSON-файлу
    :param data_type: Тип данных ('catalog' или 'stock')
    :param enterprise_code: Код предприятия для обновления времени загрузки
    """
    logging.info(f"Тип и значение enterprise_code в началеprocess_database_service: {type(enterprise_code)} - {enterprise_code}")
    async with get_async_db() as session:  # Создание сессии базы данных
        await cleanup_old_data(session)
        try:
            # Загрузка данных из файла
            with open(file_path, "r", encoding="utf-8") as json_file:
                raw_data = json.load(json_file)

                        # Очистка данных: удаление пробелов и приведение ключей к нижнему регистру
            cleaned_data = clean_json_keys(raw_data)

            if data_type == "catalog":
                # Вызов функции экспорта каталога перед сохранением данных
                try:
                    logging.info(f"Инициация экспорта каталога для предприятия {enterprise_code}")
                    await export_catalog(enterprise_code, raw_data)
                    logging.info(f"Каталог успешно экспортирован для предприятия {enterprise_code}")
                except Exception as export_error:
                    logging.error(f"Ошибка экспорта каталога для предприятия {enterprise_code}: {export_error}")
                    send_notification(f"Ошибка экспорта каталога для предприятия {enterprise_code}: {export_error}",enterprise_code)
                    raise

                # Сохранение данных в базу
                await save_catalog_data(cleaned_data, session)
                await update_last_upload(session, enterprise_code, "catalog")

            elif data_type == "stock":
                # Вызов функции экспорта стока перед сохранением данных
                try:
                    
                    await process_stock_file(enterprise_code, raw_data)
                    logging.info(f"Сток успешно экспортирован для предприятия {enterprise_code}")
                except Exception as export_error:
                    logging.error(f"Ошибка экспорта стока для предприятия {enterprise_code}: {export_error}")
                    send_notification(f"Ошибка экспорта стока для предприятия {enterprise_code}: {export_error}",enterprise_code)
                    raise

                # Сохранение данных в базу
                await save_stock_data(cleaned_data, session)
                await update_last_upload(session, enterprise_code, "stock")
            
            else:
                raise ValueError(f"Неизвестный тип данных: {data_type}")

            await session.commit()
            logging.info(f"Данные {data_type} успешно записаны в базу данных.")

        except Exception as e:
            logging.error(f"Ошибка записи данных в базу: {str(e)}")
            send_notification(f"Ошибка записи данных в базу: {str(e)} предприятия {enterprise_code}",enterprise_code)
            await session.rollback()
            raise

def clean_json_keys(data: list):
    """
    Удаляет пробелы и приводит ключи в JSON к нижнему регистру.
    :param data: Исходные данные в виде списка словарей
    :return: Данные с очищенными ключами
    """
    cleaned_data = []
    for record in data:
        cleaned_record = {k.strip().lower(): v for k, v in record.items()}
        cleaned_data.append(cleaned_record)
    return cleaned_data


async def save_catalog_data(data: list, session: AsyncSession):
    """
    Сохраняет данные каталога в таблицу InventoryData.
    :param data: Список записей каталога
    :param session: Сессия базы данных
    """
    for record in data:
        session.add(InventoryData(**record))


async def save_stock_data(data: list, session: AsyncSession):
    """
    Сохраняет данные остатков в таблицу InventoryStock.
    :param data: Список записей остатков
    :param session: Сессия базы данных
    """
    for record in data:
        session.add(InventoryStock(**record))


async def update_last_upload(session: AsyncSession, enterprise_code: str, data_type: str):
    """
    Обновляет поля last_stock_upload или last_catalog_upload в таблице EnterpriseSettings.
    :param session: Сессия базы данных
    :param enterprise_code: Код предприятия
    :param data_type: Тип данных ('catalog' или 'stock')
    """
    try:
        current_time = datetime.utcnow()

        # Используем асинхронный запрос для получения данных
        stmt = select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        result = await session.execute(stmt)
        enterprise_settings = result.scalars().one_or_none()

        if not enterprise_settings:
            raise ValueError(f"Предприятие с кодом {enterprise_code} не найдено.")

        # Обновляем соответствующее поле времени загрузки
        if data_type == "catalog":
            enterprise_settings.last_catalog_upload = current_time
        elif data_type == "stock":
            enterprise_settings.last_stock_upload = current_time

        await session.commit()  # Сохраняем изменения
        

    except Exception as e:
        await session.rollback()
        logging.error(f"Ошибка обновления времени загрузки: {str(e)}")
        send_notification(f"Ошибка обновления времени загрузки: {str(e)} предприятия {enterprise_code}",enterprise_code)
        raise
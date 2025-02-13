import os
import asyncio
import logging
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.sql import func
from datetime import datetime, timedelta, timezone
import pytz

# Установить временную зону в UTC
os.environ['TZ'] = 'UTC'
KIEV_TZ = pytz.timezone("Europe/Kiev")
now = datetime.now(tz=pytz.utc).astimezone(KIEV_TZ)

# Импорт сервисов
from app.dntrade_data_service.stock_fetch_convert import run_service
from app.google_drive.google_drive_service import extract_stock_from_google_drive
from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification  # Импортируем функцию для отправки уведомлений

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


async def create_error_report(error_message: str, enterprise_code: str):
    """Создание отчета об ошибках"""
    file_name = f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(file_name, 'a') as file:
        file.write(f"{datetime.now()} - Enterprise Code: {enterprise_code} - Error: {error_message}\n")
    logging.info(f"Ошибка сохранена в файл: {file_name}")


async def get_enterprises_for_stock(db: AsyncSession):
    """Получение списка предприятий, для которых необходимо обновить остатки."""
    try:
        db.expire_all()  # Очистка кеша SQLAlchemy перед выполнением запросов
        now = datetime.now(tz=timezone.utc).astimezone(KIEV_TZ)

        # Получение всех предприятий
        result = await db.execute(select(EnterpriseSettings))
        enterprises = result.scalars().all()

        filtered_enterprises = []
        for enterprise in enterprises:
            last_upload = enterprise.last_stock_upload
            upload_frequency = enterprise.stock_upload_frequency

            if not upload_frequency or upload_frequency <= 0:
                logging.warning(
                    f"Пропуск предприятия с кодом={enterprise.enterprise_code}: Неверная частота загрузки."
                )
                send_notification(f"Пропуск предприятия {enterprise.enterprise_code}: Неверная частота загрузки.", enterprise.enterprise_code)
                continue

            # Обработка времени загрузки
            if last_upload and last_upload.tzinfo is None:
                last_upload = last_upload.replace(tzinfo=timezone.utc)
            last_upload_kiev = last_upload.astimezone(KIEV_TZ) if last_upload else None

            # Расчет следующего времени загрузки
            next_upload_time = (last_upload_kiev + timedelta(minutes=upload_frequency)
                                if last_upload_kiev else now)

            if next_upload_time <= now:
                logging.info(
                    f"Добавлено предприятие {enterprise.enterprise_code} в очередь обновления остатков."
                )
                filtered_enterprises.append(enterprise)
            else:
                logging.info(
                    f"Пропуск предприятия {enterprise.enterprise_code}: время обновления еще не наступило."
                )
        return filtered_enterprises

    except Exception as e:
        logging.error(f"Ошибка при получении списка предприятий для обновления остатков: {str(e)}")
        send_notification(f"Ошибка при обработке предприятий в планировщике остатков: {str(e)}", "unknown")
        return []


async def process_stock_for_enterprise(db: AsyncSession, enterprise: EnterpriseSettings):
    """Обработка остатков для предприятия."""
    try:
        if enterprise.data_format == "Dntrade":
            # Запуск обработки через run_service
            await run_service(enterprise.enterprise_code)
            logging.info(f"Обработаны остатки для предприятия {enterprise.enterprise_code} (dnttrade).")
        elif enterprise.data_format == "GoogleDrive":
            # Запуск обработки через extract_stock_from_google_drive
            await extract_stock_from_google_drive (enterprise.enterprise_code)
            logging.info(f"Обработаны остатки для предприятия {enterprise.enterprise_code} (Google Drive).")
        elif enterprise.data_format == "Unipro":
            pass
        elif enterprise.data_format == "Blank":
            pass
        else:
            logging.warning(f"Неподдерживаемый формат данных для предприятия {enterprise.enterprise_code}.")
            send_notification(f"Ошибка обработки остатков: неподдерживаемый формат данных ({enterprise.enterprise_code}).", enterprise.enterprise_code)
    except Exception as e:
        logging.error(f"Ошибка обработки остатков для предприятия {enterprise.enterprise_code}: {str(e)}")
        send_notification(f"Ошибка при обработке остатков для предприятия {enterprise.enterprise_code}: {str(e)}", enterprise.enterprise_code)
        await create_error_report(str(e), enterprise.enterprise_code)
async def schedule_stock_tasks():
    """Асинхронное расписание для обработки остатков."""
    try:
        async with get_async_db() as db:
            interval = 1  # Интервал выполнения расписания в минутах
            while True:
                logging.info("Запуск планировщика обновления остатков...")
                enterprises = await get_enterprises_for_stock(db)
                if enterprises:
                    tasks = [process_stock_for_enterprise(db, e) for e in enterprises]
                    await asyncio.gather(*tasks)
                else:
                    logging.warning("Нет предприятий для обновления остатков.")
                await asyncio.sleep(interval * 60)  # Ожидание перед следующей итерацией
    except Exception as main_error:
        logging.error(f"Критическая ошибка в планировщике остатков: {str(main_error)}")
        send_notification(f"Критическая ошибка в планировщике остатков: {str(main_error)}", "unknown")
    finally:
        logging.error("Сервис stock_scheduler неожиданно остановлен.")
        send_notification("Сервис stock_scheduler неожиданно остановлен. Проверьте логи.", "stock_scheduler")


if __name__ == "__main__":
    asyncio.run(schedule_stock_tasks())
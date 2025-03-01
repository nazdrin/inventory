import os
import asyncio
import logging
import time
import pytz
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import OperationalError

# Установка временной зоны Киев
os.environ['TZ'] = 'UTC'
KIEV_TZ = pytz.timezone("Europe/Kiev")

# Импорт сервисов
from app.dntrade_data_service.stock_fetch_convert import run_service
from app.prom_data_service.prom_stock import run_prom
from app.google_drive.google_drive_service import extract_stock_from_google_drive
from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Словарь для вызова соответствующих обработчиков
PROCESSORS = {
    "Dntrade": run_service,
    "Prom": run_prom,
    "GoogleDrive": extract_stock_from_google_drive,
}

async def notify_error(message: str, enterprise_code: str = "unknown"):
    logging.error(message)
    send_notification(message, enterprise_code)

async def create_error_report(error_message: str, enterprise_code: str):
    file_name = f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(file_name, 'a') as file:
        file.write(f"{datetime.now()} - Enterprise Code: {enterprise_code} - Error: {error_message}\n")
    logging.info(f"Ошибка сохранена в файл: {file_name}")

async def get_enterprises_for_stock(db: AsyncSession):
    """Получение списка предприятий для обновления остатков."""
    try:
        db.expire_all()
        now = datetime.now(tz=timezone.utc).astimezone(KIEV_TZ)
        logging.info(f"Текущее время: {now} [Timezone: {now.tzinfo}]")

        start_time = time.time()
        result = await db.execute(select(EnterpriseSettings))
        enterprises = result.scalars().all()
        logging.info(f"SQL-запрос выполнен за {time.time() - start_time:.2f} секунд")

        return [
            enterprise for enterprise in enterprises
            if enterprise.stock_upload_frequency and enterprise.stock_upload_frequency > 0 and
            ((enterprise.last_stock_upload.astimezone(KIEV_TZ) + timedelta(minutes=enterprise.stock_upload_frequency))
             if enterprise.last_stock_upload else now) <= now
        ]
    except Exception as e:
        await notify_error(f"Ошибка при получении списка предприятий для обновления остатков: {str(e)}")
        return []

async def process_stock_for_enterprise(db: AsyncSession, enterprise: EnterpriseSettings):
    try:
        processor = PROCESSORS.get(enterprise.data_format)
        if processor:
            await processor(enterprise.enterprise_code)
            logging.info(f"Обработаны остатки для предприятия {enterprise.enterprise_code} ({enterprise.data_format}).")
        elif enterprise.data_format in ["Unipro", "Blank"]:
            logging.info(f"Пропуск обработки остатков для предприятия {enterprise.enterprise_code} ({enterprise.data_format}).")
        else:
            logging.warning(f"Неподдерживаемый формат данных для предприятия {enterprise.enterprise_code}.")
    except Exception as e:
        await notify_error(f"Ошибка обработки остатков для предприятия {enterprise.enterprise_code}: {str(e)}", enterprise.enterprise_code)
        await create_error_report(str(e), enterprise.enterprise_code)

async def schedule_stock_tasks():
    try:
        async with get_async_db() as db:
            interval = 1  # Интервал выполнения расписания в минутах
            while True:
                logging.info("Запуск планировщика обновления остатков...")
                enterprises = await get_enterprises_for_stock(db)
                
                for enterprise in enterprises:
                    await process_stock_for_enterprise(db, enterprise)
                
                await asyncio.sleep(interval * 60)
    except Exception as main_error:
        await notify_error(f"Критическая ошибка в планировщике остатков: {str(main_error)}")
    finally:
        await notify_error("Сервис stock_scheduler неожиданно остановлен.", "stock_scheduler")

if __name__ == "__main__":
    asyncio.run(schedule_stock_tasks())

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
from sqlalchemy.sql import text

# Установка временной зоны Киев
os.environ['TZ'] = 'UTC'
KIEV_TZ = pytz.timezone("Europe/Kiev")

# Импорт сервисов
from app.dntrade_data_service.fetch_convert import run_service
from app.checkbox_data_service.checkbox_catalog_conv import run_service as run_checkbox
from app.rozetka_data_service.rozetka_catalog_conv import run_service as run_rozetka
from app.dsn_data_service.dsn_catalog_conv import run_service as run_dsn
from app.prom_data_service.prom_catalog import run_prom
from app.ftp_data_service.ftp_catalog_conv import run_service as run_ftp
from app.key_crm_data_service.key_crm_catalog_conv import run_service as run_key_crm
from app.google_drive.google_drive_service import extract_catalog_from_google_drive
from app.jetvet_data_service.jetvet_google_drive import extract_catalog_from_google_drive as catalog_jetvet
from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Словарь для вызова соответствующих обработчиков
PROCESSORS = {
    "Dntrade": run_service,
    "Prom": run_prom,
    "GoogleDrive": extract_catalog_from_google_drive,
    "JetVet": catalog_jetvet,
    "Checkbox": run_checkbox,
    "Rozetka": run_rozetka,
    "Dsn": run_dsn,
    "KeyCRM": run_key_crm,
    "Ftp": run_ftp,
}

async def notify_error(message: str, enterprise_code: str = "unknown"):
    logging.error(message)
    send_notification(message, enterprise_code)

async def create_error_report(error_message: str, enterprise_code: str):
    file_name = f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(file_name, 'a') as file:
        file.write(f"{datetime.now()} - Enterprise Code: {enterprise_code} - Error: {error_message}\n")
    logging.info(f"Ошибка создания отчета об ошибках: {file_name}")

async def get_enterprises_for_catalog(db: AsyncSession):
    """Получает список предприятий для обработки каталога."""
    try:
        # Проверка соединения с БД
        try:
            
            await db.execute(text("SELECT 1"))
        except OperationalError:
            await notify_error("Соединение с базой данных закрыто, попытка восстановления...")
            await db.rollback()
            return []

        db.expire_all()
        now = datetime.now(tz=timezone.utc).astimezone(KIEV_TZ)
        logging.info(f"Текущее время: {now} [Timezone: {now.tzinfo}]")

        start_time = time.time()
        result = await db.execute(select(EnterpriseSettings))
        enterprises = result.scalars().all()
        logging.info(f"SQL-запрос выполнен за {time.time() - start_time:.2f} секунд")

        if not enterprises:
            logging.warning("В базе данных нет предприятий для обработки.")

        return [
            enterprise for enterprise in enterprises
            if enterprise.catalog_upload_frequency and enterprise.catalog_upload_frequency > 0 and
            ((enterprise.last_catalog_upload.astimezone(KIEV_TZ) + timedelta(minutes=enterprise.catalog_upload_frequency))
            if enterprise.last_catalog_upload else now) <= now
        ]
    except Exception as e:
        await notify_error(f"Ошибка при обработке предприятий в планировщике каталога: {str(e)}")
        await db.rollback()
        return []

async def process_catalog_for_enterprise(db: AsyncSession, enterprise: EnterpriseSettings):
    try:
        processor = PROCESSORS.get(enterprise.data_format)
        if processor:
            await processor(enterprise.enterprise_code)
            logging.info(f"Service run successfully for Enterprise Code={enterprise.enterprise_code} with data format '{enterprise.data_format}'")
        elif enterprise.data_format in ["Unipro", "Blank"]:
            logging.info(f"Skipping processing for Enterprise Code={enterprise.enterprise_code} with data format '{enterprise.data_format}'")
        else:
            logging.warning(f"Unsupported data format or transfer method for Enterprise Code={enterprise.enterprise_code}")

    except Exception as e:
        await notify_error(f"Ошибка обработки данных для предприятия {enterprise.enterprise_code}: {str(e)}", enterprise.enterprise_code)
        await create_error_report(str(e), enterprise.enterprise_code)

async def schedule_catalog_tasks():
    try:
        async with get_async_db() as db:
            interval = 1  # Интервал выполнения расписания в минутах
            while True:
                logging.info("Starting catalog scheduler loop...")
                enterprises = await get_enterprises_for_catalog(db)

                for enterprise in enterprises:
                    await process_catalog_for_enterprise(db, enterprise)

                await asyncio.sleep(interval * 60)
    except Exception as main_error:
        await notify_error(f"🔥 Критическая ошибка в планировщике: {str(main_error)}")
    finally:
        await notify_error("🔴 Сервис catalog_scheduler неожиданно остановлен.", "catalog_scheduler")
        
if __name__ == "__main__":
    asyncio.run(schedule_catalog_tasks())

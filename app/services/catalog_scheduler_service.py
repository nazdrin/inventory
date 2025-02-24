import os
import asyncio
import logging
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.sql import func
from datetime import datetime, timedelta, timezone
import pytz
os.environ['TZ'] = 'UTC'
KIEV_TZ = pytz.timezone("Europe/Kiev")
now = datetime.now(tz=pytz.utc).astimezone(KIEV_TZ)

# Импорт сервисов
from app.dntrade_data_service.fetch_convert import run_service
from app.prom_data_service.prom_catalog import run_prom
from app.google_drive.google_drive_service import extract_catalog_from_google_drive
from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification  # Импортируем функцию для отправки уведомлений

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Настройка временной зоны (Europe/Kiev)
KIEV_TZ = pytz.timezone("Europe/Kiev")

# Функция для создания отчета об ошибках
async def create_error_report(error_message: str, enterprise_code: str):
    file_name = f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(file_name, 'a') as file:
        file.write(f"{datetime.now()} - Enterprise Code: {enterprise_code} - Error: {error_message}\n")
    logging.info(f"Ошибка создания отчета об ошибках: {file_name}")

# Обновленная функция для получения предприятий с учетом нового условия
async def get_enterprises_for_catalog(db: AsyncSession):
    try:
        # Очистка кеша SQLAlchemy перед выполнением запросов
        db.expire_all()

        now = datetime.now(tz=timezone.utc).astimezone(KIEV_TZ)  # Текущее время с учетом часового пояса
        logging.info(f"Текущее время (now): {now} [Timezone: {now.tzinfo}]")

        # Выполнение запроса к базе данных
        result = await db.execute(select(EnterpriseSettings))
        enterprises = result.scalars().all()

        filtered_enterprises = []
        for enterprise in enterprises:
            last_upload = enterprise.last_catalog_upload
            upload_frequency = enterprise.catalog_upload_frequency

            if not upload_frequency or upload_frequency <= 0:
                logging.warning(
                    f"Пропуск предприятия с кодом={enterprise.enterprise_code}: Неверная частота загрузки."
                )
                send_notification(f"Пропуск предприятия с кодом  {enterprise.enterprise_code} .", enterprise.enterprise_code)
                continue

            # Обработка времени загрузки
            if last_upload and last_upload.tzinfo is None:
                last_upload = last_upload.replace(tzinfo=timezone.utc)
            last_upload_kiev = last_upload.astimezone(KIEV_TZ) if last_upload else None

            # Расчет следующего времени загрузки
            next_upload_time = (last_upload_kiev + timedelta(minutes=upload_frequency)
                                if last_upload_kiev else now)

            logging.info(
                f"Enterprise Code={enterprise.enterprise_code} | "
                f"Last Upload={last_upload_kiev}, Next Upload Time={next_upload_time}, Current Time={now}"
            )

            if next_upload_time <= now:
                logging.info(
                    f"Adding Enterprise Code={enterprise.enterprise_code}: Ready for Upload."
                )
                filtered_enterprises.append(enterprise)
            else:
                logging.info(
                    f"Skipping Enterprise Code={enterprise.enterprise_code}: Not Time Yet."
                )
        return filtered_enterprises

    except Exception as e:
        logging.error(f"Ошибка при обработке предприятий в планировщике каталога: {str(e)}")
        send_notification(f"Ошибка при обработке предприятий в планировщике каталога: {str(e)}", "пусто")
        return []
# Обновленная функция обработки каталога для предприятий
async def process_catalog_for_enterprise(db: AsyncSession, enterprise: EnterpriseSettings):
    try:
        # Определяем тип обработки в зависимости от формата данных
        if enterprise.data_format == "Dntrade":
            # Обработка с помощью run_service
            await run_service(enterprise.enterprise_code)
            logging.info(f"Service run successfully for Enterprise Code={enterprise.enterprise_code} with data format 'dnttrade'")
        elif enterprise.data_format == "Prom":
            # Обработка с помощью run_service
            await run_prom(enterprise.enterprise_code)
            logging.info(f"Service run successfully for Enterprise Code={enterprise.enterprise_code} with data format 'prom'")
        elif enterprise.data_format == "GoogleDrive":
            # Обработка с помощью extract_catalog_from_google_drive
            await extract_catalog_from_google_drive(enterprise.enterprise_code)
            logging.info(f"Catalog extracted successfully for Enterprise Code={enterprise.enterprise_code} with data transfer method 'googledrive'")
        elif enterprise.data_format == "Unipro":
            pass
        elif enterprise.data_format == "Blank":
            pass

        else:
            logging.warning(f"Unsupported data format or transfer method for Enterprise Code={enterprise.enterprise_code}")
            
    except Exception as e:
        logging.error(f"Error processing catalog for Enterprise Code={enterprise.enterprise_code}: {str(e)}")
        send_notification(f"Ошибка обработки данных для предприятия {enterprise.enterprise_code} .", enterprise.enterprise_code)
        await create_error_report(str(e), enterprise.enterprise_code)

# Асинхронное расписание для обработки каталогов
async def schedule_catalog_tasks():
    try:
        async with get_async_db() as db:
            interval = 1  # Интервал выполнения расписания в минутах
            while True:
                logging.info("Starting catalog scheduler loop...")
                enterprises = await get_enterprises_for_catalog(db)
                if enterprises:
                    tasks = [process_catalog_for_enterprise(db, e) for e in enterprises]
                    await asyncio.gather(*tasks)
                else:
                    logging.warning("Предприятия для обработки каталога не найдены.")
                    #send_notification("Предприятия для обработки каталога не найдены.","Пусто")
                await asyncio.sleep(interval * 60)  # Ожидание перед следующей итерацией
    except Exception as main_error:
        logging.error(f"Critical error in catalog scheduler: {str(main_error)}")
        send_notification(f"Критический сбой запуска расписания для каталога {str(main_error)}", "unknown")

# Точка входа
if __name__ == "__main__":
    asyncio.run(schedule_catalog_tasks())

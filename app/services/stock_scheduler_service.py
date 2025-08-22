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

os.environ['TZ'] = 'UTC'
KIEV_TZ = pytz.timezone("Europe/Kiev")

from app.dntrade_data_service.stock_fetch_convert import run_service
from app.checkbox_data_service.checkbox_stock_conv import run_service as run_checkbox
from app.rozetka_data_service.rozetka_stock_conv import run_service as run_rozetka
from app.key_crm_data_service.key_crm_stock_conv import run_service as run_key_crm
from app.dsn_data_service.dsn_stock_conv import run_service as run_dsn
from app.ftp_data_service.ftp_stock_conv import run_service as run_ftp
from app.prom_data_service.prom_stock import run_prom
from app.torgsoft_google_data_service.torgsoft_google_drive import run_torgsoft_google
from app.hprofit_data_service.hprofit_conv import run_service as run_hprofit
from app.ftp_tabletki_data_service.ftp_tabletki_conv import run_service as run_ftp_tabletki
from app.google_drive.google_drive_service import extract_stock_from_google_drive
from app.jetvet_data_service.jetvet_google_drive import extract_stock_from_google_drive as stock_jetvet
from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification
#from app.services.auto_confirm import main as auto_confirm_main
from app.services.order_fetcher import fetch_orders_for_enterprise 

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Словарь для вызова соответствующих обработчиков
PROCESSORS = {
    "Dntrade": run_service,
    "Prom": run_prom,
    "GoogleDrive": extract_stock_from_google_drive,
    "JetVet": stock_jetvet,
    "Checkbox": run_checkbox,
    "Rozetka": run_rozetka,
    "Dsn": run_dsn,
    "KeyCRM": run_key_crm,
    "Ftp": run_ftp,
    "HProfit": run_hprofit,
    "FtpTabletki": run_ftp_tabletki,
    "TorgsoftGoogle": run_torgsoft_google,
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
            await processor(enterprise.enterprise_code, "stock")
            logging.info(f"Обработаны остатки для предприятия {enterprise.enterprise_code} ({enterprise.data_format}).")
        elif enterprise.data_format in ["Unipro", "Blank"]:
            logging.info(f"Пропуск обработки остатков для предприятия {enterprise.enterprise_code} ({enterprise.data_format}).")
        else:
            logging.warning(f"Неподдерживаемый формат данных для предприятия {enterprise.enterprise_code}.")
    except Exception as e:
        await notify_error(f"Ошибка обработки остатков для предприятия {enterprise.enterprise_code}: {str(e)}", enterprise.enterprise_code)
        await create_error_report(str(e), enterprise.enterprise_code)

async def schedule_stock_tasks():
    """
    Главный цикл: 
    - Каждую минуту обновляет остатки
    - Запускает `main()` из auto_confirm.py
    """
    try:
        async with get_async_db() as db:
            interval = 1  # Интервал в минутах
            while True:
                logging.info("🚀 Запуск планировщика задач...")

                # 1. Обновляем остатки
                enterprises = await get_enterprises_for_stock(db)
                for enterprise in enterprises:
                    await process_stock_for_enterprise(db, enterprise)

                # 2. Вызываем авто-подтверждение заказов
                #logging.info("📦 Запуск auto_confirm.py...")
                #try:
                    #await auto_confirm_main()
                    #logging.info("✅ Авто-подтверждение заказов завершено")
                #except Exception as e:
                    #logging.error(f"❌ Ошибка в auto_confirm.py: {e}")
                    #await notify_error(f"Ошибка в auto_confirm.py: {e}")
                # 3. Получаем предприятия, где активен order_fetcher
                logging.info("📥 Поиск предприятий с флагом order_fetcher=True...")
                try:
                    result = await db.execute(
                        select(EnterpriseSettings.enterprise_code)
                        .where(EnterpriseSettings.order_fetcher == True)
                    )
                    fetcher_enterprises = [row[0] for row in result.fetchall()]

                    if fetcher_enterprises:
                        logging.info(f"🔄 Найдено {len(fetcher_enterprises)} предприятий для загрузки заказов")
                        for enterprise_code in fetcher_enterprises:
                            try:
                                await fetch_orders_for_enterprise(db, enterprise_code)
                                logging.info(f"✅ Заказы получены для {enterprise_code}")
                            except Exception as fe:
                                logging.error(f"❌ Ошибка при получении заказов для {enterprise_code}: {fe}")
                                await notify_error(f"Ошибка получения заказов для {enterprise_code}: {fe}", enterprise_code)
                    else:
                        logging.info("📭 Предприятия с order_fetcher=True не найдены – заказов не будет загружено")
                except Exception as ef:
                    logging.error(f"❌ Ошибка запроса EnterpriseSettings.order_fetcher: {ef}")
                    await notify_error(f"Ошибка получения списка предприятий для fetcher: {ef}")

                # 4. Ждем 1 минуту перед следующим циклом
                logging.info("⏳ Ожидание 1 минуты перед следующим циклом...")
                await asyncio.sleep(interval * 60)

    except Exception as main_error:
        await notify_error(f"🔥 Критическая ошибка в планировщике: {str(main_error)}")
    finally:
        await notify_error("❌ Сервис stock_scheduler неожиданно остановлен.", "stock_scheduler")

if __name__ == "__main__":
    asyncio.run(schedule_stock_tasks())

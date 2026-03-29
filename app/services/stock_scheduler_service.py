# app/services/stock_scheduler_service.py
import os
import asyncio
import logging
import time
from time import perf_counter
import pytz
from datetime import datetime, timedelta, timezone
from sqlalchemy.future import select

os.environ['TZ'] = 'UTC'
KIEV_TZ = pytz.timezone("Europe/Kiev")

# === Импорты обработчиков стока (как было) ===
from app.dntrade_data_service.stock_fetch_convert import run_service
from app.checkbox_data_service.checkbox_stock_conv import run_service as run_checkbox
from app.rozetka_data_service.rozetka_conv import run_service as run_rozetka
from app.key_crm_data_service.key_crm_stock_conv import run_service as run_key_crm
from app.dsn_data_service.dsn_conv import run_service as run_dsn
from app.ftp_data_service.ftp_stock_conv import run_service as run_ftp
from app.prom_data_service.prom_stock import run_prom
from app.torgsoft_google_data_service.torgsoft_google_drive import run_torgsoft_google
from app.torgsoft_google_multi_data_service.torgsoft_multi_google_drive import run_torgsoft_google as run_torgsoft_multi
from app.vetmanager_data_service.vetmanager_converter import run_service as run_vetmanager
from app.hprofit_data_service.hprofit_conv import run_service as run_hprofit
from app.ftp_tabletki_data_service.ftp_tabletki_conv import run_service as run_ftp_tabletki
from app.google_drive.google_drive_service import extract_stock_from_google_drive
from app.jetvet_data_service.jetvet_google_drive import extract_stock_from_google_drive as stock_jetvet
from app.saledrive_data_service.saledrive_conv import run_service as run_saledrive
from app.ftp_zoomagazin_data_service.ftp_zoomagazin_conv import run_service as run_ftp_zoomagazin
from app.ftp_multi_data_service.ftp_multi_conv import run_service as run_ftp_multi
from app.biotus_data_service.biotus_conv import run_service as run_biotus
from app.bioteca_data_service.bioteca_conv import run_service as run_bioteca

from app.business.dropship_pipeline import run_pipeline as run_business
from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification

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
    "TorgsoftGoogleMulti": run_torgsoft_multi,
    "Vetmanager": run_vetmanager,
    "FtpZoomagazin": run_ftp_zoomagazin,
    "ComboKeyCRM": run_saledrive,
    "FtpMulti": run_ftp_multi,
    "Biotus": run_biotus,
    "Bioteca": run_bioteca,
    "Business": run_business,
}

async def notify_error(message: str, enterprise_code: str = "unknown"):
    logging.error(message)
    # send_notification — синхронная функция
    send_notification(message, enterprise_code)

async def create_error_report(error_message: str, enterprise_code: str):
    file_name = f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(file_name, 'a', encoding='utf-8') as file:
        file.write(f"{datetime.now()} - Enterprise Code: {enterprise_code} - Error: {error_message}\n")
    logging.info(f"Ошибка сохранена в файл: {file_name}")

async def get_enterprises_for_stock():
    """
    Получение списка предприятий для обновления остатков по их частоте загрузки (stock_upload_frequency).
    """
    try:
        logging.info("Stock scheduler: opening read-only session to load enterprises")
        async with get_async_db(commit_on_exit=False) as db:
            db.expire_all()
            now = datetime.now(tz=timezone.utc).astimezone(KIEV_TZ)
            logging.info(f"Текущее время: {now} [Timezone: {now.tzinfo}]")

            start_time = time.time()
            result = await db.execute(select(EnterpriseSettings))
            enterprises = result.scalars().all()
            logging.info(f"SQL-запрос выполнен за {time.time() - start_time:.2f} секунд")

            return [
                {
                    "enterprise_code": enterprise.enterprise_code,
                    "data_format": enterprise.data_format,
                }
                for enterprise in enterprises
                if enterprise.stock_upload_frequency and enterprise.stock_upload_frequency > 0 and
                   ((enterprise.last_stock_upload.astimezone(KIEV_TZ) + timedelta(minutes=enterprise.stock_upload_frequency))
                    if enterprise.last_stock_upload else now) <= now
            ]
    except Exception as e:
        await notify_error(f"Ошибка при получении списка предприятий для обновления остатков: {str(e)}")
        return []

async def process_stock_for_enterprise(enterprise_code: str, data_format: str):
    """
    Запуск соответствующего обработчика стока в зависимости от data_format.
    """
    started = perf_counter()
    logging.info(
        "Stock scheduler: start enterprise_code=%s data_format=%s",
        enterprise_code,
        data_format,
    )
    try:
        processor = PROCESSORS.get(data_format)
        if processor:
            await processor(enterprise_code, "stock")
            logging.info(
                "Stock scheduler: success enterprise_code=%s data_format=%s elapsed=%.3fs",
                enterprise_code,
                data_format,
                perf_counter() - started,
            )
        elif data_format in ["Unipro", "Blank"]:
            logging.info(
                "Пропуск обработки остатков для предприятия %s (%s).",
                enterprise_code,
                data_format,
            )
        else:
            logging.warning("Неподдерживаемый формат данных для предприятия %s.", enterprise_code)
    except Exception as e:
        logging.exception(
            "Stock scheduler: failure enterprise_code=%s data_format=%s elapsed=%.3fs",
            enterprise_code,
            data_format,
            perf_counter() - started,
        )
        await notify_error(
            f"Ошибка обработки остатков для предприятия {enterprise_code}: {str(e)}",
            enterprise_code,
        )
        await create_error_report(str(e), enterprise_code)

async def schedule_stock_tasks():
    """
    Главный цикл обновления остатков:
    - Каждую минуту отбирает предприятия по частоте и запускает их обработчики.
    """
    interval_minutes = 1
    try:
        while True:
            loop_started = perf_counter()
            logging.info("🚀 Запуск планировщика остатков...")
            enterprises = await get_enterprises_for_stock()
            logging.info("Stock scheduler: enterprises queued=%d", len(enterprises))
            for enterprise in enterprises:
                await process_stock_for_enterprise(
                    enterprise_code=str(enterprise["enterprise_code"]),
                    data_format=str(enterprise.get("data_format") or ""),
                )

            logging.info(
                "Stock scheduler: cycle finished enterprises=%d elapsed=%.3fs",
                len(enterprises),
                perf_counter() - loop_started,
            )

            logging.info("⏳ Ожидание 1 минуты перед следующим циклом стока...")
            await asyncio.sleep(interval_minutes * 60)
    except Exception as main_error:
        logging.exception("Stock scheduler: session/connection failure on outer loop")
        await notify_error(f"🔥 Критическая ошибка в планировщике стока: {str(main_error)}", "stock_scheduler")
    finally:
        await notify_error("❌ Сервис stock_scheduler неожиданно остановлен.", "stock_scheduler")

if __name__ == "__main__":
    asyncio.run(schedule_stock_tasks())

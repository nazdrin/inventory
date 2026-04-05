import os
import asyncio
import logging
import time
from time import perf_counter
import pytz
from datetime import datetime, timedelta, timezone
from typing import Dict
from sqlalchemy.future import select

# Установка временной зоны Киев
os.environ['TZ'] = 'UTC'
KIEV_TZ = pytz.timezone("Europe/Kiev")

# Импорт сервисов
from app.dntrade_data_service.fetch_convert import run_service
from app.checkbox_data_service.checkbox_catalog_conv import run_service as run_checkbox
from app.rozetka_data_service.rozetka_conv import run_service as run_rozetka
from app.hprofit_data_service.hprofit_conv import run_service as run_hprofit
from app.ftp_tabletki_data_service.ftp_tabletki_conv import run_service as run_ftp_tabletki
from app.dsn_data_service.dsn_conv import run_service as run_dsn
from app.prom_data_service.prom_catalog import run_prom
from app.torgsoft_google_data_service.torgsoft_google_drive import run_torgsoft_google
from app.torgsoft_google_multi_data_service.torgsoft_multi_google_drive import run_torgsoft_google as run_torgsoft_multi
from app.vetmanager_data_service.vetmanager_converter import run_service as run_vetmanager
from app.ftp_data_service.ftp_catalog_conv import run_service as run_ftp
from app.key_crm_data_service.key_crm_catalog_conv import run_service as run_key_crm
from app.google_drive.google_drive_service import extract_catalog_from_google_drive
from app.jetvet_data_service.jetvet_google_drive import extract_catalog_from_google_drive as catalog_jetvet
from app.saledrive_data_service.saledrive_conv import run_service as run_saledrive
from app.ftp_zoomagazin_data_service.ftp_zoomagazin_conv import run_service as run_ftp_zoomagazin
from app.ftp_multi_data_service.ftp_multi_conv import run_service as run_ftp_multi
from app.biotus_data_service.biotus_conv import run_service as run_biotus
from app.bioteca_data_service.bioteca_conv import run_service as run_bioteca
from app.business.import_catalog import run_service as run_business
from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# In-memory cooldown after timeout to avoid immediate restart loops.
TIMEOUT_COOLDOWN_UNTIL: Dict[str, datetime] = {}

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
    "HProfit": run_hprofit,
    "FtpTabletki": run_ftp_tabletki,
    "TorgsoftGoogle": run_torgsoft_google,
    "TorgsoftGoogleMulti": run_torgsoft_multi,
    "Vetmanager": run_vetmanager,
    "FtpZoomagazin": run_ftp_zoomagazin,
    # "Saledrive": run_saledrive,
    "ComboKeyCRM": run_saledrive,
    "FtpMulti": run_ftp_multi,
    "Biotus": run_biotus,
    "Bioteca": run_bioteca,
    "Business": run_business,
};

async def notify_error(message: str, enterprise_code: str = "unknown"):
    logging.error(message)
    send_notification(message, enterprise_code)

async def create_error_report(error_message: str, enterprise_code: str):
    file_name = f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(file_name, 'a') as file:
        file.write(f"{datetime.now()} - Enterprise Code: {enterprise_code} - Error: {error_message}\n")
    logging.info(f"Ошибка создания отчета об ошибках: {file_name}")

async def get_enterprises_for_catalog():
    """Получает список предприятий для обработки каталога."""
    try:
        logging.info("Catalog scheduler: opening read-only session to load enterprises")
        async with get_async_db(commit_on_exit=False) as db:
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
        return []

async def process_catalog_for_enterprise(enterprise: EnterpriseSettings):
    started = perf_counter()
    try:
        if enterprise.catalog_enabled is False:
            logging.info(
                "Catalog scheduler: skip enterprise_code=%s because catalog_enabled=false",
                enterprise.enterprise_code,
            )
            return

        if (
            enterprise.data_format == "Business"
            and os.getenv("DISABLE_OLD_BUSINESS_CATALOG_SCHEDULER", "0").strip().lower() in {"1", "true", "yes", "on"}
        ):
            logging.info(
                "Skipping old Business catalog scheduler for enterprise=%s because DISABLE_OLD_BUSINESS_CATALOG_SCHEDULER is enabled",
                enterprise.enterprise_code,
            )
            return

        cooldown_until = TIMEOUT_COOLDOWN_UNTIL.get(enterprise.enterprise_code)
        now_utc = datetime.now(timezone.utc)
        if cooldown_until and now_utc < cooldown_until:
            logging.warning(
                "Skipping enterprise %s due to timeout cooldown until %s",
                enterprise.enterprise_code,
                cooldown_until.isoformat(),
            )
            return

        processor = PROCESSORS.get(enterprise.data_format)
        if processor:
            default_timeout = "7200" if enterprise.data_format == "Dntrade" else "1800"
            timeout_sec = int(
                os.getenv(
                    "DNTRADE_CATALOG_PROCESS_TIMEOUT_SEC" if enterprise.data_format == "Dntrade" else "CATALOG_PROCESS_TIMEOUT_SEC",
                    default_timeout,
                )
            )
            await asyncio.wait_for(
                processor(enterprise.enterprise_code, "catalog"),
                timeout=timeout_sec,
            )
            TIMEOUT_COOLDOWN_UNTIL.pop(enterprise.enterprise_code, None)
            logging.info(
                "Catalog scheduler: success enterprise_code=%s data_format=%s elapsed=%.3fs",
                enterprise.enterprise_code,
                enterprise.data_format,
                perf_counter() - started,
            )
        elif enterprise.data_format in ["Unipro", "Blank"]:
            logging.info(f"Skipping processing for Enterprise Code={enterprise.enterprise_code} with data format '{enterprise.data_format}'")
        else:
            logging.warning(f"Unsupported data format or transfer method for Enterprise Code={enterprise.enterprise_code}")

    except asyncio.TimeoutError:
        cooldown_minutes = int(
            os.getenv(
                "DNTRADE_CATALOG_TIMEOUT_COOLDOWN_MIN" if enterprise.data_format == "Dntrade" else "CATALOG_TIMEOUT_COOLDOWN_MIN",
                "30",
            )
        )
        TIMEOUT_COOLDOWN_UNTIL[enterprise.enterprise_code] = datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)
        await notify_error(
            f"Таймаут обработки каталога ({timeout_sec}s) для предприятия {enterprise.enterprise_code}",
            enterprise.enterprise_code,
        )
        await create_error_report("Catalog processing timeout", enterprise.enterprise_code)
    except Exception as e:
        logging.exception(
            "Catalog scheduler: failure enterprise_code=%s data_format=%s elapsed=%.3fs",
            enterprise.enterprise_code,
            enterprise.data_format,
            perf_counter() - started,
        )
        await notify_error(f"Ошибка обработки данных для предприятия {enterprise.enterprise_code}: {str(e)}", enterprise.enterprise_code)
        await create_error_report(str(e), enterprise.enterprise_code)

async def schedule_catalog_tasks():
    try:
        interval = 1  # Интервал выполнения расписания в минутах
        while True:
            loop_started = perf_counter()
            logging.info("Starting catalog scheduler loop...")
            enterprises = await get_enterprises_for_catalog()
            logging.info("Catalog scheduler: enterprises queued=%d", len(enterprises))

            for enterprise in enterprises:
                await process_catalog_for_enterprise(enterprise)

            logging.info(
                "Catalog scheduler: cycle finished enterprises=%d elapsed=%.3fs",
                len(enterprises),
                perf_counter() - loop_started,
            )

            await asyncio.sleep(interval * 60)
    except Exception as main_error:
        await notify_error(f"🔥 Критическая ошибка в планировщике: {str(main_error)}")
    finally:
        await notify_error("🔴 Сервис catalog_scheduler неожиданно остановлен.", "catalog_scheduler")
        
if __name__ == "__main__":
    asyncio.run(schedule_catalog_tasks())

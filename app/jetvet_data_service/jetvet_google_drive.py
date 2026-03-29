import logging
import os
import tempfile
import time

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy.future import select

from app.database import EnterpriseSettings, MappingBranch, get_async_db
from app.jetvet_data_service.jetvet_catalog_conv import process_jetvet_catalog
from app.jetvet_data_service.jetvet_stock_conv import process_jetvet_stock
from app.services.notification_service import send_notification

load_dotenv()


def get_logger() -> logging.Logger:
    logger = logging.getLogger("jetvet")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = get_logger()


def get_temp_dir() -> str:
    temp_dir = os.getenv("TEMP_DIR", tempfile.gettempdir())
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    return temp_dir


async def connect_to_google_drive():
    try:
        google_drive_file_name = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH")
        if not google_drive_file_name or not os.path.exists(google_drive_file_name):
            msg = f"Неверный путь к учетным данным Google Drive: {google_drive_file_name}"
            logger.error(msg)
            send_notification(msg, "Разработчик")
            raise FileNotFoundError(msg)

        credentials = service_account.Credentials.from_service_account_file(
            google_drive_file_name,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        return build("drive", "v3", credentials=credentials)
    except Exception as exc:
        msg = f"Ошибка при подключении к Google Drive: {exc}"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def fetch_files_from_folder(drive_service, folder_id: str) -> list[dict]:
    try:
        logger.info("JetVet list files: folder_id=%s", folder_id)
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name)",
        ).execute()
        files = results.get("files", [])
        logger.info("JetVet files found: folder_id=%s count=%s", folder_id, len(files))
        return files
    except Exception as exc:
        msg = f"Ошибка при получении файлов из папки {folder_id}: {exc}"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def download_file(drive_service, file_id: str, file_name: str) -> str:
    try:
        file_path = os.path.join(get_temp_dir(), file_name)
        request = drive_service.files().get_media(fileId=file_id)
        with open(file_path, "wb") as file:
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status is not None:
                    logger.info("JetVet download progress: file=%s progress=%s%%", file_name, int(status.progress() * 100))
        return file_path
    except Exception as exc:
        msg = f"Ошибка при скачивании файла {file_name}: {exc}"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def extract_catalog_from_google_drive(enterprise_code: str, file_type):
    run_started_at = time.monotonic()
    async with get_async_db() as db:
        try:
            result = await db.execute(
                select(
                    EnterpriseSettings.enterprise_code,
                    EnterpriseSettings.google_drive_folder_id_ref,
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = result.mappings().one_or_none()
            if not enterprise or not enterprise["google_drive_folder_id_ref"]:
                msg = f"Не найдена папка каталога для {enterprise_code}"
                logger.error(msg)
                send_notification(msg, "Разработчик")
                return

            drive_service = await connect_to_google_drive()
            catalog_files = await fetch_files_from_folder(drive_service, enterprise["google_drive_folder_id_ref"])
            if not catalog_files:
                logger.warning("JetVet no catalog files: enterprise_code=%s", enterprise_code)
                return

            success_count = 0
            failed_count = 0
            failed_files: list[str] = []

            for file in catalog_files:
                file_path = await download_file(drive_service, file["id"], file["name"])
                try:
                    await process_jetvet_catalog(
                        enterprise_code=enterprise_code,
                        file_path=file_path,
                        file_type="catalog",
                    )
                    success_count += 1
                    logger.info("JetVet catalog file processed: enterprise_code=%s file=%s", enterprise_code, file["name"])
                except Exception as exc:
                    failed_count += 1
                    failed_files.append(file["name"])
                    logger.error("JetVet catalog file failed: enterprise_code=%s file=%s error=%s", enterprise_code, file["name"], exc)
                    send_notification(f"Ошибка каталога JetVet {enterprise_code}: {exc}", "Разработчик")
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logger.info("JetVet temp file removed: %s", file_path)

            logger.info(
                "JetVet catalog run summary: enterprise_code=%s files=%s success=%s failed=%s elapsed=%.2fs",
                enterprise_code,
                len(catalog_files),
                success_count,
                failed_count,
                time.monotonic() - run_started_at,
            )
            if failed_files:
                logger.warning("JetVet catalog partial success: enterprise_code=%s failed_files=%s", enterprise_code, failed_files)
        except Exception as exc:
            send_notification(f"Ошибка каталога JetVet {enterprise_code}: {exc}", "Разработчик")


async def extract_stock_from_google_drive(enterprise_code: str, file_type):
    run_started_at = time.monotonic()
    async with get_async_db() as db:
        try:
            enterprise_result = await db.execute(
                select(
                    EnterpriseSettings.enterprise_code,
                    EnterpriseSettings.single_store,
                    EnterpriseSettings.store_serial,
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = enterprise_result.mappings().one_or_none()
            if not enterprise:
                msg = f"Настройки предприятия {enterprise_code} не найдены"
                logger.error(msg)
                send_notification(msg, "Разработчик")
                return

            branches_result = await db.execute(
                select(MappingBranch).where(MappingBranch.enterprise_code == enterprise_code)
            )
            branches = branches_result.scalars().all()
            if not branches:
                msg = f"Нет branch с google_folder_id для {enterprise_code}"
                logger.error(msg)
                send_notification(msg, "Разработчик")
                return

            drive_service = await connect_to_google_drive()

            processed_branches = 0
            processed_files = 0
            success_count = 0
            failed_count = 0
            failed_files: list[str] = []

            for branch in branches:
                if not branch.google_folder_id:
                    continue
                processed_branches += 1
                stock_files = await fetch_files_from_folder(drive_service, branch.google_folder_id)
                logger.info(
                    "JetVet stock branch summary: enterprise_code=%s branch=%s folder_id=%s files=%s",
                    enterprise_code,
                    branch.branch,
                    branch.google_folder_id,
                    len(stock_files),
                )

                for file in stock_files:
                    processed_files += 1
                    file_path = await download_file(drive_service, file["id"], file["name"])
                    try:
                        await process_jetvet_stock(
                            enterprise_code=enterprise_code,
                            file_path=file_path,
                            file_type="stock",
                            single_store=enterprise["single_store"],
                            store_serial=enterprise["store_serial"],
                            branch=branch.branch,
                        )
                        success_count += 1
                        logger.info(
                            "JetVet stock file processed: enterprise_code=%s branch=%s file=%s",
                            enterprise_code,
                            branch.branch,
                            file["name"],
                        )
                    except Exception as exc:
                        failed_count += 1
                        failed_files.append(f"{branch.branch}:{file['name']}")
                        logger.error(
                            "JetVet stock file failed: enterprise_code=%s branch=%s file=%s error=%s",
                            enterprise_code,
                            branch.branch,
                            file["name"],
                            exc,
                        )
                        send_notification(f"Ошибка остатков JetVet {enterprise_code}: {exc}", "Разработчик")
                    finally:
                        if os.path.exists(file_path):
                            os.remove(file_path)
                            logger.info("JetVet temp file removed: %s", file_path)

            logger.info(
                "JetVet stock run summary: enterprise_code=%s branches=%s files=%s success=%s failed=%s elapsed=%.2fs",
                enterprise_code,
                processed_branches,
                processed_files,
                success_count,
                failed_count,
                time.monotonic() - run_started_at,
            )
            if failed_files:
                logger.warning("JetVet stock partial success: enterprise_code=%s failed_files=%s", enterprise_code, failed_files)
        except Exception as exc:
            send_notification(f"Ошибка остатков JetVet {enterprise_code}: {exc}", "Разработчик")

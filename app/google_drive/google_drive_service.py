import logging
import os
import tempfile
import time

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy.future import select

from app.database import DeveloperSettings, EnterpriseSettings, get_async_db
from app.google_drive.data_validator import validate_data
from app.services.notification_service import send_notification

load_dotenv()


def get_logger() -> logging.Logger:
    logger = logging.getLogger("google_drive")
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
        if not google_drive_file_name:
            logger.error("GOOGLE_DRIVE_CREDENTIALS_PATH не задан.")
            send_notification("Переменная окружения GOOGLE_DRIVE_CREDENTIALS_PATH не задана.", "Разработчик")
            raise EnvironmentError("Не задан путь к учетным данным Google Drive.")
        if not os.path.exists(google_drive_file_name):
            logger.error("Не найден файл учетных данных Google Drive: %s", google_drive_file_name)
            send_notification(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}", "Разработчик")
            raise FileNotFoundError(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}")

        credentials = service_account.Credentials.from_service_account_file(
            google_drive_file_name,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        return build("drive", "v3", credentials=credentials)
    except Exception as exc:
        logger.error("Ошибка при подключении к Google Drive: %s", exc)
        send_notification(f"Ошибка при подключении к Google Drive: {exc}", "Разработчик")
        raise


async def fetch_files_from_folder(drive_service, folder_id: str) -> list[dict]:
    try:
        logger.info("Google Drive list files: folder_id=%s", folder_id)
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name)",
        ).execute()
        files = results.get("files", [])
        logger.info("Google Drive files found: folder_id=%s count=%s", folder_id, len(files))
        return files
    except Exception as exc:
        logger.error("Ошибка при получении файлов из папки %s: %s", folder_id, exc)
        send_notification(f"Ошибка при получении файлов из папки {folder_id}: {exc}", "Разработчик")
        raise


async def download_file(drive_service, file_id: str, file_name: str) -> str:
    try:
        request = drive_service.files().get_media(fileId=file_id)
        temp_dir = get_temp_dir()
        file_path = os.path.join(temp_dir, file_name)

        with open(file_path, "wb") as file:
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status is not None:
                    logger.info("Google Drive download progress: file=%s progress=%s%%", file_name, int(status.progress() * 100))

        return file_path
    except Exception as exc:
        logger.error("Ошибка при скачивании файла %s: %s", file_name, exc)
        send_notification(f"Ошибка при скачивании файла {file_name}: {exc}", "Разработчик")
        raise


async def _load_enterprise_drive_settings(enterprise_code: str, file_type: str):
    folder_field = "google_drive_folder_id_ref" if file_type == "catalog" else "google_drive_folder_id_rest"

    async with get_async_db() as db:
        logger.info("Google Drive settings lookup: enterprise_code=%s type=%s", enterprise_code, file_type)
        result = await db.execute(
            select(
                EnterpriseSettings.enterprise_code,
                EnterpriseSettings.single_store,
                EnterpriseSettings.store_serial,
                getattr(EnterpriseSettings, folder_field),
            ).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        enterprise = result.mappings().one_or_none()
        if not enterprise:
            logger.error("Не найдены настройки Google Drive для enterprise_code=%s", enterprise_code)
            send_notification(f"Не найдены настройки для Google Drive с кодом {enterprise_code}.", "Разработчик")
            return None

        folder_id = enterprise[folder_field]
        if not folder_id:
            logger.error("Отсутствует folder_id для enterprise_code=%s type=%s", enterprise_code, file_type)
            send_notification(
                f"Отсутствует ID папки Google Drive для {file_type} для предприятия с кодом {enterprise_code}.",
                "Разработчик",
            )
            return None

        result = await db.execute(select(DeveloperSettings).limit(1))
        developer_settings = result.scalar_one_or_none()
        if not developer_settings:
            logger.error("Не найдены настройки разработчика для Google Drive.")
            send_notification("Не найдены настройки разработчика для Google Drive.", "Разработчик")
            return None

        return {
            "enterprise_code": enterprise["enterprise_code"],
            "single_store": enterprise["single_store"],
            "store_serial": enterprise["store_serial"],
            "folder_id": folder_id,
        }


async def _process_google_drive_files(enterprise_code: str, file_type: str) -> None:
    run_started_at = time.monotonic()
    settings = await _load_enterprise_drive_settings(enterprise_code, file_type)
    if not settings:
        return

    drive_service = await connect_to_google_drive()
    logger.info("Google Drive connected: enterprise_code=%s type=%s", enterprise_code, file_type)

    files = await fetch_files_from_folder(drive_service, settings["folder_id"])
    if not files:
        logger.warning("Google Drive no files found: enterprise_code=%s type=%s folder_id=%s", enterprise_code, file_type, settings["folder_id"])
        return

    success_count = 0
    failed_count = 0
    failed_files: list[str] = []

    for file in files:
        file_path = await download_file(drive_service, file["id"], file["name"])
        try:
            is_valid = await validate_data(
                enterprise_code=enterprise_code,
                file_path=file_path,
                file_type=file_type,
                single_store=settings["single_store"],
                store_serial=settings["store_serial"],
            )
            if is_valid:
                success_count += 1
                logger.info(
                    "Google Drive file processed: enterprise_code=%s type=%s file=%s status=success",
                    enterprise_code,
                    file_type,
                    file["name"],
                )
            else:
                failed_count += 1
                failed_files.append(file["name"])
                logger.warning(
                    "Google Drive file processed: enterprise_code=%s type=%s file=%s status=failed",
                    enterprise_code,
                    file_type,
                    file["name"],
                )
        except Exception as exc:
            failed_count += 1
            failed_files.append(file["name"])
            logger.error(
                "Ошибка валидации/обработки файла %s для enterprise=%s type=%s: %s",
                file["name"],
                enterprise_code,
                file_type,
                exc,
            )
            send_notification(
                f"Ошибка обработки {file_type} файла {file['name']} для предприятия {enterprise_code}: {exc}",
                "Разработчик",
            )
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info("Google Drive temp file removed: %s", file_path)

    logger.info(
        "Google Drive run summary: enterprise_code=%s type=%s found_files=%s success=%s failed=%s elapsed=%.2fs",
        enterprise_code,
        file_type,
        len(files),
        success_count,
        failed_count,
        time.monotonic() - run_started_at,
    )
    if failed_files:
        logger.warning(
            "Google Drive partial success: enterprise_code=%s type=%s failed_files=%s",
            enterprise_code,
            file_type,
            failed_files,
        )


async def extract_stock_from_google_drive(enterprise_code: str, file_type):
    try:
        await _process_google_drive_files(enterprise_code, "stock")
    except Exception as exc:
        logger.error("Ошибка при обработке остатков для enterprise_code=%s: %s", enterprise_code, exc)
        send_notification(f"Ошибка при обработке остатков Google Drive для предприятия с кодом {enterprise_code}: {exc}", "Разработчик")


async def extract_catalog_from_google_drive(enterprise_code: str, file_type):
    try:
        await _process_google_drive_files(enterprise_code, "catalog")
    except Exception as exc:
        logger.error("Ошибка при обработке каталога для enterprise_code=%s: %s", enterprise_code, exc)
        send_notification(f"Ошибка при обработке каталога Google Drive для предприятия с кодом {enterprise_code}: {exc}", "Разработчик")

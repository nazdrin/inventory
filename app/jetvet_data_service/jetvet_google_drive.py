import logging
import os
import tempfile
from dotenv import load_dotenv
from sqlalchemy.future import select
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from app.database import get_async_db, EnterpriseSettings, MappingBranch
from app.services.notification_service import send_notification
from app.jetvet_data_service.jetvet_catalog_conv import process_jetvet_catalog
from app.jetvet_data_service.jetvet_stock_conv import process_jetvet_stock

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()


def get_temp_dir():
    temp_dir = os.getenv("TEMP_DIR", tempfile.gettempdir())
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    return temp_dir


async def connect_to_google_drive():
    try:
        google_drive_file_name = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH")
        if not google_drive_file_name or not os.path.exists(google_drive_file_name):
            msg = f"Неверный путь к учетным данным Google Drive: {google_drive_file_name}"
            logging.error(msg)
            send_notification(msg, "Разработчик")
            raise FileNotFoundError(msg)

        credentials = service_account.Credentials.from_service_account_file(
            google_drive_file_name,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        msg = f"Ошибка при подключении к Google Drive: {str(e)}"
        logging.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def fetch_files_from_folder(drive_service, folder_id):
    try:
        logging.info(f"Получение файлов из папки: {folder_id}")
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name)"
        ).execute()
        return results.get('files', [])
    except Exception as e:
        msg = f"Ошибка при получении файлов из папки {folder_id}: {str(e)}"
        logging.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def download_file(drive_service, file_id, file_name):
    try:
        file_path = os.path.join(get_temp_dir(), file_name)
        request = drive_service.files().get_media(fileId=file_id)
        with open(file_path, "wb") as file:
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logging.info(f"Скачивание {file_name}: {int(status.progress() * 100)}%")
        return file_path
    except Exception as e:
        msg = f"Ошибка при скачивании файла {file_name}: {str(e)}"
        logging.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def extract_catalog_from_google_drive(enterprise_code: str, file_type):
    async with get_async_db() as db:
        try:
            result = await db.execute(
                select(
                    EnterpriseSettings.enterprise_code,
                    EnterpriseSettings.single_store,
                    EnterpriseSettings.store_serial,
                    EnterpriseSettings.google_drive_folder_id_ref,
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = result.mappings().one_or_none()
            if not enterprise or not enterprise["google_drive_folder_id_ref"]:
                msg = f"Не найдена папка каталога для {enterprise_code}"
                logging.error(msg)
                send_notification(msg, "Разработчик")
                return

            drive_service = await connect_to_google_drive()
            catalog_files = await fetch_files_from_folder(drive_service, enterprise["google_drive_folder_id_ref"])

            for file in catalog_files:
                file_path = await download_file(drive_service, file['id'], file['name'])
                try:
                    await process_jetvet_catalog(
                        enterprise_code=enterprise_code,
                        file_path=file_path,
                        file_type="catalog",
                        
                    )
                    logging.info(f"Catalog {file['name']} обработан для {enterprise_code}")
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)
        except Exception as e:
            send_notification(f"Ошибка каталога JetVet {enterprise_code}: {str(e)}", "Разработчик")


async def extract_stock_from_google_drive(enterprise_code: str, file_type):
    async with get_async_db() as db:
        try:
            enterprise_result = await db.execute(
                select(
                    EnterpriseSettings.enterprise_code,
                    EnterpriseSettings.single_store,
                    EnterpriseSettings.store_serial
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = enterprise_result.mappings().one_or_none()
            if not enterprise:
                msg = f"Настройки предприятия {enterprise_code} не найдены"
                logging.error(msg)
                send_notification(msg, "Разработчик")
                return

            branches_result = await db.execute(
                select(MappingBranch).where(MappingBranch.enterprise_code == enterprise_code)
            )
            branches = branches_result.scalars().all()
            if not branches:
                msg = f"Нет branch с google_folder_id для {enterprise_code}"
                logging.error(msg)
                send_notification(msg, "Разработчик")
                return

            drive_service = await connect_to_google_drive()

            for branch in branches:
                if not branch.google_folder_id:
                    continue

                stock_files = await fetch_files_from_folder(drive_service, branch.google_folder_id)

                for file in stock_files:
                    file_path = await download_file(drive_service, file['id'], file['name'])
                    try:
                        await process_jetvet_stock(
                            enterprise_code=enterprise_code,
                            file_path=file_path,
                            file_type="stock",
                            single_store=enterprise["single_store"],
                            store_serial=enterprise["store_serial"],
                            branch=branch.branch
                        )
                        logging.info(f"Stock {file['name']} обработан для {enterprise_code}, branch {branch.branch}")
                    finally:
                        if os.path.exists(file_path):
                            os.remove(file_path)
        except Exception as e:
            send_notification(f"Ошибка остатков JetVet {enterprise_code}: {str(e)}", "Разработчик")
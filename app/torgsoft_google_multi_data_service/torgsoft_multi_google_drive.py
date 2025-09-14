# app/torgsoft_google/torgsoft_google_drive.py

import logging
import os
import tempfile
from typing import List, Dict, Any

from dotenv import load_dotenv
from sqlalchemy.future import select
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from app.database import get_async_db, EnterpriseSettings, MappingBranch
from app.services.notification_service import send_notification

# ВАЖНО: этот импорт будет работать после создания второго файла-конвертера
# в app/torgsoft_google/torgsoft_converter.py с функциями:
#   - async def process_torgsoft_catalog(enterprise_code: str, file_path: str, file_type: str)
#   - async def process_torgsoft_stock(enterprise_code: str, file_path: str, file_type: str, branch: str, single_store: bool, store_serial: str)
from app.torgsoft_google_multi_data_service.torgsoft_multi_converter import (
    process_torgsoft_catalog,
    process_torgsoft_stock,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _get_temp_dir() -> str:
    """Где складывать временные файлы."""
    temp_dir = os.getenv("TEMP_DIR", tempfile.gettempdir())
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir, exist_ok=True)
    return temp_dir


async def _connect_to_google_drive():
    """
    Авторизация сервисным аккаунтом и создание клиента Drive API.
    Переменная окружения: GOOGLE_DRIVE_CREDENTIALS_PATH -> путь к JSON ключу.
    """
    try:
        creds_path = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH")
        if not creds_path or not os.path.exists(creds_path):
            msg = f"Неверный путь к учетным данным Google Drive: {creds_path}"
            logging.error(msg)
            send_notification(msg, "Разработчик")
            raise FileNotFoundError(msg)

        credentials = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        return build("drive", "v3", credentials=credentials)
    except Exception as e:
        msg = f"Ошибка при подключении к Google Drive: {e}"
        logging.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def _fetch_files_from_folder(drive_service, folder_id: str) -> List[Dict[str, Any]]:
    """
    Получить список файлов из папки (id, name).
    """
    try:
        logging.info(f"Получение файлов из папки: {folder_id}")
        results = (
            drive_service.files()
            .list(q=f"'{folder_id}' in parents and trashed=false", fields="files(id, name)")
            .execute()
        )
        return results.get("files", [])
    except Exception as e:
        msg = f"Ошибка при получении файлов из папки {folder_id}: {e}"
        logging.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def _download_file(drive_service, file_id: str, file_name: str) -> str:
    """
    Скачивание файла из Drive в temp.
    """
    try:
        file_path = os.path.join(_get_temp_dir(), file_name)
        request = drive_service.files().get_media(fileId=file_id)
        with open(file_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logging.info(f"Скачивание {file_name}: {int(status.progress() * 100)}%")
        return file_path
    except Exception as e:
        msg = f"Ошибка при скачивании файла {file_name}: {e}"
        logging.error(msg)
        send_notification(msg, "Разработчик")
        raise


async def _process_catalog_flow(enterprise_code: str) -> None:
    """
    Поток обработки каталога:
    - Берём папку каталога из EnterpriseSettings.google_drive_folder_id_ref
    - Скачиваем все файлы
    - Передаём каждый файл в конвертер process_torgsoft_catalog(...)
    """
    async with get_async_db() as db:
        try:
            # Загружаем настройки предприятия
            result = await db.execute(
                select(
                    EnterpriseSettings.enterprise_code,
                    EnterpriseSettings.google_drive_folder_id_ref,
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = result.mappings().one_or_none()
            folder_id = enterprise and enterprise.get("google_drive_folder_id_ref")
            if not folder_id:
                msg = f"Не найдена папка каталога для {enterprise_code}"
                logging.error(msg)
                send_notification(msg, "Разработчик")
                return

            drive = await _connect_to_google_drive()
            files = await _fetch_files_from_folder(drive, folder_id)

            if not files:
                logging.info(f"В папке каталога ({folder_id}) нет файлов для {enterprise_code}")
                return

            for f in files:
                file_path = await _download_file(drive, f["id"], f["name"])
                try:
                    await process_torgsoft_catalog(
                        enterprise_code=enterprise_code,
                        file_path=file_path,
                        file_type="catalog",
                    )
                    logging.info(f"[Catalog] {f['name']} обработан для {enterprise_code}")
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)

        except Exception as e:
            send_notification(f"Ошибка каталога Torgsoft_google {enterprise_code}: {e}", "Разработчик")


async def _process_stock_flow(enterprise_code: str) -> None:
    """
    Поток обработки остатков:
    - Берём ветки (branch) с их Google-папками из MappingBranch.google_folder_id
    - Для каждой папки скачиваем файлы
    - Передаём в конвертер process_torgsoft_stock(...)
    """
    async with get_async_db() as db:
        try:
            # Настройки предприятия (single_store/store_serial пригодятся конвертеру при формировании branch/логики)
            ent_res = await db.execute(
                select(
                    EnterpriseSettings.enterprise_code,
                    EnterpriseSettings.single_store,
                    EnterpriseSettings.store_serial,
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = ent_res.mappings().one_or_none()
            if not enterprise:
                msg = f"Настройки предприятия {enterprise_code} не найдены"
                logging.error(msg)
                send_notification(msg, "Разработчик")
                return

            # Ветки/папки для остатков
            br_res = await db.execute(
                select(MappingBranch).where(MappingBranch.enterprise_code == enterprise_code)
            )
            branches = br_res.scalars().all()
            if not branches:
                msg = f"Нет branch с google_folder_id для {enterprise_code}"
                logging.error(msg)
                send_notification(msg, "Разработчик")
                return

            drive = await _connect_to_google_drive()

            for b in branches:
                if not b.google_folder_id:
                    continue

                files = await _fetch_files_from_folder(drive, b.google_folder_id)
                if not files:
                    logging.info(f"[Stock] В папке {b.google_folder_id} нет файлов (branch={b.branch})")
                    continue

                for f in files:
                    file_path = await _download_file(drive, f["id"], f["name"])
                    try:
                        await process_torgsoft_stock(
                            enterprise_code=enterprise_code,
                            file_path=file_path,
                            file_type="stock",
                            # branch=b.branch,
                            # single_store=enterprise.get("single_store"),
                            # store_serial=enterprise.get("store_serial"),
                        )
                        logging.info(
                            f"[Stock] {f['name']} обработан для {enterprise_code}, branch={b.branch}"
                        )
                    finally:
                        if os.path.exists(file_path):
                            os.remove(file_path)

        except Exception as e:
            send_notification(f"Ошибка остатков Torgsoft_google {enterprise_code}: {e}", "Разработчик")


async def run_torgsoft_google(enterprise_code: str, file_type: str) -> None:
    """
    Точка входа для шедулера.
    :param enterprise_code: код предприятия
    :param file_type: "catalog" или "stock"
    """
    file_type = (file_type or "").strip().lower()
    if file_type == "catalog":
        await _process_catalog_flow(enterprise_code)
    elif file_type == "stock":
        await _process_stock_flow(enterprise_code)
    else:
        msg = f"Неизвестный тип файла '{file_type}'. Ожидается 'catalog' или 'stock'."
        logging.error(msg)
        send_notification(msg, "Разработчик")

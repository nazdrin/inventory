import logging
import os
import tempfile 
from dotenv import load_dotenv
load_dotenv()
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings, DeveloperSettings
from app.google_drive.data_validator import validate_data
from app.services.notification_service import send_notification  # Функция для отправки уведомлений


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_temp_dir():
    """
    Возвращает путь к временной папке.
    Если указано в .env, используется путь из переменной окружения TEMP_DIR.
    Иначе используется системная временная папка.
    """
    temp_dir = os.getenv("TEMP_DIR", tempfile.gettempdir())
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)  # Создаём папку, если её нет
    return temp_dir

async def connect_to_google_drive():
    """
    Подключается к Google Drive API.
    """
    try:

        # Получаем путь к файлу учетных данных из переменных окружения
        google_drive_file_name = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH")
        logging.info(f"Путь к учетным данным Google Drive: {google_drive_file_name}")
        if not google_drive_file_name:
            logging.error("Переменная окружения GOOGLE_DRIVE_CREDENTIALS_PATH не задана.")
            send_notification(f"Переменная окружения GOOGLE_DRIVE_CREDENTIALS_PATH не задана.", "Разработчик")
            raise EnvironmentError("Не задан путь к учетным данным Google Drive.")
        if not os.path.exists(google_drive_file_name):
            logging.error(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}")
            send_notification(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}", "Разработчик")
            raise FileNotFoundError(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}")

        logging.info(f"Подключение к Google Drive с использованием учетных данных: {google_drive_file_name}")
        credentials = service_account.Credentials.from_service_account_file(
            google_drive_file_name,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build('drive', 'v3', credentials=credentials)
    except Exception as e:
        logging.error(f"Ошибка при подключении к Google Drive: {str(e)}")
        send_notification(f"Ошибка при подключении к Google Drive: {str(e)}", "Разработчик")
        raise

async def fetch_files_from_folder(drive_service, folder_id):
    """
    Получает список файлов из указанной папки Google Drive.
    """
    try:
        logging.info(f"Получение файлов из папки Google Drive с ID: {folder_id}")
        results = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name)"
        ).execute()
        files = results.get('files', [])
        logging.info(f"Найдено {len(files)} файлов в папке Google Drive ID: {folder_id}")
        return files
    except Exception as e:
        logging.error(f"Ошибка при получении файлов из папки {folder_id}: {str(e)}")
        send_notification(f"Ошибка при получении файлов из папки {folder_id}: {str(e)}", "Разработчик")
        raise

# async def download_file(drive_service, file_id, file_name):
#     """
#     Скачивает файл из Google Drive.
#     """
#     try:
#         request = drive_service.files().get_media(fileId=file_id)
#         file_path = f"/tmp/{file_name}"
#         with open(file_path, "wb") as file:
#             downloader = MediaIoBaseDownload(file, request)
#             done = False
#             while not done:
#                 status, done = downloader.next_chunk()
#                 logging.info(f"Скачивание {file_name}: {int(status.progress() * 100)}% завершено.")
#         return file_path
#     except Exception as e:
#         logging.error(f"Ошибка при скачивании файла {file_name}: {str(e)}")
#         send_notification(f"Ошибка при скачивании файла {file_name}: {str(e)}", "Разработчик")
#         raise

async def download_file(drive_service, file_id, file_name):
    """
    Скачивает файл из Google Drive.
    """
    try:
        request = drive_service.files().get_media(fileId=file_id)
        # Используем универсальную временную папку
        temp_dir = get_temp_dir()
        file_path = os.path.join(temp_dir, file_name)
        
        with open(file_path, "wb") as file:
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logging.info(f"Скачивание {file_name}: {int(status.progress() * 100)}% завершено.")
        
        return file_path
    except Exception as e:
        logging.error(f"Ошибка при скачивании файла {file_name}: {str(e)}")
        send_notification(f"Ошибка при скачивании файла {file_name}: {str(e)}", "Разработчик")
        raise

async def extract_stock_from_google_drive(enterprise_code: str):
    """
    Обрабатывает файлы остатков из Google Drive для указанного предприятия.
    """
    async with get_async_db() as db:
        try:
            logging.info(f"Получение настроек для предприятия с кодом: {enterprise_code}")

            # Получение настроек предприятия
            result = await db.execute(
                select(
                    EnterpriseSettings.enterprise_code,
                    EnterpriseSettings.single_store,
                    EnterpriseSettings.store_serial,
                    EnterpriseSettings.google_drive_folder_id_rest,
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = result.mappings().one_or_none()
            if not enterprise:
                logging.error(f"Не найдены настройки для предприятия с кодом {enterprise_code}.")
                send_notification(f"Не найдены настройки для обработки Google Drive  с кодом {enterprise_code}.", "Разработчик")
                return

            if not enterprise["google_drive_folder_id_rest"]:
                logging.error(f"Отсутствует ID папки Google Drive для остатков для предприятия с кодом {enterprise_code}.")
                send_notification(f"Отсутствует ID папки Google Drive для остатков для предприятия с кодом {enterprise_code}.", "Разработчик")
                return

            # Получение настроек разработчика
            result = await db.execute(select(DeveloperSettings).limit(1))
            developer_settings = result.scalar_one_or_none()
            if not developer_settings:
                logging.error("Не найдены настройки разработчика.")
                send_notification("Не найдены настройки разработчика для Google Drive", "Разработчик")
                return

            # google_drive_file_name = os.path.abspath(developer_settings.google_drive_file_name)
            # if not os.path.exists(google_drive_file_name):
            #     logging.error(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}")
            #     send_notification(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}", "Разработчик")
            #     return

            # Подключение к Google Drive
            drive_service = await connect_to_google_drive()
            logging.info(f"Успешно подключились к Google Drive для предприятия с кодом {enterprise_code}")

            # Извлечение файлов остатков
            stock_files = await fetch_files_from_folder(drive_service, enterprise["google_drive_folder_id_rest"])

            # Обработка каждого файла
            for file in stock_files:
                file_path = await download_file(drive_service, file['id'], file['name'])
                logging.info(f"Проверка остатков. Параметры: enterprise_code={enterprise_code}, "
                            f"file_path={file_path}, file_type='stock', "
                            f"single_store={enterprise['single_store']}, store_serial={enterprise['store_serial']}")
                try:
                    logging.info(f"Тип и значение enterprise_code перед вызовом validate_data: {type(enterprise_code)} - {enterprise_code}")
                    await validate_data(
                        enterprise_code=enterprise_code,
                        file_path=file_path,
                        file_type="stock",
                        single_store=enterprise["single_store"],
                        store_serial=enterprise["store_serial"],
                    )
                    logging.info(f"Stock file {file['name']} validated successfully for enterprise {enterprise_code}")
                except Exception as e:
                    logging.error(f"Ошибка валидации для stock файла {file['name']} для предприятия {enterprise_code}: {str(e)}")
                    send_notification(f"Ошибка валидации для stock файла п{file['name']} для предприятия {enterprise_code}", "Разработчик")
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logging.info(f"Удален временный файл: {file_path}")

        except Exception as e:
            logging.error(f"Ошибка при обработке остатков для предприятия с кодом {enterprise_code}: {str(e)}")
            send_notification(f"Ошибка при обработке остатков Google Drive для предприятия с кодом {enterprise_code}: {str(e)}", "Разработчик")

async def extract_catalog_from_google_drive(enterprise_code: str):
    """
    Обрабатывает файлы каталога из Google Drive для указанного предприятия.
    """
    async with get_async_db() as db:
        try:
            logging.info(f"Получение настроек для предприятия с кодом: {enterprise_code}")

            # Получение настроек предприятия
            result = await db.execute(
                select(
                    EnterpriseSettings.enterprise_code,
                    EnterpriseSettings.single_store,
                    EnterpriseSettings.store_serial,
                    EnterpriseSettings.google_drive_folder_id_ref,
                ).where(EnterpriseSettings.enterprise_code == enterprise_code)
            )
            enterprise = result.mappings().one_or_none()
            if not enterprise:
                logging.error(f"Не найдены настройки для Google Drive с кодом {enterprise_code}.")
                send_notification(f"Не найдены настройки для Google Drive с кодом {enterprise_code}.", "Разработчик")
                return

            if not enterprise["google_drive_folder_id_ref"]:
                logging.error(f"Отсутствует ID папки Google Drive для каталога для предприятия с кодом {enterprise_code}.")
                send_notification(f"Отсутствует ID папки Google Drive для каталога для предприятия с кодом {enterprise_code}.", "Разработчик")
                return

            # Получение настроек разработчика
            result = await db.execute(select(DeveloperSettings).limit(1))
            developer_settings = result.scalar_one_or_none()
            if not developer_settings:
                logging.error("Не найдены настройки разработчика.")
                send_notification("Не найдены настройки разработчика для Google Drive.", "Разработчик")
                return

            # google_drive_file_name = os.path.abspath(developer_settings.google_drive_file_name)
            # if not os.path.exists(google_drive_file_name):
            #     logging.error(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}")
            #     send_notification(f"Не найден файл учетных данных Google Drive: {google_drive_file_name}", "Разработчик")
            #     return

            # Подключение к Google Drive
            drive_service = await connect_to_google_drive()
            logging.info(f"Успешно подключились к Google Drive для предприятия с кодом {enterprise_code}")

            # Извлечение файлов каталога
            catalog_files = await fetch_files_from_folder(drive_service, enterprise["google_drive_folder_id_ref"])

            # Обработка каждого файла
            for file in catalog_files:
                file_path = await download_file(drive_service, file['id'], file['name'])
                logging.info(f"Проверка каталога. Параметры: enterprise_code={enterprise_code}, "
                            f"file_path={file_path}, file_type='catalog', "
                            f"single_store={enterprise['single_store']}, store_serial={enterprise['store_serial']}")
                try:
                    await validate_data(
                        enterprise_code=enterprise_code,
                        file_path=file_path,
                        file_type="catalog",
                        single_store=enterprise["single_store"],
                        store_serial=enterprise["store_serial"],
                    )
                    logging.info(f"Catalog file {file['name']} validated successfully for enterprise {enterprise_code}")
                except Exception as e:
                    logging.error(f"Ошибка валидации для catalog файла {file['name']} для предприятия {enterprise_code}: {str(e)}")
                    send_notification(f"Ошибка валидации для catalog файла {file['name']} для предприятия {enterprise_code}", "Разработчик")
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logging.info(f"Удален временный файл: {file_path}")

        except Exception as e:
            logging.error(f"Ошибка при обработке каталога для предприятия с кодом {enterprise_code}: {str(e)}")
            send_notification(f"Ошибка при обработке каталога Google Drive для предприятия с кодом {enterprise_code}: {str(e)}", "Разработчик")
import logging
import json
from datetime import datetime
from time import perf_counter
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import NoResultFound
from app.database import DeveloperSettings
from app.models import InventoryData, InventoryStock, EnterpriseSettings
from app.database import get_async_db
from app.services.catalog_export_service import export_catalog
from app.services.stock_export_service import process_stock_file
from app.services.stock_update_service import update_stock
from app.services.notification_service import send_notification

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def process_database_service(file_path: str, data_type: str, enterprise_code: str):
    """
    Обрабатывает данные из JSON и записывает их в базу данных.
    :param file_path: Путь к JSON-файлу
    :param data_type: Тип данных ('catalog' или 'stock')
    :param enterprise_code: Код предприятия
    """
    started = perf_counter()
    logging.info(
        "Database service start: enterprise_code=%s data_type=%s file_path=%s",
        enterprise_code,
        data_type,
        file_path,
    )

    async with get_async_db(commit_on_exit=False) as session:
        try:
            with open(file_path, "r", encoding="utf-8") as json_file:
                raw_data = json.load(json_file)
            cleaned_data = clean_json_keys(raw_data)
            records_count = len(cleaned_data)
            logging.info(
                "Database service payload loaded: enterprise_code=%s data_type=%s records_count=%s",
                enterprise_code,
                data_type,
                records_count,
            )

            if data_type == "catalog":
                settings_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=load_catalog_settings",
                    enterprise_code,
                    data_type,
                )
                enterprise_settings_result = await session.execute(
                    select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
                )
                enterprise_settings = enterprise_settings_result.scalars().one_or_none()
                developer_settings_result = await session.execute(select(DeveloperSettings).limit(1))
                developer_settings = developer_settings_result.scalars().one_or_none()
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=load_catalog_settings elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - settings_started,
                )

                delete_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=delete_old_catalog",
                    enterprise_code,
                    data_type,
                )
                await delete_old_catalog_data(session, enterprise_code)
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=delete_old_catalog elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - delete_started,
                )

                export_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=export_catalog records_count=%s",
                    enterprise_code,
                    data_type,
                    records_count,
                )
                await export_catalog(
                    enterprise_code,
                    raw_data,
                    enterprise_settings=enterprise_settings,
                    developer_settings=developer_settings,
                )  # Экспорт каталога
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=export_catalog elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - export_started,
                )

                save_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=save_catalog records_count=%s",
                    enterprise_code,
                    data_type,
                    records_count,
                )
                await save_catalog_data(cleaned_data, session, enterprise_code)
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=save_catalog elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - save_started,
                )

                flush_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=flush_catalog records_count=%s",
                    enterprise_code,
                    data_type,
                    records_count,
                )
                try:
                    await session.flush()
                except Exception:
                    logging.exception(
                        "Database service phase failure: enterprise_code=%s data_type=%s phase=flush_catalog records_count=%s",
                        enterprise_code,
                        data_type,
                        records_count,
                    )
                    raise
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=flush_catalog elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - flush_started,
                )

            elif data_type == "stock":
                delete_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=delete_old_stock",
                    enterprise_code,
                    data_type,
                )
                await delete_old_stock_data(session, enterprise_code)
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=delete_old_stock elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - delete_started,
                )

                settings_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=load_enterprise_settings",
                    enterprise_code,
                    data_type,
                )
                enterprise_settings = await session.execute(
                    select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
                )
                enterprise_settings = enterprise_settings.scalars().one_or_none()
                developer_settings = None
                if enterprise_settings:
                    developer_settings_started = perf_counter()
                    logging.info(
                        "Database service phase start: enterprise_code=%s data_type=%s phase=load_developer_settings",
                        enterprise_code,
                        data_type,
                    )
                    developer_settings_result = await session.execute(select(DeveloperSettings).limit(1))
                    developer_settings = developer_settings_result.scalars().one_or_none()
                    logging.info(
                        "Database service phase done: enterprise_code=%s data_type=%s phase=load_developer_settings elapsed=%.3fs",
                        enterprise_code,
                        data_type,
                        perf_counter() - developer_settings_started,
                    )
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=load_enterprise_settings elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - settings_started,
                )

                if enterprise_settings:
                    discount_started = perf_counter()
                    logging.info(
                        "Database service phase start: enterprise_code=%s data_type=%s phase=apply_discount_rate records_count=%s",
                        enterprise_code,
                        data_type,
                        len(cleaned_data),
                    )
                    cleaned_data = apply_discount_rate(cleaned_data, enterprise_settings.discount_rate or 0)
                    logging.info(
                        "Database service phase done: enterprise_code=%s data_type=%s phase=apply_discount_rate elapsed=%.3fs",
                        enterprise_code,
                        data_type,
                        perf_counter() - discount_started,
                    )
                    if enterprise_settings.stock_correction:
                        correction_started = perf_counter()
                        logging.info(
                            "Database service phase start: enterprise_code=%s data_type=%s phase=update_stock records_count=%s",
                            enterprise_code,
                            data_type,
                            len(cleaned_data),
                        )
                        cleaned_data = await update_stock(
                            cleaned_data,
                            enterprise_code,
                            enterprise_settings=enterprise_settings,
                            developer_settings=developer_settings,
                        )
                        logging.info(
                            "Database service phase done: enterprise_code=%s data_type=%s phase=update_stock records_count=%s elapsed=%.3fs",
                            enterprise_code,
                            data_type,
                            len(cleaned_data),
                            perf_counter() - correction_started,
                        )

                try:
                    export_started = perf_counter()
                    logging.info(
                        "Database service phase start: enterprise_code=%s data_type=%s phase=export_stock records_count=%s",
                        enterprise_code,
                        data_type,
                        len(cleaned_data),
                    )
                    await process_stock_file(
                        enterprise_code,
                        cleaned_data,
                        enterprise_settings=enterprise_settings,
                        developer_settings=developer_settings,
                    )  # Экспорт стока
                    logging.info(
                        "Database service phase done: enterprise_code=%s data_type=%s phase=export_stock elapsed=%.3fs",
                        enterprise_code,
                        data_type,
                        perf_counter() - export_started,
                    )
                except Exception as export_error:
                    logging.error(f"Ошибка экспорта стока для {enterprise_code}: {export_error}")
                    send_notification(f"Ошибка экспорта стока для {enterprise_code}: {export_error}", enterprise_code)
                    raise

                save_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=save_stock records_count=%s",
                    enterprise_code,
                    data_type,
                    len(cleaned_data),
                )
                await save_stock_data(cleaned_data, session, enterprise_code)
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=save_stock elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - save_started,
                )

                flush_started = perf_counter()
                logging.info(
                    "Database service phase start: enterprise_code=%s data_type=%s phase=flush_stock records_count=%s",
                    enterprise_code,
                    data_type,
                    len(cleaned_data),
                )
                try:
                    await session.flush()
                except Exception:
                    logging.exception(
                        "Database service phase failure: enterprise_code=%s data_type=%s phase=flush_stock records_count=%s",
                        enterprise_code,
                        data_type,
                        len(cleaned_data),
                    )
                    raise
                logging.info(
                    "Database service phase done: enterprise_code=%s data_type=%s phase=flush_stock elapsed=%.3fs",
                    enterprise_code,
                    data_type,
                    perf_counter() - flush_started,
                )

            else:
                raise ValueError(f"Неизвестный тип данных: {data_type}")

            update_started = perf_counter()
            logging.info(
                "Database service phase start: enterprise_code=%s data_type=%s phase=update_last_upload",
                enterprise_code,
                data_type,
            )
            await update_last_upload(session, enterprise_code, data_type)
            logging.info(
                "Database service phase done: enterprise_code=%s data_type=%s phase=update_last_upload elapsed=%.3fs",
                enterprise_code,
                data_type,
                perf_counter() - update_started,
            )

            commit_started = perf_counter()
            logging.info(
                "Database service phase start: enterprise_code=%s data_type=%s phase=commit",
                enterprise_code,
                data_type,
            )
            await session.commit()
            logging.info(
                "Database service phase done: enterprise_code=%s data_type=%s phase=commit elapsed=%.3fs",
                enterprise_code,
                data_type,
                perf_counter() - commit_started,
            )
            logging.info(
                "Данные %s успешно записаны в базу данных для предприятия %s elapsed=%.3fs",
                data_type,
                enterprise_code,
                perf_counter() - started,
            )

        except Exception as e:
            logging.error(f"Ошибка записи данных в базу: {str(e)}")
            logging.exception(
                "DB session failure in process_database_service enterprise_code=%s data_type=%s",
                enterprise_code,
                data_type,
            )
            send_notification(f"Ошибка записи данных в базу: {str(e)} для {enterprise_code}", enterprise_code)
            await session.rollback()
            raise

def clean_json_keys(data: list):
    """
    Удаляет пробелы и приводит ключи в JSON к нижнему регистру.
    :param data: Исходные данные в виде списка словарей
    :return: Данные с очищенными ключами
    """
    return [{k.strip().lower(): v for k, v in record.items()} for record in data]

def apply_discount_rate(data: list, discount_rate: float):
    """
    Применяет скидку к 'price_reserve' в данных.
    :param data: Список записей
    :param discount_rate: Процент скидки
    :return: Обновленные данные
    """
    if discount_rate > 0:
        for item in data:
            if 'price_reserve' in item and item['price_reserve'] is not None:
                item['price_reserve'] = round(item['price_reserve'] * (1 - discount_rate / 100), 2)
    return data

async def delete_old_catalog_data(session: AsyncSession, enterprise_code: str):
    """
    Удаляет старые данные каталога по enterprise_code.
    """
    await session.execute(
        InventoryData.__table__.delete().where(InventoryData.enterprise_code == enterprise_code)
    )

async def delete_old_stock_data(session: AsyncSession, enterprise_code: str):
    """
    Удаляет старые данные остатков по enterprise_code.
    """
    await session.execute(
        InventoryStock.__table__.delete().where(InventoryStock.enterprise_code == enterprise_code)
    )

async def save_catalog_data(data: list, session: AsyncSession, enterprise_code: str):
    """
    Сохраняет данные каталога в таблицу InventoryData.
    :param data: Список записей каталога
    :param session: Сессия базы данных
    :param enterprise_code: Код предприятия
    """
    for record in data:
        record["enterprise_code"] = enterprise_code  # Добавляем enterprise_code в данные
        session.add(InventoryData(**record))

async def save_stock_data(data: list, session: AsyncSession, enterprise_code: str):
    """
    Сохраняет данные остатков в таблицу InventoryStock.
    :param data: Список записей остатков
    :param session: Сессия базы данных
    :param enterprise_code: Код предприятия
    """
    for record in data:
        record["enterprise_code"] = enterprise_code  # Добавляем enterprise_code в данные
        session.add(InventoryStock(**record))

async def update_last_upload(session: AsyncSession, enterprise_code: str, data_type: str):
    """
    Обновляет поля last_stock_upload или last_catalog_upload в таблице EnterpriseSettings.
    :param session: Сессия базы данных
    :param enterprise_code: Код предприятия
    :param data_type: Тип данных ('catalog' или 'stock')
    """
    current_time = datetime.utcnow()
    stmt = select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    result = await session.execute(stmt)
    enterprise_settings = result.scalars().one_or_none()

    if not enterprise_settings:
        raise ValueError(f"Предприятие с кодом {enterprise_code} не найдено.")

    if data_type == "catalog":
        enterprise_settings.last_catalog_upload = current_time
    elif data_type == "stock":
        enterprise_settings.last_stock_upload = current_time

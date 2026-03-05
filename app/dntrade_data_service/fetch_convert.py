import json
import logging
import os
import tempfile
from typing import Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from sqlalchemy.future import select

from app.database import EnterpriseSettings, get_async_db
from app.dntrade_data_service.client import DEFAULT_LIMIT, fetch_products_page
from app.services.database_service import process_database_service

load_dotenv()

DEFAULT_VAT = 20
LIMIT = DEFAULT_LIMIT
MAX_PAGES = int(os.getenv("DNTRADE_CATALOG_MAX_PAGES", "2000"))
MAX_REPEAT_PAGES = int(os.getenv("DNTRADE_CATALOG_MAX_REPEAT_PAGES", "3"))
logger = logging.getLogger(__name__)

async def fetch_enterprise_settings(enterprise_code):
    """Получение настроек предприятия по enterprise_code из EnterpriseSettings."""
    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()


def transform_products(products):
    """Трансформация данных продуктов в целевой формат."""
    transformed = []
    seen_product_ids = set()

    for product in products:
        product_id = product.get("product_id")
        if not product_id:
            continue
        if product_id in seen_product_ids:
            continue  # Пропускаем дублирующийся product_id

        producer = product.get("short_description")
        if not producer or producer in [None, "", 0]:  # Фильтрация некорректных значений
            producer = ""
        transformed.append({
            "code": product_id,
            "name": product.get("title"),
            "vat": DEFAULT_VAT,
            "producer": producer,
            "barcode": product.get("barcode"),
        })
        seen_product_ids.add(product_id)  # Запоминаем обработанный product_id
    return transformed

def save_to_json(data, enterprise_code, file_type):
    """Сохранение данных в JSON-файл в указанную временную директорию из .env."""
    try:
        # Получаем временный путь из переменной окружения, иначе используем системный temp
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)

        # Формируем путь к JSON-файлу
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_data.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logging.info(f"JSON записан в файл: {json_file_path}")
        return json_file_path
    except IOError as e:
        logging.error(f"Ошибка при сохранении JSON-файла: {e}")
        return None

async def run_service(enterprise_code, file_type):
    """Основной сервис выполнения задачи."""
    logger.info("Dntrade catalog start: enterprise_code=%s", enterprise_code)
    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        logger.warning("Dntrade catalog stop: enterprise settings not found for %s", enterprise_code)
        return

    api_key = enterprise_settings.token
    if not api_key:
        logger.warning("Dntrade catalog stop: empty token for %s", enterprise_code)
        return

    all_products = []
    offset = 0
    pages_fetched = 0
    repeated_page_count = 0
    last_fingerprint: Optional[Tuple[int, str, str]] = None

    async with aiohttp.ClientSession() as session:
        while True:
            if pages_fetched >= MAX_PAGES:
                logger.warning(
                    "Dntrade catalog stop: max pages reached (%s) for enterprise_code=%s",
                    MAX_PAGES,
                    enterprise_code,
                )
                break
            response = await fetch_products_page(
                session=session,
                api_key=api_key,
                offset=offset,
                limit=LIMIT,
            )

            if response is None:
                logger.warning(
                    "Dntrade catalog stop: empty/invalid response at offset=%s enterprise_code=%s",
                    offset,
                    enterprise_code,
                )
                break  # Если нет ответа от API, прерываем цикл

            products = response.get("products", [])
            if not products:
                logger.info(
                    "Dntrade catalog stop: no more products at offset=%s enterprise_code=%s",
                    offset,
                    enterprise_code,
                )
                break  # Если список продуктов пуст, заканчиваем

            first_id = str(products[0].get("product_id", ""))
            last_id = str(products[-1].get("product_id", ""))
            current_fingerprint = (len(products), first_id, last_id)
            if current_fingerprint == last_fingerprint:
                repeated_page_count += 1
                if repeated_page_count >= MAX_REPEAT_PAGES:
                    logger.warning(
                        "Dntrade catalog stop: repeating page detected %s times at offset=%s enterprise_code=%s",
                        repeated_page_count,
                        offset,
                        enterprise_code,
                    )
                    break
            else:
                repeated_page_count = 0
            last_fingerprint = current_fingerprint

            all_products.extend(products)
            offset += len(products)  # Увеличиваем offset на реальный размер страницы
            pages_fetched += 1

            if pages_fetched % 10 == 0:
                logger.info(
                    "Dntrade catalog progress: enterprise_code=%s pages=%s products=%s offset=%s",
                    enterprise_code,
                    pages_fetched,
                    len(all_products),
                    offset,
                )

    if not all_products:
        logger.warning("Dntrade catalog stop: no products fetched for %s", enterprise_code)
        return  # Нет данных для сохранения

    transformed_data = transform_products(all_products)
    file_type = "catalog"
    json_file_path = save_to_json(transformed_data, enterprise_code, file_type)

    if not json_file_path:
        logger.error("Dntrade catalog stop: failed to write json for %s", enterprise_code)
        return  # Ошибка сохранения JSON

    await process_database_service(json_file_path, file_type, enterprise_code)
    logger.info(
        "Dntrade catalog done: enterprise_code=%s fetched_products=%s transformed=%s pages=%s",
        enterprise_code,
        len(all_products),
        len(transformed_data),
        pages_fetched,
    )

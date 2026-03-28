import logging
import json
from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import DeveloperSettings
from app.models import InventoryData, InventoryStock, EnterpriseSettings
from app.database import get_async_db
from app.services.catalog_export_service import export_catalog
from app.services.stock_export_service import process_stock_file
from app.services.stock_update_service import update_stock
from app.services.notification_service import send_notification

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


@dataclass
class SettingsContext:
    enterprise_settings: EnterpriseSettings | None = None
    developer_settings: DeveloperSettings | None = None


@dataclass
class ValidationSummary:
    records_count: int
    duplicate_count: int
    invalid_count: int
    normalized_count: int
    duplicate_samples: list[str]
    invalid_samples: list[str]


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
            raw_data, cleaned_data = _load_payload(file_path)
            records_count = len(cleaned_data)
            logging.info(
                "Database service payload loaded: enterprise_code=%s data_type=%s records_count=%s",
                enterprise_code,
                data_type,
                records_count,
            )

            if data_type == "catalog":
                settings_context = await _run_phase(
                    enterprise_code,
                    data_type,
                    "load_catalog_settings",
                    _load_catalog_settings_context,
                    session,
                    enterprise_code,
                )
                await _process_catalog_flow(
                    session=session,
                    enterprise_code=enterprise_code,
                    data_type=data_type,
                    raw_data=raw_data,
                    cleaned_data=cleaned_data,
                    records_count=records_count,
                    settings_context=settings_context,
                )

            elif data_type == "stock":
                settings_context = await _load_stock_settings_context(session, enterprise_code, data_type)
                cleaned_data = await _process_stock_flow(
                    session=session,
                    enterprise_code=enterprise_code,
                    data_type=data_type,
                    cleaned_data=cleaned_data,
                    settings_context=settings_context,
                )

            else:
                raise ValueError(f"Неизвестный тип данных: {data_type}")

            await _run_phase(
                enterprise_code,
                data_type,
                "update_last_upload",
                update_last_upload,
                session,
                enterprise_code,
                data_type,
            )
            await _run_phase(
                enterprise_code,
                data_type,
                "commit",
                session.commit,
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


def _load_payload(file_path: str) -> tuple[list, list]:
    with open(file_path, "r", encoding="utf-8") as json_file:
        raw_data = json.load(json_file)
    cleaned_data = clean_json_keys(raw_data)
    return raw_data, cleaned_data


async def _run_phase(
    enterprise_code: str,
    data_type: str,
    phase: str,
    func,
    *args,
    records_count: int | None = None,
    **kwargs,
):
    started = perf_counter()
    log_kwargs = {
        "enterprise_code": enterprise_code,
        "data_type": data_type,
        "phase": phase,
    }
    if records_count is not None:
        log_kwargs["records_count"] = records_count
        logging.info(
            "Database service phase start: enterprise_code=%s data_type=%s phase=%s records_count=%s",
            enterprise_code,
            data_type,
            phase,
            records_count,
        )
    else:
        logging.info(
            "Database service phase start: enterprise_code=%s data_type=%s phase=%s",
            enterprise_code,
            data_type,
            phase,
        )

    try:
        result = await func(*args, **kwargs)
    except Exception:
        logging.exception(
            "Database service phase failure: enterprise_code=%s data_type=%s phase=%s%s",
            enterprise_code,
            data_type,
            phase,
            f" records_count={records_count}" if records_count is not None else "",
        )
        raise

    if records_count is not None:
        logging.info(
            "Database service phase done: enterprise_code=%s data_type=%s phase=%s elapsed=%.3fs records_count=%s",
            enterprise_code,
            data_type,
            phase,
            perf_counter() - started,
            records_count,
        )
    else:
        logging.info(
            "Database service phase done: enterprise_code=%s data_type=%s phase=%s elapsed=%.3fs",
            enterprise_code,
            data_type,
            phase,
            perf_counter() - started,
        )
    return result


async def _load_catalog_settings_context(session: AsyncSession, enterprise_code: str) -> SettingsContext:
    enterprise_settings_result = await session.execute(
        select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    )
    developer_settings_result = await session.execute(select(DeveloperSettings).limit(1))
    return SettingsContext(
        enterprise_settings=enterprise_settings_result.scalars().one_or_none(),
        developer_settings=developer_settings_result.scalars().one_or_none(),
    )


async def _load_stock_settings_context(
    session: AsyncSession,
    enterprise_code: str,
    data_type: str,
) -> SettingsContext:
    settings_context = SettingsContext()
    settings_context.enterprise_settings = await _run_phase(
        enterprise_code,
        data_type,
        "load_enterprise_settings",
        _fetch_enterprise_settings,
        session,
        enterprise_code,
    )
    if settings_context.enterprise_settings:
        settings_context.developer_settings = await _run_phase(
            enterprise_code,
            data_type,
            "load_developer_settings",
            _fetch_developer_settings,
            session,
        )
    return settings_context


async def _process_catalog_flow(
    session: AsyncSession,
    enterprise_code: str,
    data_type: str,
    raw_data: list,
    cleaned_data: list,
    records_count: int,
    settings_context: SettingsContext,
):
    validation_summary = await _run_phase(
        enterprise_code,
        data_type,
        "validate_catalog_payload",
        _validate_catalog_phase,
        cleaned_data,
        enterprise_code,
        records_count=records_count,
    )
    _log_validation_summary(
        enterprise_code=enterprise_code,
        data_type=data_type,
        phase="validate_catalog_payload",
        summary=validation_summary,
    )
    await _run_phase(
        enterprise_code,
        data_type,
        "delete_old_catalog",
        delete_old_catalog_data,
        session,
        enterprise_code,
    )
    await _run_phase(
        enterprise_code,
        data_type,
        "export_catalog",
        export_catalog,
        enterprise_code,
        raw_data,
        records_count=records_count,
        enterprise_settings=settings_context.enterprise_settings,
        developer_settings=settings_context.developer_settings,
    )
    await _run_phase(
        enterprise_code,
        data_type,
        "save_catalog",
        save_catalog_data,
        cleaned_data,
        session,
        enterprise_code,
        records_count=records_count,
    )
    await _run_phase(
        enterprise_code,
        data_type,
        "flush_catalog",
        session.flush,
        records_count=records_count,
    )


async def _process_stock_flow(
    session: AsyncSession,
    enterprise_code: str,
    data_type: str,
    cleaned_data: list,
    settings_context: SettingsContext,
) -> list:
    await _run_phase(
        enterprise_code,
        data_type,
        "delete_old_stock",
        delete_old_stock_data,
        session,
        enterprise_code,
    )

    if settings_context.enterprise_settings:
        cleaned_data = await _run_phase(
            enterprise_code,
            data_type,
            "apply_discount_rate",
            _apply_discount_phase,
            cleaned_data,
            settings_context.enterprise_settings.discount_rate or 0,
            records_count=len(cleaned_data),
        )
        if settings_context.enterprise_settings.stock_correction:
            cleaned_data = await _run_phase(
                enterprise_code,
                data_type,
                "update_stock",
                update_stock,
                cleaned_data,
                enterprise_code,
                records_count=len(cleaned_data),
                enterprise_settings=settings_context.enterprise_settings,
                developer_settings=settings_context.developer_settings,
            )

    validation_summary = await _run_phase(
        enterprise_code,
        data_type,
        "validate_stock_payload",
        _validate_stock_phase,
        cleaned_data,
        enterprise_code,
        records_count=len(cleaned_data),
    )
    _log_validation_summary(
        enterprise_code=enterprise_code,
        data_type=data_type,
        phase="validate_stock_payload",
        summary=validation_summary,
    )

    await _run_phase(
        enterprise_code,
        data_type,
        "export_stock",
        process_stock_file,
        enterprise_code,
        cleaned_data,
        records_count=len(cleaned_data),
        enterprise_settings=settings_context.enterprise_settings,
        developer_settings=settings_context.developer_settings,
    )
    await _run_phase(
        enterprise_code,
        data_type,
        "save_stock",
        save_stock_data,
        cleaned_data,
        session,
        enterprise_code,
        records_count=len(cleaned_data),
    )
    await _run_phase(
        enterprise_code,
        data_type,
        "flush_stock",
        session.flush,
        records_count=len(cleaned_data),
    )
    return cleaned_data


async def _fetch_enterprise_settings(session: AsyncSession, enterprise_code: str) -> EnterpriseSettings | None:
    result = await session.execute(
        select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    )
    return result.scalars().one_or_none()


async def _fetch_developer_settings(session: AsyncSession) -> DeveloperSettings | None:
    result = await session.execute(select(DeveloperSettings).limit(1))
    return result.scalars().one_or_none()


async def _apply_discount_phase(cleaned_data: list, discount_rate: float) -> list:
    return apply_discount_rate(cleaned_data, discount_rate)


async def _validate_catalog_phase(cleaned_data: list, enterprise_code: str) -> ValidationSummary:
    required_fields = ("code", "name", "vat")
    duplicate_samples: list[str] = []
    invalid_samples: list[str] = []
    seen_codes: set[str] = set()
    duplicate_count = 0
    invalid_count = 0
    normalized_count = 0

    for index, record in enumerate(cleaned_data):
        if record.get("producer") is None:
            record["producer"] = ""
            normalized_count += 1

        missing = [
            field for field in required_fields
            if record.get(field) in (None, "")
        ]
        code = str(record.get("code") or "").strip()

        if missing:
            invalid_count += 1
            if len(invalid_samples) < 10:
                invalid_samples.append(
                    f"idx={index} missing={','.join(missing)} code={code or '<empty>'}"
                )

        if not code:
            continue

        if code in seen_codes:
            duplicate_count += 1
            if len(duplicate_samples) < 10:
                duplicate_samples.append(code)
        else:
            seen_codes.add(code)

    if invalid_count or duplicate_count:
        raise ValueError(
            "Catalog payload validation failed for "
            f"enterprise_code={enterprise_code}: invalid={invalid_count} "
            f"duplicates={duplicate_count} duplicate_samples={duplicate_samples} "
            f"invalid_samples={invalid_samples}"
        )

    return ValidationSummary(
        records_count=len(cleaned_data),
        duplicate_count=duplicate_count,
        invalid_count=invalid_count,
        normalized_count=normalized_count,
        duplicate_samples=duplicate_samples,
        invalid_samples=invalid_samples,
    )


async def _validate_stock_phase(cleaned_data: list, enterprise_code: str) -> ValidationSummary:
    required_fields = ("branch", "code", "price", "qty")
    duplicate_samples: list[str] = []
    invalid_samples: list[str] = []
    seen_keys: set[tuple[str, str]] = set()
    duplicate_count = 0
    invalid_count = 0

    for index, record in enumerate(cleaned_data):
        missing = [
            field for field in required_fields
            if record.get(field) in (None, "")
        ]
        branch = str(record.get("branch") or "").strip()
        code = str(record.get("code") or "").strip()
        key = (branch, code)

        if missing:
            invalid_count += 1
            if len(invalid_samples) < 10:
                invalid_samples.append(
                    f"idx={index} missing={','.join(missing)} branch={branch or '<empty>'} code={code or '<empty>'}"
                )

        price = record.get("price")
        price_reserve = record.get("price_reserve")
        qty = record.get("qty")
        if price is not None and price_reserve is not None and price_reserve > price:
            invalid_count += 1
            if len(invalid_samples) < 10:
                invalid_samples.append(
                    f"idx={index} branch={branch or '<empty>'} code={code or '<empty>'} price_reserve_gt_price"
                )
        if qty is not None and qty < 0:
            invalid_count += 1
            if len(invalid_samples) < 10:
                invalid_samples.append(
                    f"idx={index} branch={branch or '<empty>'} code={code or '<empty>'} negative_qty"
                )

        if not branch or not code:
            continue

        if key in seen_keys:
            duplicate_count += 1
            if len(duplicate_samples) < 10:
                duplicate_samples.append(f"{branch}:{code}")
        else:
            seen_keys.add(key)

    if invalid_count or duplicate_count:
        raise ValueError(
            "Stock payload validation failed for "
            f"enterprise_code={enterprise_code}: invalid={invalid_count} "
            f"duplicates={duplicate_count} duplicate_samples={duplicate_samples} "
            f"invalid_samples={invalid_samples}"
        )

    return ValidationSummary(
        records_count=len(cleaned_data),
        duplicate_count=duplicate_count,
        invalid_count=invalid_count,
        normalized_count=0,
        duplicate_samples=duplicate_samples,
        invalid_samples=invalid_samples,
    )


def _log_validation_summary(
    *,
    enterprise_code: str,
    data_type: str,
    phase: str,
    summary: ValidationSummary,
) -> None:
    logging.info(
        "Database service validation summary: enterprise_code=%s data_type=%s phase=%s records_count=%s invalid=%s duplicates=%s normalized=%s",
        enterprise_code,
        data_type,
        phase,
        summary.records_count,
        summary.invalid_count,
        summary.duplicate_count,
        summary.normalized_count,
    )

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

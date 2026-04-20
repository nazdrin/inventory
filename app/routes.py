from dotenv import load_dotenv
load_dotenv()
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Request, Security, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from fastapi.security import HTTPBearer
from typing import List, Any
import os
import json
import tempfile
import math
from datetime import timedelta

from app import crud, schemas, database
from app.schemas import (
    EnterpriseSettingsSchema, DeveloperSettingsSchema, DataFormatSchema, 
    MappingBranchSchema, LoginSchema, BranchMappingListItemVM, BranchMappingDetailVM,
    MappingBranchConstrainedUpdateSchema, EnterpriseListItemVM, EnterpriseDetailVM,
    EnterpriseFieldMetaVM, EnterpriseSectionVM, SupplierListItemVM, SupplierDetailVM,
    SupplierSectionVM, BusinessSettingsVM, BusinessSectionVM, BusinessSettingItemVM,
    BusinessEnterpriseCandidateVM, BusinessEnterpriseOptionVM, BusinessSettingsUpdateSchema,
    BusinessEnterpriseOperationalFieldsUpdateSchema, BusinessPricingSettingsUpdateSchema,
    BusinessStoreCreate, BusinessStoreUpdate, BusinessStoreOut, LegacyScopeOut,
    BusinessEnterpriseOptionOut,
)
from app.database import (
    DeveloperSettings, EnterpriseSettings, DataFormat, MappingBranch, AsyncSessionLocal
)
from app.services.database_service import process_database_service
from app.services.notification_service import send_notification
from app.unipro_data_service.unipro_conv import unipro_convert
from app.auth import create_access_token, verify_token
import logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from fastapi import BackgroundTasks, Body
async def get_db():
    async with AsyncSessionLocal() as db:
        yield db
# ——— Логгер "salesdrive" — пишем в ./logs/salesdrive_webhook.log и в консоль ———
LOG_DIR = os.getenv("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)

sd_logger = logging.getLogger("salesdrive")
sd_logger.setLevel(logging.INFO)
business_settings_logger = logging.getLogger("business_settings")

# ⏩ ОДИН РАЗ указываем prefix, а внутри маршрутов больше не дублируем developer_panel
router = APIRouter(prefix="/developer_panel", tags=["Developer Panel"])


from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.auth import verify_token  # как у enterprise
from app.models import (
    DropshipEnterprise,
    BusinessSettings,
    BusinessStore,
    Offer,
    BusinessStoreProductCode,
    BusinessStoreProductName,
    BusinessStoreProductPriceAdjustment,
)
from app.schemas import DropshipEnterpriseSchema
from app.business.business_store_name_generator import cleanup_store_product_names
from app.services.business_pricing_settings_resolver import (
    BUSINESS_PRICING_FIELD_SPECS,
    BUSINESS_PRICING_GROUP_SPECS,
    BusinessPricingSettingsSnapshot,
    load_business_pricing_settings_snapshot,
)
from app.business.business_store_export_dry_run import (
    build_store_catalog_dry_run,
    build_store_stock_dry_run,
)


def _mapping_semantic_store_label(data_format: str | None) -> str:
    fmt = (data_format or "").strip()
    labels = {
        "Dntrade": "External Store",
        "Bioteca": "External Store",
        "Vetmanager": "Clinic/Store Mapping",
        "FtpMulti": "Filename Mapping",
        "TorgsoftGoogleMulti": "External Store",
        "Unipro": "Source Routing Token",
    }
    return labels.get(fmt, "External Store / Mapping Value")


def _mapping_runtime_consumers(
    data_format: str | None,
    *,
    has_google_folder: bool,
    has_telegram_target: bool,
) -> list[str]:
    fmt = (data_format or "").strip()
    consumers: list[str] = ["Branch routing for stock/catalog runtime"]

    if fmt in {"Dntrade", "Bioteca"}:
        consumers.append("Adapter store-to-branch mapping")
    elif fmt == "Vetmanager":
        consumers.append("Clinic/store routing for Vetmanager stock flow")
    elif fmt == "FtpMulti":
        consumers.append("Filename-to-branch routing")
    elif fmt in {"TorgsoftGoogle", "TorgsoftGoogleMulti", "JetVet"}:
        consumers.append("File-driven branch routing")
    elif fmt == "Unipro":
        consumers.append("Source id to enterprise/branch routing")

    if has_google_folder:
        consumers.append("Google Drive folder-based branch lookup")
    if has_telegram_target:
        consumers.append("Telegram branch notifications")

    return consumers


def _mapping_field_notes(data_format: str | None, has_google_folder: bool) -> list[str]:
    notes = [
        "branch is the stable storage identity for MappingBranch and is used directly by runtime consumers",
        "store_id remains the raw storage value and should be shown with semantic UI labels instead of being renamed in storage",
    ]
    if data_format == "Vetmanager":
        notes.append("For Vetmanager, store_id may contain CSV-like clinic/store semantics rather than a single external store id")
    if data_format == "FtpMulti":
        notes.append("For FtpMulti, store_id may behave like a filename or source selector rather than a numeric store id")
    if has_google_folder:
        notes.append("google_folder_id is only relevant for file-driven branch/folder flows")
    return notes


def _mapping_overloaded_fields(data_format: str | None, has_google_folder: bool) -> list[str]:
    overloaded = ["store_id"]
    if has_google_folder or data_format in {"TorgsoftGoogle", "TorgsoftGoogleMulti", "JetVet"}:
        overloaded.append("google_folder_id")
    return overloaded


def _mapping_semantics_summary(data_format: str | None, semantic_store_label: str) -> str:
    fmt = (data_format or "").strip() or "unknown format"
    return f"Format={fmt}; store_id should be presented as '{semantic_store_label}' rather than a raw generic storage field."


def _resolve_business_settings_enterprise(
    *,
    all_enterprises: list[EnterpriseSettings],
    business_settings_row: BusinessSettings | None,
) -> EnterpriseSettings | None:
    enterprise_lookup = _enterprise_lookup_by_code(all_enterprises)
    if business_settings_row is not None:
        db_primary_code = str(business_settings_row.business_enterprise_code or "").strip() or None
        return enterprise_lookup.get(str(db_primary_code or ""))

    candidates = _filter_business_candidates(all_enterprises)
    _resolution_status, _resolution_message, resolved = _business_resolution_state(candidates)
    return resolved


def _supplier_split_cities(city_value: str | None) -> list[str]:
    raw = str(city_value or "").strip()
    if not raw:
        return []

    items: list[str] = []
    for part in raw.replace("|", ",").replace(";", ",").split(","):
        normalized = part.strip()
        if normalized:
            items.append(normalized)
    return items


def _supplier_source_summary(item: DropshipEnterprise) -> str:
    source_bits: list[str] = []
    if (item.feed_url or "").strip():
        source_bits.append("main source set")
    else:
        source_bits.append("main source empty")

    if (item.gdrive_folder or "").strip():
        source_bits.append("aux source set")

    return ", ".join(source_bits)


def _supplier_pricing_summary(item: DropshipEnterprise) -> str:
    bits: list[str] = []
    if item.is_rrp:
        bits.append("RRP")
    if item.profit_percent is not None:
        bits.append(f"opt calc {item.profit_percent:g}%")
    if item.retail_markup is not None:
        bits.append(f"retail markup {item.retail_markup:g}%")
    if item.min_markup_threshold is not None:
        bits.append(f"min add {item.min_markup_threshold:g}")
    return ", ".join(bits) if bits else "pricing defaults not configured"


def _supplier_flags_summary(item: DropshipEnterprise) -> str:
    flags: list[str] = ["active" if item.is_active else "inactive"]
    if item.use_feed_instead_of_gdrive:
        flags.append("dumping mode")
    if item.priority is not None:
        flags.append(f"priority {item.priority}")
    return ", ".join(flags)


def _supplier_display_name(item: DropshipEnterprise) -> str:
    code = str(item.code or "").strip()
    name = str(item.name or "").strip()
    if code and name:
        return f"{name} ({code})"
    return name or code or "Supplier"


def _supplier_sections() -> list[SupplierSectionVM]:
    return [
        SupplierSectionVM(key="main", title="Основное"),
        SupplierSectionVM(key="source", title="Источник"),
        SupplierSectionVM(key="pricing", title="Ценообразование"),
        SupplierSectionVM(key="orders", title="Заказы"),
        SupplierSectionVM(key="schedule", title="График недоступности"),
        SupplierSectionVM(key="technical", title="Технические", collapsible=True, default_open=False),
    ]


def _build_supplier_list_item_vm(item: DropshipEnterprise) -> SupplierListItemVM:
    return SupplierListItemVM(
        code=item.code,
        display_name=_supplier_display_name(item),
        is_active=bool(item.is_active),
        cities_list=_supplier_split_cities(item.city),
        source_summary=_supplier_source_summary(item),
        pricing_summary=_supplier_pricing_summary(item),
        flags_summary=_supplier_flags_summary(item),
    )


def _build_supplier_detail_vm(item: DropshipEnterprise) -> SupplierDetailVM:
    return SupplierDetailVM(
        code=item.code,
        display_name=_supplier_display_name(item),
        name=item.name,
        is_active=bool(item.is_active),
        salesdrive_supplier_id=item.salesdrive_supplier_id,
        biotus_orders_enabled=bool(item.biotus_orders_enabled),
        np_fulfillment_enabled=bool(item.np_fulfillment_enabled),
        schedule_enabled=bool(item.schedule_enabled),
        block_start_day=item.block_start_day,
        block_start_time=item.block_start_time,
        block_end_day=item.block_end_day,
        block_end_time=item.block_end_time,
        cities_raw=item.city,
        cities_list=_supplier_split_cities(item.city),
        feed_url=item.feed_url,
        gdrive_folder=item.gdrive_folder,
        is_rrp=bool(item.is_rrp),
        profit_percent=item.profit_percent,
        retail_markup=item.retail_markup,
        min_markup_threshold=item.min_markup_threshold,
        priority=int(item.priority or 5),
        use_feed_instead_of_gdrive=bool(item.use_feed_instead_of_gdrive),
        source_summary=_supplier_source_summary(item),
        pricing_summary=_supplier_pricing_summary(item),
        flags_summary=_supplier_flags_summary(item),
        sections=_supplier_sections(),
    )


def _enterprise_is_file_related_format(data_format: str | None) -> bool:
    return (data_format or "").strip() in {
        "GoogleDrive",
        "JetVet",
        "TorgsoftGoogle",
        "TorgsoftGoogleMulti",
        "Ftp",
        "FtpMulti",
        "FtpZoomagazin",
        "FtpTabletki",
    }


def _enterprise_has_format_specific_fields(enterprise: EnterpriseSettings) -> bool:
    return bool(
        enterprise.single_store
        or (enterprise.store_serial or "").strip()
        or (enterprise.google_drive_folder_id_ref or "").strip()
        or (enterprise.google_drive_folder_id_rest or "").strip()
    )


def _enterprise_field_meta() -> list[EnterpriseFieldMetaVM]:
    return [
        EnterpriseFieldMetaVM(key="enterprise_code", label="Код предприятия", field_type="text"),
        EnterpriseFieldMetaVM(key="enterprise_name", label="Название предприятия", field_type="text"),
        EnterpriseFieldMetaVM(
            key="data_format",
            label="Формат данных",
            field_type="select",
            help_text="Формат определяет runtime adapter. Значение Blank остаётся legacy-семантикой для части ingest-flow, а не рекомендуемым способом отключения предприятия.",
        ),
        EnterpriseFieldMetaVM(
            key="branch_id",
            label="Branch ID",
            field_type="text",
            help_text="Используется в export/routing потоках и связан с enterprise-level routing.",
        ),
        EnterpriseFieldMetaVM(
            key="catalog_upload_frequency",
            label="Частота загрузки каталога",
            field_type="number",
        ),
        EnterpriseFieldMetaVM(
            key="stock_upload_frequency",
            label="Частота загрузки остатков",
            field_type="number",
        ),
        EnterpriseFieldMetaVM(
            key="catalog_enabled",
            label="Выгрузка каталога включена",
            field_type="checkbox",
        ),
        EnterpriseFieldMetaVM(
            key="stock_enabled",
            label="Выгрузка остатков включена",
            field_type="checkbox",
        ),
        EnterpriseFieldMetaVM(
            key="order_fetcher",
            label="Получение заказов",
            field_type="checkbox",
            help_text="Отдельный toggle заказов. Не заменяет и не описывает отключение catalog/stock ingest.",
        ),
        EnterpriseFieldMetaVM(key="tabletki_login", label="Логин Tabletki", field_type="text"),
        EnterpriseFieldMetaVM(key="tabletki_password", label="Пароль Tabletki", field_type="password"),
        EnterpriseFieldMetaVM(key="auto_confirm", label="Автоматическое бронирование", field_type="checkbox"),
        EnterpriseFieldMetaVM(
            key="discount_rate",
            label="Размер скидки",
            field_type="number",
        ),
        EnterpriseFieldMetaVM(
            key="stock_correction",
            label="Коррекция остатков",
            field_type="checkbox",
        ),
        EnterpriseFieldMetaVM(
            key="token",
            label="Токен / URL / ключ подключения",
            field_type="text",
            help_text="Универсальное поле подключения. Его смысл зависит от выбранного формата.",
        ),
        EnterpriseFieldMetaVM(
            key="single_store",
            label="Single Store",
            field_type="checkbox",
            help_text="Используется только в части file/google-driven форматов.",
        ),
        EnterpriseFieldMetaVM(
            key="store_serial",
            label="Store Serial",
            field_type="text",
            help_text="Дополнительное file/google-driven поле маршрутизации.",
        ),
        EnterpriseFieldMetaVM(
            key="google_drive_folder_id_ref",
            label="Google Drive Folder ID для каталога",
            field_type="text",
            help_text="Используется только в file/google-driven сценариях.",
        ),
        EnterpriseFieldMetaVM(
            key="google_drive_folder_id_rest",
            label="Google Drive Folder ID для остатков",
            field_type="text",
            help_text="Используется только в file/google-driven сценариях.",
        ),
        EnterpriseFieldMetaVM(
            key="last_stock_upload",
            label="Последняя загрузка остатков",
            field_type="datetime",
            readonly=True,
        ),
        EnterpriseFieldMetaVM(
            key="last_catalog_upload",
            label="Последняя загрузка каталога",
            field_type="datetime",
            readonly=True,
        ),
    ]


def _enterprise_sections(show_format_fields_block: bool) -> list[EnterpriseSectionVM]:
    sections = [
        EnterpriseSectionVM(
            key="main",
            title="Основное",
            field_keys=["enterprise_code", "enterprise_name", "data_format", "branch_id"],
        ),
        EnterpriseSectionVM(
            key="scheduler",
            title="Расписание",
            field_keys=["catalog_upload_frequency", "stock_upload_frequency", "order_fetcher"],
        ),
        EnterpriseSectionVM(
            key="orders_export",
            title="Экспорт и заказы",
            field_keys=["tabletki_login", "tabletki_password", "auto_confirm", "discount_rate", "stock_correction"],
        ),
        EnterpriseSectionVM(
            key="source",
            title="Источник / подключение",
            field_keys=["token"],
        ),
    ]

    if show_format_fields_block:
        sections.append(
            EnterpriseSectionVM(
                key="format_fields",
                title="Дополнительные поля формата",
                description="Поля показываются только для file/google-driven форматов или если уже заполнены.",
                collapsible=True,
                default_open=False,
                field_keys=[
                    "single_store",
                    "store_serial",
                    "google_drive_folder_id_ref",
                    "google_drive_folder_id_rest",
                ],
            )
        )

    sections.append(
        EnterpriseSectionVM(
            key="runtime",
            title="Служебная информация",
            description="Runtime-owned state, обновляется системой и доступен только для просмотра.",
            collapsible=True,
            default_open=False,
            field_keys=["last_stock_upload", "last_catalog_upload"],
        )
    )
    return sections


def _enterprise_values(enterprise: EnterpriseSettings) -> dict:
    return {
        "enterprise_code": enterprise.enterprise_code,
        "enterprise_name": enterprise.enterprise_name,
        "data_format": enterprise.data_format,
        "branch_id": enterprise.branch_id,
        "catalog_upload_frequency": enterprise.catalog_upload_frequency,
        "stock_upload_frequency": enterprise.stock_upload_frequency,
        "catalog_enabled": bool(enterprise.catalog_enabled),
        "stock_enabled": bool(enterprise.stock_enabled),
        "order_fetcher": bool(enterprise.order_fetcher),
        "tabletki_login": enterprise.tabletki_login,
        "tabletki_password": enterprise.tabletki_password,
        "auto_confirm": bool(enterprise.auto_confirm),
        "discount_rate": enterprise.discount_rate,
        "stock_correction": bool(enterprise.stock_correction),
        "token": enterprise.token,
        "single_store": bool(enterprise.single_store),
        "store_serial": enterprise.store_serial,
        "google_drive_folder_id_ref": enterprise.google_drive_folder_id_ref,
        "google_drive_folder_id_rest": enterprise.google_drive_folder_id_rest,
        "last_stock_upload": enterprise.last_stock_upload.isoformat() if enterprise.last_stock_upload else None,
        "last_catalog_upload": enterprise.last_catalog_upload.isoformat() if enterprise.last_catalog_upload else None,
    }


def _build_enterprise_list_vm(enterprises: list[EnterpriseSettings]) -> list[EnterpriseListItemVM]:
    items: list[EnterpriseListItemVM] = []
    for enterprise in enterprises:
        data_format = (enterprise.data_format or "").strip() or None
        items.append(
            EnterpriseListItemVM(
                enterprise_code=enterprise.enterprise_code,
                enterprise_name=enterprise.enterprise_name,
                data_format=data_format,
                branch_id=enterprise.branch_id,
                catalog_upload_frequency=enterprise.catalog_upload_frequency,
                stock_upload_frequency=enterprise.stock_upload_frequency,
                catalog_enabled=bool(enterprise.catalog_enabled),
                stock_enabled=bool(enterprise.stock_enabled),
                order_fetcher=bool(enterprise.order_fetcher),
                last_stock_upload=enterprise.last_stock_upload,
                last_catalog_upload=enterprise.last_catalog_upload,
                is_blank_format=(data_format == "Blank"),
                has_format_specific_fields=(
                    _enterprise_is_file_related_format(data_format)
                    or _enterprise_has_format_specific_fields(enterprise)
                ),
            )
        )
    return items


def _build_enterprise_detail_vm(enterprise: EnterpriseSettings) -> EnterpriseDetailVM:
    data_format = (enterprise.data_format or "").strip() or None
    show_format_fields_block = (
        _enterprise_is_file_related_format(data_format)
        or _enterprise_has_format_specific_fields(enterprise)
    )
    return EnterpriseDetailVM(
        enterprise_code=enterprise.enterprise_code,
        enterprise_name=enterprise.enterprise_name,
        data_format=data_format,
        catalog_enabled=bool(enterprise.catalog_enabled),
        stock_enabled=bool(enterprise.stock_enabled),
        values=_enterprise_values(enterprise),
        field_meta=_enterprise_field_meta(),
        sections=_enterprise_sections(show_format_fields_block),
        show_format_fields_block=show_format_fields_block,
        show_runtime_block=True,
    )


def _env_bool_value(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_int_value(name: str, default: int) -> int:
    raw = (os.getenv(name) or str(default)).strip()
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_optional_value(name: str) -> str | None:
    value = (os.getenv(name) or "").strip()
    return value or None


def _env_int_list_value(name: str, default: list[int]) -> list[int]:
    raw = _env_optional_value(name)
    if not raw:
        return list(default)

    normalized: list[int] = []
    for chunk in raw.replace(";", ",").split(","):
        item = chunk.strip()
        if not item:
            continue
        try:
            normalized.append(int(item))
        except (TypeError, ValueError):
            continue

    return normalized or list(default)


def _business_planned_writable_keys() -> list[str]:
    return [
        "business_enterprise_code",
        "daily_publish_enterprise_code_override",
        "weekly_salesdrive_enterprise_code_override",
        "business_stock_enabled",
        "business_stock_interval_seconds",
        "branch_id",
        "tabletki_login",
        "tabletki_password",
        "order_fetcher",
        "auto_confirm",
        "stock_correction",
        "biotus_enable_unhandled_fallback",
        "biotus_unhandled_order_timeout_minutes",
        "biotus_fallback_additional_status_ids",
        "biotus_duplicate_status_id",
        "master_weekly_enabled",
        "master_weekly_day",
        "master_weekly_hour",
        "master_weekly_minute",
        "master_daily_publish_enabled",
        "master_daily_publish_hour",
        "master_daily_publish_minute",
        "master_daily_publish_limit",
        "master_archive_enabled",
        "master_archive_every_minutes",
        "pricing_base_thr",
        "pricing_price_band_low_max",
        "pricing_price_band_mid_max",
        "pricing_thr_add_low_uah",
        "pricing_thr_add_mid_uah",
        "pricing_thr_add_high_uah",
        "pricing_no_comp_add_low_uah",
        "pricing_no_comp_add_mid_uah",
        "pricing_no_comp_add_high_uah",
        "pricing_comp_discount_share",
        "pricing_comp_delta_min_uah",
        "pricing_comp_delta_max_uah",
        "pricing_jitter_enabled",
        "pricing_jitter_step_uah",
        "pricing_jitter_min_uah",
        "pricing_jitter_max_uah",
    ]


async def _load_all_enterprises(db: AsyncSession) -> list[EnterpriseSettings]:
    result = await db.execute(select(EnterpriseSettings).order_by(EnterpriseSettings.enterprise_name))
    return list(result.scalars().all())


async def _load_business_settings_row(db: AsyncSession) -> BusinessSettings | None:
    result = await db.execute(
        select(BusinessSettings)
        .order_by(BusinessSettings.id)
        .limit(1)
    )
    return result.scalar_one_or_none()


def _filter_business_candidates(enterprises: list[EnterpriseSettings]) -> list[EnterpriseSettings]:
    return [
        enterprise
        for enterprise in enterprises
        if str(enterprise.data_format or "").strip().lower() == "business"
    ]


def _enterprise_lookup_by_code(enterprises: list[EnterpriseSettings]) -> dict[str, EnterpriseSettings]:
    return {
        str(enterprise.enterprise_code): enterprise
        for enterprise in enterprises
        if str(enterprise.enterprise_code or "").strip()
    }


def _build_business_candidates_vm(items: list[EnterpriseSettings]) -> list[BusinessEnterpriseCandidateVM]:
    return [
        BusinessEnterpriseCandidateVM(
            enterprise_code=item.enterprise_code,
            enterprise_name=item.enterprise_name,
            data_format=item.data_format,
        )
        for item in items
    ]


def _build_business_enterprise_options_vm(items: list[EnterpriseSettings]) -> list[BusinessEnterpriseOptionVM]:
    return [
        BusinessEnterpriseOptionVM(
            enterprise_code=item.enterprise_code,
            enterprise_name=item.enterprise_name,
            data_format=item.data_format,
        )
        for item in items
    ]


def _business_resolution_state(candidates: list[EnterpriseSettings]) -> tuple[str, str, EnterpriseSettings | None]:
    if not candidates:
        return (
            "none",
            "Enterprise с data_format=Business не найден. Страница показывает только env-backed Business orchestration state.",
            None,
        )
    if len(candidates) > 1:
        return (
            "ambiguous",
            "Найдено несколько enterprise с data_format=Business. Страница показывает общий Business control-plane snapshot без выбора одного enterprise.",
            None,
        )
    return (
        "resolved",
        "Найден один Business enterprise. Он используется для read-side snapshot страницы.",
        candidates[0],
    )


def _normalize_override_value(value: str | None, primary_code: str) -> str | None:
    normalized = str(value or "").strip() or None
    if not normalized or normalized == primary_code:
        return None
    return normalized


def _normalize_optional_enterprise_text(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _resolve_business_enterprise_for_operational_write(
    *,
    business_settings_row: BusinessSettings | None,
    business_candidates: list[EnterpriseSettings],
    enterprise_lookup: dict[str, EnterpriseSettings],
) -> EnterpriseSettings:
    if business_settings_row is not None:
        primary_code = str(business_settings_row.business_enterprise_code or "").strip() or None
        if not primary_code:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Не удалось сохранить enterprise operational fields: "
                    "business_settings row существует, но business_enterprise_code пустой."
                ),
            )

        enterprise = enterprise_lookup.get(primary_code)
        if enterprise is None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Не удалось сохранить enterprise operational fields: "
                    "business_settings указывает на предприятие, которого нет в EnterpriseSettings."
                ),
            )
        return enterprise

    resolution_status, resolution_message, enterprise = _business_resolution_state(business_candidates)
    if resolution_status != "resolved" or enterprise is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "Не удалось сохранить enterprise operational fields: "
                f"{resolution_message}"
            ),
        )
    return enterprise


def _env_override_value_against_primary(name: str, primary_code: str) -> str | None:
    return _normalize_override_value(_env_optional_value(name), primary_code)


def _validate_business_settings_update(
    payload: BusinessSettingsUpdateSchema,
    *,
    enterprise_lookup: dict[str, EnterpriseSettings],
    business_candidates: list[EnterpriseSettings],
) -> None:
    primary = enterprise_lookup.get(payload.business_enterprise_code)
    if primary is None:
        raise HTTPException(status_code=400, detail="Primary business enterprise does not exist.")

    business_codes = {
        str(item.enterprise_code): item
        for item in business_candidates
    }
    if payload.business_enterprise_code not in business_codes:
        raise HTTPException(
            status_code=400,
            detail="business_enterprise_code must reference EnterpriseSettings with data_format=Business.",
        )

    for field_name, enterprise_code in (
        ("daily_publish_enterprise_code_override", payload.daily_publish_enterprise_code_override),
        ("weekly_salesdrive_enterprise_code_override", payload.weekly_salesdrive_enterprise_code_override),
    ):
        normalized = str(enterprise_code or "").strip() or None
        if normalized and normalized not in enterprise_lookup:
            raise HTTPException(
                status_code=400,
                detail=f"{field_name} must reference an existing EnterpriseSettings.enterprise_code.",
            )


async def _get_or_create_business_settings_row_for_write(
    db: AsyncSession,
    *,
    all_enterprises: list[EnterpriseSettings],
    payload: BusinessSettingsUpdateSchema,
) -> BusinessSettings:
    row = await _load_business_settings_row(db)
    if row is not None:
        return row

    business_candidates = _filter_business_candidates(all_enterprises)
    if payload.business_enterprise_code not in {
        str(item.enterprise_code) for item in business_candidates
    }:
        raise HTTPException(
            status_code=409,
            detail=(
                "business_settings row is missing and cannot be initialized from this payload: "
                "business_enterprise_code must point to an existing Business enterprise."
            ),
        )

    row = BusinessSettings(
        id=1,
        business_enterprise_code=payload.business_enterprise_code,
        business_stock_enabled=payload.business_stock_enabled,
        business_stock_interval_seconds=payload.business_stock_interval_seconds * 60,
        biotus_enterprise_code_override=_env_override_value_against_primary(
            "BIOTUS_ENTERPRISE_CODE",
            payload.business_enterprise_code,
        ),
        biotus_enable_unhandled_fallback=payload.biotus_enable_unhandled_fallback,
        biotus_unhandled_order_timeout_minutes=payload.biotus_unhandled_order_timeout_minutes,
        biotus_fallback_additional_status_ids=list(payload.biotus_fallback_additional_status_ids),
        biotus_duplicate_status_id=payload.biotus_duplicate_status_id,
        master_weekly_enabled=payload.master_weekly_enabled,
        master_weekly_day=payload.master_weekly_day,
        master_weekly_hour=payload.master_weekly_hour,
        master_weekly_minute=payload.master_weekly_minute,
        master_daily_publish_enabled=payload.master_daily_publish_enabled,
        master_daily_publish_hour=payload.master_daily_publish_hour,
        master_daily_publish_minute=payload.master_daily_publish_minute,
        master_daily_publish_limit=payload.master_daily_publish_limit,
        master_archive_enabled=payload.master_archive_enabled,
        master_archive_every_minutes=payload.master_archive_every_minutes,
    )
    db.add(row)
    await db.flush()
    return row


def _resolve_business_enterprise_for_pricing_write(
    *,
    all_enterprises: list[EnterpriseSettings],
) -> EnterpriseSettings:
    business_candidates = _filter_business_candidates(all_enterprises)
    resolution_status, resolution_message, enterprise = _business_resolution_state(business_candidates)
    if resolution_status != "resolved" or enterprise is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "Не удалось сохранить pricing fields: "
                f"{resolution_message}"
            ),
        )
    return enterprise


async def _get_or_create_business_settings_row_for_pricing_write(
    db: AsyncSession,
    *,
    all_enterprises: list[EnterpriseSettings],
) -> BusinessSettings:
    row = await _load_business_settings_row(db)
    if row is not None:
        return row

    primary_enterprise = _resolve_business_enterprise_for_pricing_write(
        all_enterprises=all_enterprises,
    )
    primary_code = str(primary_enterprise.enterprise_code)

    row = BusinessSettings(
        id=1,
        business_enterprise_code=primary_code,
        daily_publish_enterprise_code_override=_env_override_value_against_primary(
            "MASTER_DAILY_PUBLISH_ENTERPRISE",
            primary_code,
        ),
        weekly_salesdrive_enterprise_code_override=_env_override_value_against_primary(
            "MASTER_WEEKLY_SALESDRIVE_ENTERPRISE",
            primary_code,
        ),
        biotus_enterprise_code_override=_env_override_value_against_primary(
            "BIOTUS_ENTERPRISE_CODE",
            primary_code,
        ),
        biotus_enable_unhandled_fallback=_env_bool_value("BIOTUS_ENABLE_UNHANDLED_FALLBACK", "1"),
        biotus_unhandled_order_timeout_minutes=_env_int_value("BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES", 60),
        biotus_fallback_additional_status_ids=_env_int_list_value(
            "BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS",
            [9, 19, 18, 20],
        ),
        biotus_duplicate_status_id=_env_int_value("BIOTUS_DUPLICATE_STATUS_ID", 20),
        master_weekly_enabled=_env_bool_value("MASTER_WEEKLY_ENABLED", "1"),
        master_weekly_day=(_env_optional_value("MASTER_WEEKLY_DAY") or "SUN").upper(),
        master_weekly_hour=_env_int_value("MASTER_WEEKLY_HOUR", 3),
        master_weekly_minute=_env_int_value("MASTER_WEEKLY_MINUTE", 0),
        master_daily_publish_enabled=_env_bool_value("MASTER_DAILY_PUBLISH_ENABLED", "1"),
        master_daily_publish_hour=_env_int_value("MASTER_DAILY_PUBLISH_HOUR", 9),
        master_daily_publish_minute=_env_int_value("MASTER_DAILY_PUBLISH_MINUTE", 0),
        master_daily_publish_limit=_env_int_value("MASTER_DAILY_PUBLISH_LIMIT", 0),
        master_archive_enabled=_env_bool_value("MASTER_ARCHIVE_ENABLED", "1"),
        master_archive_every_minutes=_env_int_value("MASTER_ARCHIVE_EVERY_MINUTES", 60),
        business_stock_enabled=_fallback_business_stock_enabled(primary_enterprise),
        business_stock_interval_seconds=_fallback_business_stock_interval_seconds(primary_enterprise),
    )
    db.add(row)
    await db.flush()
    return row


def _business_item(
    key: str,
    label: str,
    value,
    source: str,
    *,
    group: str | None = None,
    help_text: str | None = None,
    readonly: bool = True,
) -> BusinessSettingItemVM:
    return BusinessSettingItemVM(
        key=key,
        label=label,
        value=value,
        source=source,
        group=group,
        readonly=readonly,
        help_text=help_text,
    )


def _business_presence_label(value: Any) -> str:
    return "configured" if str(value or "").strip() else "missing"


def _business_bool_status(value: bool) -> str:
    return "enabled" if value else "disabled"


def _business_joined_env_list(name: str, default: str = "") -> str:
    raw = (os.getenv(name) or default).strip()
    if not raw:
        return "—"
    parts = [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]
    return ", ".join(parts) if parts else "—"


def _business_joined_int_list(values: list[int] | tuple[int, ...] | None) -> str:
    if not values:
        return "—"
    normalized = [str(int(item)) for item in values]
    return ", ".join(normalized) if normalized else "—"

def _build_pricing_items(
    pricing_snapshot: BusinessPricingSettingsSnapshot,
) -> list[BusinessSettingItemVM]:
    group_titles = {group.key: group.title for group in BUSINESS_PRICING_GROUP_SPECS}
    items: list[BusinessSettingItemVM] = []

    for spec in BUSINESS_PRICING_FIELD_SPECS:
        items.append(
            _business_item(
                spec.key,
                spec.label,
                getattr(pricing_snapshot, spec.key),
                pricing_snapshot.source,
                group=group_titles.get(spec.ui_group),
                help_text=spec.help_text,
                readonly=False,
            )
        )

    if pricing_snapshot.inconsistency:
        items.append(
            _business_item(
                "pricing_snapshot_fallback_reason",
                "Почему используется fallback",
                pricing_snapshot.inconsistency,
                "derived",
                group="Подсказка",
                help_text="Если строка business_settings существует, но её pricing payload невалиден, runtime и страница временно используют env fallback целиком.",
            )
        )

    return items

def _fallback_business_stock_enabled(enterprise: EnterpriseSettings | None) -> bool:
    if enterprise is None:
        return True
    return bool(enterprise.stock_enabled)


def _fallback_business_stock_interval_seconds(enterprise: EnterpriseSettings | None) -> int:
    if enterprise is None:
        return 60
    frequency = getattr(enterprise, "stock_upload_frequency", None)
    if frequency is None:
        return 60
    try:
        normalized = int(frequency)
    except (TypeError, ValueError):
        return 60
    if normalized < 1:
        return 60
    return normalized * 60


def _seconds_to_stock_interval_minutes(value: int | None) -> int:
    if value is None:
        return 1
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return 1
    if normalized <= 0:
        return 1
    return max(1, math.ceil(normalized / 60))


def _business_on_off_label(value: bool) -> str:
    return "включено" if value else "выключено"


def _resolve_effective_target(explicit_name: str, fallback_name: str) -> tuple[str | None, str]:
    explicit_value = _env_optional_value(explicit_name)
    fallback_value = _env_optional_value(fallback_name)
    if explicit_value:
        return explicit_value, f"{explicit_name} (explicit)"
    if fallback_value:
        return fallback_value, f"{fallback_name} (fallback)"
    return None, "missing"


def _db_effective_target(
    override_value: str | None,
    primary_value: str | None,
    *,
    override_help: str,
) -> tuple[str | None, str, str]:
    override = str(override_value or "").strip() or None
    primary = str(primary_value or "").strip() or None
    if override:
        return override, "db-derived", override_help
    if primary:
        return primary, "db-derived", "Inherited from business_settings.business_enterprise_code."
    return None, "db-derived", "business_settings row exists, but the primary business selector is missing."


def _target_consistency_status(
    resolution_status: str,
    resolved_enterprise_code: str | None,
    master_target: str | None,
    biotus_target: str | None,
) -> tuple[str, str]:
    if resolution_status == "ambiguous":
        return (
            "ambiguous",
            "Найдено несколько enterprise с data_format=Business; связность target enterprise нельзя считать однозначной.",
        )
    if resolution_status == "none":
        return (
            "no-business-enterprise",
            "Business enterprise по data_format не найден; env target selectors остаются отдельными runtime-источниками.",
        )
    if resolved_enterprise_code and master_target and resolved_enterprise_code != master_target:
        return (
            "master-target-differs",
            "Resolved Business enterprise и master target не совпадают. Это допустимо, но требует явной операторской проверки.",
        )
    if resolved_enterprise_code and biotus_target and resolved_enterprise_code != biotus_target:
        return (
            "biotus-target-separate",
            "Контур обработки заказов использует отдельный selector и сейчас не должен считаться единым runtime target.",
        )
    return (
        "linked-but-separate",
        "MASTER targets можно показывать как связанную семью selector-ов, а контур обработки заказов остаётся отдельным contour selector.",
    )


def _db_target_consistency_status(
    *,
    db_row_exists: bool,
    primary_code: str | None,
    primary_enterprise: EnterpriseSettings | None,
    business_candidates: list[EnterpriseSettings],
    effective_daily_target: str | None,
    effective_weekly_target: str | None,
    effective_biotus_target: str | None,
) -> tuple[str, str]:
    if not db_row_exists:
        return (
            "fallback-env",
            "business_settings row отсутствует; page использует fallback на env и текущую EnterpriseSettings resolution logic.",
        )
    if not primary_code:
        return (
            "db-missing-primary",
            "business_settings row существует, но business_enterprise_code пустой или не читается.",
        )
    if primary_enterprise is None:
        return (
            "db-primary-enterprise-missing",
            "business_settings row существует, но business_enterprise_code не найден в EnterpriseSettings. Page показывает DB-backed selectors без silent env fallback.",
        )
    if len(business_candidates) > 1:
        return (
            "db-overrides-ambiguous-enterprises",
            "В EnterpriseSettings найдено несколько data_format=Business, но page использует explicit primary selector из business_settings.",
        )
    if effective_biotus_target and effective_biotus_target != primary_code:
        return (
            "db-biotus-target-separate",
            "Контур обработки заказов использует отдельный contour selector и читается из DB override/primary model без runtime unification.",
        )
    if (
        (effective_daily_target and effective_daily_target != primary_code)
        or (effective_weekly_target and effective_weekly_target != primary_code)
    ):
        return (
            "db-master-target-overrides",
            "Daily/weekly master targets читаются из business_settings и частично переопределяют primary business enterprise.",
        )
    return (
        "db-primary-aligned",
        "business_settings является source of truth для first-migration fields; effective master targets наследуются от primary business_enterprise_code.",
    )


def _build_business_sections(
    enterprise: EnterpriseSettings | None,
    *,
    resolution_status: str,
    all_enterprises: list[EnterpriseSettings],
    business_settings_row: BusinessSettings | None,
    business_candidates: list[EnterpriseSettings],
    pricing_snapshot: BusinessPricingSettingsSnapshot,
) -> list[BusinessSectionVM]:
    old_catalog_disabled = _env_bool_value("DISABLE_OLD_BUSINESS_CATALOG_SCHEDULER", "0")
    use_master_mapping = _env_bool_value("USE_MASTER_MAPPING_FOR_STOCK", "0")
    planned_write_supported = True
    enterprise_lookup = _enterprise_lookup_by_code(all_enterprises)
    business_settings_exists = business_settings_row is not None

    if business_settings_row is not None:
        db_primary_code = str(business_settings_row.business_enterprise_code or "").strip() or None
        db_primary_enterprise = enterprise_lookup.get(str(db_primary_code or ""))
        master_catalog_target = db_primary_code
        master_catalog_target_source = "db"
        master_catalog_target_help = "Primary Business target is read from business_settings.business_enterprise_code."

        explicit_daily_target = (
            str(business_settings_row.daily_publish_enterprise_code_override or "").strip() or None
        )
        explicit_weekly_target = (
            str(business_settings_row.weekly_salesdrive_enterprise_code_override or "").strip() or None
        )
        explicit_biotus_target = (
            str(business_settings_row.biotus_enterprise_code_override or "").strip() or None
        )
        explicit_daily_source = "db"
        explicit_weekly_source = "db"
        explicit_biotus_source = "db"

        effective_daily_target, daily_target_source, daily_target_help = _db_effective_target(
            explicit_daily_target,
            db_primary_code,
            override_help="Derived from daily_publish_enterprise_code_override.",
        )
        effective_weekly_target, weekly_target_source, weekly_target_help = _db_effective_target(
            explicit_weekly_target,
            db_primary_code,
            override_help="Derived from weekly_salesdrive_enterprise_code_override.",
        )
        biotus_target, biotus_target_source, biotus_target_help = _db_effective_target(
            explicit_biotus_target,
            db_primary_code,
            override_help="Derived from biotus_enterprise_code_override.",
        )
        _consistency_status, _consistency_note = _db_target_consistency_status(
            db_row_exists=True,
            primary_code=db_primary_code,
            primary_enterprise=db_primary_enterprise,
            business_candidates=business_candidates,
            effective_daily_target=effective_daily_target,
            effective_weekly_target=effective_weekly_target,
            effective_biotus_target=biotus_target,
        )
        target_enterprise = db_primary_enterprise
        target_resolution_status = "db-primary-enterprise-missing" if db_primary_enterprise is None else "db-primary"
        target_resolution_source = "db"
        target_resolution_help = (
            "business_settings row exists, but the referenced primary enterprise is missing in EnterpriseSettings."
            if db_primary_enterprise is None
            else "Target enterprise for first-migration fields is resolved from business_settings."
        )
        target_primary_code = db_primary_code
    else:
        master_catalog_target = _env_optional_value("MASTER_CATALOG_ENTERPRISE_CODE")
        master_catalog_target_source = "env-fallback"
        master_catalog_target_help = "Fallback path: MASTER_CATALOG_ENTERPRISE_CODE from env is used because business_settings row is missing."
        explicit_daily_target = _env_optional_value("MASTER_DAILY_PUBLISH_ENTERPRISE")
        explicit_weekly_target = _env_optional_value("MASTER_WEEKLY_SALESDRIVE_ENTERPRISE")
        explicit_biotus_target = _env_optional_value("BIOTUS_ENTERPRISE_CODE")
        explicit_daily_source = "env-fallback"
        explicit_weekly_source = "env-fallback"
        explicit_biotus_source = "env-fallback"
        effective_daily_target, daily_target_source = _resolve_effective_target(
            "MASTER_DAILY_PUBLISH_ENTERPRISE",
            "MASTER_CATALOG_ENTERPRISE_CODE",
        )
        effective_weekly_target, weekly_target_source = _resolve_effective_target(
            "MASTER_WEEKLY_SALESDRIVE_ENTERPRISE",
            "MASTER_CATALOG_ENTERPRISE_CODE",
        )
        daily_target_help = "Fallback env logic: explicit selector overrides MASTER_CATALOG_ENTERPRISE_CODE."
        weekly_target_help = "Fallback env logic: explicit selector overrides MASTER_CATALOG_ENTERPRISE_CODE."
        biotus_target = explicit_biotus_target
        biotus_target_source = "env-fallback"
        biotus_target_help = "Fallback env selector is used only because business_settings row is missing."
        _consistency_status, _consistency_note = _target_consistency_status(
            resolution_status,
            enterprise.enterprise_code if enterprise is not None else None,
            master_catalog_target,
            biotus_target,
        )
        target_enterprise = enterprise
        target_resolution_status = resolution_status
        target_resolution_source = "EnterpriseSettings-derived"
        target_resolution_help = "Fallback path: current Business enterprise resolution is derived from EnterpriseSettings.data_format=Business."
        target_primary_code = enterprise.enterprise_code if enterprise is not None else None

    if business_settings_row is not None:
        business_stock_enabled_value = bool(business_settings_row.business_stock_enabled)
        business_stock_enabled_source = "db"
        business_stock_enabled_help = (
            "Отдельный Business stock scheduler включён и читает своё состояние из business_settings."
        )
        business_stock_interval_seconds_value = _seconds_to_stock_interval_minutes(
            business_settings_row.business_stock_interval_seconds
        )
        business_stock_interval_seconds_source = "db"
        business_stock_interval_seconds_help = (
            "Интервал запуска обработки остатков в минутах. Используется DB-first."
        )
        biotus_enable_unhandled_fallback_value = bool(business_settings_row.biotus_enable_unhandled_fallback)
        biotus_enable_unhandled_fallback_source = "db"
        biotus_enable_unhandled_fallback_help = "DB-first policy flag from business_settings."
        biotus_unhandled_order_timeout_minutes_value = int(
            business_settings_row.biotus_unhandled_order_timeout_minutes
        )
        biotus_unhandled_order_timeout_minutes_source = "db"
        biotus_unhandled_order_timeout_minutes_help = "DB-first fallback timeout policy from business_settings."
        biotus_fallback_additional_status_ids_value = _business_joined_int_list(
            list(business_settings_row.biotus_fallback_additional_status_ids or [])
        )
        biotus_fallback_additional_status_ids_source = "db"
        biotus_fallback_additional_status_ids_help = (
            "Stored in business_settings as integer array; UI edits it as comma-separated SalesDrive status ids."
        )
        biotus_duplicate_status_id_value = int(business_settings_row.biotus_duplicate_status_id)
        biotus_duplicate_status_id_source = "db"
        biotus_duplicate_status_id_help = "DB-first duplicate status policy from business_settings."
    else:
        business_stock_enabled_value = _fallback_business_stock_enabled(target_enterprise)
        business_stock_enabled_source = "derived"
        business_stock_enabled_help = (
            "business_settings row отсутствует, поэтому scheduler fallback-ит к старой gating-семантике через EnterpriseSettings.stock_enabled."
        )
        business_stock_interval_seconds_value = _seconds_to_stock_interval_minutes(
            _fallback_business_stock_interval_seconds(target_enterprise)
        )
        business_stock_interval_seconds_source = "derived"
        business_stock_interval_seconds_help = (
            "business_settings row отсутствует, поэтому используется fallback в минутах от EnterpriseSettings.stock_upload_frequency."
        )
        biotus_enable_unhandled_fallback_value = _env_bool_value("BIOTUS_ENABLE_UNHANDLED_FALLBACK", "1")
        biotus_enable_unhandled_fallback_source = "env-fallback"
        biotus_enable_unhandled_fallback_help = (
            "Fallback env policy is used only because business_settings row is missing."
        )
        biotus_unhandled_order_timeout_minutes_value = _env_int_value(
            "BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES",
            60,
        )
        biotus_unhandled_order_timeout_minutes_source = "env-fallback"
        biotus_unhandled_order_timeout_minutes_help = (
            "Fallback env policy is used only because business_settings row is missing."
        )
        biotus_fallback_additional_status_ids_value = _business_joined_env_list(
            "BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS",
            "9,19,18,20",
        )
        biotus_fallback_additional_status_ids_source = "env-fallback"
        biotus_fallback_additional_status_ids_help = (
            "Fallback env policy is used only because business_settings row is missing."
        )
        biotus_duplicate_status_id_value = _env_int_value("BIOTUS_DUPLICATE_STATUS_ID", 20)
        biotus_duplicate_status_id_source = "env-fallback"
        biotus_duplicate_status_id_help = (
            "Fallback env policy is used only because business_settings row is missing."
        )

    token_hint_target_code = target_primary_code or master_catalog_target
    token_hint_target = enterprise_lookup.get(str(token_hint_target_code or ""))
    token_presence = bool((getattr(token_hint_target, "token", None) or "").strip()) if token_hint_target else False

    if target_enterprise is not None:
        target_items = [
            _business_item("enterprise_name", "Текущее предприятие", target_enterprise.enterprise_name, "EnterpriseSettings", group="Сейчас используется"),
            _business_item("enterprise_code", "Код предприятия", target_enterprise.enterprise_code, "EnterpriseSettings", group="Сейчас используется"),
            _business_item("data_format", "Формат данных", target_enterprise.data_format, "EnterpriseSettings", group="Сейчас используется"),
        ]
    else:
        target_items = [
            _business_item(
                "target_resolution_status",
                "Статус выбора предприятия",
                target_resolution_status,
                target_resolution_source,
                group="Сейчас используется",
                help_text=target_resolution_help,
            ),
            _business_item(
                "enterprise_code_missing",
                "Код предприятия",
                target_primary_code,
                "db" if business_settings_exists else "computed",
                group="Сейчас используется",
                help_text=(
                    "В business_settings указано предприятие, которого сейчас нет в EnterpriseSettings."
                    if business_settings_exists
                    else "Не удалось определить предприятие по fallback-логике."
                ),
            ),
        ]

    target_items.extend(
        [
            _business_item(
                "branch_id",
                "Branch ID",
                target_enterprise.branch_id if target_enterprise is not None else None,
                "EnterpriseSettings" if target_enterprise is not None else "derived",
                group="Операционные поля",
                help_text="Используется в export/routing потоках Business enterprise и пока хранится в EnterpriseSettings.",
                readonly=False,
            ),
        ]
    )

    integration_items = [
        _business_item(
            "tabletki_login",
            "Логин Tabletki",
            target_enterprise.tabletki_login if target_enterprise is not None else None,
            "EnterpriseSettings" if target_enterprise is not None else "derived",
            group="Операционные поля",
            help_text="Business runtime использует эти креды для заказов, отмен и export-paths. Хранение пока остаётся в EnterpriseSettings.",
            readonly=False,
        ),
        _business_item(
            "tabletki_password",
            "Пароль Tabletki",
            target_enterprise.tabletki_password if target_enterprise is not None else None,
            "EnterpriseSettings" if target_enterprise is not None else "derived",
            group="Операционные поля",
            help_text="В режиме просмотра пароль скрыт. При сохранении значение пишется в EnterpriseSettings resolved Business enterprise.",
            readonly=False,
        ),
        _business_item(
            "token_masked",
            "SalesDrive API key",
            "Настроен" if token_presence else "Не задан",
            "EnterpriseSettings" if token_hint_target is not None else "derived",
            group="Операционные поля",
            help_text="Текущий токен не возвращается API. Оставьте поле пустым в режиме редактирования, чтобы не менять текущий токен.",
            readonly=False,
        ),
    ]

    stock_items = [
        _business_item(
            "business_stock_enabled",
            "Включить обработку стока",
            business_stock_enabled_value,
            business_stock_enabled_source,
            group="Scheduler control",
            help_text=business_stock_enabled_help,
            readonly=False,
        ),
        _business_item(
            "business_stock_interval_seconds",
            "Интервал запуска, минут",
            business_stock_interval_seconds_value,
            business_stock_interval_seconds_source,
            group="Scheduler control",
            help_text=business_stock_interval_seconds_help,
            readonly=False,
        ),
        _business_item(
            "stock_correction",
            "Коррекция остатков",
            bool(target_enterprise.stock_correction) if target_enterprise is not None else None,
            "EnterpriseSettings" if target_enterprise is not None else "derived",
            group="Политика стока",
            help_text="Если включено, stock runtime вычитает активные заказы Tabletki перед записью остатков.",
            readonly=False,
        ),
    ]

    orders_items = [
        _business_item(
            "order_fetcher",
            "Получение заказов",
            bool(target_enterprise.order_fetcher) if target_enterprise is not None else None,
            "EnterpriseSettings" if target_enterprise is not None else "derived",
            group="Основной контур заказов",
            help_text="Включает получение заказов для основного предприятия. Значение хранится в EnterpriseSettings.",
            readonly=False,
        ),
        _business_item(
            "auto_confirm",
            "Автоматическое бронирование",
            bool(target_enterprise.auto_confirm) if target_enterprise is not None else None,
            "EnterpriseSettings" if target_enterprise is not None else "derived",
            group="Основной контур заказов",
            help_text="Автоматически подтверждает заказы при наличии товара.",
            readonly=False,
        ),
        _business_item(
            "biotus_enterprise_code",
            "Предприятие обработки заказов",
            biotus_target,
            biotus_target_source,
            group="Предприятие обработки заказов",
            help_text="Используется для определения, к какому предприятию применяется обработка заказов. Поле только для чтения.",
        ),
        _business_item(
            "biotus_enable_unhandled_fallback",
            "Обрабатывать необработанные заказы",
            biotus_enable_unhandled_fallback_value,
            biotus_enable_unhandled_fallback_source,
            group="Дополнительная обработка заказов",
            help_text="Если включено, система дополнительно проверяет заказы, которые не попали в основной контур.",
            readonly=False,
        ),
        _business_item(
            "biotus_unhandled_order_timeout_minutes",
            "Ожидание перед дополнительной обработкой, минут",
            biotus_unhandled_order_timeout_minutes_value,
            biotus_unhandled_order_timeout_minutes_source,
            group="Дополнительная обработка заказов",
            help_text="Через сколько минут заказ считается просроченным для дополнительной обработки.",
            readonly=False,
        ),
        _business_item(
            "biotus_fallback_additional_status_ids",
            "Дополнительные статусы SalesDrive",
            biotus_fallback_additional_status_ids_value,
            biotus_fallback_additional_status_ids_source,
            group="Дополнительная обработка заказов",
            help_text="Список status id через запятую. Используется для дополнительной обработки заказов.",
            readonly=False,
        ),
        _business_item(
            "biotus_duplicate_status_id",
            "Статус для дублей",
            biotus_duplicate_status_id_value,
            biotus_duplicate_status_id_source,
            group="Дополнительная обработка заказов",
            help_text="Status id в SalesDrive, который ставится заказам с дублирующимся телефоном.",
            readonly=False,
        ),
    ]

    master_items = [
        _business_item("master_scheduler_enabled", "Автозапуск master-контура", _env_bool_value("MASTER_SCHEDULER_ENABLED", "1"), "env", group="Только чтение"),
        _business_item(
            "master_weekly_enabled",
            "Еженедельное обновление",
            bool(business_settings_row.master_weekly_enabled) if business_settings_exists else _env_bool_value("MASTER_WEEKLY_ENABLED", "1"),
            "db" if business_settings_exists else "env-fallback",
            group="Еженедельный запуск",
            readonly=False,
        ),
        _business_item(
            "master_weekly_day",
            "День",
            business_settings_row.master_weekly_day if business_settings_exists else (_env_optional_value("MASTER_WEEKLY_DAY") or "SUN"),
            "db" if business_settings_exists else "env-fallback",
            group="Еженедельный запуск",
            readonly=False,
        ),
        _business_item(
            "master_weekly_hour",
            "Час",
            int(business_settings_row.master_weekly_hour) if business_settings_exists else _env_int_value("MASTER_WEEKLY_HOUR", 3),
            "db" if business_settings_exists else "env-fallback",
            group="Еженедельный запуск",
            readonly=False,
        ),
        _business_item(
            "master_weekly_minute",
            "Минута",
            int(business_settings_row.master_weekly_minute) if business_settings_exists else _env_int_value("MASTER_WEEKLY_MINUTE", 0),
            "db" if business_settings_exists else "env-fallback",
            group="Еженедельный запуск",
            readonly=False,
        ),
        _business_item("master_weekly_salesdrive_batch_size", "Размер пакета для SalesDrive", _env_int_value("MASTER_WEEKLY_SALESDRIVE_BATCH_SIZE", 100), "env", group="Только чтение"),
        _business_item(
            "master_daily_publish_enabled",
            "Ежедневная выгрузка",
            bool(business_settings_row.master_daily_publish_enabled) if business_settings_exists else _env_bool_value("MASTER_DAILY_PUBLISH_ENABLED", "1"),
            "db" if business_settings_exists else "env-fallback",
            group="Ежедневная выгрузка",
            readonly=False,
        ),
        _business_item(
            "master_daily_publish_hour",
            "Час",
            int(business_settings_row.master_daily_publish_hour) if business_settings_exists else _env_int_value("MASTER_DAILY_PUBLISH_HOUR", 9),
            "db" if business_settings_exists else "env-fallback",
            group="Ежедневная выгрузка",
            readonly=False,
        ),
        _business_item(
            "master_daily_publish_minute",
            "Минута",
            int(business_settings_row.master_daily_publish_minute) if business_settings_exists else _env_int_value("MASTER_DAILY_PUBLISH_MINUTE", 0),
            "db" if business_settings_exists else "env-fallback",
            group="Ежедневная выгрузка",
            readonly=False,
        ),
        _business_item(
            "master_daily_publish_limit",
            "Лимит публикации",
            int(business_settings_row.master_daily_publish_limit) if business_settings_exists else _env_int_value("MASTER_DAILY_PUBLISH_LIMIT", 0),
            "db" if business_settings_exists else "env-fallback",
            group="Ежедневная выгрузка",
            readonly=False,
        ),
        _business_item(
            "master_archive_enabled",
            "Загрузка архива",
            bool(business_settings_row.master_archive_enabled) if business_settings_exists else _env_bool_value("MASTER_ARCHIVE_ENABLED", "1"),
            "db" if business_settings_exists else "env-fallback",
            group="Архив",
            readonly=False,
        ),
        _business_item(
            "master_archive_every_minutes",
            "Интервал, минут",
            int(business_settings_row.master_archive_every_minutes) if business_settings_exists else _env_int_value("MASTER_ARCHIVE_EVERY_MINUTES", 60),
            "db" if business_settings_exists else "env-fallback",
            group="Архив",
            readonly=False,
        ),
        _business_item(
            "master_target_fallback_note",
            "Как выбирается предприятие",
            (
                "Если отдельное предприятие не указано в разделе выше, используется основное."
                if business_settings_exists
                else "Пока строка business_settings не создана, используется fallback из ENV."
            ),
            "derived",
            group="Подсказка",
        ),
    ]
    pricing_items = _build_pricing_items(pricing_snapshot)

    return [
        BusinessSectionVM(
            key="target_enterprise",
            title="Business Enterprise",
            description="Основное Business предприятие и его операционный branch routing.",
            readonly=False,
            items=target_items,
        ),
        BusinessSectionVM(
            key="master_catalog",
            title="Master Catalog",
            description="Настройки запуска и публикации для master-контура. Сохраняются в business_settings.",
            readonly=False,
            items=master_items,
        ),
        BusinessSectionVM(
            key="integration_access",
            title="Интеграция / доступ",
            description="Enterprise-level operational fields для Business runtime. Значения редактируются здесь, но пока хранятся в EnterpriseSettings.",
            readonly=False,
            items=integration_items,
        ),
        BusinessSectionVM(
            key="orders_biotus",
            title="Заказы",
            description="Настройки основного контура заказов, дополнительной обработки и предприятия, к которому применяется этот контур.",
            readonly=False,
            items=orders_items,
        ),
        BusinessSectionVM(
            key="pricing",
            title="Ценообразование",
            description="DB-first pricing control-plane для Business runtime. Изменения применятся в следующем запуске pipeline.",
            readonly=False,
            items=pricing_items,
        ),
        BusinessSectionVM(
            key="stock_mapping_mode",
            title="Stock",
            description="Коррекция остатков для Business enterprise. Operational toggle пока хранится в EnterpriseSettings.",
            readonly=False,
            items=stock_items,
        ),
    ]


async def _build_business_settings_vm(db: AsyncSession) -> BusinessSettingsVM:
    all_enterprises = await _load_all_enterprises(db)
    business_settings_row = await _load_business_settings_row(db)
    pricing_snapshot = await load_business_pricing_settings_snapshot(db)
    candidates = _filter_business_candidates(all_enterprises)
    fallback_resolution_status, fallback_resolution_message, fallback_resolved = _business_resolution_state(candidates)
    enterprise_lookup = _enterprise_lookup_by_code(all_enterprises)
    resolved_enterprise = _resolve_business_settings_enterprise(
        all_enterprises=all_enterprises,
        business_settings_row=business_settings_row,
    )

    if business_settings_row is not None:
        db_primary_code = str(business_settings_row.business_enterprise_code or "").strip() or None
        db_primary_enterprise = enterprise_lookup.get(str(db_primary_code or ""))
        if db_primary_enterprise is not None:
            resolution_status = "db-primary"
            resolution_message = (
                "Страница читает control-plane поля из business_settings."
            )
            resolved = db_primary_enterprise
        else:
            resolution_status = "db-primary-enterprise-missing"
            resolution_message = (
                "В business_settings указано предприятие, которого нет в EnterpriseSettings. "
                "Страница не делает silent fallback и показывает это состояние явно."
            )
            resolved = None
    else:
        resolution_status = fallback_resolution_status
        resolution_message = f"Строка business_settings пока отсутствует, поэтому страница использует fallback. {fallback_resolution_message}"
        resolved = fallback_resolved

    token_present = bool((getattr(resolved_enterprise, "token", None) or "").strip()) if resolved_enterprise else False
    business_settings_logger.info(
        "Business Settings token presence resolved for enterprise_code=%s token_present=%s",
        resolved_enterprise.enterprise_code if resolved_enterprise is not None else None,
        token_present,
    )

    return BusinessSettingsVM(
        resolution_status=resolution_status,
        resolution_message=resolution_message,
        resolved_enterprise_code=resolved.enterprise_code if resolved else None,
        resolved_enterprise_name=resolved.enterprise_name if resolved else None,
        token_present=token_present,
        business_candidates=_build_business_candidates_vm(candidates),
        enterprise_options=_build_business_enterprise_options_vm(all_enterprises),
        writable_supported=True,
        deferred_write_reason=None,
        planned_writable_keys=_business_planned_writable_keys(),
        sections=_build_business_sections(
            resolved,
            resolution_status=fallback_resolution_status,
            all_enterprises=all_enterprises,
            business_settings_row=business_settings_row,
            business_candidates=candidates,
            pricing_snapshot=pricing_snapshot,
        ),
    )


def _build_mapping_conflict_flags(rows: list[dict]) -> dict[str, list[str]]:
    duplicate_store_keys: set[tuple[str, str]] = set()
    duplicate_folder_keys: set[tuple[str, str]] = set()
    store_seen: dict[tuple[str, str], int] = {}
    folder_seen: dict[tuple[str, str], int] = {}

    for row in rows:
        enterprise_code = str(row["enterprise_code"])
        store_id = str(row.get("store_id") or "").strip()
        folder_id = str(row.get("google_folder_id") or "").strip()
        if store_id:
            key = (enterprise_code, store_id)
            store_seen[key] = store_seen.get(key, 0) + 1
        if folder_id:
            key = (enterprise_code, folder_id)
            folder_seen[key] = folder_seen.get(key, 0) + 1

    for key, count in store_seen.items():
        if count > 1:
            duplicate_store_keys.add(key)
    for key, count in folder_seen.items():
        if count > 1:
            duplicate_folder_keys.add(key)

    conflict_flags: dict[str, list[str]] = {}
    for row in rows:
        branch = str(row["branch"])
        enterprise_code = str(row["enterprise_code"])
        store_id = str(row.get("store_id") or "").strip()
        folder_id = str(row.get("google_folder_id") or "").strip()
        flags: list[str] = []
        if store_id and (enterprise_code, store_id) in duplicate_store_keys:
            flags.append("duplicate_store_id_within_enterprise")
        if folder_id and (enterprise_code, folder_id) in duplicate_folder_keys:
            flags.append("duplicate_google_folder_id_within_enterprise")
        conflict_flags[branch] = flags
    return conflict_flags


def _build_branch_mapping_list_vm(rows: list[dict]) -> list[BranchMappingListItemVM]:
    conflict_flags_by_branch = _build_mapping_conflict_flags(rows)
    items: list[BranchMappingListItemVM] = []

    for row in rows:
        enterprise_code = str(row["enterprise_code"])
        enterprise_name = str(row.get("enterprise_name") or "").strip()
        data_format = str(row.get("data_format") or "").strip() or None
        branch = str(row["branch"])
        store_id = str(row.get("store_id") or "")
        google_folder_id = row.get("google_folder_id")
        id_telegram = row.get("id_telegram") or []
        has_telegram_target = bool(id_telegram)
        semantic_store_label = _mapping_semantic_store_label(data_format)
        runtime_consumers = _mapping_runtime_consumers(
            data_format,
            has_google_folder=bool(google_folder_id),
            has_telegram_target=has_telegram_target,
        )

        items.append(
            BranchMappingListItemVM(
                mapping_key=branch,
                enterprise_code=enterprise_code,
                enterprise_display_label=f"{enterprise_name} ({enterprise_code})" if enterprise_name else enterprise_code,
                branch=branch,
                semantic_store_label=semantic_store_label,
                store_mapping_value=store_id,
                google_folder_id=google_folder_id,
                has_telegram_target=has_telegram_target,
                field_semantics_summary=_mapping_semantics_summary(data_format, semantic_store_label),
                runtime_usage_hints_summary="; ".join(runtime_consumers),
                conflict_flags=conflict_flags_by_branch.get(branch, []),
                readonly_fields=["mapping_key"],
            )
        )

    return items


def _build_branch_mapping_detail_vm(row: dict, conflict_flags: list[str]) -> BranchMappingDetailVM:
    enterprise_code = str(row["enterprise_code"])
    enterprise_name = str(row.get("enterprise_name") or "").strip()
    data_format = str(row.get("data_format") or "").strip() or None
    branch = str(row["branch"])
    google_folder_id = row.get("google_folder_id")
    id_telegram = row.get("id_telegram") or []
    semantic_store_label = _mapping_semantic_store_label(data_format)
    runtime_consumers = _mapping_runtime_consumers(
        data_format,
        has_google_folder=bool(google_folder_id),
        has_telegram_target=bool(id_telegram),
    )

    return BranchMappingDetailVM(
        mapping_key=branch,
        enterprise_code=enterprise_code,
        enterprise_display_label=f"{enterprise_name} ({enterprise_code})" if enterprise_name else enterprise_code,
        data_format=data_format,
        branch=branch,
        store_id=str(row.get("store_id") or ""),
        semantic_store_label=semantic_store_label,
        google_folder_id=google_folder_id,
        id_telegram=list(id_telegram),
        runtime_consumers=runtime_consumers,
        runtime_usage_hints_summary="; ".join(runtime_consumers),
        field_notes=_mapping_field_notes(data_format, bool(google_folder_id)),
        overloaded_fields=_mapping_overloaded_fields(data_format, bool(google_folder_id)),
        conflict_flags=conflict_flags,
        readonly_fields=["mapping_key"],
        computed_fields=[
            "enterprise_display_label",
            "semantic_store_label",
            "runtime_consumers",
            "runtime_usage_hints_summary",
            "conflict_flags",
        ],
    )


async def _get_branch_mapping_view_row(db: AsyncSession, branch: str) -> dict:
    result = await db.execute(
        select(
            MappingBranch.enterprise_code,
            MappingBranch.branch,
            MappingBranch.store_id,
            MappingBranch.google_folder_id,
            MappingBranch.id_telegram,
            EnterpriseSettings.enterprise_name,
            EnterpriseSettings.data_format,
        )
        .outerjoin(
            EnterpriseSettings,
            EnterpriseSettings.enterprise_code == MappingBranch.enterprise_code,
        )
        .where(MappingBranch.branch == branch)
    )
    row = result.mappings().one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Mapping branch not found.")
    return dict(row)


async def _build_branch_mapping_detail_response(
    db: AsyncSession,
    branch: str,
) -> BranchMappingDetailVM:
    row = await _get_branch_mapping_view_row(db, branch)
    sibling_rows_result = await db.execute(
        select(
            MappingBranch.enterprise_code,
            MappingBranch.branch,
            MappingBranch.store_id,
            MappingBranch.google_folder_id,
        ).where(MappingBranch.enterprise_code == row["enterprise_code"])
    )
    sibling_rows = [dict(r._mapping) for r in sibling_rows_result.fetchall()]
    conflict_flags = _build_mapping_conflict_flags(sibling_rows).get(branch, [])
    return _build_branch_mapping_detail_vm(row, conflict_flags)

# List
@router.get("/dropship/enterprises/", dependencies=[Depends(verify_token)])
async def get_all_dropship_enterprises(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DropshipEnterprise))
    items = result.scalars().all()
    return [DropshipEnterpriseSchema.model_validate(i, from_attributes=True) for i in items]


@router.get("/suppliers/view/", response_model=List[SupplierListItemVM], dependencies=[Depends(verify_token)])
async def get_suppliers_view(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DropshipEnterprise))
    items = result.scalars().all()
    return [_build_supplier_list_item_vm(item) for item in items]


@router.get("/suppliers/view/{code}", response_model=SupplierDetailVM, dependencies=[Depends(verify_token)])
async def get_supplier_view_detail(code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DropshipEnterprise).where(DropshipEnterprise.code == code))
    item = result.scalars().first()
    if not item:
        raise HTTPException(status_code=404, detail="Supplier not found.")
    return _build_supplier_detail_vm(item)

# Get by code
@router.get("/dropship/enterprises/{code}", dependencies=[Depends(verify_token)])
async def get_dropship_enterprise(code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DropshipEnterprise).where(DropshipEnterprise.code == code))
    item = result.scalars().first()
    if not item:
        raise HTTPException(status_code=404, detail="Dropship enterprise not found.")
    return DropshipEnterpriseSchema.model_validate(item, from_attributes=True)

# Create
@router.post("/dropship/enterprises/", dependencies=[Depends(verify_token)])
async def create_dropship_enterprise(payload: DropshipEnterpriseSchema, db: AsyncSession = Depends(get_db)):
    exists = await db.execute(select(DropshipEnterprise).where(DropshipEnterprise.code == payload.code))
    if exists.scalars().first():
        raise HTTPException(status_code=400, detail="Dropship enterprise with this code already exists.")

    obj = DropshipEnterprise(**payload.model_dump())
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return DropshipEnterpriseSchema.model_validate(obj, from_attributes=True)

# Update
@router.put("/dropship/enterprises/{code}", dependencies=[Depends(verify_token)])
async def update_dropship_enterprise(code: str, payload: DropshipEnterpriseSchema, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DropshipEnterprise).where(DropshipEnterprise.code == code))
    obj = result.scalars().first()
    if not obj:
        raise HTTPException(status_code=404, detail="Dropship enterprise not found.")

    for k, v in payload.model_dump(exclude_unset=True).items():
        setattr(obj, k, v)

    await db.commit()
    await db.refresh(obj)
    return {"detail": "Dropship enterprise updated successfully", "data": DropshipEnterpriseSchema.model_validate(obj, from_attributes=True)}

# 🔐 Авторизация
@router.post("/login/", summary="Login User")
async def login_user(credentials: LoginSchema, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DeveloperSettings).filter(
        DeveloperSettings.developer_login == credentials.developer_login,
        DeveloperSettings.developer_password == credentials.developer_password
    ))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid login or password.")

    access_token = create_access_token(data={"sub": user.developer_login}, expires_delta=timedelta(hours=1))
    return {"access_token": access_token, "token_type": "bearer"}

# 🔒 Эндпоинты разработчиков
@router.get("/developer/settings/{developer_login}", dependencies=[Depends(verify_token)])
async def get_developer_settings_by_login(developer_login: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DeveloperSettings).filter(DeveloperSettings.developer_login == developer_login))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="Developer not found.")
    return user

@router.put("/developer/settings/{developer_login}", dependencies=[Depends(verify_token)])
async def update_developer_settings(developer_login: str, settings: DeveloperSettingsSchema, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DeveloperSettings).filter(DeveloperSettings.developer_login == developer_login))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="Developer not found.")
    for key, value in settings.dict().items():
        setattr(user, key, value)
    await db.commit()
    return user

# 🔒 Эндпоинты предприятий
@router.get("/enterprise/settings/", dependencies=[Depends(verify_token)])
async def get_all_enterprises(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EnterpriseSettings))
    enterprises = result.scalars().all()
    return enterprises if enterprises else []


@router.get(
    "/enterprise/settings/view",
    response_model=List[EnterpriseListItemVM],
    dependencies=[Depends(verify_token)],
)
async def get_all_enterprises_view(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EnterpriseSettings).order_by(EnterpriseSettings.enterprise_name))
    enterprises = result.scalars().all()
    return _build_enterprise_list_vm(enterprises)


@router.get(
    "/business/settings/view",
    response_model=BusinessSettingsVM,
    dependencies=[Depends(verify_token)],
)
async def get_business_settings_view(db: AsyncSession = Depends(get_db)):
    return await _build_business_settings_vm(db)


@router.put(
    "/business/settings/master-scope",
    response_model=BusinessSettingsVM,
    dependencies=[Depends(verify_token)],
)
async def update_business_settings_master_scope(
    payload: BusinessSettingsUpdateSchema,
    db: AsyncSession = Depends(get_db),
):
    all_enterprises = await _load_all_enterprises(db)
    enterprise_lookup = _enterprise_lookup_by_code(all_enterprises)
    business_candidates = _filter_business_candidates(all_enterprises)
    _validate_business_settings_update(
        payload,
        enterprise_lookup=enterprise_lookup,
        business_candidates=business_candidates,
    )

    row = await _get_or_create_business_settings_row_for_write(
        db,
        all_enterprises=all_enterprises,
        payload=payload,
    )

    row.business_enterprise_code = payload.business_enterprise_code
    row.daily_publish_enterprise_code_override = _normalize_override_value(
        payload.daily_publish_enterprise_code_override,
        payload.business_enterprise_code,
    )
    row.weekly_salesdrive_enterprise_code_override = _normalize_override_value(
        payload.weekly_salesdrive_enterprise_code_override,
        payload.business_enterprise_code,
    )
    row.business_stock_enabled = payload.business_stock_enabled
    row.business_stock_interval_seconds = payload.business_stock_interval_seconds * 60
    row.biotus_enable_unhandled_fallback = payload.biotus_enable_unhandled_fallback
    row.biotus_unhandled_order_timeout_minutes = payload.biotus_unhandled_order_timeout_minutes
    row.biotus_fallback_additional_status_ids = list(payload.biotus_fallback_additional_status_ids)
    row.biotus_duplicate_status_id = payload.biotus_duplicate_status_id
    row.master_weekly_enabled = payload.master_weekly_enabled
    row.master_weekly_day = payload.master_weekly_day
    row.master_weekly_hour = payload.master_weekly_hour
    row.master_weekly_minute = payload.master_weekly_minute
    row.master_daily_publish_enabled = payload.master_daily_publish_enabled
    row.master_daily_publish_hour = payload.master_daily_publish_hour
    row.master_daily_publish_minute = payload.master_daily_publish_minute
    row.master_daily_publish_limit = payload.master_daily_publish_limit
    row.master_archive_enabled = payload.master_archive_enabled
    row.master_archive_every_minutes = payload.master_archive_every_minutes

    await db.commit()
    return await _build_business_settings_vm(db)

@router.put(
    "/business/settings/pricing-scope",
    response_model=BusinessSettingsVM,
    dependencies=[Depends(verify_token)],
)
async def update_business_settings_pricing_scope(
    payload: BusinessPricingSettingsUpdateSchema,
    db: AsyncSession = Depends(get_db),
):
    all_enterprises = await _load_all_enterprises(db)
    row = await _get_or_create_business_settings_row_for_pricing_write(
        db,
        all_enterprises=all_enterprises,
    )

    row.pricing_base_thr = payload.pricing_base_thr
    row.pricing_price_band_low_max = payload.pricing_price_band_low_max
    row.pricing_price_band_mid_max = payload.pricing_price_band_mid_max
    row.pricing_thr_add_low_uah = payload.pricing_thr_add_low_uah
    row.pricing_thr_add_mid_uah = payload.pricing_thr_add_mid_uah
    row.pricing_thr_add_high_uah = payload.pricing_thr_add_high_uah
    row.pricing_no_comp_add_low_uah = payload.pricing_no_comp_add_low_uah
    row.pricing_no_comp_add_mid_uah = payload.pricing_no_comp_add_mid_uah
    row.pricing_no_comp_add_high_uah = payload.pricing_no_comp_add_high_uah
    row.pricing_comp_discount_share = payload.pricing_comp_discount_share
    row.pricing_comp_delta_min_uah = payload.pricing_comp_delta_min_uah
    row.pricing_comp_delta_max_uah = payload.pricing_comp_delta_max_uah
    row.pricing_jitter_enabled = payload.pricing_jitter_enabled
    row.pricing_jitter_step_uah = payload.pricing_jitter_step_uah
    row.pricing_jitter_min_uah = payload.pricing_jitter_min_uah
    row.pricing_jitter_max_uah = payload.pricing_jitter_max_uah

    business_settings_logger.info(
        "Business Settings pricing update saved: row_exists=%s source=business_settings",
        True,
    )

    await db.commit()
    return await _build_business_settings_vm(db)


@router.put(
    "/business/settings/enterprise-operational-scope",
    response_model=BusinessSettingsVM,
    dependencies=[Depends(verify_token)],
)
async def update_business_settings_enterprise_operational_scope(
    payload: BusinessEnterpriseOperationalFieldsUpdateSchema,
    db: AsyncSession = Depends(get_db),
):
    all_enterprises = await _load_all_enterprises(db)
    business_settings_row = await _load_business_settings_row(db)
    enterprise_lookup = _enterprise_lookup_by_code(all_enterprises)
    business_candidates = _filter_business_candidates(all_enterprises)

    enterprise = _resolve_business_enterprise_for_operational_write(
        business_settings_row=business_settings_row,
        business_candidates=business_candidates,
        enterprise_lookup=enterprise_lookup,
    )
    business_settings_logger.info(
        "Business Settings token update received for enterprise_code=%s token_present=%s",
        enterprise.enterprise_code,
        bool(payload.token),
    )

    enterprise.branch_id = payload.branch_id
    enterprise.tabletki_login = _normalize_optional_enterprise_text(payload.tabletki_login)
    enterprise.tabletki_password = _normalize_optional_enterprise_text(payload.tabletki_password)
    if payload.token is not None:
        enterprise.token = _normalize_optional_enterprise_text(payload.token)
    enterprise.order_fetcher = bool(payload.order_fetcher)
    enterprise.auto_confirm = bool(payload.auto_confirm)
    enterprise.stock_correction = bool(payload.stock_correction)

    await db.commit()
    return await _build_business_settings_vm(db)


@router.put(
    "/business/settings/enterprise-operational-scope",
    response_model=BusinessSettingsVM,
    dependencies=[Depends(verify_token)],
)
async def update_business_settings_enterprise_operational_scope(
    payload: BusinessEnterpriseOperationalFieldsUpdateSchema,
    db: AsyncSession = Depends(get_db),
):
    all_enterprises = await _load_all_enterprises(db)
    business_settings_row = await _load_business_settings_row(db)
    enterprise_lookup = _enterprise_lookup_by_code(all_enterprises)
    business_candidates = _filter_business_candidates(all_enterprises)

    enterprise = _resolve_business_enterprise_for_operational_write(
        business_settings_row=business_settings_row,
        business_candidates=business_candidates,
        enterprise_lookup=enterprise_lookup,
    )
    business_settings_logger.info(
        "Business Settings token update received for enterprise_code=%s token_present=%s",
        enterprise.enterprise_code,
        bool(payload.token),
    )

    enterprise.branch_id = payload.branch_id
    enterprise.tabletki_login = _normalize_optional_enterprise_text(payload.tabletki_login)
    enterprise.tabletki_password = _normalize_optional_enterprise_text(payload.tabletki_password)
    if payload.token is not None:
        enterprise.token = _normalize_optional_enterprise_text(payload.token)
    enterprise.order_fetcher = bool(payload.order_fetcher)
    enterprise.auto_confirm = bool(payload.auto_confirm)
    enterprise.stock_correction = bool(payload.stock_correction)

    await db.commit()
    return await _build_business_settings_vm(db)


@router.get("/enterprise/settings/{enterprise_code}", dependencies=[Depends(verify_token)])
async def get_enterprise_by_code(enterprise_code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code))
    enterprise = result.scalars().first()
    if not enterprise:
        raise HTTPException(status_code=404, detail="Enterprise not found.")
    return EnterpriseSettingsSchema.model_validate(enterprise, from_attributes=True)


@router.get(
    "/enterprise/settings/{enterprise_code}/view",
    response_model=EnterpriseDetailVM,
    dependencies=[Depends(verify_token)],
)
async def get_enterprise_by_code_view(enterprise_code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code))
    enterprise = result.scalars().first()
    if not enterprise:
        raise HTTPException(status_code=404, detail="Enterprise not found.")
    return _build_enterprise_detail_vm(enterprise)

@router.post("/enterprise/settings/", dependencies=[Depends(verify_token)])
async def create_enterprise(settings: EnterpriseSettingsSchema, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == settings.enterprise_code))
    if existing.scalars().first():
        raise HTTPException(status_code=400, detail="Enterprise with this code already exists.")

    new_enterprise = EnterpriseSettings(**settings.model_dump())
    db.add(new_enterprise)
    await db.commit()
    await db.refresh(new_enterprise)
    return EnterpriseSettingsSchema.model_validate(new_enterprise, from_attributes=True)

@router.put("/enterprise/settings/{enterprise_code}", dependencies=[Depends(verify_token)])
async def update_enterprise_settings(enterprise_code: str, updated_settings: EnterpriseSettingsSchema, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code))
    enterprise = result.scalars().first()
    if not enterprise:
        raise HTTPException(status_code=404, detail="Enterprise not found.")

    for key, value in updated_settings.model_dump(exclude_unset=True).items():
        setattr(enterprise, key, value)

    await db.commit()
    await db.refresh(enterprise)
    return {"detail": "Enterprise settings updated successfully", "data": enterprise}

# 🔒 Эндпоинты форматов данных
@router.post("/data_formats/", dependencies=[Depends(verify_token)])
async def add_data_format(data_format: DataFormatSchema, db: AsyncSession = Depends(get_db)):
    existing_format = await db.execute(select(DataFormat).filter(DataFormat.format_name == data_format.format_name))
    if existing_format.scalars().first():
        raise HTTPException(status_code=400, detail="Data format already exists.")
    
    new_format = DataFormat(format_name=data_format.format_name)
    db.add(new_format)
    await db.commit()
    await db.refresh(new_format)
    return {"detail": "Data format added successfully", "data": new_format}

@router.get("/data_formats/", dependencies=[Depends(verify_token)])
async def get_data_formats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DataFormat))
    return result.scalars().all()

# 🔒 Mapping Branch read models
@router.get(
    "/mapping_branch/view/",
    response_model=List[BranchMappingListItemVM],
    dependencies=[Depends(verify_token)],
)
async def get_mapping_branch_view_list(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(
            MappingBranch.enterprise_code,
            MappingBranch.branch,
            MappingBranch.store_id,
            MappingBranch.google_folder_id,
            MappingBranch.id_telegram,
            EnterpriseSettings.enterprise_name,
            EnterpriseSettings.data_format,
        )
        .outerjoin(
            EnterpriseSettings,
            EnterpriseSettings.enterprise_code == MappingBranch.enterprise_code,
        )
        .order_by(MappingBranch.enterprise_code, MappingBranch.branch)
    )
    rows = [dict(row._mapping) for row in result.fetchall()]
    return _build_branch_mapping_list_vm(rows)


@router.get(
    "/mapping_branch/view/{branch}",
    response_model=BranchMappingDetailVM,
    dependencies=[Depends(verify_token)],
)
async def get_mapping_branch_view_detail(branch: str, db: AsyncSession = Depends(get_db)):
    return await _build_branch_mapping_detail_response(db, branch)


# 🔒 Mapping Branch
@router.post("/mapping_branch/", dependencies=[Depends(verify_token)])
async def create_mapping_branch(mapping_data: MappingBranchSchema, db: AsyncSession = Depends(get_db)):
    normalized_branch = (mapping_data.branch or "").strip()
    normalized_store_id = (mapping_data.store_id or "").strip()
    normalized_enterprise_code = (mapping_data.enterprise_code or "").strip()
    normalized_google_folder_id = mapping_data.google_folder_id
    if isinstance(normalized_google_folder_id, str):
        normalized_google_folder_id = normalized_google_folder_id.strip() or None

    if not normalized_branch:
        raise HTTPException(status_code=400, detail="branch must not be empty.")
    if not normalized_store_id:
        raise HTTPException(status_code=400, detail="store_id must not be empty.")
    if not normalized_enterprise_code:
        raise HTTPException(status_code=400, detail="enterprise_code must not be empty.")

    existing_entry = await db.execute(select(MappingBranch).filter(MappingBranch.branch == normalized_branch))
    if existing_entry.scalars().first():
        raise HTTPException(status_code=400, detail="Branch already exists.")

    new_entry = MappingBranch(
        enterprise_code=normalized_enterprise_code,
        branch=normalized_branch,
        store_id=normalized_store_id,
        google_folder_id=normalized_google_folder_id,
        id_telegram=mapping_data.id_telegram or [],
    )
    db.add(new_entry)
    await db.commit()
    await db.refresh(new_entry)

    return {"detail": "Mapping branch created successfully", "data": new_entry}


@router.put(
    "/mapping_branch/{branch}",
    response_model=BranchMappingDetailVM,
    dependencies=[Depends(verify_token)],
)
async def update_mapping_branch(
    branch: str,
    payload: MappingBranchConstrainedUpdateSchema,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(MappingBranch).where(MappingBranch.branch == branch))
    mapping_entry = result.scalars().first()
    if not mapping_entry:
        raise HTTPException(status_code=404, detail="Mapping branch not found.")

    normalized_store_id = (payload.store_id or "").strip()
    if not normalized_store_id:
        raise HTTPException(status_code=400, detail="store_id must not be empty.")

    normalized_google_folder_id = payload.google_folder_id
    if isinstance(normalized_google_folder_id, str):
        normalized_google_folder_id = normalized_google_folder_id.strip() or None

    mapping_entry.store_id = normalized_store_id
    mapping_entry.google_folder_id = normalized_google_folder_id

    await db.commit()
    await db.refresh(mapping_entry)

    return await _build_branch_mapping_detail_response(db, branch)


def _normalize_optional_query_string(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


async def _get_business_store_or_404(db: AsyncSession, store_id: int) -> BusinessStore:
    result = await db.execute(select(BusinessStore).where(BusinessStore.id == int(store_id)))
    store = result.scalar_one_or_none()
    if store is None:
        raise HTTPException(status_code=404, detail="BusinessStore not found.")
    return store


@router.get(
    "/business-stores",
    response_model=List[BusinessStoreOut],
    dependencies=[Depends(verify_token)],
)
async def get_business_stores(
    enterprise_code: str | None = Query(default=None),
    migration_status: str | None = Query(default=None),
    is_active: bool | None = Query(default=None),
    legacy_scope_key: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(BusinessStore).order_by(BusinessStore.store_name.asc(), BusinessStore.store_code.asc())

    normalized_enterprise_code = _normalize_optional_query_string(enterprise_code)
    normalized_migration_status = _normalize_optional_query_string(migration_status)
    normalized_legacy_scope_key = _normalize_optional_query_string(legacy_scope_key)

    if normalized_enterprise_code:
        stmt = stmt.where(BusinessStore.enterprise_code == normalized_enterprise_code)
    if normalized_migration_status:
        stmt = stmt.where(BusinessStore.migration_status == normalized_migration_status)
    if is_active is not None:
        stmt = stmt.where(BusinessStore.is_active.is_(bool(is_active)))
    if normalized_legacy_scope_key:
        stmt = stmt.where(BusinessStore.legacy_scope_key == normalized_legacy_scope_key)

    result = await db.execute(stmt)
    return [BusinessStoreOut.model_validate(row, from_attributes=True) for row in result.scalars().all()]


@router.get(
    "/business-stores/meta/legacy-scopes",
    response_model=List[LegacyScopeOut],
    dependencies=[Depends(verify_token)],
)
async def get_business_store_legacy_scopes(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(
            Offer.city.label("legacy_scope_key"),
            func.count(Offer.id).label("rows_count"),
            func.count(func.distinct(Offer.product_code)).label("products_count"),
        )
        .group_by(Offer.city)
        .order_by(Offer.city.asc())
    )
    rows = result.fetchall()
    return [
        LegacyScopeOut(
            legacy_scope_key=str(row.legacy_scope_key),
            rows_count=int(row.rows_count or 0),
            products_count=int(row.products_count or 0),
        )
        for row in rows
        if str(row.legacy_scope_key or "").strip()
    ]


@router.get(
    "/business-stores/meta/business-enterprises",
    response_model=List[BusinessEnterpriseOptionOut],
    dependencies=[Depends(verify_token)],
)
async def get_business_store_business_enterprises(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(EnterpriseSettings)
        .where(func.lower(func.coalesce(EnterpriseSettings.data_format, "")) == "business")
        .order_by(EnterpriseSettings.enterprise_name.asc(), EnterpriseSettings.enterprise_code.asc())
    )
    rows = result.scalars().all()
    return [
        BusinessEnterpriseOptionOut(
            enterprise_code=row.enterprise_code,
            enterprise_name=row.enterprise_name,
            branch_id=row.branch_id,
            catalog_enabled=bool(row.catalog_enabled),
            stock_enabled=bool(row.stock_enabled),
            order_fetcher=bool(row.order_fetcher),
        )
        for row in rows
    ]


@router.get(
    "/business-stores/{store_id}",
    response_model=BusinessStoreOut,
    dependencies=[Depends(verify_token)],
)
async def get_business_store(store_id: int, db: AsyncSession = Depends(get_db)):
    store = await _get_business_store_or_404(db, store_id)
    return BusinessStoreOut.model_validate(store, from_attributes=True)


@router.post(
    "/business-stores",
    response_model=BusinessStoreOut,
    dependencies=[Depends(verify_token)],
)
async def create_business_store(payload: BusinessStoreCreate, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(
        select(BusinessStore).where(BusinessStore.store_code == payload.store_code)
    )
    if existing.scalar_one_or_none() is not None:
        raise HTTPException(status_code=400, detail="BusinessStore with this store_code already exists.")

    obj = BusinessStore(
        store_code=payload.store_code,
        store_name=payload.store_name,
        legal_entity_name=payload.legal_entity_name,
        tax_identifier=payload.tax_identifier,
        is_active=payload.is_active,
        is_legacy_default=payload.is_legacy_default,
        enterprise_code=payload.enterprise_code,
        legacy_scope_key=payload.legacy_scope_key,
        tabletki_enterprise_code=payload.tabletki_enterprise_code,
        tabletki_branch=payload.tabletki_branch,
        salesdrive_enterprise_code=payload.salesdrive_enterprise_code,
        salesdrive_enterprise_id=payload.salesdrive_enterprise_id,
        salesdrive_store_name=payload.salesdrive_store_name,
        catalog_enabled=payload.catalog_enabled,
        stock_enabled=payload.stock_enabled,
        orders_enabled=payload.orders_enabled,
        catalog_only_in_stock=payload.catalog_only_in_stock,
        code_strategy=payload.code_strategy,
        code_prefix=payload.code_prefix,
        name_strategy=payload.name_strategy,
        extra_markup_enabled=payload.extra_markup_enabled,
        extra_markup_mode=payload.extra_markup_mode,
        extra_markup_min=payload.extra_markup_min,
        extra_markup_max=payload.extra_markup_max,
        extra_markup_strategy=payload.extra_markup_strategy,
        takes_over_legacy_scope=payload.takes_over_legacy_scope,
        migration_status=payload.migration_status,
    )
    db.add(obj)
    await db.commit()
    await db.refresh(obj)
    return BusinessStoreOut.model_validate(obj, from_attributes=True)


@router.put(
    "/business-stores/{store_id}",
    response_model=BusinessStoreOut,
    dependencies=[Depends(verify_token)],
)
async def update_business_store(
    store_id: int,
    payload: BusinessStoreUpdate,
    db: AsyncSession = Depends(get_db),
):
    store = await _get_business_store_or_404(db, store_id)
    for key, value in payload.model_dump(exclude_unset=True).items():
        setattr(store, key, value)

    await db.commit()
    await db.refresh(store)
    return BusinessStoreOut.model_validate(store, from_attributes=True)


@router.post(
    "/business-stores/{store_id}/dry-run",
    dependencies=[Depends(verify_token)],
)
async def dry_run_business_store(
    store_id: int,
    payload: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
):
    await _get_business_store_or_404(db, store_id)
    auto_generate_missing_codes = bool(payload.get("auto_generate_missing_codes", False))
    auto_generate_missing_names = bool(payload.get("auto_generate_missing_names", False))
    auto_generate_missing_price_adjustments = bool(
        payload.get("auto_generate_missing_price_adjustments", False)
    )
    stock = await build_store_stock_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=auto_generate_missing_codes,
        auto_generate_missing_price_adjustments=auto_generate_missing_price_adjustments,
    )
    catalog = await build_store_catalog_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=auto_generate_missing_codes,
        auto_generate_missing_names=auto_generate_missing_names,
    )
    warnings = list(stock.get("warnings") or []) + list(catalog.get("warnings") or [])
    return {
        "store_id": int(store_id),
        "auto_generate_missing_codes": auto_generate_missing_codes,
        "auto_generate_missing_names": auto_generate_missing_names,
        "auto_generate_missing_price_adjustments": auto_generate_missing_price_adjustments,
        "stock": {
            "status": stock.get("status"),
            "total_offer_rows": stock.get("total_offer_rows"),
            "unique_internal_products": stock.get("unique_internal_products"),
            "products_with_mapping": stock.get("products_with_mapping"),
            "products_missing_mapping": stock.get("products_missing_mapping"),
            "extra_markup_enabled": stock.get("extra_markup_enabled"),
            "extra_markup_mode": stock.get("extra_markup_mode"),
            "extra_markup_min": stock.get("extra_markup_min"),
            "extra_markup_max": stock.get("extra_markup_max"),
            "products_with_price_adjustment": stock.get("products_with_price_adjustment"),
            "products_missing_price_adjustment": stock.get("products_missing_price_adjustment"),
            "sample_items": stock.get("sample_items") or [],
            "missing_mapping_samples": stock.get("missing_mapping_samples") or [],
            "missing_price_adjustment_samples": stock.get("missing_price_adjustment_samples") or [],
        },
        "catalog": {
            "status": catalog.get("status"),
            "catalog_source": catalog.get("catalog_source"),
            "code_strategy": catalog.get("code_strategy"),
            "name_strategy": catalog.get("name_strategy"),
            "master_catalog_total": catalog.get("master_catalog_total"),
            "catalog_products_to_export": catalog.get("catalog_products_to_export"),
            "products_with_mapping": catalog.get("products_with_mapping"),
            "products_missing_mapping": catalog.get("products_missing_mapping"),
            "products_with_name_mapping": catalog.get("products_with_name_mapping"),
            "products_missing_name_mapping": catalog.get("products_missing_name_mapping"),
            "sample_items": catalog.get("sample_items") or [],
            "missing_mapping_samples": catalog.get("missing_mapping_samples") or [],
            "missing_name_samples": catalog.get("missing_name_samples") or [],
        },
        "warnings": warnings,
    }


@router.post(
    "/business-stores/{store_id}/generate-missing-codes",
    dependencies=[Depends(verify_token)],
)
async def generate_business_store_missing_codes(store_id: int, db: AsyncSession = Depends(get_db)):
    await _get_business_store_or_404(db, store_id)

    before_stock = await build_store_stock_dry_run(db, int(store_id), auto_generate_missing_codes=False)
    before_catalog = await build_store_catalog_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=False,
        auto_generate_missing_names=False,
    )
    before_count_result = await db.execute(
        select(func.count(BusinessStoreProductCode.id)).where(BusinessStoreProductCode.store_id == int(store_id))
    )
    before_count = int(before_count_result.scalar_one() or 0)

    await build_store_stock_dry_run(db, int(store_id), auto_generate_missing_codes=True)
    await build_store_catalog_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=True,
        auto_generate_missing_names=False,
    )
    await db.commit()

    after_count_result = await db.execute(
        select(func.count(BusinessStoreProductCode.id)).where(BusinessStoreProductCode.store_id == int(store_id))
    )
    after_count = int(after_count_result.scalar_one() or 0)

    stock = await build_store_stock_dry_run(db, int(store_id), auto_generate_missing_codes=False)
    catalog = await build_store_catalog_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=False,
        auto_generate_missing_names=False,
    )

    return {
        "store_id": int(store_id),
        "generated_codes": max(0, after_count - before_count),
        "before": {
            "stock_missing_mappings": before_stock.get("products_missing_mapping"),
            "catalog_missing_mappings": before_catalog.get("products_missing_mapping"),
        },
        "after": {
            "stock_missing_mappings": stock.get("products_missing_mapping"),
            "catalog_missing_mappings": catalog.get("products_missing_mapping"),
        },
        "stock": {
            "status": stock.get("status"),
            "total_offer_rows": stock.get("total_offer_rows"),
            "unique_internal_products": stock.get("unique_internal_products"),
            "products_with_mapping": stock.get("products_with_mapping"),
            "products_missing_mapping": stock.get("products_missing_mapping"),
            "sample_items": stock.get("sample_items") or [],
            "missing_mapping_samples": stock.get("missing_mapping_samples") or [],
        },
        "catalog": {
            "status": catalog.get("status"),
            "catalog_source": catalog.get("catalog_source"),
            "name_strategy": catalog.get("name_strategy"),
            "master_catalog_total": catalog.get("master_catalog_total"),
            "catalog_products_to_export": catalog.get("catalog_products_to_export"),
            "products_with_mapping": catalog.get("products_with_mapping"),
            "products_missing_mapping": catalog.get("products_missing_mapping"),
            "products_with_name_mapping": catalog.get("products_with_name_mapping"),
            "products_missing_name_mapping": catalog.get("products_missing_name_mapping"),
            "sample_items": catalog.get("sample_items") or [],
            "missing_mapping_samples": catalog.get("missing_mapping_samples") or [],
            "missing_name_samples": catalog.get("missing_name_samples") or [],
        },
        "warnings": list(stock.get("warnings") or []) + list(catalog.get("warnings") or []),
    }


@router.post(
    "/business-stores/{store_id}/generate-missing-names",
    dependencies=[Depends(verify_token)],
)
async def generate_business_store_missing_names(store_id: int, db: AsyncSession = Depends(get_db)):
    store = await _get_business_store_or_404(db, store_id)

    before_catalog = await build_store_catalog_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=False,
        auto_generate_missing_names=False,
    )
    before_count = int(
        (
            await db.execute(
                select(func.count(BusinessStoreProductName.id)).where(
                    BusinessStoreProductName.store_id == int(store_id)
                )
            )
        ).scalar_one()
        or 0
    )

    generated_catalog = await build_store_catalog_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=False,
        auto_generate_missing_names=True,
    )
    await db.commit()

    after_count = int(
        (
            await db.execute(
                select(func.count(BusinessStoreProductName.id)).where(
                    BusinessStoreProductName.store_id == int(store_id)
                )
            )
        ).scalar_one()
        or 0
    )
    after_catalog = await build_store_catalog_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=False,
        auto_generate_missing_names=False,
    )

    return {
        "store_id": int(store_id),
        "store_code": store.store_code,
        "generated_names": max(0, after_count - before_count),
        "summary": {
            "generated_count": max(0, after_count - before_count),
            "missing_count_after": after_catalog.get("products_missing_name_mapping"),
            "generated_preview_samples": [
                item
                for item in (generated_catalog.get("sample_items") or [])
                if item.get("name_mapping_generated")
            ][:20],
        },
        "before": {
            "catalog_missing_name_mappings": before_catalog.get("products_missing_name_mapping"),
        },
        "after": {
            "catalog_missing_name_mappings": after_catalog.get("products_missing_name_mapping"),
        },
        "catalog": {
            "status": after_catalog.get("status"),
            "catalog_source": after_catalog.get("catalog_source"),
            "name_strategy": after_catalog.get("name_strategy"),
            "catalog_products_to_export": after_catalog.get("catalog_products_to_export"),
            "products_with_name_mapping": after_catalog.get("products_with_name_mapping"),
            "products_missing_name_mapping": after_catalog.get("products_missing_name_mapping"),
            "sample_items": after_catalog.get("sample_items") or [],
            "missing_name_samples": after_catalog.get("missing_name_samples") or [],
        },
    }


@router.post(
    "/business-stores/{store_id}/cleanup-product-names",
    dependencies=[Depends(verify_token)],
)
async def cleanup_business_store_product_names(
    store_id: int,
    payload: dict[str, Any] = Body(default={}),
    db: AsyncSession = Depends(get_db),
):
    await _get_business_store_or_404(db, store_id)
    if not bool(payload.get("confirm")):
        raise HTTPException(status_code=400, detail="confirm=true is required for cleanup-product-names.")

    result = await cleanup_store_product_names(
        db,
        int(store_id),
        mode=str(payload.get("mode") or "deactivate"),
    )
    await db.commit()
    return result


@router.post(
    "/business-stores/{store_id}/generate-missing-price-adjustments",
    dependencies=[Depends(verify_token)],
)
async def generate_business_store_missing_price_adjustments(
    store_id: int,
    db: AsyncSession = Depends(get_db),
):
    store = await _get_business_store_or_404(db, store_id)

    before_stock = await build_store_stock_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=False,
        auto_generate_missing_price_adjustments=False,
    )
    before_count = int(
        (
            await db.execute(
                select(func.count(BusinessStoreProductPriceAdjustment.id)).where(
                    BusinessStoreProductPriceAdjustment.store_id == int(store_id)
                )
            )
        ).scalar_one()
        or 0
    )

    generated_stock = await build_store_stock_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=False,
        auto_generate_missing_price_adjustments=True,
    )
    await db.commit()

    after_count = int(
        (
            await db.execute(
                select(func.count(BusinessStoreProductPriceAdjustment.id)).where(
                    BusinessStoreProductPriceAdjustment.store_id == int(store_id)
                )
            )
        ).scalar_one()
        or 0
    )
    after_stock = await build_store_stock_dry_run(
        db,
        int(store_id),
        auto_generate_missing_codes=False,
        auto_generate_missing_price_adjustments=False,
    )

    return {
        "store_id": int(store_id),
        "store_code": store.store_code,
        "generated_price_adjustments": max(0, after_count - before_count),
        "summary": {
            "generated_count": max(0, after_count - before_count),
            "missing_count_after": after_stock.get("products_missing_price_adjustment"),
            "generated_preview_samples": [
                item
                for item in (generated_stock.get("sample_items") or [])
                if item.get("price_adjustment_generated")
            ][:20],
        },
        "before": {
            "stock_missing_price_adjustments": before_stock.get("products_missing_price_adjustment"),
        },
        "after": {
            "stock_missing_price_adjustments": after_stock.get("products_missing_price_adjustment"),
        },
        "stock": {
            "status": after_stock.get("status"),
            "extra_markup_enabled": after_stock.get("extra_markup_enabled"),
            "extra_markup_mode": after_stock.get("extra_markup_mode"),
            "extra_markup_min": after_stock.get("extra_markup_min"),
            "extra_markup_max": after_stock.get("extra_markup_max"),
            "products_with_price_adjustment": after_stock.get("products_with_price_adjustment"),
            "products_missing_price_adjustment": after_stock.get("products_missing_price_adjustment"),
            "sample_items": after_stock.get("sample_items") or [],
            "missing_price_adjustment_samples": after_stock.get("missing_price_adjustment_samples") or [],
        },
    }

# 🟢 Публичные эндпоинты
@router.post("/unipro/data")
async def receive_unipro_data(request: Request, body: dict):
    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    os.makedirs(temp_dir, exist_ok=True)
    json_file_path = os.path.join(temp_dir, "unipro_data.json")

    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(body, json_file, ensure_ascii=False, indent=4)

    await unipro_convert(json_file_path)
    return {"status": "success", "message": "Data received and processed"}

@router.post("/catalog/")
async def upload_catalog(file: UploadFile, enterprise_code: str, db: AsyncSession = Depends(get_db)):
    return {"message": "Catalog data processed successfully."}

@router.post("/stock/")
async def upload_stock(file: UploadFile, enterprise_code: str, db: AsyncSession = Depends(get_db)):
    return {"message": "Stock data processed successfully."}
# ⬇️ НОВЫЙ ПУБЛИЧНЫЙ ЭНДПОИНТ (БЕЗ verify_token)
from app.business.salesdrive_webhook import process_salesdrive_webhook  # заглушка, см. ниже
from app.salesdrive_simple.webhook import process_salesdrive_simple_webhook


@router.post("/webhooks/salesdrive-simple/{branch}", summary="SalesDriveSimple Webhook (public)")
async def salesdrive_simple_webhook(
    branch: str,
    request: Request,
    background: BackgroundTasks,
):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object.")

    data = payload.get("data")
    data_obj = data if isinstance(data, dict) else {}
    sd_logger.info(
        "SalesDriveSimple webhook accepted: branch=%s externalId=%s id=%s statusId=%s",
        branch,
        data_obj.get("externalId"),
        data_obj.get("id"),
        data_obj.get("statusId"),
    )
    background.add_task(process_salesdrive_simple_webhook, payload, branch)
    return {"status": "accepted"}

@router.post("/webhooks/salesdrive", summary="SalesDrive Webhook (public)")
async def salesdrive_webhook(
    payload: dict = Body(
        ...,
        title="SalesDrive payload",
        description="Сырой JSON, который присылает SalesDrive",
        example={
            "info": {
                "webhookType": "order",
                "webhookEvent": "new_order",
                "account": "demo"
            },
            "data": {
                "id": 12345,
                "statusId": 10,
                "products": [
                    {"name": "Тест", "amount": 1, "price": 100}
                ]
            }
        }
    ),
    request: Request = None,
    background: BackgroundTasks = None
):
    # Заголовки без чувствительных данных
    headers_safe = {
        k: ("<redacted>" if k.lower() == "authorization" else v)
        for k, v in request.headers.items()
    }
    sd_logger.info("📥 SalesDrive webhook: %s %s", request.method, request.url.path)
    sd_logger.info("Headers: %s", json.dumps(headers_safe, ensure_ascii=False))

    # Короткая сводка
    info = (payload.get("info") or {})
    data = (payload.get("data") or {})
    sd_logger.info(
        "Summary: webhookType=%s webhookEvent=%s account=%s order_id=%s status_id=%s",
        info.get("webhookType"), info.get("webhookEvent"), info.get("account"),
        data.get("id"), data.get("statusId")
    )

    # Полный «как есть» JSON
    sd_logger.info("Payload:\n%s", json.dumps(payload, ensure_ascii=False, indent=2))

    # Фоновая обработка (заглушка)
    background.add_task(process_salesdrive_webhook, payload)

    return {"ok": True}

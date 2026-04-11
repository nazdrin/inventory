from dotenv import load_dotenv
load_dotenv()
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Request, Security
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi.security import HTTPBearer
from typing import List, Any
import os
import json
import tempfile
from datetime import timedelta

from app import crud, schemas, database
from app.schemas import (
    EnterpriseSettingsSchema, DeveloperSettingsSchema, DataFormatSchema, 
    MappingBranchSchema, LoginSchema, BranchMappingListItemVM, BranchMappingDetailVM,
    MappingBranchConstrainedUpdateSchema, EnterpriseListItemVM, EnterpriseDetailVM,
    EnterpriseFieldMetaVM, EnterpriseSectionVM, SupplierListItemVM, SupplierDetailVM,
    SupplierSectionVM, BusinessSettingsVM, BusinessSectionVM, BusinessSettingItemVM,
    BusinessEnterpriseCandidateVM, BusinessEnterpriseOptionVM, BusinessSettingsUpdateSchema,
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

# ⏩ ОДИН РАЗ указываем prefix, а внутри маршрутов больше не дублируем developer_panel
router = APIRouter(prefix="/developer_panel", tags=["Developer Panel"])


from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.auth import verify_token  # как у enterprise
from app.models import DropshipEnterprise, BusinessSettings
from app.schemas import DropshipEnterpriseSchema


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


def _business_planned_writable_keys() -> list[str]:
    return [
        "business_enterprise_code",
        "daily_publish_enterprise_code_override",
        "weekly_salesdrive_enterprise_code_override",
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
            "Biotus target остаётся отдельным selector и сейчас не должен считаться unified runtime target.",
        )
    return (
        "linked-but-separate",
        "MASTER targets можно показывать как связанную семью selector-ов, а Biotus target остаётся отдельным contour selector.",
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
            "Biotus target остаётся отдельным contour selector и читается из DB override/primary model без runtime unification.",
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
) -> list[BusinessSectionVM]:
    business_exists = enterprise is not None or resolution_status == "ambiguous"
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
        consistency_status, consistency_note = _db_target_consistency_status(
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
        consistency_status, consistency_note = _target_consistency_status(
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
            _business_item(
                "target_resolution_status",
                "Target resolution mode",
                target_resolution_status,
                target_resolution_source,
                group="Primary business target",
                help_text=target_resolution_help,
            ),
            _business_item("enterprise_name", "Название предприятия", target_enterprise.enterprise_name, "EnterpriseSettings", group="Primary business target"),
            _business_item("enterprise_code", "Код предприятия", target_enterprise.enterprise_code, "EnterpriseSettings", group="Primary business target"),
            _business_item("data_format", "Текущий data_format", target_enterprise.data_format, "EnterpriseSettings", group="Primary business target"),
            _business_item("branch_id", "Branch ID", target_enterprise.branch_id, "EnterpriseSettings", group="Primary business target"),
            _business_item("catalog_enabled", "Каталог включён", bool(target_enterprise.catalog_enabled), "EnterpriseSettings", group="Primary business target"),
            _business_item("stock_enabled", "Сток включён", bool(target_enterprise.stock_enabled), "EnterpriseSettings", group="Primary business target"),
            _business_item("order_fetcher", "Получение заказов", bool(target_enterprise.order_fetcher), "EnterpriseSettings", group="Primary business target"),
        ]
    else:
        target_items = [
            _business_item(
                "target_resolution_status",
                "Target resolution mode",
                target_resolution_status,
                target_resolution_source,
                group="Primary business target",
                help_text=target_resolution_help,
            ),
            _business_item(
                "enterprise_code_missing",
                "Primary business enterprise code",
                target_primary_code,
                "db" if business_settings_exists else "computed",
                group="Primary business target",
                help_text=(
                    "DB row exists but the referenced enterprise is missing in EnterpriseSettings."
                    if business_settings_exists
                    else "Business enterprise could not be resolved from EnterpriseSettings fallback logic."
                ),
            ),
        ]

    target_items.extend(
        [
            _business_item(
                "business_enterprise_code",
                "Primary business enterprise code",
                master_catalog_target,
                master_catalog_target_source,
                group="Primary business target",
                help_text=master_catalog_target_help,
                readonly=False,
            ),
            _business_item(
                "master_daily_publish_enterprise_explicit",
                "Daily publish explicit target",
                explicit_daily_target,
                explicit_daily_source,
                group="Linked runtime targets",
                help_text=(
                    "NULL in DB means inherit primary business_enterprise_code."
                    if business_settings_exists
                    else "Fallback env selector is used only because business_settings row is missing."
                ),
                readonly=False,
            ),
            _business_item(
                "master_daily_publish_enterprise_effective",
                "Daily publish effective target",
                effective_daily_target,
                daily_target_source,
                group="Linked runtime targets",
                help_text=daily_target_help,
            ),
            _business_item(
                "master_weekly_salesdrive_enterprise_explicit",
                "Weekly SalesDrive explicit target",
                explicit_weekly_target,
                explicit_weekly_source,
                group="Linked runtime targets",
                help_text=(
                    "NULL in DB means inherit primary business_enterprise_code."
                    if business_settings_exists
                    else "Fallback env selector is used only because business_settings row is missing."
                ),
                readonly=False,
            ),
            _business_item(
                "master_weekly_salesdrive_enterprise_effective",
                "Weekly SalesDrive effective target",
                effective_weekly_target,
                weekly_target_source,
                group="Linked runtime targets",
                help_text=weekly_target_help,
            ),
            _business_item(
                "biotus_enterprise_code_target",
                "BIOTUS_ENTERPRISE_CODE",
                biotus_target,
                biotus_target_source,
                group="Separate runtime target",
                help_text=(
                    biotus_target_help
                    if business_settings_exists
                    else "Biotus target показывается отдельно и не объединяется в единый runtime selector."
                ),
            ),
            _business_item(
                "target_consistency_status",
                "Target consistency status",
                consistency_status,
                "computed" if not business_settings_exists else "db",
                group="Consistency",
            ),
            _business_item(
                "target_consistency_note",
                "Consistency note",
                consistency_note,
                "computed" if not business_settings_exists else "db",
                group="Consistency",
            ),
        ]
    )

    legacy_items = [
        _business_item(
            "old_business_contour_status",
            "Статус старого Business catalog contour",
            "scheduler-disabled" if old_catalog_disabled else "scheduler-reachable",
            "computed",
        ),
        _business_item(
            "disable_old_business_catalog_scheduler",
            "DISABLE_OLD_BUSINESS_CATALOG_SCHEDULER",
            old_catalog_disabled,
            "env",
        ),
        _business_item(
            "scheduler_disabled",
            "Scheduler disabled",
            old_catalog_disabled,
            "computed",
        ),
        _business_item(
            "business_enterprise_exists",
            "Business enterprise найден",
            business_exists,
            "computed",
        ),
        _business_item(
            "manual_reachability",
            "Manual reachability",
            "CLI/direct call still possible",
            "computed",
            help_text="Old Business contour остаётся manually reachable через import_catalog.py и не считается удалённым.",
        ),
    ]

    stock_items = [
        _business_item(
            "use_master_mapping_for_stock",
            "USE_MASTER_MAPPING_FOR_STOCK",
            use_master_mapping,
            "env",
        ),
        _business_item(
            "mapping_mode_status",
            "Текущий mapping mode",
            "master-first with legacy fallback" if use_master_mapping else "legacy mapping primary",
            "computed",
        ),
        _business_item(
            "catalog_mapping_dependency",
            "catalog_mapping runtime dependency",
            "order_sender + dropship fallback still read catalog_mapping",
            "computed",
            help_text="Это snapshot статуса по текущему коду, без deep runtime probe.",
        ),
    ]

    orders_items = [
        _business_item(
            "biotus_enterprise_code",
            "BIOTUS_ENTERPRISE_CODE",
            biotus_target,
            biotus_target_source,
            group="Target",
            help_text=biotus_target_help,
        ),
        _business_item(
            "biotus_enable_unhandled_fallback",
            "BIOTUS_ENABLE_UNHANDLED_FALLBACK",
            biotus_enable_unhandled_fallback_value,
            biotus_enable_unhandled_fallback_source,
            group="Fallback policy",
            help_text=biotus_enable_unhandled_fallback_help,
            readonly=False,
        ),
        _business_item(
            "biotus_unhandled_order_timeout_minutes",
            "BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES",
            biotus_unhandled_order_timeout_minutes_value,
            biotus_unhandled_order_timeout_minutes_source,
            group="Fallback policy",
            help_text=biotus_unhandled_order_timeout_minutes_help,
            readonly=False,
        ),
        _business_item(
            "biotus_fallback_additional_status_ids",
            "BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS",
            biotus_fallback_additional_status_ids_value,
            biotus_fallback_additional_status_ids_source,
            group="Fallback policy",
            help_text=biotus_fallback_additional_status_ids_help,
            readonly=False,
        ),
        _business_item(
            "biotus_duplicate_status_id",
            "BIOTUS_DUPLICATE_STATUS_ID",
            biotus_duplicate_status_id_value,
            biotus_duplicate_status_id_source,
            group="Fallback policy",
            help_text=biotus_duplicate_status_id_help,
            readonly=False,
        ),
        _business_item(
            "biotus_time_default_minutes",
            "BIOTUS_TIME_DEFAULT_MINUTES",
            _env_int_value("BIOTUS_TIME_DEFAULT_MINUTES", 30),
            "env",
            group="Timing window",
        ),
        _business_item(
            "biotus_time_switch_hour",
            "BIOTUS_TIME_SWITCH_HOUR",
            _env_int_value("BIOTUS_TIME_SWITCH_HOUR", 12),
            "env",
            group="Timing window",
        ),
        _business_item(
            "biotus_time_switch_end_hour",
            "BIOTUS_TIME_SWITCH_END_HOUR",
            _env_int_value("BIOTUS_TIME_SWITCH_END_HOUR", 13),
            "env",
            group="Timing window",
        ),
        _business_item(
            "biotus_time_after_switch_minutes",
            "BIOTUS_TIME_AFTER_SWITCH_MINUTES",
            _env_int_value("BIOTUS_TIME_AFTER_SWITCH_MINUTES", 15),
            "env",
            group="Timing window",
        ),
        _business_item(
            "biotus_tz",
            "BIOTUS_TZ",
            _env_optional_value("BIOTUS_TZ") or "Europe/Kyiv",
            "env",
            group="Timing window",
        ),
        _business_item(
            "tabletki_cancel_reason_default",
            "TABLETKI_CANCEL_REASON_DEFAULT",
            _env_int_value("TABLETKI_CANCEL_REASON_DEFAULT", 18),
            "env",
            group="Fallback policy",
        ),
        _business_item(
            "allowed_suppliers",
            "ALLOWED_SUPPLIERS",
            _business_joined_env_list("ALLOWED_SUPPLIERS", "38;41"),
            "env",
            group="Transitional fallback",
            help_text="Fallback allowlist. Основной контур выбора поставщиков уже смещается в supplier-model driven flags.",
        ),
    ]

    master_items = [
        _business_item("master_scheduler_enabled", "MASTER_SCHEDULER_ENABLED", _env_bool_value("MASTER_SCHEDULER_ENABLED", "1"), "env", group="Scheduler gates"),
        _business_item(
            "master_catalog_enterprise_code_vm",
            "Primary business enterprise code",
            master_catalog_target,
            master_catalog_target_source,
            group="Target selectors",
        ),
        _business_item(
            "master_daily_publish_enterprise",
            "Daily publish explicit target",
            explicit_daily_target,
            explicit_daily_source,
            group="Target selectors",
        ),
        _business_item("master_daily_publish_effective_target", "Daily publish effective target", effective_daily_target, daily_target_source, group="Target selectors"),
        _business_item(
            "master_weekly_salesdrive_enterprise",
            "Weekly SalesDrive explicit target",
            explicit_weekly_target,
            explicit_weekly_source,
            group="Target selectors",
        ),
        _business_item("master_weekly_salesdrive_effective_target", "Weekly SalesDrive effective target", effective_weekly_target, weekly_target_source, group="Target selectors"),
        _business_item(
            "master_weekly_enabled",
            "MASTER_WEEKLY_ENABLED",
            bool(business_settings_row.master_weekly_enabled) if business_settings_exists else _env_bool_value("MASTER_WEEKLY_ENABLED", "1"),
            "db" if business_settings_exists else "env-fallback",
            group="Weekly orchestration",
            readonly=False,
        ),
        _business_item(
            "master_weekly_day",
            "MASTER_WEEKLY_DAY",
            business_settings_row.master_weekly_day if business_settings_exists else (_env_optional_value("MASTER_WEEKLY_DAY") or "SUN"),
            "db" if business_settings_exists else "env-fallback",
            group="Weekly orchestration",
            readonly=False,
        ),
        _business_item(
            "master_weekly_hour",
            "MASTER_WEEKLY_HOUR",
            int(business_settings_row.master_weekly_hour) if business_settings_exists else _env_int_value("MASTER_WEEKLY_HOUR", 3),
            "db" if business_settings_exists else "env-fallback",
            group="Weekly orchestration",
            readonly=False,
        ),
        _business_item(
            "master_weekly_minute",
            "MASTER_WEEKLY_MINUTE",
            int(business_settings_row.master_weekly_minute) if business_settings_exists else _env_int_value("MASTER_WEEKLY_MINUTE", 0),
            "db" if business_settings_exists else "env-fallback",
            group="Weekly orchestration",
            readonly=False,
        ),
        _business_item("master_weekly_salesdrive_batch_size", "MASTER_WEEKLY_SALESDRIVE_BATCH_SIZE", _env_int_value("MASTER_WEEKLY_SALESDRIVE_BATCH_SIZE", 100), "env", group="Weekly orchestration"),
        _business_item(
            "master_daily_publish_enabled",
            "MASTER_DAILY_PUBLISH_ENABLED",
            bool(business_settings_row.master_daily_publish_enabled) if business_settings_exists else _env_bool_value("MASTER_DAILY_PUBLISH_ENABLED", "1"),
            "db" if business_settings_exists else "env-fallback",
            group="Daily publish",
            readonly=False,
        ),
        _business_item(
            "master_daily_publish_hour",
            "MASTER_DAILY_PUBLISH_HOUR",
            int(business_settings_row.master_daily_publish_hour) if business_settings_exists else _env_int_value("MASTER_DAILY_PUBLISH_HOUR", 9),
            "db" if business_settings_exists else "env-fallback",
            group="Daily publish",
            readonly=False,
        ),
        _business_item(
            "master_daily_publish_minute",
            "MASTER_DAILY_PUBLISH_MINUTE",
            int(business_settings_row.master_daily_publish_minute) if business_settings_exists else _env_int_value("MASTER_DAILY_PUBLISH_MINUTE", 0),
            "db" if business_settings_exists else "env-fallback",
            group="Daily publish",
            readonly=False,
        ),
        _business_item(
            "master_daily_publish_limit",
            "MASTER_DAILY_PUBLISH_LIMIT",
            int(business_settings_row.master_daily_publish_limit) if business_settings_exists else _env_int_value("MASTER_DAILY_PUBLISH_LIMIT", 0),
            "db" if business_settings_exists else "env-fallback",
            group="Daily publish",
            readonly=False,
        ),
        _business_item(
            "master_archive_enabled",
            "MASTER_ARCHIVE_ENABLED",
            bool(business_settings_row.master_archive_enabled) if business_settings_exists else _env_bool_value("MASTER_ARCHIVE_ENABLED", "1"),
            "db" if business_settings_exists else "env-fallback",
            group="Archive",
            readonly=False,
        ),
        _business_item(
            "master_archive_every_minutes",
            "MASTER_ARCHIVE_EVERY_MINUTES",
            int(business_settings_row.master_archive_every_minutes) if business_settings_exists else _env_int_value("MASTER_ARCHIVE_EVERY_MINUTES", 60),
            "db" if business_settings_exists else "env-fallback",
            group="Archive",
            readonly=False,
        ),
        _business_item(
            "master_target_fallback_note",
            "Target fallback note",
            (
                "DB-first: daily/weekly effective targets use override when present, otherwise business_enterprise_code."
                if business_settings_exists
                else "Fallback-only mode: daily/weekly explicit targets fallback to MASTER_CATALOG_ENTERPRISE_CODE when empty."
            ),
            "db" if business_settings_exists else "computed",
            group="Target selectors",
        ),
        _business_item(
            "writable_scope_status",
            "Writable first scope",
            "enabled-for-master-control-plane",
            "computed",
            help_text="Write support включён только для DB-backed primary/daily/weekly/archive fields, уже читаемых page и master runtime.",
        ),
    ]

    pricing_items = [
        _business_item("base_thr", "BASE_THR", _env_optional_value("BASE_THR") or "0.08", "env", group="Базовая модель"),
        _business_item(
            "use_master_mapping_for_stock_pricing",
            "USE_MASTER_MAPPING_FOR_STOCK",
            use_master_mapping,
            "env",
            group="Базовая модель",
            help_text="Не pricing-only флаг, но напрямую влияет на Business stock/order mapping contour.",
        ),
        _business_item("price_band_low_max", "PRICE_BAND_LOW_MAX", _env_optional_value("PRICE_BAND_LOW_MAX") or "100", "env", group="Диапазоны цен"),
        _business_item("price_band_mid_max", "PRICE_BAND_MID_MAX", _env_optional_value("PRICE_BAND_MID_MAX") or "400", "env", group="Диапазоны цен"),
        _business_item("thr_mult_low", "THR_MULT_LOW", _env_optional_value("THR_MULT_LOW") or "1.0", "env", group="Мультипликаторы"),
        _business_item("thr_mult_mid", "THR_MULT_MID", _env_optional_value("THR_MULT_MID") or "1.0", "env", group="Мультипликаторы"),
        _business_item("thr_mult_high", "THR_MULT_HIGH", _env_optional_value("THR_MULT_HIGH") or "1.0", "env", group="Мультипликаторы"),
        _business_item("no_comp_mult_low", "NO_COMP_MULT_LOW", _env_optional_value("NO_COMP_MULT_LOW") or "1.0", "env", group="Мультипликаторы"),
        _business_item("no_comp_mult_mid", "NO_COMP_MULT_MID", _env_optional_value("NO_COMP_MULT_MID") or "1.0", "env", group="Мультипликаторы"),
        _business_item("no_comp_mult_high", "NO_COMP_MULT_HIGH", _env_optional_value("NO_COMP_MULT_HIGH") or "1.0", "env", group="Мультипликаторы"),
        _business_item("comp_delta_min_uah", "COMP_DELTA_MIN_UAH", _env_optional_value("COMP_DELTA_MIN_UAH") or "2", "env", group="Конкурентное поведение"),
        _business_item("comp_delta_max_uah", "COMP_DELTA_MAX_UAH", _env_optional_value("COMP_DELTA_MAX_UAH") or "15", "env", group="Конкурентное поведение"),
        _business_item("comp_discount_share", "COMP_DISCOUNT_SHARE", _env_optional_value("COMP_DISCOUNT_SHARE") or "0.01", "env", group="Конкурентное поведение"),
        _business_item("price_jitter_enabled", "PRICE_JITTER_ENABLED", _env_bool_value("PRICE_JITTER_ENABLED", "0"), "env", group="Jitter"),
        _business_item("price_jitter_step_uah", "PRICE_JITTER_STEP_UAH", _env_optional_value("PRICE_JITTER_STEP_UAH") or "0.5", "env", group="Jitter"),
        _business_item("price_jitter_min_uah", "PRICE_JITTER_MIN_UAH", _env_optional_value("PRICE_JITTER_MIN_UAH"), "env", group="Jitter"),
        _business_item("price_jitter_max_uah", "PRICE_JITTER_MAX_UAH", _env_optional_value("PRICE_JITTER_MAX_UAH"), "env", group="Jitter"),
    ]

    salesdrive_items = [
        _business_item(
            "salesdrive_product_handler_url_status",
            "SALESDRIVE_PRODUCT_HANDLER_URL",
            _business_presence_label(_env_optional_value("SALESDRIVE_PRODUCT_HANDLER_URL")),
            "env",
            group="Handler endpoints",
            help_text="Показывается только presence/status, без вывода raw endpoint в UI.",
        ),
        _business_item(
            "salesdrive_category_handler_url_status",
            "SALESDRIVE_CATEGORY_HANDLER_URL",
            _business_presence_label(_env_optional_value("SALESDRIVE_CATEGORY_HANDLER_URL")),
            "env",
            group="Handler endpoints",
            help_text="Показывается только presence/status, без вывода raw endpoint в UI.",
        ),
        _business_item(
            "salesdrive_token_presence",
            "Enterprise token presence",
            "present" if token_presence else "missing",
            "secret-hidden",
            group="Auth source",
            help_text=(
                f"Проверка token presence для enterprise={token_hint_target_code}."
                if token_hint_target_code
                else "Resolved Business/master target enterprise для token hint не найден."
            ),
        ),
        _business_item(
            "salesdrive_base_url_note",
            "SALESDRIVE_BASE_URL note",
            "Current order/Biotus contour still uses hardcoded SalesDrive base URL constant instead of env source-of-truth.",
            "transitional",
            group="Runtime note",
        ),
    ]

    technical_items = [
        _business_item("enterprise_settings_owner", "EnterpriseSettings owner", "enterprise runtime fields still owned by EnterpriseSettings", "EnterpriseSettings", group="Source of truth"),
        _business_item(
            "db_owner",
            "DB-first settings",
            (
                "business_settings is the source of truth for first-migration fields in this page snapshot"
                if business_settings_exists
                else "business_settings row missing; DB-first storage exists but is not currently used by this page snapshot"
            ),
            "db" if business_settings_exists else "transitional",
            group="Source of truth",
        ),
        _business_item("env_owner", "Env-backed settings", "pricing and Biotus runtime consumers still come from env", "env", group="Source of truth"),
        _business_item("transitional_state", "Transitional settings", "old Business catalog contour, ALLOWED_SUPPLIERS and mapping fallback remain transitional", "transitional", group="Source of truth"),
        _business_item("secret_hidden", "Secret-hidden values", "token/API-key values are represented only as presence/status hints", "secret-hidden", group="Visibility rules"),
        _business_item(
            "write_support",
            "Запись orchestration settings",
            planned_write_supported,
            "computed",
            group="Visibility rules",
            help_text="Writable scope intentionally ограничен master fields plus bounded Biotus fallback/status policy fields in business_settings.",
        ),
    ]

    return [
        BusinessSectionVM(
            key="target_enterprise",
            title="Целевое предприятие",
            description="DB-backed primary selector и nullable overrides для daily/weekly master contour. Изменяемы только поля control-plane, уже читаемые page и master runtime.",
            readonly=False,
            items=target_items,
        ),
        BusinessSectionVM(
            key="master_catalog",
            title="Master Catalog",
            description="DB-backed weekly/daily/archive settings. Изменения применяются к business_settings и используются master contour на следующем цикле.",
            readonly=False,
            items=master_items,
        ),
        BusinessSectionVM(
            key="legacy_catalog",
            title="Старый каталог",
            description="Статус old Business catalog contour без изменения runtime ownership.",
            readonly=True,
            items=legacy_items,
        ),
        BusinessSectionVM(
            key="stock_mapping_mode",
            title="Сток / Mapping Mode",
            description="Read-only snapshot текущего режима сопоставления и fallback semantics.",
            readonly=True,
            items=stock_items,
        ),
        BusinessSectionVM(
            key="pricing",
            title="Ценообразование",
            description="Read-only snapshot pricing control-plane из dropship pipeline. Секция intentionally не включает write logic.",
            readonly=True,
            items=pricing_items,
        ),
        BusinessSectionVM(
            key="orders_biotus",
            title="Заказы / Biotus",
            description="Bounded writable Biotus fallback/status policy. Timing window, scheduler/runtime flags and external credentials remain outside this scope.",
            readonly=False,
            items=orders_items,
        ),
        BusinessSectionVM(
            key="salesdrive_integrations",
            title="SalesDrive / Интеграции",
            description="Read-only visibility по handler endpoints и auth/provenance hints без показа секретов.",
            readonly=True,
            items=salesdrive_items,
        ),
        BusinessSectionVM(
            key="technical_status",
            title="Технический статус",
            description="Короткие ownership/provenance hints для текущей Business control-plane surface.",
            readonly=True,
            items=technical_items,
        ),
    ]


async def _build_business_settings_vm(db: AsyncSession) -> BusinessSettingsVM:
    all_enterprises = await _load_all_enterprises(db)
    business_settings_row = await _load_business_settings_row(db)
    candidates = _filter_business_candidates(all_enterprises)
    fallback_resolution_status, fallback_resolution_message, fallback_resolved = _business_resolution_state(candidates)
    enterprise_lookup = _enterprise_lookup_by_code(all_enterprises)

    if business_settings_row is not None:
        db_primary_code = str(business_settings_row.business_enterprise_code or "").strip() or None
        db_primary_enterprise = enterprise_lookup.get(str(db_primary_code or ""))
        if db_primary_enterprise is not None:
            resolution_status = "db-primary"
            resolution_message = (
                "Business Settings page uses business_settings as primary source for DB-backed control-plane fields."
            )
            resolved = db_primary_enterprise
        else:
            resolution_status = "db-primary-enterprise-missing"
            resolution_message = (
                "business_settings row exists, but business_enterprise_code is missing in EnterpriseSettings. "
                "Page stays DB-first for control-plane fields and surfaces inconsistency instead of falling back silently."
            )
            resolved = None
    else:
        resolution_status = fallback_resolution_status
        resolution_message = f"business_settings row is missing; page uses fallback env/EnterpriseSettings read path. {fallback_resolution_message}"
        resolved = fallback_resolved

    return BusinessSettingsVM(
        resolution_status=resolution_status,
        resolution_message=resolution_message,
        resolved_enterprise_code=resolved.enterprise_code if resolved else None,
        resolved_enterprise_name=resolved.enterprise_name if resolved else None,
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

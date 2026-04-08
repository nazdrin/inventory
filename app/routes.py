from dotenv import load_dotenv
load_dotenv()
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Request, Security
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi.security import HTTPBearer
from typing import List
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
    SupplierSectionVM,
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
from app.models import DropshipEnterprise
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

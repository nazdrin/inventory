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
    MappingBranchSchema, LoginSchema, BranchMappingListItemVM, BranchMappingDetailVM
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

# List
@router.get("/dropship/enterprises/", dependencies=[Depends(verify_token)])
async def get_all_dropship_enterprises(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(DropshipEnterprise))
    items = result.scalars().all()
    return [DropshipEnterpriseSchema.model_validate(i, from_attributes=True) for i in items]

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

@router.get("/enterprise/settings/{enterprise_code}", dependencies=[Depends(verify_token)])
async def get_enterprise_by_code(enterprise_code: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code))
    enterprise = result.scalars().first()
    if not enterprise:
        raise HTTPException(status_code=404, detail="Enterprise not found.")
    return EnterpriseSettingsSchema.model_validate(enterprise, from_attributes=True)

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
    return _build_branch_mapping_detail_vm(dict(row), conflict_flags)


# 🔒 Mapping Branch
@router.post("/mapping_branch/", dependencies=[Depends(verify_token)])
async def create_mapping_branch(mapping_data: MappingBranchSchema, db: AsyncSession = Depends(get_db)):
    existing_entry = await db.execute(select(MappingBranch).filter(MappingBranch.branch == mapping_data.branch))
    if existing_entry.scalars().first():
        raise HTTPException(status_code=400, detail="Branch already exists.")

    new_entry = MappingBranch(**mapping_data.dict())
    db.add(new_entry)
    await db.commit()
    await db.refresh(new_entry)

    return {"detail": "Mapping branch created successfully", "data": new_entry}

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

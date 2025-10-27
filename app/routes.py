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
    MappingBranchSchema, LoginSchema
)
from app.database import (
    DeveloperSettings, EnterpriseSettings, DataFormat, MappingBranch, AsyncSessionLocal
)
from app.services.database_service import process_database_service
from app.services.notification_service import send_notification
from app.unipro_data_service.unipro_conv import unipro_convert
from app.auth import create_access_token, verify_token
import logging
import os
import json
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from fastapi import BackgroundTasks, Request, HTTPException,Body

# ——— Логгер "salesdrive" — пишем в ./logs/salesdrive_webhook.log и в консоль ———
LOG_DIR = os.getenv("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)

sd_logger = logging.getLogger("salesdrive")
sd_logger.setLevel(logging.INFO)

# ⏩ ОДИН РАЗ указываем prefix, а внутри маршрутов больше не дублируем developer_panel
router = APIRouter(prefix="/developer_panel", tags=["Developer Panel"])

security = HTTPBearer()

async def get_db():
    async with AsyncSessionLocal() as db:
        yield db

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
    return EnterpriseSettingsSchema.from_orm(enterprise)

@router.post("/enterprise/settings/", dependencies=[Depends(verify_token)])
async def create_enterprise(settings: EnterpriseSettingsSchema, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == settings.enterprise_code))
    if existing.scalars().first():
        raise HTTPException(status_code=400, detail="Enterprise with this code already exists.")

    new_enterprise = EnterpriseSettings(**settings.dict())
    db.add(new_enterprise)
    await db.commit()
    await db.refresh(new_enterprise)
    return new_enterprise

@router.put("/enterprise/settings/{enterprise_code}", dependencies=[Depends(verify_token)])
async def update_enterprise_settings(enterprise_code: str, updated_settings: EnterpriseSettingsSchema, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code))
    enterprise = result.scalars().first()
    if not enterprise:
        raise HTTPException(status_code=404, detail="Enterprise not found.")

    for key, value in updated_settings.dict(exclude_unset=True).items():
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
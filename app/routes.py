import os
import json
from fastapi import APIRouter, HTTPException, Depends, UploadFile, Request, Security
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from dotenv import load_dotenv
from typing import List
import tempfile
from datetime import timedelta
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app import crud, schemas, database
from app.schemas import EnterpriseSettingsSchema, DeveloperSettingsSchema, DataFormatSchema, MappingBranchSchema, LoginSchema
from app.database import DeveloperSettings, EnterpriseSettings, DataFormat, MappingBranch, AsyncSessionLocal
from app.services.database_service import process_database_service
from app.services.notification_service import send_notification
from app.unipro_data_service.unipro_conv import unipro_convert
from app.auth import create_access_token, verify_token

# Загружаем переменные окружения
load_dotenv()
router = APIRouter()

# Security для FastAPI
security = HTTPBearer()

# Dependency для получения сессии БД
async def get_db():
    async with AsyncSessionLocal() as db:
        yield db

# ----------------------------
# 🔐 Эндпоинт для авторизации (логин)
# ----------------------------

@router.post("/login/", summary="Login User", tags=["Auth"])
async def login_user(credentials: LoginSchema, db: AsyncSession = Depends(get_db)):
    """Авторизация пользователя и выдача JWT-токена"""
    result = await db.execute(select(DeveloperSettings).filter(
        DeveloperSettings.developer_login == credentials.developer_login,
        DeveloperSettings.developer_password == credentials.developer_password
    ))
    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid login or password.")

    # Генерация JWT-токена
    access_token = create_access_token(
        data={"sub": user.developer_login},
        expires_delta=timedelta(hours=1)
    )
    return {"access_token": access_token, "token_type": "bearer"}

# ----------------------------
# 🔒 Защищённые эндпоинты (JWT-токен обязателен)
# ----------------------------

@router.get("/developer/settings/{developer_login}", dependencies=[Security(verify_token)], tags=["Protected"])
async def get_developer_settings_by_login(developer_login: str, db: AsyncSession = Depends(get_db)):
    """Получить настройки конкретного разработчика по логину."""
    result = await db.execute(select(DeveloperSettings).filter(DeveloperSettings.developer_login == developer_login))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="Developer not found.")
    return user

@router.put("/developer/settings/{developer_login}", dependencies=[Security(verify_token)], tags=["Protected"])
async def update_developer_settings(developer_login: str, settings: DeveloperSettingsSchema, db: AsyncSession = Depends(get_db)):
    """Обновить настройки разработчика по логину."""
    result = await db.execute(select(DeveloperSettings).filter(DeveloperSettings.developer_login == developer_login))
    user = result.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="Developer not found.")
    for key, value in settings.dict().items():
        setattr(user, key, value)
    await db.commit()
    return user

@router.get("/enterprise/settings/", dependencies=[Security(verify_token)], tags=["Protected"])
async def get_all_enterprises(db: AsyncSession = Depends(get_db)):
    """Получить список всех предприятий."""
    result = await db.execute(select(EnterpriseSettings))
    enterprises = result.scalars().all()
    return enterprises if enterprises else []

# ----------------------------
# 🟢 Открытые эндпоинты (без авторизации)
# ----------------------------

@router.post("/developer_panel/unipro/data", tags=["Public"])
async def receive_unipro_data(request: Request, body: dict):
    """Эндпоинт для получения данных от Unipro через POST-запрос."""
    try:
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)
        file_type = "unipro_data"
        json_file_path = os.path.join(temp_dir, f"{file_type}.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(body, json_file, ensure_ascii=False, indent=4)

        await unipro_convert(json_file_path)
        return {"status": "success", "message": "Данные успешно получены и записаны в лог"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/catalog/", tags=["Public"])
async def upload_catalog(file: UploadFile, enterprise_code: str, db: AsyncSession = Depends(get_db)):
    """Загрузка каталога"""
    return {"message": "Catalog data processed successfully."}

@router.post("/stock/", tags=["Public"])
async def upload_stock(file: UploadFile, enterprise_code: str, db: AsyncSession = Depends(get_db)):
    """Загрузка остатков"""
    return {"message": "Stock data processed successfully."}

# ----------------------------
# 🔒 Защищённые эндпоинты для предприятий
# ----------------------------

@router.get("/enterprise/settings/{enterprise_code}", dependencies=[Security(verify_token)], tags=["Protected"])
async def get_enterprise_by_code(enterprise_code: str, db: AsyncSession = Depends(get_db)):
    """Получить настройки конкретного предприятия."""
    result = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code))
    enterprise = result.scalars().first()
    if not enterprise:
        raise HTTPException(status_code=404, detail="Enterprise not found.")
    return EnterpriseSettingsSchema.from_orm(enterprise)

@router.post("/enterprise/settings/", dependencies=[Security(verify_token)], tags=["Protected"])
async def create_enterprise(settings: EnterpriseSettingsSchema, db: AsyncSession = Depends(get_db)):
    """Добавить новое предприятие."""
    existing = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == settings.enterprise_code))
    existing_enterprise = existing.scalars().first()
    if existing_enterprise:
        raise HTTPException(status_code=400, detail="Enterprise with this code already exists.")
    
    new_enterprise = EnterpriseSettings(**settings.dict())
    db.add(new_enterprise)
    await db.commit()
    await db.refresh(new_enterprise)
    return new_enterprise

@router.put("/enterprise/settings/{enterprise_code}", dependencies=[Security(verify_token)], tags=["Protected"])
async def update_enterprise_settings(
    enterprise_code: str,
    updated_settings: EnterpriseSettingsSchema,
    db: AsyncSession = Depends(get_db),
):
    """Обновить настройки предприятия по коду."""
    result = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code))
    enterprise = result.scalars().first()
    if not enterprise:
        raise HTTPException(status_code=404, detail="Enterprise not found.")

    for key, value in updated_settings.dict(exclude_unset=True).items():
        setattr(enterprise, key, value)

    await db.commit()
    await db.refresh(enterprise)
    return {"detail": "Enterprise settings updated successfully", "data": enterprise}

# ----------------------------
# 🔒 Защищённые эндпоинты для data_formats
# ----------------------------

@router.post("/data_formats/", dependencies=[Security(verify_token)], tags=["Protected"])
async def add_data_format(data_format: schemas.DataFormatSchema, db: AsyncSession = Depends(get_db)):
    """Добавить новый формат данных."""
    existing_format = await db.execute(select(DataFormat).filter(DataFormat.format_name == data_format.format_name))
    existing = existing_format.scalars().first()
    if existing:
        raise HTTPException(status_code=400, detail="Data format already exists.")
    new_format = DataFormat(format_name=data_format.format_name)
    db.add(new_format)
    await db.commit()
    await db.refresh(new_format)
    return {"detail": "Data format added successfully", "data": new_format}

@router.get("/data_formats/", dependencies=[Depends(verify_token)])
async def get_data_formats(db: AsyncSession = Depends(get_db)):
    """Получить список всех форматов данных (требуется аутентификация)."""
    result = await db.execute(select(DataFormat))
    formats = result.scalars().all()
    return formats

@router.post("/mapping_branch/", dependencies=[Security(verify_token)], tags=["Protected"])
async def create_mapping_branch(
    mapping_data: MappingBranchSchema, 
    db: AsyncSession = Depends(get_db)
):
    """Создание новой записи MappingBranch с авторизацией"""

    # Проверяем, существует ли запись с таким branch
    existing_entry = await db.execute(
        select(MappingBranch).filter(MappingBranch.branch == mapping_data.branch)
    )
    if existing_entry.scalars().first():
        raise HTTPException(status_code=400, detail="Branch already exists.")

    # Создаем новую запись
    new_entry = MappingBranch(**mapping_data.model_dump())
    db.add(new_entry)
    await db.commit()
    await db.refresh(new_entry)

    return {
        "detail": "Mapping branch created successfully",
        "data": new_entry
    }
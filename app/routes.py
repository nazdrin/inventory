from fastapi import APIRouter, HTTPException, Depends,UploadFile, Request
from sqlalchemy.ext.asyncio import AsyncSession
from app import crud, schemas, database
from app.schemas import EnterpriseSettingsSchema
from app.schemas import DeveloperSettingsSchema
from app.schemas import DataFormatSchema
from fastapi.encoders import jsonable_encoder
from app.database import DeveloperSettings, EnterpriseSettings, DataFormat
from sqlalchemy.future import select
from app.services.database_service import process_database_service
from fastapi import APIRouter, HTTPException, Request
from app.services.notification_service import send_notification
import json
import os
from app.unipro_data_service.unipro_conv import unipro_convert
import tempfile
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()
router = APIRouter()

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.database import MappingBranch
from app.schemas import MappingBranchSchema
from app.database import AsyncSessionLocal

router = APIRouter()

# Dependency для получения сессии БД
async def get_db():
    async with AsyncSessionLocal() as db:
        yield db

@router.post("/mapping_branch/")
async def create_mapping_branch(
    mapping_data: MappingBranchSchema, 
    db: AsyncSession = Depends(get_db)
):
    """
    Эндпоинт для создания записи в таблице mapping_branch.
    """
    # Проверяем, существует ли запись с таким branch
    existing_entry = await db.execute(select(MappingBranch).filter(MappingBranch.branch == mapping_data.branch))
    if existing_entry.scalars().first():
        raise HTTPException(status_code=400, detail="Branch already exists.")

    # Создаем новую запись
    new_entry = MappingBranch(**mapping_data.dict())
    db.add(new_entry)
    await db.commit()
    await db.refresh(new_entry)

    return {"detail": "Mapping branch created successfully", "data": new_entry}


@router.post("/developer_panel/unipro/data")
async def receive_unipro_data(request: Request, body: dict):
    """
    Эндпоинт для получения данных от Unipro через POST-запрос.
    """
    try:
        # Определяем временную директорию
        temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
        os.makedirs(temp_dir, exist_ok=True)  # Создаём папку, если её нет
        file_type = "unipro_data"
        json_file_path = os.path.join(temp_dir, f"{file_type}.json")

        # Записываем входные данные в JSON в указанную временную папку
        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(body, json_file, ensure_ascii=False, indent=4)

        await unipro_convert(json_file_path)
        return {"status": "success", "message": "Данные успешно получены и записаны в лог"}
    except Exception as e:
        print(f"❌ Ошибка обработки данных: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Dependency для работы с базой данных
async def get_db():
    """Создание сессии для работы с базой данных."""
    async with database.AsyncSessionLocal() as db:
        yield db

# ----------------------------
# Эндпоинт для авторизации
# ----------------------------

@router.post("/login/")
async def login_user(credentials: schemas.LoginSchema, db: AsyncSession = Depends(get_db)):
    print("Received credentials:", credentials.dict())  # <-- Логирование

    # Асинхронный запрос с использованием `select`
    result = await db.execute(select(DeveloperSettings).filter(
        DeveloperSettings.developer_login == credentials.developer_login,
        DeveloperSettings.developer_password == credentials.developer_password,
    ))

    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=401, detail="Invalid login or password.")

    return {"developer_login": user.developer_login, "token": "dummy_token"}

# ----------------------------
# Эндпоинты для управления настройками разработчиков
# ----------------------------
@router.get("/developer/settings/")
async def get_all_developer_settings(db: AsyncSession = Depends(get_db)):
    """Получить глобальные настройки всех разработчиков."""
    result = await db.execute(select(DeveloperSettings))
    developers = result.scalars().all()
    return [{"developer_login": dev.developer_login, "error_email_developer": dev.error_email_developer} for dev in developers]

@router.get("/developer/settings/{developer_login}")
async def get_developer_settings_by_login(developer_login: str, db: AsyncSession = Depends(get_db)):
    """Получить настройки конкретного разработчика по логину."""
    result = await db.execute(select(DeveloperSettings).filter(DeveloperSettings.developer_login == developer_login))
    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=404, detail="Developer not found.")
    return user

@router.put("/developer/settings/{developer_login}")
async def update_developer_settings(
    developer_login: str,
    settings: DeveloperSettingsSchema,
    db: AsyncSession = Depends(get_db),
):
    """Обновить настройки разработчика по логину."""
    result = await db.execute(select(DeveloperSettings).filter(DeveloperSettings.developer_login == developer_login))
    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=404, detail="Developer not found.")

    for key, value in settings.dict().items():
        setattr(user, key, value)
    
    # Коммит изменений
    await db.commit()
    return user

# ----------------------------
# Эндпоинты для управления настройками предприятий
# ----------------------------
@router.get("/enterprise/settings/")
async def get_all_enterprises(db: AsyncSession = Depends(get_db)):
    """
    Получить список всех предприятий.
    """
    result = await db.execute(select(EnterpriseSettings))
    enterprises = result.scalars().all()
    
    if not enterprises:
        return []  # Вернём пустой список, если записей нет
    return enterprises

@router.get("/enterprise/settings/{enterprise_code}")
async def get_enterprise_by_code(enterprise_code: str, db: AsyncSession = Depends(get_db)):
    """
    Получить настройки конкретного предприятия.
    """
    result = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == enterprise_code))
    enterprise = result.scalars().first()
    
    if not enterprise:
        raise HTTPException(status_code=404, detail="Enterprise not found.")
    
    return EnterpriseSettingsSchema.from_orm(enterprise)

@router.post("/enterprise/settings/")
async def create_enterprise(settings: EnterpriseSettingsSchema, db: AsyncSession = Depends(get_db)):
    """
    Добавить новое предприятие.
    """
    existing = await db.execute(select(EnterpriseSettings).filter(EnterpriseSettings.enterprise_code == settings.enterprise_code))
    existing_enterprise = existing.scalars().first()

    if existing_enterprise:
        raise HTTPException(status_code=400, detail="Enterprise with this code already exists.")
    
    new_enterprise = EnterpriseSettings(**settings.dict())
    db.add(new_enterprise)
    await db.commit()
    await db.refresh(new_enterprise)
    
    return new_enterprise

@router.put("/enterprise/settings/{enterprise_code}")
async def update_enterprise_settings(
    enterprise_code: str,
    updated_settings: EnterpriseSettingsSchema,
    db: AsyncSession = Depends(get_db),
):
    """
    Обновить настройки предприятия по коду.
    """
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
# Эндпоинты для управления data_formats
# ----------------------------

@router.post("/data_formats/")
async def add_data_format(data_format: schemas.DataFormatSchema, db: AsyncSession = Depends(get_db)):
    """
    Добавить новый формат данных.
    """
    existing_format = await db.execute(select(DataFormat).filter(DataFormat.format_name == data_format.format_name))
    existing = existing_format.scalars().first()
    
    if existing:
        raise HTTPException(status_code=400, detail="Data format already exists.")
    
    new_format = DataFormat(format_name=data_format.format_name)
    db.add(new_format)
    await db.commit()
    await db.refresh(new_format)
    return {"detail": "Data format added successfully", "data": new_format}

@router.get("/data_formats/")
async def get_data_formats(db: AsyncSession = Depends(get_db)):
    """
    Получить список всех форматов данных.
    """
    result = await db.execute(select(DataFormat))
    formats = result.scalars().all()
    return formats

@router.delete("/data_formats/{format_id}")
async def delete_data_format(format_id: int, db: AsyncSession = Depends(get_db)):
    """
    Удалить формат данных по ID.
    """
    result = await db.execute(select(DataFormat).filter(DataFormat.id == format_id))
    format_to_delete = result.scalars().first()
    
    if not format_to_delete:
        raise HTTPException(status_code=404, detail="Data format not found.")
    
    await db.delete(format_to_delete)
    await db.commit()
    return {"detail": "Data format deleted successfully"}
@router.post("/catalog/")
async def upload_catalog(file: UploadFile, enterprise_code: str, db: AsyncSession = Depends(get_db)):
    try:
        file_content = await file.read()
        file_path = f"/tmp/{file.filename}"
        
        with open(file_path, "wb") as f:
            f.write(file_content)

        # Вызов функции обработки данных
        await process_database_service(file_path, "catalog", enterprise_code)
        
        return {"message": "Catalog data processed successfully."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stock/")
async def upload_stock(file: UploadFile, enterprise_code: str, db: AsyncSession = Depends(get_db)):
    try:
        file_content = await file.read()
        file_path = f"/tmp/{file.filename}"

        with open(file_path, "wb") as f:
            f.write(file_content)

        # Вызов функции обработки данных
        await process_database_service(file_path, "stock", enterprise_code)
        
        return {"message": "Stock data processed successfully."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



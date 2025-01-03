from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import Base, engine, AsyncSessionLocal, DeveloperSettings
from app.schemas import LoginSchema
from app.developer_panel.routes import router as developer_router

#from app.tabletki_data_service.app.routers.data_upload import router as data_upload_router
#from app.tabletki_data_service.app.routers.validation import router as validation_router
#from app.tabletki_data_service.app.routers.transformation import router as transformation_router
#from app.tabletki_data_service.app.routers.notification import router as notification_router

# Инициализация FastAPI приложения
app = FastAPI()

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешает запросы со всех источников
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Асинхронная инициализация таблиц
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        # Создаем все таблицы при старте приложения
        await conn.run_sync(Base.metadata.create_all)

# Dependency для работы с базой данных
def get_db():
    db = AsyncSessionLocal()
    try:
        yield db
    finally:
        db.close()

# Подключение маршрутов
app.include_router(developer_router, prefix="/developer_panel", tags=["Developer Panel"])

#app.include_router(data_upload_router, prefix="/tabletki_data_service/data_upload", tags=["Data Upload"])
#app.include_router(validation_router, prefix="/tabletki_data_service/validation", tags=["Validation"])
#app.include_router(transformation_router, prefix="/tabletki_data_service/transformation", tags=["Transformation"])
#app.include_router(notification_router, prefix="/tabletki_data_service/notification", tags=["Notification"])

# Приветственный эндпоинт
@app.get("/")
def root():
    return {"message": "Welcome to Inventory Service"}

# Авторизация
@app.post("/developer_panel/login/")
def login_user(credentials: LoginSchema, db: AsyncSession = Depends(get_db)):
    user = db.query(DeveloperSettings).filter(
        DeveloperSettings.developer_login == credentials.developer_login,
        DeveloperSettings.developer_password == credentials.developer_password,
    ).first()
    if not user:
        raise HTTPException(status_code=401, detail="Invalid login or password.")
    return {
        "developer_login": user.developer_login,
        "error_email_developer": user.error_email_developer,
        "token": "dummy_token",  # Временное значение, заменить при реализации авторизации
    }
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import Base, engine, AsyncSessionLocal, DeveloperSettings, create_tables
from app.schemas import LoginSchema
from app.routes import router as developer_router

# Инициализация FastAPI приложения
app = FastAPI()
app.include_router(developer_router)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешает запросы со всех источников
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    await create_tables()

# Подключение маршрутов
app.include_router(developer_router, prefix="/developer_panel", tags=["Developer Panel"])

# Приветственный эндпоинт
@app.get("/")
def root():
    return {"message": "Welcome to Inventory Service"}

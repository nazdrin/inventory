from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from .models import Base, DeveloperSettings, InventoryData, InventoryStock, ReservedItems, DataFormat, EnterpriseSettings, ClientNotifications  # Импортируем все модели
from contextlib import asynccontextmanager
from .config import config  # Импортируем конфигурацию
import logging


# Указываем строку подключения к базе данных
#DATABASE_URL = "postgresql+asyncpg://postgres:your_password@localhost/inventory_db"

# Создаем асинхронный движок для подключения к базе данных
engine = create_async_engine(config.DATABASE_URL, echo=True)

# Создаем SessionLocal для работы с асинхронными сессиями
AsyncSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Функция для создания таблиц
async def create_tables():
    # Используем асинхронную сессию для создания таблиц
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Асинхронная функция для получения сессии базы данных
@asynccontextmanager
async def get_async_db():
    async_session = AsyncSessionLocal()  # Создаем сессию
    try:
        logging.info("Попытка создания сессии базы данных")
        yield async_session
    except Exception as e:
        logging.error(f"Ошибка при работе с базой данных: {e}")
    finally:
        await async_session.close()  # Закрываем сессию явно
        logging.info("Сессия базы данных закрыта")
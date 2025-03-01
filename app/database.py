import os

# from sqlalchemy import create_engine

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from .models import Base, DeveloperSettings, InventoryData, InventoryStock, ReservedItems, DataFormat, EnterpriseSettings, ClientNotifications, MappingBranch
from contextlib import asynccontextmanager
import logging

# Читаем DATABASE_URL из переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL")

# Проверяем, что переменная окружения установлена
if not DATABASE_URL:
    raise ValueError("Переменная окружения DATABASE_URL не установлена")

# Создаем асинхронный движок для подключения к базе данных
# engine = create_async_engine(DATABASE_URL, echo=True)
engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,  # Устанавливаем размер пула соединений
    max_overflow=5,  # Сколько дополнительных соединений можно открыть
    pool_recycle=600,  # Обновление соединений каждые 10 минут
    pool_pre_ping=True  # Проверка соединения перед запросом
)
# Создаем SessionLocal для работы с асинхронными сессиями
AsyncSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Функция для создания таблиц
async def create_tables():
    # Используем асинхронную сессию для создания таблиц
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# !# Асинхронная функция для получения сессии базы данных
# @asynccontextmanager
# async def get_async_db():
#     async_session = AsyncSessionLocal()  # Создаем сессию
#     try:
#         logging.info("Попытка создания сессии базы данных")
#         yield async_session
#     except Exception as e:
#         logging.error(f"Ошибка при работе с базой данных: {e}")
#     finally:
#         await async_session.close()  # Закрываем сессию явно
#         logging.info("Сессия базы данных закрыта")

@asynccontextmanager
async def get_async_db():
    async_session = AsyncSessionLocal()
    try:
        logging.info("Попытка создания сессии базы данных")
        yield async_session  # Передаем сессию в использование
        await async_session.commit()  # Фиксируем транзакции после работы
    except Exception as e:
        await async_session.rollback()  # Откатываем транзакцию при ошибке
        logging.error(f"Ошибка при работе с базой данных: {e}")
        raise  # Пробрасываем ошибку дальше
    finally:
        await async_session.close()  # Закрываем сессию явно
        logging.info("Сессия базы данных закрыта")
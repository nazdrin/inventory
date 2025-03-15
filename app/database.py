import os

# from sqlalchemy import create_engine

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy.sql import text

from .models import Base, DeveloperSettings, InventoryData, InventoryStock, ReservedItems, DataFormat, EnterpriseSettings, ClientNotifications, MappingBranch
from contextlib import asynccontextmanager
import logging

# Читаем DATABASE_URL из переменных окружения
DATABASE_URL = os.getenv("DATABASE_URL")

# Проверяем, что переменная окружения установлена
if not DATABASE_URL:
    raise ValueError("Переменная окружения DATABASE_URL не установлена")

# Создаем асинхронный движок для подключения к базе данных


engine = create_async_engine(
    DATABASE_URL,
    pool_size=5,  # Уменьшаем количество одновременных соединений
    max_overflow=2,  # Дополнительные соединения при нагрузке
    pool_recycle=300,  # Закрываем соединение каждые 5 минут
    pool_timeout=30,  # Ожидание свободного соединения – 30 сек
    pool_pre_ping=True  # Проверяем соединение перед использованием
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


@asynccontextmanager
async def get_async_db():
    """Контекстный менеджер для управления асинхронной сессией."""
    async_session = AsyncSessionLocal()
    try:
        logging.info("📡 Создание сессии базы данных")

        # Проверяем соединение перед работой
        try:
            await async_session.execute(text("SELECT 1"))
        except Exception:
            logging.warning("🔴 Соединение с БД потеряно, пересоздаём сессию...")
            await async_session.rollback()
            yield async_session
            return

        yield async_session  # Позволяет использовать `async with get_async_db() as db:`
        await async_session.commit()
    except Exception as e:
        await async_session.rollback()  # Откат зависших транзакций
        logging.error(f"🔥 Ошибка в сессии БД: {e}")
        raise
    finally:
        await async_session.close()
        logging.info("✅ Сессия базы данных закрыта")
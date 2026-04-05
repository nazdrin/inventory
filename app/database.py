from dotenv import load_dotenv
load_dotenv()
import os
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from .models import Base, DeveloperSettings, InventoryData, InventoryStock, ReservedItems, DataFormat, EnterpriseSettings, ClientNotifications, MappingBranch, CatalogMapping
from contextlib import asynccontextmanager
import logging
DATABASE_URL = os.getenv("DATABASE_URL")
logger = logging.getLogger(__name__)

# Проверяем, что переменная окружения установлена
if not DATABASE_URL:
    raise ValueError("Переменная окружения DATABASE_URL не установлена")



# Создаем асинхронный движок для подключения к базе данных
engine = create_async_engine(
    DATABASE_URL,
    pool_size=5,  # Уменьшаем количество одновременных соединений
    max_overflow=2,  # Дополнительные соединения при нагрузке
    pool_recycle=1200,  # Закрываем соединение каждые 5 минут
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
async def get_async_db(*, commit_on_exit: bool = True):
    """Контекстный менеджер для управления асинхронной сессией.

    По умолчанию сохраняет историческое поведение с commit на выходе.
    Для read-only/долгих runtime-сценариев можно передать commit_on_exit=False,
    чтобы не держать лишнюю commit boundary на завершении контекста.
    """
    async_session = AsyncSessionLocal()
    try:
        # Проверяем соединение перед работой
        try:
            await async_session.execute(text("SELECT 1"))
        except Exception:
            logger.warning("🔴 Соединение с БД потеряно при открытии сессии, выполняется rollback")
            await async_session.rollback()
            yield async_session
            return

        yield async_session  # Позволяет использовать `async with get_async_db() as db:`
        if commit_on_exit:
            logger.debug("DB session commit on exit")
            await async_session.commit()
    except Exception as e:
        try:
            await async_session.rollback()  # Откат зависших транзакций
        except Exception as rollback_error:
            logger.error("🔥 Ошибка rollback в сессии БД: %s", rollback_error)
        logger.error(f"🔥 Ошибка в сессии БД: {e}")
        raise
    finally:
        await async_session.close()

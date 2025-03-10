import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from app.models import Base  


from dotenv import load_dotenv

# print("Current sys.path:")
# print("\n".join(sys.path))  # Выведет пути, откуда Python ищет модули

# Загружаем переменные окружения из .env
load_dotenv()

# Конфигурация Alembic
config = context.config

# Если в alembic.ini не указан sqlalchemy.url, берем его из переменной окружения
db_url = os.getenv("DATABASE_URL").replace("asyncpg", "psycopg2")
config.set_main_option("sqlalchemy.url", db_url)

# Настройка логирования
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Укажите метаданные ваших моделей для автогенерации миграций
target_metadata = Base.metadata  # Используем метаданные моделей

def run_migrations_offline() -> None:
    """Запуск миграций в оффлайн-режиме."""
    context.configure(
        url=db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Запуск миграций в онлайн-режиме."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
import os
from dotenv import load_dotenv

# Загрузка .env файла
load_dotenv()

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"

class DevelopmentConfig(Config):
    ENV = "development"

class TestingConfig(Config):
    ENV = "testing"

class ProductionConfig(Config):
    ENV = "production"
# Словарь конфигураций
CONFIGS = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "production": ProductionConfig,
}

# Получаем текущее окружение
ENV = os.getenv("FLASK_ENV", "development")
config = CONFIGS.get(ENV, DevelopmentConfig)

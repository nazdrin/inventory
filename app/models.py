from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime, ForeignKey, ARRAY
from sqlalchemy.orm import declarative_mixin, declarative_base
from datetime import datetime
from zoneinfo import ZoneInfo

Base = declarative_base()

# Функция для получения текущего времени с учётом временной зоны
def now_with_timezone():
    return datetime.now(ZoneInfo("Europe/Kiev"))

# Mixin для временных меток
@declarative_mixin
class TimestampMixin:
    updated_at = Column(
        DateTime(timezone=True),
        default=now_with_timezone,
        onupdate=now_with_timezone,
)
# Таблица номенклатуры
class InventoryData(Base, TimestampMixin):
    __tablename__ = "inventory_data"

    code = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    producer = Column(String, nullable=False)
    vat = Column(Float, nullable=False)
    morion = Column(String, nullable=True)
    tabletki = Column(String, nullable=True)
    barcode = Column(String, nullable=True)
    optima = Column(String, nullable=True)
    badm = Column(String, nullable=True)
    venta = Column(String, nullable=True)
    enterprise_code = Column(String, ForeignKey("enterprise_settings.enterprise_code"), nullable=False)

# Таблица остатков
class InventoryStock(Base, TimestampMixin):
    __tablename__ = "inventory_stock"

    branch = Column(String, primary_key=True)
    code = Column(String, primary_key=True)
    price = Column(Float, nullable=False)
    qty = Column(Integer, nullable=False)
    price_reserve = Column(Float, nullable=True)
    enterprise_code = Column(String, ForeignKey("enterprise_settings.enterprise_code"), nullable=False)

# Таблица забронированных товаров
class ReservedItems(Base):
    __tablename__ = "reserved_items"
    branch = Column(String, primary_key=True)
    code = Column(String, primary_key=True)
    qty = Column(Integer, nullable=False)

# Таблица форматов данных
class DataFormat(Base):
    __tablename__ = "data_formats"
    id = Column(Integer, primary_key=True, index=True)
    format_name = Column(String, unique=True, nullable=False)

# Таблица настроек предприятий
class EnterpriseSettings(Base):
    __tablename__ = "enterprise_settings"
    enterprise_code = Column(String, primary_key=True)
    enterprise_name = Column(String, nullable=False)
    branch_id = Column(String, nullable=False)
    tabletki_login = Column(String, nullable=True)
    tabletki_password = Column(String, nullable=True)
    token = Column(String, nullable=True)
    stock_upload_frequency = Column(Integer, nullable=True)
    catalog_upload_frequency = Column(Integer, nullable=True)
    google_drive_folder_id_ref = Column(String, nullable=True)
    google_drive_folder_id_rest = Column(String, nullable=True)
    data_format = Column(String, nullable=True)
    discount_rate = Column(Float, nullable=True)
    single_store = Column(Boolean, nullable=True)
    order_fetcher = Column(Boolean, nullable=True)
    auto_confirm = Column(Boolean, nullable=True)
    store_serial = Column(String, nullable=True)
    last_stock_upload = Column(DateTime, nullable=True)
    last_catalog_upload = Column(DateTime, nullable=True)
    stock_correction = Column(Boolean, default=False)

# Сопоставление аптек 
class MappingBranch(Base):
    __tablename__ = "mapping_branch"
    enterprise_code = Column(String, nullable=False)
    branch = Column(String, primary_key=True)
    store_id = Column(String, nullable=False)
    id_telegram = Column(ARRAY(String), nullable=True)

# Таблица глобальных настроек разработчика
class DeveloperSettings(Base):
    __tablename__ = "developer_settings"
    developer_login = Column(String, primary_key=True)
    developer_password = Column(String, nullable=False)
    endpoint_catalog = Column(String, nullable=True)
    endpoint_stock = Column(String, nullable=True)
    endpoint_orders = Column(String, nullable=True)
    telegram_token_developer = Column(String, nullable=True)
    message_orders = Column(Boolean, nullable=True)
    morion = Column(String, nullable=True)
    tabletki = Column(String, nullable=True)
    barcode = Column(String, nullable=True)
    optima = Column(String, nullable=True)
    badm = Column(String, nullable=True)
    venta = Column(String, nullable=True)

# Таблица уведомлений
class ClientNotifications(Base, TimestampMixin):
    __tablename__ = "client_notifications"
    id = Column(Integer, primary_key=True, autoincrement=True)
    enterprise_code = Column(String, nullable=False)
    message = Column(String, nullable=False)
    is_read = Column(Boolean, default=False)
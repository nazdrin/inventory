from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    CheckConstraint,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import declarative_base, declarative_mixin

Base = declarative_base()

# Функция для получения текущего времени с учётом временной зоны
def now_with_timezone():
    return datetime.now(ZoneInfo("Europe/Kiev"))

# Mixin для временных меток
@declarative_mixin
class TimestampMixin:
    # Значение по умолчанию задаём на стороне БД (server_default=func.now()),
    # чтобы вставки через SQL или миграции не падали. Разрешаем NULL,
    # чтобы не конфликтовать при добавлении колонок в таблицы с данными.
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),   # ← дефолт в БД (NOW() в Postgres)
        nullable=True,               # ← допускаем NULL для совместимости миграций
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),   # ← первичное значение в БД
        onupdate=func.now(),         # ← при UPDATE проставит ORM (когда обновляешь через SQLAlchemy)
        nullable=True,               # ← то же самое по причинам выше
    )
# Таблица номенклатуры
class InventoryData(Base, TimestampMixin):
    __tablename__ = "inventory_data"

    code = Column(String, nullable=False)
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

    __table_args__ = (
        PrimaryKeyConstraint("code", "enterprise_code"),
    )


# Таблица остатков
class InventoryStock(Base, TimestampMixin):
    __tablename__ = "inventory_stock"

    branch = Column(String, primary_key=True)
    code = Column(String, primary_key=True)
    price = Column(Numeric(12, 2), nullable=False)
    qty = Column(Integer, nullable=False)
    price_reserve = Column(Numeric(12, 2), nullable=True)
    enterprise_code = Column(String, ForeignKey("enterprise_settings.enterprise_code"), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "(price_reserve IS NULL) OR (price_reserve <= price)",
            name="ck_inventory_stock_price_reserve_le_price"
        ),
        Index("ix_inventory_stock_ent_code", "enterprise_code"),
        Index("ix_inventory_stock_ent_code_item", "enterprise_code", "code"),
        Index("ix_inventory_stock_available", "enterprise_code",
              postgresql_where=text("qty > 0")),
    )

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
class EnterpriseSettings(Base, TimestampMixin):
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
    single_store = Column(Boolean, nullable=False, server_default=text("false"))
    order_fetcher = Column(Boolean, nullable=False, server_default=text("false"))
    auto_confirm = Column(Boolean, nullable=False, server_default=text("false"))
    store_serial = Column(String, nullable=True)
    last_stock_upload = Column(DateTime, nullable=True)
    last_catalog_upload = Column(DateTime, nullable=True)
    stock_correction = Column(Boolean, nullable=False, server_default=text("false"))

    __table_args__ = (
        Index("ix_enterprise_settings_data_format", "data_format"),
        Index("ix_enterprise_settings_single_store", "single_store"),
    )

# Сопоставление аптек 
class MappingBranch(Base):
    __tablename__ = "mapping_branch"
    enterprise_code = Column(String, nullable=False)
    branch = Column(String, primary_key=True)
    store_id = Column(String, nullable=False)
    google_folder_id = Column(String, nullable=True)
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



class CatalogMapping(Base):
    __tablename__ = "catalog_mapping"
    ID = Column(String, primary_key=True)
    Name = Column(String, nullable=True)
    Producer = Column(String, nullable=True)
    Barcode = Column(String, nullable=True)
    Guid = Column(String, nullable=True)
    Code_Tabletki = Column(String, nullable=True)
    Name_D1  = Column(String, nullable=True, server_default=text("''"))
    Name_D2  = Column(String, nullable=True, server_default=text("''"))
    Name_D3  = Column(String, nullable=True, server_default=text("''"))
    Name_D4  = Column(String, nullable=True, server_default=text("''"))
    Name_D5  = Column(String, nullable=True, server_default=text("''"))
    Name_D6  = Column(String, nullable=True, server_default=text("''"))
    Name_D7  = Column(String, nullable=True, server_default=text("''"))
    Name_D8  = Column(String, nullable=True, server_default=text("''"))
    Name_D9  = Column(String, nullable=True, server_default=text("''"))
    Name_D10 = Column(String, nullable=True, server_default=text("''"))
    Code_D1  = Column(String, nullable=True, server_default=text("''"))
    Code_D2  = Column(String, nullable=True, server_default=text("''"))
    Code_D3  = Column(String, nullable=True, server_default=text("''"))
    Code_D4  = Column(String, nullable=True, server_default=text("''"))
    Code_D5  = Column(String, nullable=True, server_default=text("''"))
    Code_D6  = Column(String, nullable=True, server_default=text("''"))
    Code_D7  = Column(String, nullable=True, server_default=text("''"))
    Code_D8  = Column(String, nullable=True, server_default=text("''"))
    Code_D9  = Column(String, nullable=True, server_default=text("''"))
    Code_D10 = Column(String, nullable=True, server_default=text("''"))

    Name_D11 = Column(String, nullable=True, server_default=text("''"))
    Name_D12 = Column(String, nullable=True, server_default=text("''"))
    Name_D13 = Column(String, nullable=True, server_default=text("''"))
    Name_D14 = Column(String, nullable=True, server_default=text("''"))
    Name_D15 = Column(String, nullable=True, server_default=text("''"))
    Name_D16 = Column(String, nullable=True, server_default=text("''"))
    Name_D17 = Column(String, nullable=True, server_default=text("''"))
    Name_D18 = Column(String, nullable=True, server_default=text("''"))
    Name_D19 = Column(String, nullable=True, server_default=text("''"))
    Name_D20 = Column(String, nullable=True, server_default=text("''"))

    Code_D11 = Column(String, nullable=True, server_default=text("''"))
    Code_D12 = Column(String, nullable=True, server_default=text("''"))
    Code_D13 = Column(String, nullable=True, server_default=text("''"))
    Code_D14 = Column(String, nullable=True, server_default=text("''"))
    Code_D15 = Column(String, nullable=True, server_default=text("''"))
    Code_D16 = Column(String, nullable=True, server_default=text("''"))
    Code_D17 = Column(String, nullable=True, server_default=text("''"))
    Code_D18 = Column(String, nullable=True, server_default=text("''"))
    Code_D19 = Column(String, nullable=True, server_default=text("''"))
    Code_D20 = Column(String, nullable=True, server_default=text("''"))

class DropshipEnterprise(Base, TimestampMixin):
    __tablename__ = "dropship_enterprises"

    code = Column(String, primary_key=True, index=True, doc="Уникальный код предприятия")
    name = Column(String, nullable=False, doc="Название предприятия")

    # Эти поля могут отсутствовать, поэтому разрешаем NULL
    feed_url = Column(String, nullable=True, doc="Ссылка на прайс-фид")
    gdrive_folder = Column(String, nullable=True, doc="Папка на Google Drive")
    city = Column(String, nullable=True, doc="Город")

    # Флаги — NOT NULL + дефолт на уровне БД (устойчиво к «пустым» вставкам)
    is_rrp = Column(Boolean, nullable=False, server_default=text("false"), doc="Флаг — есть ли РРЦ")
    is_wholesale = Column(Boolean, nullable=False, server_default=text("true"), doc="Флаг — опт или розница")
    is_active = Column(Boolean, nullable=False, server_default=text("true"), doc="Флаг активности")
    api_orders_enabled = Column(Boolean, nullable=False, server_default=text("false"), doc="Флаг — заказы через API")
    weekend_work = Column(Boolean, nullable=False, server_default=text("false"), doc="Флаг — работает в выходные")
    use_feed_instead_of_gdrive = Column(Boolean, nullable=False, server_default=text("true"),
                                        doc="Флаг — использовать ФИД (если False — Google Drive)")

    # Числа — пусть будут NULL, если не заданы
    profit_percent = Column(Float, nullable=True, doc="Процент заработка")
    retail_markup = Column(Float, nullable=True, doc="Наценка для розницы")
    min_markup_threshold = Column(Float, nullable=True, doc="Минимальный порог наценки")

    # Приоритет — NOT NULL с дефолтом и ограничением диапазона
    priority = Column(Integer, nullable=False, server_default=text("5"), doc="Приоритет (1–10)")

    __table_args__ = (
        Index("ix_dropship_enterprises_is_active", "is_active"),
        Index("ix_dropship_enterprises_priority", "priority"),
        CheckConstraint("priority BETWEEN 1 AND 10", name="ck_dropship_enterprises_priority_range"),
    )
class CompetitorPrice(Base, TimestampMixin):
    __tablename__ = "competitor_prices"

    # Составной первичный ключ
    code = Column(String, primary_key=True, index=True, doc="Код товара")
    city = Column(String, primary_key=True, doc="Город")
    competitor_price = Column(Numeric(12, 2), nullable=False, doc="Цена конкурента")

    __table_args__ = (
        CheckConstraint("competitor_price >= 0", name="ck_competitor_prices_price_nonneg"),
        Index("ix_competitor_prices_city", "city"),
        Index("ix_competitor_prices_code", "code"),
    )
class Offer(Base):
    __tablename__ = "offers"

    id = Column(BigInteger, primary_key=True)  # службовий PK
    product_code  = Column(String, nullable=False, index=True, doc="Загальний код товару")
    supplier_code = Column(String, nullable=False, index=True, doc="Код/назва постачальника")
    city          = Column(String, nullable=False, index=True, doc="Місто")
    price         = Column(Numeric(12, 2), nullable=False, doc="Ціна у валюті currency")

    # Оптовая цена (опционально для совместимости)
    wholesale_price = Column(
        Numeric(12, 2),
        nullable=True,
        doc="Оптовая цена"
    )

    stock         = Column(Integer, nullable=False, default=0, doc="Доступний залишок ≥0")
    updated_at    = Column(DateTime(timezone=True), nullable=False,
                           server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("product_code", "supplier_code", "city",
                         name="uq_offers_product_supplier_city"),
        Index("ix_offers_city_product_price", "city", "product_code", "price"),
        Index("ix_offers_city_stock_pos", "city",
              postgresql_where=text("stock > 0")),
    )
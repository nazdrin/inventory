from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


# Схема для авторизации пользователя
class LoginSchema(BaseModel):
    developer_login: str
    developer_password: str


# Схема для номенклатуры
class InventoryDataSchema(BaseModel):
    code: str
    name: str
    producer: str
    vat: float
    morion: Optional[str] = None
    tabletki: Optional[str] = None
    barcode: Optional[str] = None
    optima: Optional[str] = None
    badm: Optional[str] = None
    venta: Optional[str] = None
    enterprise_code: str
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# Схема для остатков
class InventoryStockSchema(BaseModel):
    branch: str
    code: str
    price: float
    qty: int
    price_reserve: float
    enterprise_code: str

    class Config:
        from_attributes = True


# Схема для забронированных товаров
class ReservedItemsSchema(BaseModel):
    branch: str
    code: str
    qty: int

    class Config:
        from_attributes = True


# Схема для настроек предприятий
class EnterpriseSettingsSchema(BaseModel):
    enterprise_code: str
    enterprise_name: str
    tabletki_login: Optional[str] = None
    tabletki_password: Optional[str] = None
    token: Optional[str] = None # Токен подключения к API предприятия
    data_format: Optional[str] = None  # Поставщик данных
    single_store: Optional[bool] = False
    order_fetcher: Optional[bool] = False
    auto_confirm: Optional[bool] = False
    store_serial: Optional[str] = None  # Серийный номер магазина
    stock_upload_frequency: Optional[int] = None  # Частота загрузки остатков
    catalog_upload_frequency: Optional[int] = None  # Частота загрузки каталога
    stock_correction: Optional[bool] = False  # Коррекция остатков
    google_drive_folder_id_ref: Optional[str] = None
    google_drive_folder_id_rest: Optional[str] = None
    branch_id: Optional[str] = None
    discount_rate: Optional[float] = None  # Скидка
    last_stock_upload: Optional[datetime] = None  # Дата последней загрузки остатков
    last_catalog_upload: Optional[datetime] = None  # Дата последней загрузки каталога

    class Config:
        from_attributes = True  # Включено для использования from_orm


# Схема таблицы mapping
class MappingBranchSchema(BaseModel):
    enterprise_code: str
    branch: str
    store_id: str
    id_telegram: Optional[List[str]]
    class Config:
        from_attributes = True  # Включено для использования from_orm


# Схема для глобальных настроек системы
class DeveloperSettingsSchema(BaseModel):
    developer_login: str
    developer_password: str
    endpoint_catalog: Optional[str] = None
    endpoint_stock: Optional[str] = None
    endpoint_orders: Optional[str] = None
    telegram_token_developer: Optional[str] = None
    message_orders: Optional[bool] = False
    morion: Optional[str] = None
    tabletki: Optional[str] = None
    barcode: Optional[str] = None
    optima: Optional[str] = None
    badm: Optional[str] = None
    venta: Optional[str] = None

    class Config:
        from_attributes = True


# Схема для форматов данных
class DataFormatSchema(BaseModel):
    id: Optional[str] = None  # ID формата данных
    format_name: str  # Название формата данных

    class Config:
        from_attributes = True
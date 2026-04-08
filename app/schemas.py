from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
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
    catalog_enabled: Optional[bool] = True
    stock_enabled: Optional[bool] = True
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


class EnterpriseFieldMetaVM(BaseModel):
    key: str
    label: str
    field_type: str
    readonly: bool = False
    help_text: Optional[str] = None


class EnterpriseSectionVM(BaseModel):
    key: str
    title: str
    description: Optional[str] = None
    collapsible: bool = False
    default_open: bool = True
    field_keys: List[str] = Field(default_factory=list)


class EnterpriseListItemVM(BaseModel):
    enterprise_code: str
    enterprise_name: str
    data_format: Optional[str] = None
    branch_id: Optional[str] = None
    catalog_upload_frequency: Optional[int] = None
    stock_upload_frequency: Optional[int] = None
    catalog_enabled: bool = True
    stock_enabled: bool = True
    order_fetcher: bool = False
    last_stock_upload: Optional[datetime] = None
    last_catalog_upload: Optional[datetime] = None
    is_blank_format: bool = False
    has_format_specific_fields: bool = False


class EnterpriseDetailVM(BaseModel):
    enterprise_code: str
    enterprise_name: str
    data_format: Optional[str] = None
    catalog_enabled: bool = True
    stock_enabled: bool = True
    values: Dict[str, Any] = Field(default_factory=dict)
    field_meta: List[EnterpriseFieldMetaVM] = Field(default_factory=list)
    sections: List[EnterpriseSectionVM] = Field(default_factory=list)
    show_format_fields_block: bool = False
    show_runtime_block: bool = True


# Схема таблицы mapping
class MappingBranchSchema(BaseModel):
    enterprise_code: str
    branch: str
    store_id: str
    google_folder_id: Optional[str] = None
    id_telegram: Optional[List[str]] = None
    class Config:
        from_attributes = True  # Включено для использования from_orm


class BranchMappingListItemVM(BaseModel):
    mapping_key: str
    enterprise_code: str
    enterprise_display_label: str
    branch: str
    semantic_store_label: str
    store_mapping_value: str
    google_folder_id: Optional[str] = None
    has_telegram_target: bool = False
    field_semantics_summary: str
    runtime_usage_hints_summary: str
    conflict_flags: List[str] = Field(default_factory=list)
    readonly_fields: List[str] = Field(default_factory=list)


class BranchMappingDetailVM(BaseModel):
    mapping_key: str
    enterprise_code: str
    enterprise_display_label: str
    data_format: Optional[str] = None
    branch: str
    store_id: str
    semantic_store_label: str
    google_folder_id: Optional[str] = None
    id_telegram: List[str] = Field(default_factory=list)
    runtime_consumers: List[str] = Field(default_factory=list)
    runtime_usage_hints_summary: str
    field_notes: List[str] = Field(default_factory=list)
    overloaded_fields: List[str] = Field(default_factory=list)
    conflict_flags: List[str] = Field(default_factory=list)
    readonly_fields: List[str] = Field(default_factory=list)
    computed_fields: List[str] = Field(default_factory=list)


class MappingBranchConstrainedUpdateSchema(BaseModel):
    store_id: str
    google_folder_id: Optional[str] = None


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
  
        # === Dropship enterprises ===
class DropshipEnterpriseSchema(BaseModel):
    code: str
    name: str
    feed_url: Optional[str] = None
    gdrive_folder: Optional[str] = None

    is_rrp: Optional[bool] = False
    is_wholesale: Optional[bool] = True
    profit_percent: Optional[float] = None
    retail_markup: Optional[float] = None
    min_markup_threshold: Optional[float] = None

    is_active: Optional[bool] = True
    api_orders_enabled: Optional[bool] = False
    priority: Optional[int] = 5
    weekend_work: Optional[bool] = False
    use_feed_instead_of_gdrive: Optional[bool] = True
    city: Optional[str] = None

    class Config:
        from_attributes = True


class SupplierSectionVM(BaseModel):
    key: str
    title: str
    collapsible: bool = False
    default_open: bool = True


class SupplierListItemVM(BaseModel):
    code: str
    display_name: str
    is_active: bool = True
    cities_list: List[str] = Field(default_factory=list)
    source_summary: str
    pricing_summary: str
    flags_summary: str


class SupplierDetailVM(BaseModel):
    code: str
    display_name: str
    name: str
    is_active: bool = True
    cities_raw: Optional[str] = None
    cities_list: List[str] = Field(default_factory=list)
    feed_url: Optional[str] = None
    gdrive_folder: Optional[str] = None
    is_rrp: bool = False
    profit_percent: Optional[float] = None
    retail_markup: Optional[float] = None
    min_markup_threshold: Optional[float] = None
    priority: int = 5
    use_feed_instead_of_gdrive: bool = False
    source_summary: str
    pricing_summary: str
    flags_summary: str
    sections: List[SupplierSectionVM] = Field(default_factory=list)

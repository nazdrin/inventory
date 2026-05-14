from decimal import Decimal
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Dict, Any, Literal
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


class PaymentPeriodRequest(BaseModel):
    period_from: str
    period_to: str


class SalesDrivePaymentImportRequest(PaymentPeriodRequest):
    payment_type: Literal["incoming", "outcoming", "all"] = "all"


class OrderReportSyncRequest(PaymentPeriodRequest):
    enterprise_code: Optional[str] = None
    limit: int = Field(default=100, ge=1, le=500)
    max_pages: int = Field(default=20, ge=1, le=200)


class OrderReportExpenseSettingUpsert(BaseModel):
    enterprise_code: str
    expense_percent: Decimal = Field(default=Decimal("0"), ge=0)
    active_from: str
    active_to: Optional[str] = None


class AccountBalanceAdjustmentUpsert(BaseModel):
    account_id: int
    period_month: Optional[str] = None
    balance_date: Optional[str] = None
    actual_balance: Optional[Decimal] = None
    opening_balance_adjustment: Decimal = Decimal("0")
    closing_balance_adjustment: Decimal = Decimal("0")
    actual_opening_balance: Optional[Decimal] = None
    actual_closing_balance: Optional[Decimal] = None
    comment: Optional[str] = None
    created_by: Optional[str] = None
    approved_by: Optional[str] = None


class BusinessOrganizationBase(BaseModel):
    salesdrive_organization_id: Optional[str] = None
    short_name: str
    full_name: Optional[str] = None
    tax_id: Optional[str] = None
    entity_type: Literal["fop", "company", "individual", "other"] = "other"
    verification_status: Literal["draft", "needs_review", "verified", "archived"] = "needs_review"
    vat_enabled: bool = False
    vat_payer: bool = False
    without_stamp: bool = False
    signer_name: Optional[str] = None
    signer_position: Optional[str] = None
    chief_accountant_name: Optional[str] = None
    cashier_name: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    is_active: bool = True
    notes: Optional[str] = None

    @field_validator(
        "salesdrive_organization_id",
        "short_name",
        "full_name",
        "tax_id",
        "signer_name",
        "signer_position",
        "chief_accountant_name",
        "cashier_name",
        "address",
        "postal_code",
        "city",
        "region",
        "country",
        "phone",
        "notes",
        mode="before",
    )
    @classmethod
    def _normalize_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("short_name")
    @classmethod
    def _require_short_name(cls, value: Optional[str]) -> str:
        if not value:
            raise ValueError("short_name is required")
        return value


class BusinessOrganizationCreate(BusinessOrganizationBase):
    pass


class BusinessOrganizationUpdate(BaseModel):
    salesdrive_organization_id: Optional[str] = None
    short_name: Optional[str] = None
    full_name: Optional[str] = None
    tax_id: Optional[str] = None
    entity_type: Optional[Literal["fop", "company", "individual", "other"]] = None
    verification_status: Optional[Literal["draft", "needs_review", "verified", "archived"]] = None
    vat_enabled: Optional[bool] = None
    vat_payer: Optional[bool] = None
    without_stamp: Optional[bool] = None
    signer_name: Optional[str] = None
    signer_position: Optional[str] = None
    chief_accountant_name: Optional[str] = None
    cashier_name: Optional[str] = None
    address: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    phone: Optional[str] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None

    @field_validator(
        "salesdrive_organization_id",
        "short_name",
        "full_name",
        "tax_id",
        "signer_name",
        "signer_position",
        "chief_accountant_name",
        "cashier_name",
        "address",
        "postal_code",
        "city",
        "region",
        "country",
        "phone",
        "notes",
        mode="before",
    )
    @classmethod
    def _normalize_update_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class BusinessAccountBase(BaseModel):
    salesdrive_account_id: Optional[str] = None
    account_number: str
    account_title: Optional[str] = None
    label: Optional[str] = None
    card_mask: Optional[str] = None
    currency: str = "UAH"
    bank_name: Optional[str] = None
    mfo: Optional[str] = None
    is_active: bool = True

    @field_validator(
        "salesdrive_account_id",
        "account_number",
        "account_title",
        "label",
        "card_mask",
        "currency",
        "bank_name",
        "mfo",
        mode="before",
    )
    @classmethod
    def _normalize_account_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("account_number")
    @classmethod
    def _require_account_number(cls, value: Optional[str]) -> str:
        if not value:
            raise ValueError("account_number is required")
        return value


class BusinessAccountCreate(BusinessAccountBase):
    pass


class BusinessAccountUpdate(BaseModel):
    salesdrive_account_id: Optional[str] = None
    account_number: Optional[str] = None
    account_title: Optional[str] = None
    label: Optional[str] = None
    card_mask: Optional[str] = None
    currency: Optional[str] = None
    bank_name: Optional[str] = None
    mfo: Optional[str] = None
    is_active: Optional[bool] = None

    @field_validator(
        "salesdrive_account_id",
        "account_number",
        "account_title",
        "label",
        "card_mask",
        "currency",
        "bank_name",
        "mfo",
        mode="before",
    )
    @classmethod
    def _normalize_update_account_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class BusinessAccountOut(BusinessAccountBase):
    id: int
    business_entity_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CheckboxCashRegisterBase(BaseModel):
    business_store_id: Optional[int] = None
    enterprise_code: Optional[str] = None
    register_name: str
    cash_register_code: str
    checkbox_license_key: Optional[str] = None
    cashier_login: Optional[str] = None
    cashier_password: Optional[str] = None
    cashier_pin: Optional[str] = None
    api_base_url: Optional[str] = None
    is_test_mode: bool = True
    is_active: bool = True
    is_default: bool = False
    shift_open_mode: Literal["manual", "scheduled", "first_status_4", "on_fiscalization"] = "on_fiscalization"
    shift_open_time: Optional[str] = None
    shift_close_time: Optional[str] = None
    timezone: str = "Europe/Kiev"
    receipt_notifications_enabled: bool = False
    shift_notifications_enabled: bool = True
    notes: Optional[str] = None

    @field_validator(
        "enterprise_code",
        "register_name",
        "cash_register_code",
        "checkbox_license_key",
        "cashier_login",
        "cashier_password",
        "cashier_pin",
        "api_base_url",
        "shift_open_time",
        "shift_close_time",
        "timezone",
        "notes",
        mode="before",
    )
    @classmethod
    def _normalize_register_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("register_name", "cash_register_code")
    @classmethod
    def _require_register_text(cls, value: Optional[str]) -> str:
        if not value:
            raise ValueError("register_name and cash_register_code are required")
        return value


class CheckboxCashRegisterCreate(CheckboxCashRegisterBase):
    pass


class CheckboxCashRegisterUpdate(BaseModel):
    business_store_id: Optional[int] = None
    enterprise_code: Optional[str] = None
    register_name: Optional[str] = None
    cash_register_code: Optional[str] = None
    checkbox_license_key: Optional[str] = None
    cashier_login: Optional[str] = None
    cashier_password: Optional[str] = None
    cashier_pin: Optional[str] = None
    api_base_url: Optional[str] = None
    is_test_mode: Optional[bool] = None
    is_active: Optional[bool] = None
    is_default: Optional[bool] = None
    shift_open_mode: Optional[Literal["manual", "scheduled", "first_status_4", "on_fiscalization"]] = None
    shift_open_time: Optional[str] = None
    shift_close_time: Optional[str] = None
    timezone: Optional[str] = None
    receipt_notifications_enabled: Optional[bool] = None
    shift_notifications_enabled: Optional[bool] = None
    notes: Optional[str] = None

    @field_validator(
        "enterprise_code",
        "register_name",
        "cash_register_code",
        "checkbox_license_key",
        "cashier_login",
        "cashier_password",
        "cashier_pin",
        "api_base_url",
        "shift_open_time",
        "shift_close_time",
        "timezone",
        "notes",
        mode="before",
    )
    @classmethod
    def _normalize_update_register_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class CheckboxCashRegisterOut(BaseModel):
    id: int
    business_organization_id: int
    business_store_id: Optional[int] = None
    enterprise_code: Optional[str] = None
    register_name: str
    cash_register_code: str
    checkbox_license_key_set: bool = False
    cashier_login: Optional[str] = None
    cashier_password_set: bool = False
    cashier_pin_set: bool = False
    api_base_url: Optional[str] = None
    is_test_mode: bool
    is_active: bool
    is_default: bool
    shift_open_mode: str
    shift_open_time: Optional[str] = None
    shift_close_time: Optional[str] = None
    timezone: str
    receipt_notifications_enabled: bool
    shift_notifications_enabled: bool
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class CheckboxReceiptExclusionBase(BaseModel):
    cash_register_id: Optional[int] = None
    supplier_code: str
    supplier_name: Optional[str] = None
    is_active: bool = True
    comment: Optional[str] = None

    @field_validator("supplier_code", "supplier_name", "comment", mode="before")
    @classmethod
    def _normalize_exclusion_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("supplier_code")
    @classmethod
    def _require_supplier_code(cls, value: Optional[str]) -> str:
        if not value:
            raise ValueError("supplier_code is required")
        return value


class CheckboxReceiptExclusionCreate(CheckboxReceiptExclusionBase):
    pass


class CheckboxReceiptExclusionUpdate(BaseModel):
    cash_register_id: Optional[int] = None
    supplier_code: Optional[str] = None
    supplier_name: Optional[str] = None
    is_active: Optional[bool] = None
    comment: Optional[str] = None

    @field_validator("supplier_code", "supplier_name", "comment", mode="before")
    @classmethod
    def _normalize_update_exclusion_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class CheckboxReceiptExclusionOut(CheckboxReceiptExclusionBase):
    id: int
    business_organization_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class BusinessOrganizationOut(BusinessOrganizationBase):
    id: int
    accounts: List[BusinessAccountOut] = Field(default_factory=list)
    cash_registers: List[CheckboxCashRegisterOut] = Field(default_factory=list)
    receipt_exclusions: List[CheckboxReceiptExclusionOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class PaymentCounterpartySupplierMappingUpsert(BaseModel):
    supplier_code: str
    supplier_salesdrive_id: Optional[int] = None
    match_type: Literal["tax_id", "exact", "contains", "search_text_contains"] = "exact"
    field_scope: Literal["tax_id", "counterparty_name", "purpose", "comment", "search_text"] = "counterparty_name"
    counterparty_pattern: Optional[str] = None
    counterparty_tax_id: Optional[str] = None
    priority: int = 100
    is_active: bool = True
    notes: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


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
    business_runtime_mode: Literal["baseline", "custom"] = "baseline"
    business_stock_mode: Literal["baseline_legacy", "store_aware"] = "baseline_legacy"
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


class BusinessEnterpriseCandidateVM(BaseModel):
    enterprise_code: str
    enterprise_name: str
    data_format: Optional[str] = None


class BusinessEnterpriseOptionVM(BaseModel):
    enterprise_code: str
    enterprise_name: str
    data_format: Optional[str] = None


class BusinessSettingItemVM(BaseModel):
    key: str
    label: str
    value: Any = None
    source: str
    group: Optional[str] = None
    readonly: bool = True
    help_text: Optional[str] = None


class BusinessSectionVM(BaseModel):
    key: str
    title: str
    description: Optional[str] = None
    readonly: bool = True
    items: List[BusinessSettingItemVM] = Field(default_factory=list)


class BusinessSettingsVM(BaseModel):
    resolution_status: str
    resolution_message: str
    resolved_enterprise_code: Optional[str] = None
    resolved_enterprise_name: Optional[str] = None
    token_present: bool = False
    business_candidates: List[BusinessEnterpriseCandidateVM] = Field(default_factory=list)
    enterprise_options: List[BusinessEnterpriseOptionVM] = Field(default_factory=list)
    writable_supported: bool = False
    deferred_write_reason: Optional[str] = None
    planned_writable_keys: List[str] = Field(default_factory=list)
    sections: List[BusinessSectionVM] = Field(default_factory=list)


class BusinessSettingsUpdateSchema(BaseModel):
    business_enterprise_code: str
    daily_publish_enterprise_code_override: Optional[str] = None
    weekly_salesdrive_enterprise_code_override: Optional[str] = None
    business_stock_enabled: bool
    business_stock_interval_seconds: int = Field(ge=1)
    biotus_enable_unhandled_fallback: bool
    biotus_unhandled_order_timeout_minutes: int = Field(ge=0)
    biotus_fallback_additional_status_ids: List[int] = Field(min_length=1)
    biotus_duplicate_status_id: int = Field(ge=1)
    master_weekly_enabled: bool
    master_weekly_day: Literal["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    master_weekly_hour: int = Field(ge=0, le=23)
    master_weekly_minute: int = Field(ge=0, le=59)
    master_daily_publish_enabled: bool
    master_daily_publish_hour: int = Field(ge=0, le=23)
    master_daily_publish_minute: int = Field(ge=0, le=59)
    master_daily_publish_limit: int = Field(ge=0)
    master_archive_enabled: bool
    master_archive_every_minutes: int = Field(ge=1)

    @field_validator(
        "business_enterprise_code",
        "daily_publish_enterprise_code_override",
        "weekly_salesdrive_enterprise_code_override",
        mode="before",
    )
    @classmethod
    def _normalize_enterprise_code(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("business_enterprise_code")
    @classmethod
    def _require_primary_code(cls, value: Optional[str]) -> str:
        if not value:
            raise ValueError("business_enterprise_code is required")
        return value

    @field_validator("biotus_fallback_additional_status_ids", mode="before")
    @classmethod
    def _normalize_biotus_status_ids(cls, value: Any) -> List[int]:
        if value is None:
            raise ValueError("biotus_fallback_additional_status_ids is required")
        if isinstance(value, str):
            parts = [item.strip() for item in value.replace(";", ",").split(",")]
            normalized = [int(item) for item in parts if item]
        elif isinstance(value, (list, tuple, set)):
            normalized = [int(item) for item in value]
        else:
            raise ValueError("biotus_fallback_additional_status_ids must be a list of integers")

        if not normalized:
            raise ValueError("biotus_fallback_additional_status_ids must not be empty")
        if any(item < 1 for item in normalized):
            raise ValueError("biotus_fallback_additional_status_ids must contain only positive integers")
        return normalized

    @field_validator("master_weekly_day", mode="before")
    @classmethod
    def _normalize_weekly_day(cls, value: Any) -> str:
        normalized = str(value or "").strip().upper()
        if not normalized:
            raise ValueError("master_weekly_day is required")
        return normalized

class BusinessPricingSettingsUpdateSchema(BaseModel):
    """Future bounded pricing write contract for Business Settings.

    Phase 1 freeze only:
    - not wired into routes yet
    - not used by runtime yet
    - intended to define the exact DB-backed pricing control-plane payload
    """

    pricing_base_thr: Decimal = Field(ge=0)
    pricing_price_band_low_max: Decimal = Field(ge=0)
    pricing_price_band_mid_max: Decimal = Field(ge=0)
    pricing_thr_add_low_uah: Decimal = Field(ge=0)
    pricing_thr_add_mid_uah: Decimal = Field(ge=0)
    pricing_thr_add_high_uah: Decimal = Field(ge=0)
    pricing_no_comp_add_low_uah: Decimal = Field(ge=0)
    pricing_no_comp_add_mid_uah: Decimal = Field(ge=0)
    pricing_no_comp_add_high_uah: Decimal = Field(ge=0)
    pricing_comp_discount_share: Decimal = Field(ge=0)
    pricing_comp_delta_min_uah: Decimal = Field(ge=0)
    pricing_comp_delta_max_uah: Decimal = Field(ge=0)
    pricing_jitter_enabled: bool
    pricing_jitter_step_uah: Decimal = Field(gt=0)
    pricing_jitter_min_uah: Decimal
    pricing_jitter_max_uah: Decimal

    @model_validator(mode="after")
    def _validate_cross_field_constraints(self) -> "BusinessPricingSettingsUpdateSchema":
        if self.pricing_price_band_mid_max < self.pricing_price_band_low_max:
            raise ValueError("pricing_price_band_mid_max must be >= pricing_price_band_low_max")

        if self.pricing_comp_discount_share >= Decimal("1"):
            raise ValueError("pricing_comp_discount_share must be < 1")

        if self.pricing_comp_delta_max_uah < self.pricing_comp_delta_min_uah:
            raise ValueError("pricing_comp_delta_max_uah must be >= pricing_comp_delta_min_uah")

        if self.pricing_jitter_max_uah < self.pricing_jitter_min_uah:
            raise ValueError("pricing_jitter_max_uah must be >= pricing_jitter_min_uah")

        return self

class BusinessEnterpriseOperationalFieldsUpdateSchema(BaseModel):
    branch_id: str
    tabletki_login: Optional[str] = None
    tabletki_password: Optional[str] = None
    token: Optional[str] = None
    order_fetcher: bool
    auto_confirm: bool
    stock_correction: bool

    @field_validator("branch_id", mode="before")
    @classmethod
    def _normalize_branch_id(cls, value: Any) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("branch_id is required")
        return normalized

    @field_validator("tabletki_login", "tabletki_password", "token", mode="before")
    @classmethod
    def _normalize_optional_credentials(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


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


class BusinessStoreBase(BaseModel):
    store_name: str
    business_organization_id: Optional[int] = None
    legal_entity_name: Optional[str] = None
    tax_identifier: Optional[str] = None
    is_active: bool = True
    is_legacy_default: bool = False
    enterprise_code: Optional[str] = None
    legacy_scope_key: Optional[str] = None
    tabletki_enterprise_code: Optional[str] = None
    tabletki_branch: Optional[str] = None
    salesdrive_enterprise_code: Optional[str] = None
    salesdrive_enterprise_id: Optional[int] = None
    salesdrive_store_name: Optional[str] = None
    catalog_enabled: bool = False
    stock_enabled: bool = False
    orders_enabled: bool = False
    catalog_only_in_stock: bool = True
    code_strategy: Literal["legacy_same", "opaque_mapping", "prefix_mapping"] = "opaque_mapping"
    code_prefix: Optional[str] = None
    name_strategy: Literal["base", "supplier_random"] = "base"
    extra_markup_enabled: bool = False
    extra_markup_mode: Literal["percent"] = "percent"
    extra_markup_min: Optional[Decimal] = None
    extra_markup_max: Optional[Decimal] = None
    extra_markup_strategy: Literal["stable_per_product"] = "stable_per_product"
    takes_over_legacy_scope: bool = False
    migration_status: Literal[
        "draft",
        "dry_run",
        "stock_live",
        "catalog_stock_live",
        "orders_live",
        "disabled",
    ] = "draft"

    @field_validator(
        "store_name",
        "legal_entity_name",
        "tax_identifier",
        "enterprise_code",
        "legacy_scope_key",
        "tabletki_enterprise_code",
        "tabletki_branch",
        "salesdrive_enterprise_code",
        "salesdrive_store_name",
        "code_prefix",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("store_name")
    @classmethod
    def _require_store_name(cls, value: Optional[str]) -> str:
        if not value:
            raise ValueError("store_name is required")
        return value

    @model_validator(mode="after")
    def _validate_extra_markup(self) -> "BusinessStoreBase":
        if self.extra_markup_enabled:
            if self.extra_markup_min is None or self.extra_markup_max is None:
                raise ValueError("extra_markup_min and extra_markup_max are required when extra_markup_enabled=true")
            if self.extra_markup_min < 0 or self.extra_markup_max < 0:
                raise ValueError("extra_markup_min and extra_markup_max must be >= 0")
            if self.extra_markup_max < self.extra_markup_min:
                raise ValueError("extra_markup_max must be >= extra_markup_min")
            if self.extra_markup_max > Decimal("100"):
                raise ValueError("extra_markup_max must be <= 100")
        elif self.extra_markup_min is not None and self.extra_markup_max is not None:
            if self.extra_markup_min < 0 or self.extra_markup_max < 0:
                raise ValueError("extra_markup_min and extra_markup_max must be >= 0")
            if self.extra_markup_max < self.extra_markup_min:
                raise ValueError("extra_markup_max must be >= extra_markup_min")
        return self


class BusinessStoreCreate(BusinessStoreBase):
    store_code: str

    @field_validator("store_code", mode="before")
    @classmethod
    def _normalize_store_code(cls, value: Any) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("store_code is required")
        return normalized


class BusinessStoreUpdate(BaseModel):
    store_name: Optional[str] = None
    business_organization_id: Optional[int] = None
    legal_entity_name: Optional[str] = None
    tax_identifier: Optional[str] = None
    is_active: Optional[bool] = None
    is_legacy_default: Optional[bool] = None
    enterprise_code: Optional[str] = None
    legacy_scope_key: Optional[str] = None
    tabletki_enterprise_code: Optional[str] = None
    tabletki_branch: Optional[str] = None
    salesdrive_enterprise_code: Optional[str] = None
    salesdrive_enterprise_id: Optional[int] = None
    salesdrive_store_name: Optional[str] = None
    catalog_enabled: Optional[bool] = None
    stock_enabled: Optional[bool] = None
    orders_enabled: Optional[bool] = None
    catalog_only_in_stock: Optional[bool] = None
    code_strategy: Optional[Literal["legacy_same", "opaque_mapping", "prefix_mapping"]] = None
    code_prefix: Optional[str] = None
    name_strategy: Optional[Literal["base", "supplier_random"]] = None
    extra_markup_enabled: Optional[bool] = None
    extra_markup_mode: Optional[Literal["percent"]] = None
    extra_markup_min: Optional[Decimal] = None
    extra_markup_max: Optional[Decimal] = None
    extra_markup_strategy: Optional[Literal["stable_per_product"]] = None
    takes_over_legacy_scope: Optional[bool] = None
    migration_status: Optional[
        Literal[
            "draft",
            "dry_run",
            "stock_live",
            "catalog_stock_live",
            "orders_live",
            "disabled",
        ]
    ] = None

    @field_validator(
        "store_name",
        "legal_entity_name",
        "tax_identifier",
        "enterprise_code",
        "legacy_scope_key",
        "tabletki_enterprise_code",
        "tabletki_branch",
        "salesdrive_enterprise_code",
        "salesdrive_store_name",
        "code_prefix",
        mode="before",
    )
    @classmethod
    def _normalize_optional_update_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @model_validator(mode="after")
    def _validate_update_extra_markup(self) -> "BusinessStoreUpdate":
        if self.extra_markup_min is not None and self.extra_markup_min < 0:
            raise ValueError("extra_markup_min must be >= 0")
        if self.extra_markup_max is not None and self.extra_markup_max < 0:
            raise ValueError("extra_markup_max must be >= 0")
        if self.extra_markup_min is not None and self.extra_markup_max is not None:
            if self.extra_markup_max < self.extra_markup_min:
                raise ValueError("extra_markup_max must be >= extra_markup_min")
            if self.extra_markup_max > Decimal("100"):
                raise ValueError("extra_markup_max must be <= 100")
        return self


class BusinessStoreOut(BaseModel):
    id: int
    store_code: str
    store_name: str
    business_organization_id: Optional[int] = None
    organization_short_name: Optional[str] = None
    organization_tax_id: Optional[str] = None
    organization_salesdrive_id: Optional[str] = None
    legal_entity_name: Optional[str] = None
    tax_identifier: Optional[str] = None
    is_active: bool
    is_legacy_default: bool
    enterprise_code: Optional[str] = None
    legacy_scope_key: Optional[str] = None
    tabletki_enterprise_code: Optional[str] = None
    tabletki_branch: Optional[str] = None
    salesdrive_enterprise_code: Optional[str] = None
    salesdrive_enterprise_id: Optional[int] = None
    salesdrive_store_name: Optional[str] = None
    catalog_enabled: bool
    stock_enabled: bool
    orders_enabled: bool
    catalog_only_in_stock: bool
    code_strategy: str
    code_prefix: Optional[str] = None
    name_strategy: str
    extra_markup_enabled: bool
    extra_markup_mode: str
    extra_markup_min: Optional[Decimal] = None
    extra_markup_max: Optional[Decimal] = None
    extra_markup_strategy: str
    takes_over_legacy_scope: bool
    migration_status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class LegacyScopeOut(BaseModel):
    legacy_scope_key: str
    rows_count: int
    products_count: int


class BusinessStoreSupplierSettingsBase(BaseModel):
    is_active: bool = True
    priority_override: Optional[int] = None
    min_markup_threshold: Optional[Decimal] = None
    extra_markup_enabled: bool = False
    extra_markup_mode: Optional[Literal["percent"]] = None
    extra_markup_value: Optional[Decimal] = None
    extra_markup_min: Optional[Decimal] = None
    extra_markup_max: Optional[Decimal] = None
    dumping_mode: Optional[bool] = None

    @model_validator(mode="after")
    def _validate_markup_fields(self) -> "BusinessStoreSupplierSettingsBase":
        if self.priority_override is not None and self.priority_override < 0:
            raise ValueError("priority_override must be >= 0")
        if self.min_markup_threshold is not None and self.min_markup_threshold < 0:
            raise ValueError("min_markup_threshold must be >= 0")
        for field_name in ("extra_markup_value", "extra_markup_min", "extra_markup_max"):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be >= 0")
        if self.extra_markup_min is not None and self.extra_markup_max is not None:
            if self.extra_markup_max < self.extra_markup_min:
                raise ValueError("extra_markup_max must be >= extra_markup_min")
        if self.extra_markup_enabled and self.extra_markup_mode is None:
            raise ValueError("extra_markup_mode is required when extra_markup_enabled=true")
        return self


class BusinessStoreSupplierSettingsUpsertSchema(BusinessStoreSupplierSettingsBase):
    supplier_code: str

    @field_validator("supplier_code", mode="before")
    @classmethod
    def _normalize_supplier_code(cls, value: Any) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("supplier_code is required")
        return normalized

    @model_validator(mode="after")
    def _require_min_markup_threshold(self) -> "BusinessStoreSupplierSettingsUpsertSchema":
        if self.min_markup_threshold is None:
            raise ValueError("min_markup_threshold is required")
        return self


class BusinessStoreSupplierSettingsOut(BusinessStoreSupplierSettingsBase):
    id: int
    store_id: int
    supplier_code: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class BusinessStoreOfferSchema(BaseModel):
    id: int
    store_id: int
    enterprise_code: str
    tabletki_branch: str
    supplier_code: str
    product_code: str
    market_scope_key: Optional[str] = None
    base_price: Optional[Decimal] = None
    effective_price: Decimal
    wholesale_price: Optional[Decimal] = None
    stock: int
    priority_used: Optional[int] = None
    price_source: Optional[str] = None
    pricing_context: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class BusinessSupplierStoreSettingsOverviewOut(BaseModel):
    id: int
    store_id: int
    store_code: str
    store_name: str
    enterprise_code: Optional[str] = None
    tabletki_branch: Optional[str] = None
    is_active: bool
    priority_override: Optional[int] = None
    min_markup_threshold: Optional[Decimal] = None
    extra_markup_enabled: bool
    extra_markup_mode: Optional[str] = None
    extra_markup_value: Optional[Decimal] = None
    extra_markup_min: Optional[Decimal] = None
    extra_markup_max: Optional[Decimal] = None
    dumping_mode: Optional[bool] = None
    updated_at: datetime


class BusinessEnterpriseOptionOut(BaseModel):
    enterprise_code: str
    enterprise_name: str
    branch_id: Optional[str] = None
    catalog_enabled: bool
    stock_enabled: bool
    order_fetcher: bool


class BusinessStoreMappingBranchOptionOut(BaseModel):
    enterprise_code: str
    branch: str
    mapping_store_hint: Optional[str] = None
    is_primary_enterprise_branch: bool = False


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
    salesdrive_supplier_id: Optional[int] = None
    biotus_orders_enabled: Optional[bool] = False
    np_fulfillment_enabled: Optional[bool] = False
    schedule_enabled: Optional[bool] = False
    block_start_day: Optional[int] = None
    block_start_time: Optional[str] = None
    block_end_day: Optional[int] = None
    block_end_time: Optional[str] = None

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
    connected_stores_count: int = 0
    connected_store_labels: List[str] = Field(default_factory=list)
    connected_stores_summary: str = ""


class SupplierDetailVM(BaseModel):
    code: str
    display_name: str
    name: str
    is_active: bool = True
    salesdrive_supplier_id: Optional[int] = None
    biotus_orders_enabled: bool = False
    np_fulfillment_enabled: bool = False
    schedule_enabled: bool = False
    block_start_day: Optional[int] = None
    block_start_time: Optional[str] = None
    block_end_day: Optional[int] = None
    block_end_time: Optional[str] = None
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

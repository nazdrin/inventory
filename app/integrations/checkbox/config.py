from __future__ import annotations

import os
from dataclasses import dataclass


TRUE_VALUES = {"1", "true", "yes", "on"}


def env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in TRUE_VALUES


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_list(name: str) -> set[str]:
    raw = os.getenv(name, "")
    return {item.strip() for item in raw.split(",") if item.strip()}


@dataclass(frozen=True)
class CheckboxSettings:
    enabled_enterprises: set[str]
    api_base_url: str
    client_name: str
    client_version: str
    access_key: str | None
    license_key: str | None
    cashier_login: str | None
    cashier_password: str | None
    cashier_pin: str | None
    test_mode: bool
    default_cash_register_code: str
    shift_open_on_demand: bool
    receipt_poll_interval_sec: float
    receipt_poll_timeout_sec: float
    receipt_retry_max_attempts: int
    default_payment_method_id: int
    default_tax_code: int | None
    salesdrive_update_check_enabled: bool
    salesdrive_check_field: str
    telegram_notifications_enabled: bool
    telegram_receipt_notifications_enabled: bool
    telegram_shift_notifications_enabled: bool

    def is_enabled_for_enterprise(self, enterprise_code: str | int | None) -> bool:
        code = str(enterprise_code or "").strip()
        return bool(code and code in self.enabled_enterprises)


def load_checkbox_settings() -> CheckboxSettings:
    default_tax_raw = os.getenv("CHECKBOX_DEFAULT_TAX_CODE", "8").strip()
    try:
        default_tax_code = int(default_tax_raw) if default_tax_raw else None
    except ValueError:
        default_tax_code = 8

    return CheckboxSettings(
        enabled_enterprises=env_list("CHECKBOX_ENABLED_ENTERPRISES"),
        api_base_url=os.getenv("CHECKBOX_API_BASE_URL", "https://api.checkbox.ua").rstrip("/"),
        client_name=os.getenv("CHECKBOX_CLIENT_NAME", "inventory_service"),
        client_version=os.getenv("CHECKBOX_CLIENT_VERSION", "1.0"),
        access_key=os.getenv("CHECKBOX_ACCESS_KEY") or None,
        license_key=os.getenv("CHECKBOX_LICENSE_KEY") or None,
        cashier_login=os.getenv("CHECKBOX_CASHIER_LOGIN") or None,
        cashier_password=os.getenv("CHECKBOX_CASHIER_PASSWORD") or None,
        cashier_pin=os.getenv("CHECKBOX_CASHIER_PIN") or None,
        test_mode=env_bool("CHECKBOX_TEST_MODE", "1"),
        default_cash_register_code=os.getenv("CHECKBOX_DEFAULT_CASH_REGISTER_CODE", "default"),
        shift_open_on_demand=env_bool("CHECKBOX_SHIFT_OPEN_ON_DEMAND", "1"),
        receipt_poll_interval_sec=env_float("CHECKBOX_RECEIPT_POLL_INTERVAL_SEC", 2.0),
        receipt_poll_timeout_sec=env_float("CHECKBOX_RECEIPT_POLL_TIMEOUT_SEC", 30.0),
        receipt_retry_max_attempts=env_int("CHECKBOX_RECEIPT_RETRY_MAX_ATTEMPTS", 5),
        default_payment_method_id=env_int("CHECKBOX_DEFAULT_PAYMENT_METHOD_ID", 20),
        default_tax_code=default_tax_code,
        salesdrive_update_check_enabled=env_bool("CHECKBOX_SALESDRIVE_UPDATE_CHECK_ENABLED", "1"),
        salesdrive_check_field=os.getenv("CHECKBOX_SALESDRIVE_CHECK_FIELD", "check"),
        telegram_notifications_enabled=env_bool("CHECKBOX_TELEGRAM_NOTIFICATIONS_ENABLED", "1"),
        telegram_receipt_notifications_enabled=env_bool("CHECKBOX_TELEGRAM_RECEIPT_NOTIFICATIONS_ENABLED", "1"),
        telegram_shift_notifications_enabled=env_bool("CHECKBOX_TELEGRAM_SHIFT_NOTIFICATIONS_ENABLED", "1"),
    )

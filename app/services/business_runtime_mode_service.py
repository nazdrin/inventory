from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import EnterpriseSettings


BASELINE_BUSINESS_RUNTIME_MODE = "baseline"
CUSTOM_BUSINESS_RUNTIME_MODE = "custom"
VALID_BUSINESS_RUNTIME_MODES = {
    BASELINE_BUSINESS_RUNTIME_MODE,
    CUSTOM_BUSINESS_RUNTIME_MODE,
}

BASELINE_LEGACY_STOCK_RUNTIME_PATH = "baseline_legacy"
STORE_AWARE_STOCK_RUNTIME_PATH = "store_aware"
BASELINE_LEGACY_CATALOG_RUNTIME_PATH = "baseline_legacy"
CUSTOM_CATALOG_RUNTIME_PATH = "enterprise_identity"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_business_runtime_mode(
    value: Any,
    *,
    default: str = BASELINE_BUSINESS_RUNTIME_MODE,
) -> str:
    normalized = _clean_text(value)
    if normalized in VALID_BUSINESS_RUNTIME_MODES:
        return normalized
    return default


def derive_stock_runtime_path(business_runtime_mode: Any) -> str:
    normalized = normalize_business_runtime_mode(business_runtime_mode)
    if normalized == CUSTOM_BUSINESS_RUNTIME_MODE:
        return STORE_AWARE_STOCK_RUNTIME_PATH
    return BASELINE_LEGACY_STOCK_RUNTIME_PATH


def derive_catalog_runtime_path(business_runtime_mode: Any) -> str:
    normalized = normalize_business_runtime_mode(business_runtime_mode)
    if normalized == CUSTOM_BUSINESS_RUNTIME_MODE:
        return CUSTOM_CATALOG_RUNTIME_PATH
    return BASELINE_LEGACY_CATALOG_RUNTIME_PATH


def build_business_runtime_mode_report(
    enterprise_code: str,
    business_runtime_mode: Any,
    *,
    source: str,
) -> dict[str, Any]:
    normalized_enterprise_code = _clean_text(enterprise_code)
    normalized_runtime_mode = normalize_business_runtime_mode(business_runtime_mode)
    stock_runtime_path = derive_stock_runtime_path(normalized_runtime_mode)
    catalog_runtime_path = derive_catalog_runtime_path(normalized_runtime_mode)
    return {
        "business_runtime_mode": normalized_runtime_mode,
        "runtime_mode_source": source,
        "enterprise_code": normalized_enterprise_code,
        "is_baseline_mode": normalized_runtime_mode == BASELINE_BUSINESS_RUNTIME_MODE,
        "is_custom_mode": normalized_runtime_mode == CUSTOM_BUSINESS_RUNTIME_MODE,
        "stock_runtime_path": stock_runtime_path,
        "catalog_runtime_path": catalog_runtime_path,
        "stock_mode": stock_runtime_path,
        "stock_mode_source": source,
        "runtime_switch_enabled": True,
        "live_send_enabled": True,
    }


async def resolve_business_runtime_mode_from_db(
    session: AsyncSession,
    enterprise_code: str,
) -> dict[str, Any]:
    normalized_enterprise_code = _clean_text(enterprise_code)
    if not normalized_enterprise_code:
        return build_business_runtime_mode_report(
            normalized_enterprise_code,
            BASELINE_BUSINESS_RUNTIME_MODE,
            source="missing_enterprise_code_default",
        )

    enterprise_runtime_mode = (
        await session.execute(
            select(EnterpriseSettings.business_runtime_mode)
            .where(EnterpriseSettings.enterprise_code == normalized_enterprise_code)
            .limit(1)
        )
    ).scalar_one_or_none()

    if enterprise_runtime_mode is None:
        return build_business_runtime_mode_report(
            normalized_enterprise_code,
            BASELINE_BUSINESS_RUNTIME_MODE,
            source="missing_enterprise_settings_default",
        )

    return build_business_runtime_mode_report(
        normalized_enterprise_code,
        enterprise_runtime_mode,
        source="enterprise_settings",
    )

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.business_runtime_mode_service import (
    BASELINE_BUSINESS_RUNTIME_MODE,
    BASELINE_LEGACY_STOCK_RUNTIME_PATH,
    CUSTOM_BUSINESS_RUNTIME_MODE,
    STORE_AWARE_STOCK_RUNTIME_PATH,
    build_business_runtime_mode_report,
    derive_stock_runtime_path,
    normalize_business_runtime_mode,
    resolve_business_runtime_mode_from_db,
)


BASELINE_LEGACY_STOCK_MODE = BASELINE_LEGACY_STOCK_RUNTIME_PATH
STORE_AWARE_STOCK_MODE = STORE_AWARE_STOCK_RUNTIME_PATH
VALID_BUSINESS_STOCK_MODES = {
    BASELINE_LEGACY_STOCK_MODE,
    STORE_AWARE_STOCK_MODE,
}


def normalize_business_stock_mode(
    value: Any,
    *,
    default: str = BASELINE_LEGACY_STOCK_MODE,
) -> str:
    normalized_runtime_mode = normalize_business_runtime_mode(
        value,
        default=BASELINE_BUSINESS_RUNTIME_MODE if default == BASELINE_LEGACY_STOCK_MODE else CUSTOM_BUSINESS_RUNTIME_MODE,
    )
    return derive_stock_runtime_path(normalized_runtime_mode)


def build_business_stock_mode_report(
    enterprise_code: str,
    stock_mode: Any,
    *,
    source: str,
) -> dict[str, Any]:
    runtime_mode = (
        CUSTOM_BUSINESS_RUNTIME_MODE
        if str(stock_mode or "").strip() == STORE_AWARE_STOCK_MODE
        else BASELINE_BUSINESS_RUNTIME_MODE
    )
    return build_business_runtime_mode_report(
        enterprise_code,
        runtime_mode,
        source=source,
    )


def resolve_business_stock_mode(enterprise_code: str) -> dict[str, Any]:
    return build_business_runtime_mode_report(
        enterprise_code,
        BASELINE_BUSINESS_RUNTIME_MODE,
        source="legacy_default",
    )


async def resolve_business_stock_mode_from_db(
    session: AsyncSession,
    enterprise_code: str,
) -> dict[str, Any]:
    return await resolve_business_runtime_mode_from_db(session, enterprise_code)

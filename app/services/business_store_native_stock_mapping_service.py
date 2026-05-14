from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BusinessEnterpriseProductCode,
    BusinessStore,
)
from app.services.business_runtime_mode_service import CUSTOM_BUSINESS_RUNTIME_MODE


@dataclass(frozen=True)
class StockCodeResolution:
    external_code: str | None
    mapping_mode: str
    identity_mode: str
    source: str
    warning: str | None = None


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _uses_external_code_mapping(store: BusinessStore) -> bool:
    return not (
        bool(getattr(store, "is_legacy_default", False))
        or _clean_text(getattr(store, "code_strategy", None)).lower() == "legacy_same"
    )


async def _load_enterprise_code_map(
    session: AsyncSession,
    *,
    enterprise_codes: list[str],
) -> dict[tuple[str, str], BusinessEnterpriseProductCode]:
    normalized = [_clean_text(item) for item in enterprise_codes if _clean_text(item)]
    if not normalized:
        return {}
    rows = (
        await session.execute(
            select(BusinessEnterpriseProductCode).where(
                BusinessEnterpriseProductCode.enterprise_code.in_(normalized),
                BusinessEnterpriseProductCode.is_active.is_(True),
            )
        )
    ).scalars().all()
    return {
        (_clean_text(row.enterprise_code), _clean_text(row.internal_product_code)): row
        for row in rows
        if _clean_text(row.enterprise_code) and _clean_text(row.internal_product_code)
    }


async def load_stock_mapping_context(
    session: AsyncSession,
    *,
    stores: list[BusinessStore],
) -> dict[str, Any]:
    enterprise_map = await _load_enterprise_code_map(
        session,
        enterprise_codes=[_clean_text(store.enterprise_code) for store in stores],
    )
    return {
        "enterprise_mapping_enabled": True,
        "enterprise_map": enterprise_map,
    }


def resolve_stock_external_code(
    *,
    mapping_context: dict[str, Any],
    store: BusinessStore,
    internal_product_code: str,
    business_runtime_mode: str,
) -> StockCodeResolution:
    normalized_internal_code = _clean_text(internal_product_code)
    if not normalized_internal_code:
        return StockCodeResolution(
            external_code=None,
            mapping_mode="unknown",
            identity_mode="unknown",
            source="missing_internal_product_code",
            warning="Internal product code is empty.",
        )

    code_strategy = _clean_text(getattr(store, "code_strategy", None)).lower() or "legacy_same"
    if business_runtime_mode != CUSTOM_BUSINESS_RUNTIME_MODE:
        return StockCodeResolution(
            external_code=normalized_internal_code,
            mapping_mode="legacy_same",
            identity_mode="legacy_same",
            source="baseline_runtime_internal_code",
        )

    if not _uses_external_code_mapping(store):
        return StockCodeResolution(
            external_code=normalized_internal_code,
            mapping_mode="legacy_same",
            identity_mode="legacy_same",
            source="store_legacy_same_strategy",
        )

    enterprise_mapping = (mapping_context.get("enterprise_map") or {}).get(
        (_clean_text(store.enterprise_code), normalized_internal_code)
    )
    if enterprise_mapping is None:
        return StockCodeResolution(
            external_code=None,
            mapping_mode="enterprise_level",
            identity_mode="enterprise_level",
            source="enterprise_mapping_missing",
            warning="Missing enterprise-level stock code mapping.",
        )
    return StockCodeResolution(
        external_code=_clean_text(enterprise_mapping.external_product_code) or None,
        mapping_mode="enterprise_level",
        identity_mode="enterprise_level",
        source="business_enterprise_product_codes",
    )

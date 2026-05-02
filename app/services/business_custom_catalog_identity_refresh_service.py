from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_catalog_preview import resolve_store_catalog_candidate_scope
from app.business.business_store_code_generator import (
    build_external_code_for_store,
    ensure_store_product_code,
)
from app.business.business_store_name_generator import ensure_store_product_name
from app.business.business_store_name_generator import (
    choose_stable_random_name,
    get_supplier_name_candidates_for_store_product,
)
from app.models import (
    BusinessEnterpriseProductCode,
    BusinessEnterpriseProductName,
    BusinessStore,
    BusinessStoreProductCode,
    BusinessStoreProductName,
)
from app.services.business_runtime_mode_service import (
    CUSTOM_BUSINESS_RUNTIME_MODE,
    resolve_business_runtime_mode_from_db,
)


logger = logging.getLogger(__name__)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


async def _get_store_or_fail(session: AsyncSession, store_id: int) -> BusinessStore:
    store = (
        await session.execute(
            select(BusinessStore).where(BusinessStore.id == int(store_id)).limit(1)
        )
    ).scalar_one_or_none()
    if store is None:
        raise ValueError(f"BusinessStore not found for store_id={store_id}")
    return store


async def _load_store_code_map(
    session: AsyncSession,
    *,
    store_id: int,
) -> dict[str, BusinessStoreProductCode]:
    rows = (
        await session.execute(
            select(BusinessStoreProductCode).where(
                BusinessStoreProductCode.store_id == int(store_id),
            )
        )
    ).scalars().all()
    return {
        _clean_text(row.internal_product_code): row
        for row in rows
        if _clean_text(row.internal_product_code)
    }


async def _load_store_name_map(
    session: AsyncSession,
    *,
    store_id: int,
) -> dict[str, BusinessStoreProductName]:
    rows = (
        await session.execute(
            select(BusinessStoreProductName).where(
                BusinessStoreProductName.store_id == int(store_id),
            )
        )
    ).scalars().all()
    return {
        _clean_text(row.internal_product_code): row
        for row in rows
        if _clean_text(row.internal_product_code)
    }


async def _load_enterprise_code_maps(
    session: AsyncSession,
    *,
    enterprise_code: str,
) -> tuple[dict[str, BusinessEnterpriseProductCode], dict[str, BusinessEnterpriseProductCode]]:
    rows = (
        await session.execute(
            select(BusinessEnterpriseProductCode).where(
                BusinessEnterpriseProductCode.enterprise_code == _clean_text(enterprise_code),
            )
        )
    ).scalars().all()
    by_internal = {
        _clean_text(row.internal_product_code): row
        for row in rows
        if _clean_text(row.internal_product_code)
    }
    by_external = {
        _clean_text(row.external_product_code): row
        for row in rows
        if _clean_text(row.external_product_code)
    }
    return by_internal, by_external


async def _load_enterprise_name_map(
    session: AsyncSession,
    *,
    enterprise_code: str,
) -> dict[str, BusinessEnterpriseProductName]:
    rows = (
        await session.execute(
            select(BusinessEnterpriseProductName).where(
                BusinessEnterpriseProductName.enterprise_code == _clean_text(enterprise_code),
            )
        )
    ).scalars().all()
    return {
        _clean_text(row.internal_product_code): row
        for row in rows
        if _clean_text(row.internal_product_code)
    }


def _base_product_name(row: Any) -> str:
    return (
        _clean_text(getattr(row, "name_ua", None))
        or _clean_text(getattr(row, "name_ru", None))
        or _clean_text(getattr(row, "sku", None))
    )


def _store_requires_name_mapping(store: BusinessStore) -> bool:
    return _clean_text(getattr(store, "name_strategy", None)).lower() != "base"


def _planned_store_code(store: BusinessStore, internal_product_code: str) -> tuple[str | None, str | None]:
    try:
        external_code = build_external_code_for_store(store, internal_product_code)
    except Exception:
        return None, None

    code_strategy = _clean_text(getattr(store, "code_strategy", None)).lower() or "opaque_mapping"
    if bool(getattr(store, "is_legacy_default", False)) or code_strategy == "legacy_same":
        return external_code, "legacy_same"
    if code_strategy == "prefix_mapping":
        return external_code, "prefix_mapping"
    return external_code, "generated"


async def refresh_custom_catalog_identity_mappings(
    session: AsyncSession,
    *,
    store_id: int,
    dry_run: bool = True,
) -> dict[str, Any]:
    store = await _get_store_or_fail(session, int(store_id))
    runtime_report = await resolve_business_runtime_mode_from_db(
        session,
        _clean_text(store.enterprise_code),
    )
    business_runtime_mode = _clean_text(runtime_report.get("business_runtime_mode"))
    if business_runtime_mode != CUSTOM_BUSINESS_RUNTIME_MODE:
        return {
            "status": "skipped",
            "store_id": int(store.id),
            "store_code": store.store_code,
            "enterprise_code": store.enterprise_code,
            "business_runtime_mode": business_runtime_mode,
            "candidate_source": None,
            "candidate_products": 0,
            "warnings": [],
            "errors": [],
            "skip_reason": "baseline_runtime_mode",
        }

    scope = await resolve_store_catalog_candidate_scope(
        session,
        int(store.id),
        preferred_source="store_native_offers",
        respect_catalog_only_in_stock=False,
    )
    candidate_rows = list(scope.get("candidate_rows") or [])
    candidate_codes = [
        _clean_text(row.sku)
        for row in candidate_rows
        if _clean_text(getattr(row, "sku", None))
    ]
    candidate_name_by_code = {
        _clean_text(row.sku): _base_product_name(row)
        for row in candidate_rows
        if _clean_text(getattr(row, "sku", None))
    }

    store_code_map = await _load_store_code_map(session, store_id=int(store.id))
    store_name_map = await _load_store_name_map(session, store_id=int(store.id))
    enterprise_code_by_internal, enterprise_code_by_external = await _load_enterprise_code_maps(
        session,
        enterprise_code=_clean_text(store.enterprise_code),
    )
    enterprise_name_by_internal = await _load_enterprise_name_map(
        session,
        enterprise_code=_clean_text(store.enterprise_code),
    )
    existing_store_codes_initial = len(store_code_map)
    existing_store_names_initial = len(store_name_map)
    existing_enterprise_codes_initial = len(enterprise_code_by_internal)
    existing_enterprise_names_initial = len(enterprise_name_by_internal)

    warnings = list(scope.get("warnings") or [])
    errors: list[str] = []
    samples: list[dict[str, Any]] = []

    created_store_codes = 0
    created_store_names = 0
    created_enterprise_codes = 0
    created_enterprise_names = 0
    skipped_store_names = 0
    skipped_enterprise_codes = 0
    skipped_enterprise_names = 0

    for internal_product_code in candidate_codes:
        store_code_row = store_code_map.get(internal_product_code)
        if store_code_row is None:
            if bool(dry_run):
                planned_external_code, planned_code_source = _planned_store_code(store, internal_product_code)
                if planned_external_code:
                    created_store_codes += 1
                    store_code_row = BusinessStoreProductCode(
                        store_id=int(store.id),
                        internal_product_code=internal_product_code,
                        external_product_code=planned_external_code,
                        code_source=planned_code_source or "generated",
                        is_active=True,
                    )
            else:
                store_code_row = await ensure_store_product_code(session, int(store.id), internal_product_code)
                store_code_map[internal_product_code] = store_code_row
                created_store_codes += 1

        if _store_requires_name_mapping(store):
            store_name_row = store_name_map.get(internal_product_code)
            if store_name_row is None:
                if bool(dry_run):
                    dry_name_candidate = choose_stable_random_name(
                        int(store.id),
                        internal_product_code,
                        await get_supplier_name_candidates_for_store_product(
                            session,
                            store,
                            internal_product_code,
                        ),
                    )
                    if dry_name_candidate is None:
                        skipped_store_names += 1
                    else:
                        created_store_names += 1
                else:
                    store_name_row = await ensure_store_product_name(session, int(store.id), internal_product_code)
                    if store_name_row is None:
                        skipped_store_names += 1
                    else:
                        store_name_map[internal_product_code] = store_name_row
                        created_store_names += 1

        existing_enterprise_code = enterprise_code_by_internal.get(internal_product_code)
        if existing_enterprise_code is None:
            if store_code_row is None or not _clean_text(store_code_row.external_product_code):
                skipped_enterprise_codes += 1
                continue
            conflicting = enterprise_code_by_external.get(_clean_text(store_code_row.external_product_code))
            if conflicting is not None and _clean_text(conflicting.internal_product_code) != internal_product_code:
                skipped_enterprise_codes += 1
                warning = (
                    "Enterprise code conflict for internal_product_code="
                    f"{internal_product_code} external_product_code={store_code_row.external_product_code}"
                )
                warnings.append(warning)
                if len(samples) < 20:
                    samples.append(
                        {
                            "internal_product_code": internal_product_code,
                            "reason": "enterprise_code_conflict",
                            "external_product_code": _clean_text(store_code_row.external_product_code),
                        }
                    )
                continue
            created_enterprise_codes += 1
            if not bool(dry_run):
                obj = BusinessEnterpriseProductCode(
                    enterprise_code=_clean_text(store.enterprise_code),
                    internal_product_code=internal_product_code,
                    external_product_code=_clean_text(store_code_row.external_product_code),
                    code_source=_clean_text(store_code_row.code_source) or "backfilled_from_store",
                    is_active=bool(store_code_row.is_active),
                )
                session.add(obj)
                await session.flush()
                enterprise_code_by_internal[internal_product_code] = obj
                enterprise_code_by_external[_clean_text(obj.external_product_code)] = obj

        existing_enterprise_name = enterprise_name_by_internal.get(internal_product_code)
        if existing_enterprise_name is None:
            name_value = None
            name_source = None
            source_supplier_id = None
            source_supplier_code = None
            source_supplier_product_id = None
            source_supplier_product_name_raw = None

            if _store_requires_name_mapping(store):
                store_name_row = store_name_map.get(internal_product_code)
                if store_name_row is not None:
                    name_value = _clean_text(store_name_row.external_product_name)
                    name_source = _clean_text(store_name_row.name_source) or "backfilled_from_store"
                    source_supplier_id = store_name_row.source_supplier_id
                    source_supplier_code = store_name_row.source_supplier_code
                    source_supplier_product_id = store_name_row.source_supplier_product_id
                    source_supplier_product_name_raw = store_name_row.source_supplier_product_name_raw
            else:
                name_value = candidate_name_by_code.get(internal_product_code)
                name_source = "cleaned"

            if not _clean_text(name_value):
                skipped_enterprise_names += 1
                continue

            created_enterprise_names += 1
            if not bool(dry_run):
                obj = BusinessEnterpriseProductName(
                    enterprise_code=_clean_text(store.enterprise_code),
                    internal_product_code=internal_product_code,
                    external_product_name=_clean_text(name_value),
                    name_source=_clean_text(name_source) or "cleaned",
                    source_supplier_id=source_supplier_id,
                    source_supplier_code=source_supplier_code,
                    source_supplier_product_id=source_supplier_product_id,
                    source_supplier_product_name_raw=source_supplier_product_name_raw,
                    is_active=True,
                )
                session.add(obj)
                await session.flush()
                enterprise_name_by_internal[internal_product_code] = obj

    result = {
        "status": "ok" if not errors else "error",
        "dry_run": bool(dry_run),
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": store.enterprise_code,
        "business_runtime_mode": business_runtime_mode,
        "candidate_source": scope.get("catalog_source"),
        "candidate_source_type": scope.get("candidate_source_type"),
        "candidate_products": len(candidate_codes),
        "existing_store_codes": existing_store_codes_initial,
        "existing_store_names": existing_store_names_initial,
        "existing_enterprise_codes": existing_enterprise_codes_initial,
        "existing_enterprise_names": existing_enterprise_names_initial,
        "created_store_codes": created_store_codes,
        "created_store_names": created_store_names,
        "created_enterprise_codes": created_enterprise_codes,
        "created_enterprise_names": created_enterprise_names,
        "skipped_store_names": skipped_store_names,
        "skipped_enterprise_codes": skipped_enterprise_codes,
        "skipped_enterprise_names": skipped_enterprise_names,
        "warnings": warnings,
        "errors": errors,
        "samples": samples,
    }

    logger.info(
        (
            "Business custom catalog identity refresh: enterprise_code=%s store_code=%s dry_run=%s "
            "candidate_source=%s candidate_products=%s created_store_codes=%s created_store_names=%s "
            "created_enterprise_codes=%s created_enterprise_names=%s skipped_enterprise_codes=%s skipped_enterprise_names=%s"
        ),
        store.enterprise_code,
        store.store_code,
        bool(dry_run),
        result["candidate_source"],
        result["candidate_products"],
        created_store_codes,
        created_store_names,
        created_enterprise_codes,
        created_enterprise_names,
        skipped_enterprise_codes,
        skipped_enterprise_names,
    )
    for warning in warnings[:20]:
        logger.warning(
            "Business custom catalog identity refresh warning: enterprise_code=%s store_code=%s warning=%s",
            store.enterprise_code,
            store.store_code,
            warning,
        )
    return result

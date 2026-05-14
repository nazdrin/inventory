from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BusinessEnterpriseProductCode,
    BusinessEnterpriseProductName,
    BusinessStore,
    DeveloperSettings,
    EnterpriseSettings,
    MasterCatalog,
)
from app.business.business_store_catalog_preview import resolve_store_catalog_candidate_scope


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _base_product_name(row: MasterCatalog) -> str:
    return _clean_text(row.name_ua) or _clean_text(row.name_ru) or _clean_text(row.sku)


def _preview_brand(row: MasterCatalog) -> str | None:
    normalized = _clean_text(getattr(row, "brand", None))
    return normalized or None


def _preview_barcode(row: MasterCatalog) -> str | None:
    normalized = _clean_text(getattr(row, "barcode", None))
    return normalized or None


def _preview_manufacturer(row: MasterCatalog) -> str | None:
    normalized = _clean_text(getattr(row, "manufacturer", None))
    return normalized or None


async def _get_enterprise_or_fail(session: AsyncSession, enterprise_code: str) -> EnterpriseSettings:
    normalized_enterprise_code = _clean_text(enterprise_code)
    if not normalized_enterprise_code:
        raise ValueError("enterprise_code is required")

    row = (
        await session.execute(
            select(EnterpriseSettings).where(
                EnterpriseSettings.enterprise_code == normalized_enterprise_code
            ).limit(1)
        )
    ).scalar_one_or_none()
    if row is None:
        raise ValueError(f"EnterpriseSettings not found for enterprise_code={normalized_enterprise_code}")
    return row


async def _get_developer_settings(session: AsyncSession) -> DeveloperSettings | None:
    return (await session.execute(select(DeveloperSettings).limit(1))).scalar_one_or_none()


async def resolve_enterprise_catalog_scope_store(
    session: AsyncSession,
    enterprise: EnterpriseSettings,
) -> tuple[BusinessStore | None, str | None]:
    normalized_enterprise_code = _clean_text(enterprise.enterprise_code)
    branch_id = _clean_text(enterprise.branch_id)
    if not normalized_enterprise_code or not branch_id:
        return None, "missing_catalog_scope_store"

    rows = (
        await session.execute(
            select(BusinessStore).where(
                BusinessStore.enterprise_code == normalized_enterprise_code,
                BusinessStore.tabletki_branch == branch_id,
                BusinessStore.is_active.is_(True),
            )
        )
    ).scalars().all()

    if not rows:
        return None, "missing_catalog_scope_store"
    if len(rows) > 1:
        return None, "ambiguous_catalog_scope_store"
    return rows[0], None


async def _load_enterprise_product_code_map(
    session: AsyncSession,
    enterprise_code: str,
) -> dict[str, BusinessEnterpriseProductCode]:
    rows = (
        await session.execute(
            select(BusinessEnterpriseProductCode).where(
                BusinessEnterpriseProductCode.enterprise_code == _clean_text(enterprise_code),
                BusinessEnterpriseProductCode.is_active.is_(True),
            )
        )
    ).scalars().all()
    return {
        _clean_text(row.internal_product_code): row
        for row in rows
        if _clean_text(row.internal_product_code)
    }


async def _load_enterprise_product_name_map(
    session: AsyncSession,
    enterprise_code: str,
) -> dict[str, BusinessEnterpriseProductName]:
    rows = (
        await session.execute(
            select(BusinessEnterpriseProductName).where(
                BusinessEnterpriseProductName.enterprise_code == _clean_text(enterprise_code),
                BusinessEnterpriseProductName.is_active.is_(True),
            )
        )
    ).scalars().all()
    return {
        _clean_text(row.internal_product_code): row
        for row in rows
        if _clean_text(row.internal_product_code)
    }


async def build_enterprise_catalog_payload_preview(
    session: AsyncSession,
    enterprise_code: str,
    limit: int | None = None,
    assortment_mode: str = "master_all",
    store_id: int | None = None,
) -> dict[str, Any]:
    warnings: list[str] = []
    errors: list[str] = []
    normalized_assortment_mode = _clean_text(assortment_mode).lower() or "master_all"

    try:
        enterprise = await _get_enterprise_or_fail(session, enterprise_code)
    except ValueError as exc:
        return {
            "status": "error",
            "enterprise_code": _clean_text(enterprise_code),
            "enterprise_name": None,
            "assortment_mode": normalized_assortment_mode,
            "candidate_source": None,
            "store_id_used_for_assortment": int(store_id) if store_id is not None else None,
            "catalog_scope_store_id": None,
            "catalog_scope_store_code": None,
            "catalog_scope_branch": None,
            "catalog_scope_key": None,
            "catalog_scope_source": None,
            "catalog_scope_error_reason": None,
            "catalog_only_in_stock_source": None,
            "catalog_only_in_stock": None,
            "tabletki_branch": None,
            "master_catalog_total": 0,
            "candidate_products": 0,
            "exportable_products": 0,
            "not_exportable_products": 0,
            "missing_code_mapping": 0,
            "missing_name_mapping": 0,
            "endpoint_preview": None,
            "sample_rows": [],
            "payload_preview": [],
            "warnings": warnings,
            "errors": [str(exc)],
        }

    normalized_enterprise_code = _clean_text(enterprise.enterprise_code)
    branch_id = _clean_text(enterprise.branch_id)
    if _clean_text(enterprise.data_format).lower() != "business":
        warnings.append(
            f"EnterpriseSettings.data_format={enterprise.data_format!r}; expected 'Business' for enterprise-level catalog preview."
        )
    if not bool(enterprise.catalog_enabled):
        warnings.append("EnterpriseSettings.catalog_enabled is false; preview is read-only and does not switch runtime.")
    if not branch_id:
        errors.append("EnterpriseSettings.branch_id is required for enterprise-level catalog preview.")

    developer_settings = await _get_developer_settings(session)
    endpoint_preview = None
    if developer_settings is None:
        warnings.append("DeveloperSettings not found; endpoint_preview is unavailable.")
    elif not _clean_text(developer_settings.endpoint_catalog):
        warnings.append("DeveloperSettings.endpoint_catalog is empty; endpoint_preview is unavailable.")
    elif branch_id:
        endpoint_preview = f"{_clean_text(developer_settings.endpoint_catalog)}/Import/Ref/{branch_id}"

    candidate_source = "master_all"
    store_id_used_for_assortment: int | None = None
    catalog_scope_store: BusinessStore | None = None
    catalog_scope_error_reason: str | None = None
    catalog_scope_source: str | None = None
    catalog_only_in_stock_source: str | None = None
    catalog_only_in_stock: bool | None = None
    if normalized_assortment_mode == "master_all":
        stmt = select(MasterCatalog)
        if hasattr(MasterCatalog, "is_archived"):
            stmt = stmt.where(MasterCatalog.is_archived.is_(False))
        stmt = stmt.order_by(MasterCatalog.sku.asc())
        master_rows = list((await session.execute(stmt)).scalars().all())
        candidate_rows = master_rows
    elif normalized_assortment_mode == "store_compatible":
        catalog_scope_store, catalog_scope_error = await resolve_enterprise_catalog_scope_store(session, enterprise)
        catalog_scope_source = "enterprise_branch_match"
        catalog_only_in_stock_source = "catalog_scope_store"
        if catalog_scope_error == "missing_catalog_scope_store":
            catalog_scope_error_reason = catalog_scope_error
            errors.append(
                "No active BusinessStore found for enterprise catalog scope: expected exactly one store with tabletki_branch matching EnterpriseSettings.branch_id."
            )
            master_rows = []
            candidate_rows = []
        elif catalog_scope_error == "ambiguous_catalog_scope_store":
            catalog_scope_error_reason = catalog_scope_error
            errors.append(
                "Ambiguous enterprise catalog scope: multiple active BusinessStore rows match EnterpriseSettings.branch_id."
            )
            master_rows = []
            candidate_rows = []
        else:
            catalog_only_in_stock = bool(catalog_scope_store.catalog_only_in_stock)
            scope = await resolve_store_catalog_candidate_scope(
                session,
                int(catalog_scope_store.id),
                preferred_source="store_native_offers",
            )
            store = scope["store"]
            store_enterprise_code = _clean_text(store.enterprise_code)
            if store_enterprise_code and store_enterprise_code != normalized_enterprise_code:
                errors.append(
                    "catalog scope store does not belong to the requested enterprise_code."
                )
            warnings.extend(scope["warnings"])
            candidate_source = str(scope["catalog_source"])
            store_id_used_for_assortment = int(store.id)
            master_rows = list(scope["master_rows"])
            candidate_rows = list(scope["candidate_rows"])
    else:
        errors.append(f"Unsupported assortment_mode={normalized_assortment_mode!r}")
        master_rows = []
        candidate_rows = []

    code_map = await _load_enterprise_product_code_map(session, normalized_enterprise_code)
    name_map = await _load_enterprise_product_name_map(session, normalized_enterprise_code)

    if not code_map:
        warnings.append("No active enterprise-level code mappings were found.")
    if not name_map:
        warnings.append("No active enterprise-level name mappings were found.")

    payload_rows: list[dict[str, Any]] = []
    exportable_products = 0
    missing_code_mapping = 0
    missing_name_mapping = 0

    for row in candidate_rows:
        internal_product_code = _clean_text(row.sku)
        if not internal_product_code:
            continue

        reasons: list[str] = []
        code_mapping = code_map.get(internal_product_code)
        name_mapping = name_map.get(internal_product_code)
        external_product_code = _clean_text(code_mapping.external_product_code) if code_mapping is not None else None
        external_product_name = _clean_text(name_mapping.external_product_name) if name_mapping is not None else None

        if not external_product_code:
            reasons.append("missing_enterprise_code_mapping")
            missing_code_mapping += 1
        if not external_product_name:
            reasons.append("missing_enterprise_name_mapping")
            missing_name_mapping += 1

        exportable = len(reasons) == 0 and not errors
        if exportable:
            exportable_products += 1

        payload_rows.append(
            {
                "internal_product_code": internal_product_code,
                "external_product_code": external_product_code,
                "base_name": _base_product_name(row),
                "external_product_name": external_product_name,
                "barcode": _preview_barcode(row),
                "manufacturer": _preview_manufacturer(row),
                "brand": _preview_brand(row),
                "tabletki_enterprise_code": normalized_enterprise_code,
                "tabletki_branch": branch_id or None,
                "exportable": exportable,
                "reasons": reasons,
            }
        )

    limited_payload_rows = payload_rows if limit is None else payload_rows[: max(0, int(limit))]
    not_exportable_products = max(0, len(candidate_rows) - exportable_products)

    status = "ok"
    if errors:
        status = "error"
    elif warnings:
        status = "warning"

    return {
        "status": status,
        "enterprise_code": normalized_enterprise_code,
        "enterprise_name": _clean_text(enterprise.enterprise_name) or None,
        "enterprise_catalog_enabled": bool(enterprise.catalog_enabled),
        "catalog_gate_source": "enterprise_settings",
        "assortment_mode": normalized_assortment_mode,
        "candidate_source": candidate_source,
        "store_id_used_for_assortment": store_id_used_for_assortment,
        "catalog_scope_store_id": int(catalog_scope_store.id) if catalog_scope_store is not None else None,
        "catalog_scope_store_code": _clean_text(catalog_scope_store.store_code) or None if catalog_scope_store is not None else None,
        "catalog_scope_branch": _clean_text(catalog_scope_store.tabletki_branch) or None if catalog_scope_store is not None else None,
        "catalog_scope_key": _clean_text(catalog_scope_store.legacy_scope_key) or None if catalog_scope_store is not None else None,
        "catalog_scope_source": catalog_scope_source,
        "catalog_scope_error_reason": catalog_scope_error_reason,
        "catalog_only_in_stock_source": catalog_only_in_stock_source,
        "catalog_only_in_stock": catalog_only_in_stock,
        "tabletki_branch": branch_id or None,
        "catalog_runtime_path": "enterprise_identity",
        "master_catalog_total": len(master_rows),
        "candidate_products": len(candidate_rows),
        "exportable_products": exportable_products,
        "not_exportable_products": not_exportable_products,
        "missing_code_mapping": missing_code_mapping,
        "missing_name_mapping": missing_name_mapping,
        "endpoint_preview": endpoint_preview,
        "sample_rows": limited_payload_rows[:20],
        "payload_preview": limited_payload_rows,
        "warnings": warnings,
        "errors": errors,
    }

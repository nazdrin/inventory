from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_baseline_catalog_preview import (
    build_baseline_business_catalog_payload_preview,
)
from app.models import (
    BusinessStore,
    BusinessStoreOffer,
    BusinessStoreProductCode,
    BusinessStoreProductName,
    MasterCatalog,
    Offer,
)
from app.services.business_runtime_mode_service import (
    BASELINE_BUSINESS_RUNTIME_MODE,
    resolve_business_runtime_mode_from_db,
)


def _base_product_name(row: MasterCatalog) -> str:
    return str(row.name_ua or "").strip() or str(row.name_ru or "").strip() or str(row.sku or "").strip()


async def _get_store_or_fail(session: AsyncSession, store_id: int) -> BusinessStore:
    store = (
        await session.execute(
            select(BusinessStore).where(BusinessStore.id == int(store_id)).limit(1)
        )
    ).scalar_one_or_none()
    if store is None:
        raise ValueError(f"BusinessStore not found for store_id={store_id}")
    return store


async def _load_store_product_code_map(
    session: AsyncSession,
    store_id: int,
) -> dict[str, BusinessStoreProductCode]:
    rows = (
        await session.execute(
            select(BusinessStoreProductCode).where(
                BusinessStoreProductCode.store_id == int(store_id),
                BusinessStoreProductCode.is_active.is_(True),
            )
        )
    ).scalars().all()
    return {str(row.internal_product_code): row for row in rows}


async def _load_store_product_name_map(
    session: AsyncSession,
    store_id: int,
) -> dict[str, BusinessStoreProductName]:
    rows = (
        await session.execute(
            select(BusinessStoreProductName).where(
                BusinessStoreProductName.store_id == int(store_id),
                BusinessStoreProductName.is_active.is_(True),
            )
        )
    ).scalars().all()
    return {str(row.internal_product_code): row for row in rows}


async def _stock_limited_product_codes(
    session: AsyncSession,
    store: BusinessStore,
) -> tuple[set[str], list[str]]:
    warnings: list[str] = []
    legacy_scope_key = str(store.legacy_scope_key or "").strip()
    if not legacy_scope_key:
        warnings.append("Store has empty legacy_scope_key; stock-limited catalog preview cannot resolve offers scope.")
        return set(), warnings

    rows = (
        await session.execute(
            select(Offer.product_code)
            .where(
                Offer.city == legacy_scope_key,
                func.coalesce(Offer.stock, 0) > 0,
            )
            .distinct()
        )
    ).scalars().all()
    product_codes = {
        str(value or "").strip()
        for value in rows
        if str(value or "").strip()
    }
    return product_codes, warnings


async def _store_native_product_codes(
    session: AsyncSession,
    store: BusinessStore,
    *,
    stock_only: bool | None = None,
) -> tuple[set[str], list[str]]:
    warnings: list[str] = []

    stmt = select(BusinessStoreOffer.product_code).where(
        BusinessStoreOffer.store_id == int(store.id),
    )
    use_stock_only = bool(store.catalog_only_in_stock) if stock_only is None else bool(stock_only)
    if use_stock_only:
        stmt = stmt.where(func.coalesce(BusinessStoreOffer.stock, 0) > 0)

    rows = (await session.execute(stmt.distinct())).scalars().all()
    product_codes = {
        str(value or "").strip()
        for value in rows
        if str(value or "").strip()
    }
    if not product_codes:
        warnings.append(
            "No business_store_offers rows found for store-native catalog scope."
        )
    return product_codes, warnings


async def resolve_store_catalog_candidate_scope(
    session: AsyncSession,
    store_id: int,
    *,
    preferred_source: str | None = None,
    respect_catalog_only_in_stock: bool = True,
) -> dict[str, Any]:
    store = await _get_store_or_fail(session, int(store_id))

    warnings: list[str] = []
    normalized_source = str(preferred_source or "").strip().lower() or "legacy_offers"
    if normalized_source not in {"legacy_offers", "store_native_offers"}:
        raise ValueError(f"Unsupported preferred_source={preferred_source!r}")

    candidate_product_codes: set[str] = set()
    if normalized_source == "store_native_offers":
        use_stock_only = bool(store.catalog_only_in_stock) and bool(respect_catalog_only_in_stock)
        candidate_product_codes, stock_warnings = await _store_native_product_codes(
            session,
            store,
            stock_only=use_stock_only,
        )
        warnings.extend(stock_warnings)
        catalog_source = (
            "stock_limited_store_native_offers"
            if use_stock_only
            else "all_store_native_offers"
        )
    else:
        catalog_source = "stock_limited" if bool(store.catalog_only_in_stock) else "all_products"
        if store.catalog_only_in_stock:
            candidate_product_codes, stock_warnings = await _stock_limited_product_codes(session, store)
            warnings.extend(stock_warnings)

    stmt = select(MasterCatalog)
    if hasattr(MasterCatalog, "is_archived"):
        stmt = stmt.where(MasterCatalog.is_archived.is_(False))
    stmt = stmt.order_by(MasterCatalog.sku.asc())
    master_rows = list((await session.execute(stmt)).scalars().all())

    candidate_rows = master_rows
    if normalized_source == "store_native_offers" or store.catalog_only_in_stock:
        candidate_rows = [
            row for row in master_rows
            if str(row.sku or "").strip() in candidate_product_codes
        ]

    return {
        "store": store,
        "warnings": warnings,
        "catalog_source": catalog_source,
        "master_rows": master_rows,
        "candidate_rows": candidate_rows,
        "candidate_product_codes": candidate_product_codes,
        "candidate_source_type": normalized_source,
    }


def _preview_brand(row: MasterCatalog) -> str | None:
    value = getattr(row, "brand", None)
    normalized = str(value or "").strip()
    return normalized or None


def _preview_barcode(row: MasterCatalog) -> str | None:
    normalized = str(getattr(row, "barcode", "") or "").strip()
    return normalized or None


def _preview_manufacturer(row: MasterCatalog) -> str | None:
    normalized = str(getattr(row, "manufacturer", "") or "").strip()
    return normalized or None


async def build_store_catalog_payload_preview(
    session: AsyncSession,
    store_id: int,
    limit: int | None = None,
    include_not_exportable: bool = True,
) -> dict[str, Any]:
    scope = await resolve_store_catalog_candidate_scope(session, int(store_id))
    store = scope["store"]
    warnings: list[str] = list(scope["warnings"])
    catalog_source = str(scope["catalog_source"])
    master_rows = list(scope["master_rows"])
    candidate_rows = list(scope["candidate_rows"])

    if not str(store.tabletki_enterprise_code or "").strip():
        warnings.append("Store tabletki_enterprise_code is empty; preview target is incomplete.")
    if not str(store.tabletki_branch or "").strip():
        warnings.append("Store tabletki_branch is empty; preview target is incomplete.")

    code_map = await _load_store_product_code_map(session, int(store.id))
    name_map = await _load_store_product_name_map(session, int(store.id))

    code_strategy = str(store.code_strategy or "legacy_same").strip().lower() or "legacy_same"
    name_strategy = str(store.name_strategy or "base").strip().lower() or "base"

    payload_rows: list[dict[str, Any]] = []
    not_exportable_samples: list[dict[str, Any]] = []
    exportable_products = 0
    missing_code_mapping = 0
    missing_name_mapping = 0

    for row in candidate_rows:
        internal_product_code = str(row.sku or "").strip()
        if not internal_product_code:
            continue

        base_name = _base_product_name(row)
        reasons: list[str] = []

        if bool(store.is_legacy_default) or code_strategy == "legacy_same":
            external_product_code = internal_product_code
        else:
            code_mapping = code_map.get(internal_product_code)
            external_product_code = code_mapping.external_product_code if code_mapping is not None else None
            if external_product_code is None:
                reasons.append("missing_code_mapping")
                missing_code_mapping += 1

        if name_strategy == "base":
            external_product_name = base_name
        else:
            name_mapping = name_map.get(internal_product_code)
            external_product_name = name_mapping.external_product_name if name_mapping is not None else None
            if external_product_name is None:
                reasons.append("missing_name_mapping")
                missing_name_mapping += 1

        exportable = len(reasons) == 0
        if exportable:
            exportable_products += 1
        elif len(not_exportable_samples) < 50:
            not_exportable_samples.append(
                {
                    "internal_product_code": internal_product_code,
                    "external_product_code": external_product_code,
                    "base_name": base_name,
                    "external_product_name": external_product_name,
                    "reasons": reasons,
                }
            )

        preview_row = {
            "internal_product_code": internal_product_code,
            "external_product_code": external_product_code,
            "base_name": base_name,
            "external_product_name": external_product_name,
            "barcode": _preview_barcode(row),
            "manufacturer": _preview_manufacturer(row),
            "brand": _preview_brand(row),
            "exportable": exportable,
            "reasons": reasons,
        }
        if include_not_exportable or exportable:
            payload_rows.append(preview_row)

    if name_strategy == "supplier_random" and not name_map:
        warnings.append("Store name_strategy=supplier_random but no active name mappings were found.")
    if not (bool(store.is_legacy_default) or code_strategy == "legacy_same") and not code_map:
        warnings.append("Store code_strategy requires external mappings but no active code mappings were found.")

    limited_payload_rows = payload_rows if limit is None else payload_rows[: max(0, int(limit))]

    return {
        "status": "ok",
        "identity_mode": "store_level",
        "assortment_mode": "store_native",
        "store": {
            "store_id": int(store.id),
            "store_code": store.store_code,
            "store_name": store.store_name,
            "enterprise_code": store.enterprise_code,
            "legacy_scope_key": store.legacy_scope_key,
            "tabletki_enterprise_code": store.tabletki_enterprise_code,
            "tabletki_branch": store.tabletki_branch,
            "catalog_only_in_stock": bool(store.catalog_only_in_stock),
            "code_strategy": store.code_strategy,
            "name_strategy": store.name_strategy,
        },
        "summary": {
            "master_catalog_total": len(master_rows),
            "candidate_products": len(candidate_rows),
            "exportable_products": exportable_products,
            "not_exportable_products": max(0, len(candidate_rows) - exportable_products),
            "missing_code_mapping": missing_code_mapping,
            "missing_name_mapping": missing_name_mapping,
            "catalog_source": catalog_source,
            "enterprise_catalog_enabled": None,
            "store_catalog_enabled_deprecated": bool(store.catalog_enabled),
            "catalog_gate_source": "store_and_enterprise_legacy",
            "catalog_scope_store_id": int(store.id),
            "catalog_scope_store_code": store.store_code,
            "catalog_scope_branch": store.tabletki_branch,
            "catalog_scope_key": store.legacy_scope_key,
            "catalog_scope_source": "selected_store_legacy",
            "catalog_only_in_stock_source": "selected_store_legacy",
            "catalog_only_in_stock": bool(store.catalog_only_in_stock),
            "target_branch": store.tabletki_branch,
            "target_branch_source": "business_store",
        },
        "payload_preview": limited_payload_rows,
        "not_exportable_samples": not_exportable_samples,
        "warnings": warnings,
    }


async def build_effective_business_store_catalog_payload_preview(
    session: AsyncSession,
    store_id: int,
    limit: int | None = None,
    include_not_exportable: bool = True,
) -> dict[str, Any]:
    store = await _get_store_or_fail(session, int(store_id))
    runtime_mode_report = await resolve_business_runtime_mode_from_db(session, str(store.enterprise_code or ""))

    if runtime_mode_report.get("business_runtime_mode") == BASELINE_BUSINESS_RUNTIME_MODE:
        baseline_preview = await build_baseline_business_catalog_payload_preview(
            session,
            enterprise_code=str(store.enterprise_code or ""),
            limit=limit,
        )
        payload_rows = list(baseline_preview.get("payload_preview") or [])
        if not include_not_exportable:
            payload_rows = [row for row in payload_rows if bool(row.get("exportable"))]

        return {
            "status": baseline_preview.get("status", "ok"),
            "identity_mode": "baseline_legacy",
            "assortment_mode": baseline_preview.get("assortment_mode") or "baseline_legacy",
            "business_runtime_mode": runtime_mode_report.get("business_runtime_mode"),
            "runtime_mode_source": runtime_mode_report.get("runtime_mode_source"),
            "catalog_runtime_path": runtime_mode_report.get("catalog_runtime_path"),
            "store": {
                "store_id": int(store.id),
                "store_code": store.store_code,
                "store_name": store.store_name,
                "enterprise_code": store.enterprise_code,
                "legacy_scope_key": store.legacy_scope_key,
                "tabletki_enterprise_code": store.tabletki_enterprise_code,
                "tabletki_branch": store.tabletki_branch,
                "catalog_only_in_stock": bool(store.catalog_only_in_stock),
                "code_strategy": store.code_strategy,
                "name_strategy": store.name_strategy,
                "target_branch": baseline_preview.get("tabletki_branch"),
            },
            "summary": {
                "master_catalog_total": baseline_preview.get("master_catalog_total", 0),
                "candidate_products": baseline_preview.get("candidate_products", 0),
                "exportable_products": baseline_preview.get("exportable_products", 0),
                "not_exportable_products": baseline_preview.get("not_exportable_products", 0),
                "missing_code_mapping": 0,
                "missing_name_mapping": 0,
                "catalog_source": baseline_preview.get("candidate_source"),
                "enterprise_catalog_enabled": baseline_preview.get("enterprise_catalog_enabled"),
                "store_catalog_enabled_deprecated": bool(store.catalog_enabled),
                "catalog_gate_source": "enterprise_settings",
                "catalog_scope_store_id": None,
                "catalog_scope_store_code": None,
                "catalog_scope_branch": None,
                "catalog_scope_key": None,
                "catalog_scope_source": "enterprise_settings",
                "catalog_only_in_stock_source": None,
                "catalog_only_in_stock": None,
                "target_branch": baseline_preview.get("tabletki_branch"),
                "target_branch_source": "enterprise_settings",
                "endpoint_preview": baseline_preview.get("endpoint_preview"),
            },
            "payload_preview": payload_rows,
            "not_exportable_samples": [],
            "warnings": list(baseline_preview.get("warnings") or []),
            "errors": list(baseline_preview.get("errors") or []),
        }

    from app.business.business_enterprise_catalog_preview import (
        build_enterprise_catalog_payload_preview,
    )

    enterprise_preview = await build_enterprise_catalog_payload_preview(
        session,
        enterprise_code=str(store.enterprise_code or ""),
        limit=limit,
        assortment_mode="store_compatible",
        store_id=int(store.id),
    )

    payload_rows = list(enterprise_preview.get("payload_preview") or [])
    if not include_not_exportable:
        payload_rows = [row for row in payload_rows if bool(row.get("exportable"))]

    not_exportable_samples = [
        {
            "internal_product_code": row.get("internal_product_code"),
            "external_product_code": row.get("external_product_code"),
            "base_name": row.get("base_name"),
            "external_product_name": row.get("external_product_name"),
            "reasons": list(row.get("reasons") or []),
        }
        for row in (enterprise_preview.get("payload_preview") or [])
        if not bool(row.get("exportable"))
    ][:50]

    return {
        "status": enterprise_preview.get("status", "ok"),
        "identity_mode": "enterprise_level",
        "assortment_mode": enterprise_preview.get("assortment_mode") or "store_compatible",
        "business_runtime_mode": runtime_mode_report.get("business_runtime_mode"),
        "runtime_mode_source": runtime_mode_report.get("runtime_mode_source"),
        "catalog_runtime_path": runtime_mode_report.get("catalog_runtime_path"),
        "store": {
            "store_id": int(store.id),
            "store_code": store.store_code,
            "store_name": store.store_name,
            "enterprise_code": store.enterprise_code,
            "legacy_scope_key": store.legacy_scope_key,
            "tabletki_enterprise_code": store.tabletki_enterprise_code,
            "tabletki_branch": store.tabletki_branch,
            "catalog_only_in_stock": bool(store.catalog_only_in_stock),
            "code_strategy": store.code_strategy,
            "name_strategy": store.name_strategy,
            "target_branch": enterprise_preview.get("tabletki_branch"),
        },
        "summary": {
            "master_catalog_total": enterprise_preview.get("master_catalog_total", 0),
            "candidate_products": enterprise_preview.get("candidate_products", 0),
            "exportable_products": enterprise_preview.get("exportable_products", 0),
            "not_exportable_products": enterprise_preview.get("not_exportable_products", 0),
            "missing_code_mapping": enterprise_preview.get("missing_code_mapping", 0),
            "missing_name_mapping": enterprise_preview.get("missing_name_mapping", 0),
            "catalog_source": enterprise_preview.get("candidate_source"),
            "enterprise_catalog_enabled": enterprise_preview.get("enterprise_catalog_enabled"),
            "store_catalog_enabled_deprecated": bool(store.catalog_enabled),
            "catalog_gate_source": "enterprise_settings",
            "catalog_scope_store_id": enterprise_preview.get("catalog_scope_store_id"),
            "catalog_scope_store_code": enterprise_preview.get("catalog_scope_store_code"),
            "catalog_scope_branch": enterprise_preview.get("catalog_scope_branch"),
            "catalog_scope_key": enterprise_preview.get("catalog_scope_key"),
            "catalog_scope_source": enterprise_preview.get("catalog_scope_source"),
            "catalog_only_in_stock_source": enterprise_preview.get("catalog_only_in_stock_source"),
            "catalog_only_in_stock": enterprise_preview.get("catalog_only_in_stock"),
            "target_branch": enterprise_preview.get("tabletki_branch"),
            "target_branch_source": "enterprise_settings",
            "endpoint_preview": enterprise_preview.get("endpoint_preview"),
        },
        "payload_preview": payload_rows,
        "not_exportable_samples": not_exportable_samples,
        "warnings": list(enterprise_preview.get("warnings") or []),
        "errors": list(enterprise_preview.get("errors") or []),
    }

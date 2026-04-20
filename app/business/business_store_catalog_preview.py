from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BusinessStore,
    BusinessStoreProductCode,
    BusinessStoreProductName,
    MasterCatalog,
    Offer,
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
    store = await _get_store_or_fail(session, int(store_id))

    warnings: list[str] = []
    catalog_source = "stock_limited" if bool(store.catalog_only_in_stock) else "all_products"
    stock_codes: set[str] = set()
    if store.catalog_only_in_stock:
        stock_codes, stock_warnings = await _stock_limited_product_codes(session, store)
        warnings.extend(stock_warnings)

    if not str(store.tabletki_enterprise_code or "").strip():
        warnings.append("Store tabletki_enterprise_code is empty; preview target is incomplete.")
    if not str(store.tabletki_branch or "").strip():
        warnings.append("Store tabletki_branch is empty; preview target is incomplete.")

    stmt = select(MasterCatalog)
    if hasattr(MasterCatalog, "is_archived"):
        stmt = stmt.where(MasterCatalog.is_archived.is_(False))
    stmt = stmt.order_by(MasterCatalog.sku.asc())
    master_rows = list((await session.execute(stmt)).scalars().all())

    candidate_rows = master_rows
    if store.catalog_only_in_stock:
        candidate_rows = [row for row in master_rows if str(row.sku or "").strip() in stock_codes]

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
        },
        "payload_preview": limited_payload_rows,
        "not_exportable_samples": not_exportable_samples,
        "warnings": warnings,
    }

from __future__ import annotations

from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_order_mapper import (
    ORIGINAL_EXTERNAL_GOODS_CODE_FIELD,
    normalize_store_order_payload,
    resolve_business_store_for_order,
)
from app.models import CatalogMapping, CatalogSupplierMapping, InventoryStock, MasterCatalog, Offer


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _stringify_decimal(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _build_mock_order(
    *,
    branch_id: str | None,
    external_product_code: str,
    qty: int,
    price: float | Decimal,
) -> dict[str, Any]:
    return {
        "id": "SIM_ORDER",
        "code": "SIM_ORDER_CODE",
        "branchID": branch_id,
        "rows": [
            {
                "goodsCode": str(external_product_code),
                "goodsName": "Mock product",
                "goodsProducer": "",
                "qty": int(qty),
                "price": _stringify_decimal(price),
            }
        ],
    }


async def _readiness_offers(
    session: AsyncSession,
    *,
    internal_product_code: str,
    legacy_scope_key: str | None,
) -> dict[str, Any]:
    all_rows = (
        await session.execute(
            select(Offer).where(Offer.product_code == str(internal_product_code))
        )
    ).scalars().all()

    scope_rows = [
        row for row in all_rows
        if legacy_scope_key and str(row.city or "").strip() == str(legacy_scope_key)
    ]

    sample_row = scope_rows[0] if scope_rows else (all_rows[0] if all_rows else None)
    return {
        "offers_found_total": len(all_rows),
        "offers_found_for_scope": len(scope_rows),
        "sample": (
            {
                "supplier_code": sample_row.supplier_code,
                "price": _stringify_decimal(sample_row.price),
                "stock": int(sample_row.stock or 0),
                "city": sample_row.city,
            }
            if sample_row is not None
            else None
        ),
    }


async def _readiness_catalog_supplier_mapping(
    session: AsyncSession,
    *,
    internal_product_code: str,
) -> dict[str, Any]:
    rows = (
        await session.execute(
            select(CatalogSupplierMapping).where(
                CatalogSupplierMapping.sku == str(internal_product_code)
            )
        )
    ).scalars().all()
    sample_row = rows[0] if rows else None
    return {
        "supplier_mappings_found": len(rows),
        "sample": (
            {
                "supplier_id": sample_row.supplier_id,
                "supplier_code": sample_row.supplier_code,
                "barcode": sample_row.barcode,
                "supplier_product_name_raw": sample_row.supplier_product_name_raw,
            }
            if sample_row is not None
            else None
        ),
    }


async def _readiness_master_catalog(
    session: AsyncSession,
    *,
    internal_product_code: str,
) -> dict[str, Any]:
    row = (
        await session.execute(
            select(MasterCatalog).where(MasterCatalog.sku == str(internal_product_code)).limit(1)
        )
    ).scalar_one_or_none()
    return {
        "master_catalog_found": row is not None,
        "sample": (
            {
                "is_archived": bool(row.is_archived),
                "name_ua": row.name_ua,
                "name_ru": row.name_ru,
            }
            if row is not None
            else None
        ),
    }


async def _readiness_catalog_mapping(
    session: AsyncSession,
    *,
    internal_product_code: str,
) -> dict[str, Any]:
    row = (
        await session.execute(
            select(CatalogMapping).where(CatalogMapping.ID == str(internal_product_code)).limit(1)
        )
    ).scalar_one_or_none()
    return {
        "catalog_mapping_found": row is not None,
    }


async def _readiness_inventory_stock(
    session: AsyncSession,
    *,
    internal_product_code: str,
    branch_id: str | None,
) -> dict[str, Any]:
    all_rows = (
        await session.execute(
            select(InventoryStock).where(InventoryStock.code == str(internal_product_code))
        )
    ).scalars().all()

    branch_rows = [
        row for row in all_rows
        if branch_id and str(row.branch or "").strip() == str(branch_id)
    ]

    sample_row = branch_rows[0] if branch_rows else (all_rows[0] if all_rows else None)
    return {
        "inventory_stock_found_total": len(all_rows),
        "inventory_stock_found_for_branch": len(branch_rows),
        "sample": (
            {
                "branch": sample_row.branch,
                "qty": int(sample_row.qty or 0),
                "price": _stringify_decimal(sample_row.price),
            }
            if sample_row is not None
            else None
        ),
    }


async def simulate_store_order_after_reverse_mapping(
    session: AsyncSession,
    *,
    store_id: int | None = None,
    store_code: str | None = None,
    tabletki_branch: str | int | None = None,
    tabletki_enterprise_code: str | int | None = None,
    external_product_code: str,
    qty: int = 1,
    price: float | Decimal = 100,
) -> dict[str, Any]:
    warnings: list[str] = []
    errors: list[str] = []

    resolved_store = await resolve_business_store_for_order(
        session,
        store_id=store_id,
        store_code=store_code,
        tabletki_branch=tabletki_branch,
        tabletki_enterprise_code=tabletki_enterprise_code,
    )

    resolved_branch = _clean_text(tabletki_branch)
    if resolved_branch is None and resolved_store is not None:
        resolved_branch = _clean_text(resolved_store.tabletki_branch)

    incoming_order = _build_mock_order(
        branch_id=resolved_branch,
        external_product_code=str(external_product_code),
        qty=int(qty),
        price=price,
    )

    mapper_result = await normalize_store_order_payload(
        session,
        order_payload=incoming_order,
        store_id=store_id,
        store_code=store_code,
        tabletki_branch=tabletki_branch if tabletki_branch is not None else resolved_branch,
        tabletki_enterprise_code=tabletki_enterprise_code,
    )

    mapper_status = str(mapper_result.get("status") or "")
    warnings.extend(list(mapper_result.get("warnings") or []))
    errors.extend(list(mapper_result.get("errors") or []))

    report: dict[str, Any] = {
        "status": mapper_status,
        "mapper_status": mapper_status,
        "store": {
            "store_id": mapper_result.get("store_id"),
            "store_code": mapper_result.get("store_code"),
            "legacy_scope_key": getattr(resolved_store, "legacy_scope_key", None) if resolved_store is not None else None,
            "tabletki_branch": resolved_branch,
        },
        "code_mapping": {
            "external_product_code": str(external_product_code),
            "internal_product_code": None,
            "original_preserved_field": ORIGINAL_EXTERNAL_GOODS_CODE_FIELD,
            "original_preserved_value": None,
        },
        "normalized_order": mapper_result.get("order"),
        "readiness": {},
        "warnings": warnings,
        "errors": errors,
    }

    if mapper_status != "ok":
        return report

    normalized_order = mapper_result.get("order") or {}
    rows = normalized_order.get("rows") or []
    first_row = rows[0] if rows and isinstance(rows[0], dict) else {}
    internal_product_code = _clean_text(first_row.get("goodsCode"))
    original_preserved_value = _clean_text(first_row.get(ORIGINAL_EXTERNAL_GOODS_CODE_FIELD))

    report["code_mapping"] = {
        "external_product_code": str(external_product_code),
        "internal_product_code": internal_product_code,
        "original_preserved_field": ORIGINAL_EXTERNAL_GOODS_CODE_FIELD,
        "original_preserved_value": original_preserved_value,
    }

    if not internal_product_code:
        errors.append("Normalized order does not contain internal goodsCode after successful mapper result.")
        report["status"] = "error"
        return report

    offers_readiness = await _readiness_offers(
        session,
        internal_product_code=internal_product_code,
        legacy_scope_key=_clean_text(getattr(resolved_store, "legacy_scope_key", None)),
    )
    supplier_mapping_readiness = await _readiness_catalog_supplier_mapping(
        session,
        internal_product_code=internal_product_code,
    )
    master_catalog_readiness = await _readiness_master_catalog(
        session,
        internal_product_code=internal_product_code,
    )
    catalog_mapping_readiness = await _readiness_catalog_mapping(
        session,
        internal_product_code=internal_product_code,
    )
    inventory_stock_readiness = await _readiness_inventory_stock(
        session,
        internal_product_code=internal_product_code,
        branch_id=_clean_text(normalized_order.get("branchID")),
    )

    report["readiness"] = {
        "offers": offers_readiness,
        "catalog_supplier_mapping": supplier_mapping_readiness,
        "master_catalog": master_catalog_readiness,
        "catalog_mapping": catalog_mapping_readiness,
        "inventory_stock": inventory_stock_readiness,
    }

    core_ready = bool(
        offers_readiness["offers_found_total"] > 0
        and (
            master_catalog_readiness["master_catalog_found"]
            or supplier_mapping_readiness["supplier_mappings_found"] > 0
        )
    )
    inventory_ready = bool(inventory_stock_readiness["inventory_stock_found_for_branch"] > 0)

    if not core_ready:
        errors.append(
            "Mapped internal product code is not sufficiently represented in Offer + (MasterCatalog or CatalogSupplierMapping)."
        )
        report["status"] = "error"
        return report

    if not inventory_ready:
        warnings.append(
            "InventoryStock does not contain this internal code for the normalized branch; legacy auto_confirm may fail without special handling."
        )
        report["status"] = "warning"
        return report

    report["status"] = "ok"
    return report

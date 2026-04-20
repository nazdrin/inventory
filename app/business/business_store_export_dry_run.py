from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_code_generator import ensure_store_product_code
from app.business.business_store_name_generator import ensure_store_product_name
from app.business.business_store_price_adjustment_generator import (
    apply_extra_markup,
    ensure_store_product_price_adjustment,
)
from app.business.business_store_resolver import get_active_business_stores
from app.models import (
    BusinessStore,
    BusinessStoreProductCode,
    BusinessStoreProductName,
    BusinessStoreProductPriceAdjustment,
    MasterCatalog,
    Offer,
)


@dataclass
class _SelectedOffer:
    internal_product_code: str
    supplier_code: str | None
    qty: int
    price: Decimal | None
    updated_at: Any


async def _get_store_by_id(session: AsyncSession, store_id: int) -> BusinessStore | None:
    result = await session.execute(
        select(BusinessStore).where(BusinessStore.id == int(store_id)).limit(1)
    )
    return result.scalar_one_or_none()


async def _get_store_by_code(session: AsyncSession, store_code: str) -> BusinessStore | None:
    normalized_store_code = str(store_code or "").strip()
    if not normalized_store_code:
        return None

    result = await session.execute(
        select(BusinessStore).where(BusinessStore.store_code == normalized_store_code).limit(1)
    )
    return result.scalar_one_or_none()


def _stringify_decimal(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _base_product_name(row: MasterCatalog) -> str:
    return str(row.name_ua or "").strip() or str(row.name_ru or "").strip() or str(row.sku or "").strip()


def _best_offer_sort_key(offer: Offer) -> tuple[Any, ...]:
    return (
        offer.price if offer.price is not None else Decimal("9999999999"),
        -(int(offer.stock or 0)),
        offer.updated_at or 0,
        str(offer.supplier_code or ""),
    )


def _build_inactive_report(store: BusinessStore) -> dict[str, Any]:
    return {
        "status": "inactive",
        "store_id": int(store.id),
        "store_code": store.store_code,
        "store_name": store.store_name,
        "migration_status": store.migration_status,
        "takes_over_legacy_scope": bool(store.takes_over_legacy_scope),
        "legacy_scope_key": store.legacy_scope_key,
        "tabletki_enterprise_code": store.tabletki_enterprise_code,
        "tabletki_branch": store.tabletki_branch,
        "salesdrive_enterprise_id": store.salesdrive_enterprise_id,
        "warnings": ["Store is inactive; export dry-run was not built."],
    }


async def _load_store_product_code_map(
    session: AsyncSession,
    store_id: int,
) -> dict[str, BusinessStoreProductCode]:
    result = await session.execute(
        select(BusinessStoreProductCode).where(
            BusinessStoreProductCode.store_id == int(store_id),
            BusinessStoreProductCode.is_active.is_(True),
        )
    )
    rows = result.scalars().all()
    return {str(row.internal_product_code): row for row in rows}


async def _load_store_product_name_map(
    session: AsyncSession,
    store_id: int,
) -> dict[str, BusinessStoreProductName]:
    result = await session.execute(
        select(BusinessStoreProductName).where(
            BusinessStoreProductName.store_id == int(store_id),
            BusinessStoreProductName.is_active.is_(True),
        )
    )
    rows = result.scalars().all()
    return {str(row.internal_product_code): row for row in rows}


async def _load_store_product_price_adjustment_map(
    session: AsyncSession,
    store_id: int,
) -> dict[str, BusinessStoreProductPriceAdjustment]:
    result = await session.execute(
        select(BusinessStoreProductPriceAdjustment).where(
            BusinessStoreProductPriceAdjustment.store_id == int(store_id),
            BusinessStoreProductPriceAdjustment.is_active.is_(True),
        )
    )
    rows = result.scalars().all()
    return {str(row.internal_product_code): row for row in rows}


async def _ensure_or_get_mapping(
    session: AsyncSession,
    store: BusinessStore,
    internal_product_code: str,
    mapping_by_internal_code: dict[str, BusinessStoreProductCode],
    *,
    auto_generate_missing_codes: bool,
) -> tuple[str | None, bool]:
    existing = mapping_by_internal_code.get(internal_product_code)
    if existing is not None:
        return existing.external_product_code, False

    if not auto_generate_missing_codes:
        return None, False

    generated = await ensure_store_product_code(session, int(store.id), internal_product_code)
    mapping_by_internal_code[internal_product_code] = generated
    return generated.external_product_code, True


async def _ensure_or_get_name_mapping(
    session: AsyncSession,
    store: BusinessStore,
    internal_product_code: str,
    mapping_by_internal_code: dict[str, BusinessStoreProductName],
    *,
    auto_generate_missing_names: bool,
) -> tuple[BusinessStoreProductName | None, bool]:
    existing = mapping_by_internal_code.get(internal_product_code)
    if existing is not None:
        return existing, False

    if not auto_generate_missing_names:
        return None, False

    generated = await ensure_store_product_name(session, int(store.id), internal_product_code)
    if generated is not None:
        mapping_by_internal_code[internal_product_code] = generated
        return generated, True
    return None, False


async def _ensure_or_get_price_adjustment(
    session: AsyncSession,
    store: BusinessStore,
    internal_product_code: str,
    adjustments_by_internal_code: dict[str, BusinessStoreProductPriceAdjustment],
    *,
    auto_generate_missing_price_adjustments: bool,
) -> tuple[BusinessStoreProductPriceAdjustment | None, bool]:
    existing = adjustments_by_internal_code.get(internal_product_code)
    if existing is not None:
        return existing, False

    if not auto_generate_missing_price_adjustments:
        return None, False

    generated = await ensure_store_product_price_adjustment(session, int(store.id), internal_product_code)
    if generated is not None:
        adjustments_by_internal_code[internal_product_code] = generated
        return generated, True
    return None, False


async def _collect_best_stock_offers(
    session: AsyncSession,
    store: BusinessStore,
) -> tuple[list[Offer], list[_SelectedOffer], list[str]]:
    warnings: list[str] = []
    legacy_scope_key = str(store.legacy_scope_key or "").strip()
    if not legacy_scope_key:
        warnings.append("Store has empty legacy_scope_key; stock scope cannot be resolved.")
        return [], [], warnings

    result = await session.execute(
        select(Offer)
        .where(Offer.city == legacy_scope_key)
        .order_by(Offer.product_code.asc(), Offer.price.asc(), Offer.updated_at.desc())
    )
    all_scope_rows = list(result.scalars().all())
    positive_rows = [row for row in all_scope_rows if int(row.stock or 0) > 0]

    grouped: dict[str, list[Offer]] = defaultdict(list)
    for row in positive_rows:
        product_code = str(row.product_code or "").strip()
        if not product_code:
            continue
        grouped[product_code].append(row)

    selected_rows: list[_SelectedOffer] = []
    for internal_product_code, rows in grouped.items():
        best_offer = min(rows, key=_best_offer_sort_key)
        selected_rows.append(
            _SelectedOffer(
                internal_product_code=internal_product_code,
                supplier_code=str(best_offer.supplier_code or "").strip() or None,
                qty=int(best_offer.stock or 0),
                price=best_offer.price,
                updated_at=best_offer.updated_at,
            )
        )

    selected_rows.sort(key=lambda item: item.internal_product_code)
    warnings.append(
        "Best offer selection is approximated locally for dry-run and does not import app.business.dropship_pipeline."
    )
    return all_scope_rows, selected_rows, warnings


async def build_store_stock_dry_run(
    session: AsyncSession,
    store_id: int,
    auto_generate_missing_codes: bool = False,
    auto_generate_missing_price_adjustments: bool = False,
) -> dict[str, Any]:
    store = await _get_store_by_id(session, int(store_id))
    if store is None:
        return {
            "status": "store_not_found",
            "store_id": int(store_id),
            "warnings": [f"BusinessStore not found for store_id={store_id}"],
        }

    if not store.is_active:
        return _build_inactive_report(store)

    all_scope_rows, selected_rows, warnings = await _collect_best_stock_offers(session, store)
    mapping_by_internal_code = await _load_store_product_code_map(session, int(store.id))
    adjustments_by_internal_code = await _load_store_product_price_adjustment_map(session, int(store.id))

    missing_mappings: list[str] = []
    missing_price_adjustments: list[str] = []
    sample_items: list[dict[str, Any]] = []
    products_with_mapping = 0
    products_with_price_adjustment = 0
    markup_enabled = bool(store.extra_markup_enabled)
    markup_min = store.extra_markup_min
    markup_max = store.extra_markup_max
    markup_config_valid = (
        markup_enabled
        and markup_min is not None
        and markup_max is not None
        and Decimal(str(markup_min)) >= 0
        and Decimal(str(markup_max)) >= Decimal(str(markup_min))
    )
    if markup_enabled and not markup_config_valid:
        warnings.append(
            "Store extra markup is enabled but extra_markup_min/extra_markup_max are missing or invalid; preview uses base price."
        )

    for row in selected_rows:
        external_product_code, generated = await _ensure_or_get_mapping(
            session,
            store,
            row.internal_product_code,
            mapping_by_internal_code,
            auto_generate_missing_codes=auto_generate_missing_codes,
        )
        if external_product_code:
            products_with_mapping += 1
        else:
            missing_mappings.append(row.internal_product_code)

        adjustment = None
        adjustment_generated = False
        if markup_config_valid:
            adjustment, adjustment_generated = await _ensure_or_get_price_adjustment(
                session,
                store,
                row.internal_product_code,
                adjustments_by_internal_code,
                auto_generate_missing_price_adjustments=auto_generate_missing_price_adjustments,
            )
            if adjustment is not None:
                products_with_price_adjustment += 1
            else:
                missing_price_adjustments.append(row.internal_product_code)

        final_preview_price = row.price
        markup_percent = None
        if adjustment is not None:
            markup_percent = adjustment.markup_percent
            final_preview_price = apply_extra_markup(row.price, adjustment.markup_percent)
        elif row.price is not None:
            final_preview_price = apply_extra_markup(row.price, None)

        if len(sample_items) < 20:
            sample_items.append(
                {
                    "internal_product_code": row.internal_product_code,
                    "external_product_code": external_product_code,
                    "qty": int(row.qty),
                    "base_price": _stringify_decimal(row.price),
                    "markup_percent": _stringify_decimal(markup_percent),
                    "final_store_price_preview": _stringify_decimal(final_preview_price),
                    "supplier_code": row.supplier_code,
                    "mapping_generated": bool(generated),
                    "price_adjustment_generated": bool(adjustment_generated),
                }
            )

    unique_internal_products = len(
        {str(row.product_code or "").strip() for row in all_scope_rows if str(row.product_code or "").strip()}
    )

    return {
        "status": "ok",
        "store_id": int(store.id),
        "store_code": store.store_code,
        "store_name": store.store_name,
        "migration_status": store.migration_status,
        "takes_over_legacy_scope": bool(store.takes_over_legacy_scope),
        "legacy_scope_key": store.legacy_scope_key,
        "tabletki_enterprise_code": store.tabletki_enterprise_code,
        "tabletki_branch": store.tabletki_branch,
        "salesdrive_enterprise_id": store.salesdrive_enterprise_id,
        "total_offer_rows": len(all_scope_rows),
        "unique_internal_products": unique_internal_products,
        "products_with_positive_stock": len(selected_rows),
        "products_with_mapping": products_with_mapping,
        "products_missing_mapping": len(missing_mappings),
        "extra_markup_enabled": markup_enabled,
        "extra_markup_mode": store.extra_markup_mode,
        "extra_markup_min": _stringify_decimal(markup_min),
        "extra_markup_max": _stringify_decimal(markup_max),
        "products_with_price_adjustment": products_with_price_adjustment,
        "products_missing_price_adjustment": len(missing_price_adjustments),
        "sample_items": sample_items,
        "missing_mapping_samples": missing_mappings[:20],
        "missing_price_adjustment_samples": missing_price_adjustments[:20],
        "warnings": warnings,
    }


async def build_store_catalog_dry_run(
    session: AsyncSession,
    store_id: int,
    auto_generate_missing_codes: bool = False,
    auto_generate_missing_names: bool = False,
) -> dict[str, Any]:
    store = await _get_store_by_id(session, int(store_id))
    if store is None:
        return {
            "status": "store_not_found",
            "store_id": int(store_id),
            "source": "master_catalog",
            "warnings": [f"BusinessStore not found for store_id={store_id}"],
        }

    if not store.is_active:
        report = _build_inactive_report(store)
        report["source"] = "master_catalog"
        report["catalog_only_in_stock"] = bool(store.catalog_only_in_stock)
        return report

    warnings: list[str] = []
    stock_limited_codes: set[str] = set()
    if store.catalog_only_in_stock:
        _, selected_rows, stock_warnings = await _collect_best_stock_offers(session, store)
        stock_limited_codes = {row.internal_product_code for row in selected_rows}
        warnings.extend(stock_warnings)

    stmt = select(MasterCatalog)
    if hasattr(MasterCatalog, "is_archived"):
        stmt = stmt.where(MasterCatalog.is_archived.is_(False))
    stmt = stmt.order_by(MasterCatalog.sku.asc())

    result = await session.execute(stmt)
    master_rows = list(result.scalars().all())

    filtered_rows = master_rows
    if store.catalog_only_in_stock:
        filtered_rows = [row for row in master_rows if str(row.sku or "").strip() in stock_limited_codes]

    mapping_by_internal_code = await _load_store_product_code_map(session, int(store.id))
    names_by_internal_code = await _load_store_product_name_map(session, int(store.id))
    missing_mappings: list[str] = []
    missing_name_mappings: list[str] = []
    sample_items: list[dict[str, Any]] = []
    products_with_mapping = 0
    products_with_name_mapping = 0
    exportable_products = 0
    name_strategy = str(store.name_strategy or "base").strip().lower() or "base"

    for row in filtered_rows:
        internal_product_code = str(row.sku or "").strip()
        if not internal_product_code:
            continue
        base_name = _base_product_name(row)

        external_product_code, generated = await _ensure_or_get_mapping(
            session,
            store,
            internal_product_code,
            mapping_by_internal_code,
            auto_generate_missing_codes=auto_generate_missing_codes,
        )
        if external_product_code:
            products_with_mapping += 1
        else:
            missing_mappings.append(internal_product_code)

        external_product_name = base_name
        name_source = "base"
        name_generated = False
        exportable_in_catalog = True

        if name_strategy == "supplier_random":
            name_mapping, name_generated = await _ensure_or_get_name_mapping(
                session,
                store,
                internal_product_code,
                names_by_internal_code,
                auto_generate_missing_names=auto_generate_missing_names,
            )
            if name_mapping is None:
                external_product_name = None
                name_source = None
                exportable_in_catalog = False
                missing_name_mappings.append(internal_product_code)
            else:
                external_product_name = name_mapping.external_product_name
                name_source = name_mapping.name_source
                products_with_name_mapping += 1
        else:
            products_with_name_mapping += 1

        if exportable_in_catalog:
            exportable_products += 1

        if len(sample_items) < 20:
            sample_items.append(
                {
                    "internal_product_code": internal_product_code,
                    "external_product_code": external_product_code,
                    "base_name": base_name,
                    "external_product_name": external_product_name,
                    "name_source": name_source,
                    "barcode": row.barcode,
                    "mapping_generated": bool(generated),
                    "name_mapping_generated": bool(name_generated),
                    "exportable_in_catalog": bool(exportable_in_catalog),
                }
            )

    return {
        "status": "ok",
        "store_id": int(store.id),
        "store_code": store.store_code,
        "store_name": store.store_name,
        "source": "master_catalog",
        "catalog_source": "stock_limited" if store.catalog_only_in_stock else "all_products",
        "migration_status": store.migration_status,
        "catalog_only_in_stock": bool(store.catalog_only_in_stock),
        "code_strategy": store.code_strategy,
        "name_strategy": name_strategy,
        "master_catalog_total": len(master_rows),
        "stock_limited_products": len(stock_limited_codes),
        "catalog_products_to_export": exportable_products,
        "products_with_mapping": products_with_mapping,
        "products_missing_mapping": len(missing_mappings),
        "products_with_name_mapping": products_with_name_mapping,
        "products_missing_name_mapping": len(missing_name_mappings),
        "sample_items": sample_items,
        "missing_mapping_samples": missing_mappings[:20],
        "missing_name_samples": missing_name_mappings[:20],
        "warnings": warnings,
    }


async def build_business_stores_dry_run(
    session: AsyncSession,
    enterprise_code: str | None = None,
    auto_generate_missing_codes: bool = False,
    auto_generate_missing_names: bool = False,
    auto_generate_missing_price_adjustments: bool = False,
) -> dict[str, Any]:
    stores = await get_active_business_stores(session, enterprise_code=enterprise_code)
    store_reports: list[dict[str, Any]] = []

    for store in stores:
        stock_report = await build_store_stock_dry_run(
            session,
            int(store.id),
            auto_generate_missing_codes=auto_generate_missing_codes,
            auto_generate_missing_price_adjustments=auto_generate_missing_price_adjustments,
        )
        catalog_report = await build_store_catalog_dry_run(
            session,
            int(store.id),
            auto_generate_missing_codes=auto_generate_missing_codes,
            auto_generate_missing_names=auto_generate_missing_names,
        )
        store_reports.append(
            {
                "store_id": int(store.id),
                "store_code": store.store_code,
                "store_name": store.store_name,
                "migration_status": store.migration_status,
                "stock": stock_report,
                "catalog": catalog_report,
            }
        )

    return {
        "status": "ok",
        "enterprise_code": str(enterprise_code or "").strip() or None,
        "stores_count": len(store_reports),
        "auto_generate_missing_codes": bool(auto_generate_missing_codes),
        "auto_generate_missing_names": bool(auto_generate_missing_names),
        "auto_generate_missing_price_adjustments": bool(auto_generate_missing_price_adjustments),
        "stores": store_reports,
    }


async def build_single_store_dry_run(
    session: AsyncSession,
    *,
    store_id: int | None = None,
    store_code: str | None = None,
    auto_generate_missing_codes: bool = False,
    auto_generate_missing_names: bool = False,
    auto_generate_missing_price_adjustments: bool = False,
) -> dict[str, Any]:
    store: BusinessStore | None = None
    if store_id is not None:
        store = await _get_store_by_id(session, int(store_id))
    elif store_code:
        store = await _get_store_by_code(session, str(store_code))

    if store is None:
        return {
            "status": "store_not_found",
            "store_id": int(store_id) if store_id is not None else None,
            "store_code": str(store_code or "").strip() or None,
            "warnings": ["Store was not found by provided identifier."],
        }

    return {
        "status": "ok",
        "store_id": int(store.id),
        "store_code": store.store_code,
        "store_name": store.store_name,
        "stock": await build_store_stock_dry_run(
            session,
            int(store.id),
            auto_generate_missing_codes=auto_generate_missing_codes,
            auto_generate_missing_price_adjustments=auto_generate_missing_price_adjustments,
        ),
        "catalog": await build_store_catalog_dry_run(
            session,
            int(store.id),
            auto_generate_missing_codes=auto_generate_missing_codes,
            auto_generate_missing_names=auto_generate_missing_names,
        ),
    }

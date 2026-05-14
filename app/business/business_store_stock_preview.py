from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_price_adjustment_generator import apply_extra_markup
from app.models import (
    BusinessEnterpriseProductCode,
    BusinessStore,
    BusinessStoreProductPriceAdjustment,
    Offer,
)


@dataclass
class _SelectedOffer:
    internal_product_code: str
    supplier_code: str | None
    qty: int
    price: Decimal | None
    updated_at: Any


async def _get_store_or_fail(session: AsyncSession, store_id: int) -> BusinessStore:
    store = (
        await session.execute(
            select(BusinessStore).where(BusinessStore.id == int(store_id)).limit(1)
        )
    ).scalar_one_or_none()
    if store is None:
        raise ValueError(f"BusinessStore not found for store_id={store_id}")
    return store


async def _load_enterprise_product_code_map(
    session: AsyncSession,
    enterprise_code: str,
) -> dict[str, BusinessEnterpriseProductCode]:
    normalized_enterprise_code = str(enterprise_code or "").strip()
    if not normalized_enterprise_code:
        return {}

    rows = (
        await session.execute(
            select(BusinessEnterpriseProductCode).where(
                BusinessEnterpriseProductCode.enterprise_code == normalized_enterprise_code,
                BusinessEnterpriseProductCode.is_active.is_(True),
            )
        )
    ).scalars().all()
    return {str(row.internal_product_code): row for row in rows}


async def _load_store_product_price_adjustment_map(
    session: AsyncSession,
    store_id: int,
) -> dict[str, BusinessStoreProductPriceAdjustment]:
    rows = (
        await session.execute(
            select(BusinessStoreProductPriceAdjustment).where(
                BusinessStoreProductPriceAdjustment.store_id == int(store_id),
                BusinessStoreProductPriceAdjustment.is_active.is_(True),
            )
        )
    ).scalars().all()
    return {str(row.internal_product_code): row for row in rows}


def _stringify_decimal(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _round_preview_price_to_integer(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def _best_offer_sort_key(offer: Offer) -> tuple[Any, ...]:
    return (
        offer.price if offer.price is not None else Decimal("9999999999"),
        -(int(offer.stock or 0)),
        -(int(offer.updated_at.timestamp())) if getattr(offer, "updated_at", None) else 0,
        str(offer.supplier_code or ""),
    )


async def _collect_best_stock_offers(
    session: AsyncSession,
    store: BusinessStore,
) -> tuple[list[Offer], list[_SelectedOffer], list[str]]:
    warnings: list[str] = []
    legacy_scope_key = str(store.legacy_scope_key or "").strip()
    if not legacy_scope_key:
        warnings.append("Store has empty legacy_scope_key; stock preview cannot resolve offers scope.")
        return [], [], warnings

    all_scope_rows = list(
        (
            await session.execute(
                select(Offer)
                .where(Offer.city == legacy_scope_key)
                .order_by(Offer.product_code.asc(), Offer.price.asc(), Offer.updated_at.desc())
            )
        ).scalars().all()
    )
    positive_rows = [row for row in all_scope_rows if int(row.stock or 0) > 0]

    grouped: dict[str, list[Offer]] = defaultdict(list)
    for row in positive_rows:
        product_code = str(row.product_code or "").strip()
        if product_code:
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
        "Best offer selection is approximated locally for stock preview and does not import app.business.dropship_pipeline."
    )
    return all_scope_rows, selected_rows, warnings


async def build_store_stock_payload_preview(
    session: AsyncSession,
    store_id: int,
    limit: int | None = 100,
    include_not_exportable: bool = True,
) -> dict[str, Any]:
    store = await _get_store_or_fail(session, int(store_id))

    all_scope_rows, selected_rows, warnings = await _collect_best_stock_offers(session, store)
    code_strategy = str(store.code_strategy or "legacy_same").strip().lower() or "legacy_same"
    code_mapping_mode = (
        "legacy_same"
        if bool(store.is_legacy_default) or code_strategy == "legacy_same"
        else "enterprise_level"
    )
    code_map = await _load_enterprise_product_code_map(session, str(store.enterprise_code or ""))
    adjustment_map = await _load_store_product_price_adjustment_map(session, int(store.id))

    if not str(store.tabletki_enterprise_code or "").strip():
        warnings.append("Store tabletki_enterprise_code is empty; preview target is incomplete.")
    if not str(store.tabletki_branch or "").strip():
        warnings.append("Store tabletki_branch is empty; preview target is incomplete.")

    markup_enabled = bool(store.extra_markup_enabled)
    markup_min = store.extra_markup_min
    markup_max = store.extra_markup_max
    markup_strategy = str(store.extra_markup_strategy or "stable_per_product").strip() or "stable_per_product"

    if not (bool(store.is_legacy_default) or code_strategy == "legacy_same") and not code_map:
        warnings.append(
            "Store code_strategy requires external mappings but no active {} code mappings were found.".format(
                "enterprise-level" if code_mapping_mode == "enterprise_level" else "store-level"
            )
        )
    if markup_enabled and not adjustment_map:
        warnings.append("Store extra markup is enabled but no active price adjustments were found.")

    payload_rows: list[dict[str, Any]] = []
    not_exportable_samples: list[dict[str, Any]] = []
    exportable_products = 0
    missing_code_mapping = 0
    missing_price_adjustment = 0
    markup_applied_products = 0

    for row in selected_rows:
        reasons: list[str] = []

        if bool(store.is_legacy_default) or code_strategy == "legacy_same":
            external_product_code = row.internal_product_code
        else:
            code_mapping = code_map.get(row.internal_product_code)
            external_product_code = code_mapping.external_product_code if code_mapping is not None else None
            if external_product_code is None:
                reasons.append(
                    "missing_enterprise_code_mapping" if code_mapping_mode == "enterprise_level" else "missing_code_mapping"
                )
                missing_code_mapping += 1

        adjustment = None
        markup_percent = None
        final_store_price_preview = (
            _round_preview_price_to_integer(apply_extra_markup(row.price, None))
            if row.price is not None
            else None
        )
        if markup_enabled:
            adjustment = adjustment_map.get(row.internal_product_code)
            if adjustment is None:
                reasons.append("missing_price_adjustment")
                missing_price_adjustment += 1
            else:
                markup_percent = adjustment.markup_percent
                markup_applied_products += 1
                final_store_price_preview = _round_preview_price_to_integer(
                    apply_extra_markup(row.price, adjustment.markup_percent)
                )

        exportable = len(reasons) == 0
        if exportable:
            exportable_products += 1
        elif len(not_exportable_samples) < 50:
            not_exportable_samples.append(
                {
                    "internal_product_code": row.internal_product_code,
                    "external_product_code": external_product_code,
                    "supplier_code": row.supplier_code,
                    "qty": int(row.qty),
                    "base_price": _stringify_decimal(row.price),
                    "markup_percent": _stringify_decimal(markup_percent),
                    "final_store_price_preview": _stringify_decimal(final_store_price_preview),
                    "reasons": reasons,
                }
            )

        preview_row = {
            "internal_product_code": row.internal_product_code,
            "external_product_code": external_product_code,
            "supplier_code": row.supplier_code,
            "qty": int(row.qty),
            "base_price": _stringify_decimal(row.price),
            "markup_percent": _stringify_decimal(markup_percent),
            "final_store_price_preview": _stringify_decimal(final_store_price_preview),
            "tabletki_enterprise_code": store.tabletki_enterprise_code,
            "tabletki_branch": store.tabletki_branch,
            "exportable": exportable,
            "reasons": reasons,
        }
        if include_not_exportable or exportable:
            payload_rows.append(preview_row)

    limited_payload_rows = payload_rows if limit is None else payload_rows[: max(0, int(limit))]

    return {
        "status": "ok",
        "code_mapping_mode": code_mapping_mode,
        "store": {
            "store_id": int(store.id),
            "store_code": store.store_code,
            "store_name": store.store_name,
            "enterprise_code": store.enterprise_code,
            "legacy_scope_key": store.legacy_scope_key,
            "tabletki_enterprise_code": store.tabletki_enterprise_code,
            "tabletki_branch": store.tabletki_branch,
            "code_strategy": store.code_strategy,
            "extra_markup_enabled": markup_enabled,
            "extra_markup_min": _stringify_decimal(markup_min),
            "extra_markup_max": _stringify_decimal(markup_max),
            "extra_markup_strategy": markup_strategy,
        },
        "summary": {
            "offer_rows_total": len(all_scope_rows),
            "candidate_products": len(selected_rows),
            "exportable_products": exportable_products,
            "not_exportable_products": max(0, len(selected_rows) - exportable_products),
            "missing_code_mapping": missing_code_mapping,
            "missing_price_adjustment": missing_price_adjustment,
            "markup_applied_products": markup_applied_products,
            "stock_source": "offers",
            "code_mapping_mode": code_mapping_mode,
            "target_branch": store.tabletki_branch,
            "target_branch_source": "business_store",
        },
        "payload_preview": limited_payload_rows,
        "not_exportable_samples": not_exportable_samples,
        "warnings": warnings,
    }

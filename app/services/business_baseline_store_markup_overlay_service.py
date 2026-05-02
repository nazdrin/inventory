from __future__ import annotations

from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_price_adjustment_generator import (
    apply_extra_markup,
    generate_stable_markup_percent,
)
from app.models import BusinessStore, BusinessStoreProductPriceAdjustment


SUPPORTED_MARKUP_MODE = "percent"
SUPPORTED_MARKUP_STRATEGY = "stable_per_product"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    normalized = _clean_text(value)
    if not normalized:
        return None
    try:
        return Decimal(normalized)
    except Exception:
        return None


async def _load_active_stores_for_enterprise(
    session: AsyncSession,
    enterprise_code: str,
) -> list[BusinessStore]:
    normalized_enterprise_code = _clean_text(enterprise_code)
    if not normalized_enterprise_code:
        return []
    rows = (
        await session.execute(
            select(BusinessStore)
            .where(
                BusinessStore.enterprise_code == normalized_enterprise_code,
                BusinessStore.is_active.is_(True),
            )
            .order_by(BusinessStore.store_code.asc(), BusinessStore.id.asc())
        )
    ).scalars().all()
    return list(rows)


async def _load_adjustments_by_store_id(
    session: AsyncSession,
    store_ids: list[int],
) -> dict[int, dict[str, Decimal]]:
    if not store_ids:
        return {}
    rows = (
        await session.execute(
            select(BusinessStoreProductPriceAdjustment).where(
                BusinessStoreProductPriceAdjustment.store_id.in_(store_ids),
                BusinessStoreProductPriceAdjustment.is_active.is_(True),
            )
        )
    ).scalars().all()

    result: dict[int, dict[str, Decimal]] = {}
    for row in rows:
        result.setdefault(int(row.store_id), {})[str(row.internal_product_code)] = Decimal(str(row.markup_percent))
    return result


def _resolve_store_markup_percent(
    store: BusinessStore,
    internal_product_code: str,
    adjustments_by_store_id: dict[int, dict[str, Decimal]],
) -> tuple[Decimal | None, str | None]:
    if not bool(store.extra_markup_enabled):
        return None, "markup_disabled"

    markup_mode = _clean_text(store.extra_markup_mode) or SUPPORTED_MARKUP_MODE
    if markup_mode != SUPPORTED_MARKUP_MODE:
        return None, "unsupported_markup_mode"

    markup_strategy = _clean_text(store.extra_markup_strategy) or SUPPORTED_MARKUP_STRATEGY
    if markup_strategy != SUPPORTED_MARKUP_STRATEGY:
        return None, "unsupported_markup_strategy"

    store_adjustments = adjustments_by_store_id.get(int(store.id), {})
    existing = store_adjustments.get(internal_product_code)
    if existing is not None:
        return existing, "stored_adjustment"

    min_percent = _decimal_or_none(store.extra_markup_min)
    max_percent = _decimal_or_none(store.extra_markup_max)
    if min_percent is None or max_percent is None:
        return None, "missing_markup_bounds"
    if min_percent < 0 or max_percent < 0 or max_percent < min_percent:
        return None, "invalid_markup_bounds"

    generated = generate_stable_markup_percent(
        int(store.id),
        internal_product_code,
        min_percent,
        max_percent,
    )
    return generated, "generated_from_store_range"


def _update_row_prices(
    row: dict[str, Any],
    markup_percent: Decimal,
) -> tuple[bool, str | None, str | None, str | None, str | None]:
    base_price = _decimal_or_none(row.get("price"))
    base_reserve_price = _decimal_or_none(row.get("price_reserve"))
    if base_price is None:
        return False, None, None, None, None

    final_price = apply_extra_markup(base_price, markup_percent)
    final_reserve_price = apply_extra_markup(
        base_reserve_price if base_reserve_price is not None else base_price,
        markup_percent,
    )
    if final_price is None or final_reserve_price is None:
        return False, None, None, None, None

    before_price = format(base_price, "f")
    after_price = format(final_price, "f")
    before_reserve = format(base_reserve_price, "f") if base_reserve_price is not None else None
    after_reserve = format(final_reserve_price, "f")

    changed = (
        final_price != base_price
        or base_reserve_price is None
        or final_reserve_price != base_reserve_price
    )
    row["price"] = float(final_price)
    row["price_reserve"] = float(final_reserve_price)
    return changed, before_price, after_price, before_reserve, after_reserve


async def apply_store_markup_overlay_to_baseline_stock_payload(
    session: AsyncSession,
    *,
    enterprise_code: str,
    payload_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_enterprise_code = _clean_text(enterprise_code)
    cloned_rows = [dict(row) for row in payload_rows]

    stores = await _load_active_stores_for_enterprise(session, normalized_enterprise_code)
    branch_candidates: dict[str, list[BusinessStore]] = {}
    for store in stores:
        branch = _clean_text(store.tabletki_branch)
        if not branch:
            continue
        branch_candidates.setdefault(branch, []).append(store)

    unique_store_by_branch: dict[str, BusinessStore] = {}
    ambiguous_branches: set[str] = set()
    for branch, candidates in branch_candidates.items():
        if len(candidates) == 1:
            unique_store_by_branch[branch] = candidates[0]
        else:
            ambiguous_branches.add(branch)

    adjustments_by_store_id = await _load_adjustments_by_store_id(
        session,
        [int(store.id) for store in unique_store_by_branch.values()],
    )

    rows_changed = 0
    rows_seen = 0
    overlay_attempted_rows = 0
    branches_used: set[str] = set()
    skipped_reasons_by_branch: dict[str, str] = {}
    warnings: list[str] = []
    source_counts = {
        "stored_adjustment": 0,
        "generated_from_store_range": 0,
    }
    sample_changes: list[dict[str, Any]] = []

    for branch in sorted(ambiguous_branches):
        skipped_reasons_by_branch[branch] = "ambiguous_active_stores_for_branch"

    for row in cloned_rows:
        branch = _clean_text(row.get("branch"))
        internal_product_code = _clean_text(row.get("code"))
        if not branch or not internal_product_code:
            continue
        rows_seen += 1

        if branch in ambiguous_branches:
            continue

        store = unique_store_by_branch.get(branch)
        if store is None:
            skipped_reasons_by_branch.setdefault(branch, "missing_active_store_for_branch")
            continue

        markup_percent, source = _resolve_store_markup_percent(
            store,
            internal_product_code,
            adjustments_by_store_id,
        )
        if markup_percent is None:
            skipped_reasons_by_branch.setdefault(branch, str(source or "markup_not_available"))
            continue

        overlay_attempted_rows += 1
        branches_used.add(branch)
        if source in source_counts:
            source_counts[source] += 1

        changed, before_price, after_price, before_reserve, after_reserve = _update_row_prices(row, markup_percent)
        if not changed:
            continue

        rows_changed += 1
        if len(sample_changes) < 20:
            sample_changes.append(
                {
                    "branch": branch,
                    "store_code": store.store_code,
                    "internal_product_code": internal_product_code,
                    "markup_percent": format(markup_percent, "f"),
                    "adjustment_source": source,
                    "price_before": before_price,
                    "price_after": after_price,
                    "price_reserve_before": before_reserve,
                    "price_reserve_after": after_reserve,
                }
            )

    if ambiguous_branches:
        warnings.append(
            "Baseline stock markup overlay skipped ambiguous branches: "
            + ", ".join(sorted(ambiguous_branches))
        )
    if skipped_reasons_by_branch:
        warnings.append(
            "Baseline stock markup overlay skipped some branches without usable store markup configuration."
        )

    overlay_report = {
        "store_markup_overlay_applied": bool(rows_changed > 0 or overlay_attempted_rows > 0),
        "store_markup_rows_seen": rows_seen,
        "store_markup_rows_changed": rows_changed,
        "store_markup_overlay_attempted_rows": overlay_attempted_rows,
        "store_markup_branches_used": sorted(branches_used),
        "store_markup_branches_skipped": [
            {"branch": branch, "reason": reason}
            for branch, reason in sorted(skipped_reasons_by_branch.items())
        ],
        "store_markup_warnings": warnings,
        "store_markup_adjustment_sources": source_counts,
        "store_markup_sample_changes": sample_changes,
        "store_markup_active_store_branches_total": len(unique_store_by_branch),
        "store_markup_ambiguous_branches_total": len(ambiguous_branches),
    }
    return cloned_rows, overlay_report

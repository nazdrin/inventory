from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.dropship_pipeline import _get_stock_priority_suppliers
from app.models import BusinessStoreOffer


@dataclass(frozen=True)
class SelectedStoreOffer:
    store_offer: BusinessStoreOffer
    selection_debug: dict[str, Any]


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("9999999999")
    return Decimal(str(value))


def _timestamp_or_zero(value: datetime | None) -> int:
    if value is None:
        return 0
    return int(value.timestamp())


def _offer_sort_key(
    offer: BusinessStoreOffer,
    *,
    priority_suppliers: set[str],
) -> tuple[Any, ...]:
    supplier_code = _clean_text(offer.supplier_code).upper()
    preferred = supplier_code in priority_suppliers
    priority_used = int(offer.priority_used or 0)
    return (
        0 if preferred else 1,
        _as_decimal(offer.effective_price),
        -priority_used,
        -int(offer.stock or 0),
        -_timestamp_or_zero(getattr(offer, "updated_at", None)),
        _clean_text(offer.supplier_code),
    )


async def load_store_native_offers(
    session: AsyncSession,
    *,
    store_ids: list[int],
) -> list[BusinessStoreOffer]:
    if not store_ids:
        return []
    rows = (
        await session.execute(
            select(BusinessStoreOffer)
            .where(
                BusinessStoreOffer.store_id.in_([int(item) for item in store_ids]),
                BusinessStoreOffer.stock > 0,
            )
            .order_by(
                BusinessStoreOffer.store_id.asc(),
                BusinessStoreOffer.product_code.asc(),
                BusinessStoreOffer.supplier_code.asc(),
            )
        )
    ).scalars().all()
    return list(rows)


def select_best_store_native_offers(
    offers: list[BusinessStoreOffer],
) -> tuple[list[SelectedStoreOffer], dict[str, Any]]:
    priority_suppliers = _get_stock_priority_suppliers()
    grouped: dict[tuple[int, str], list[BusinessStoreOffer]] = defaultdict(list)
    for offer in offers:
        key = (int(offer.store_id), _clean_text(offer.product_code))
        if key[1]:
            grouped[key].append(offer)

    selected: list[SelectedStoreOffer] = []
    for (_store_id, product_code), rows in grouped.items():
        sorted_rows = sorted(
            rows,
            key=lambda item: _offer_sort_key(item, priority_suppliers=priority_suppliers),
        )
        winner = sorted_rows[0]
        runner_up = sorted_rows[1] if len(sorted_rows) > 1 else None
        selection_debug = {
            "product_code": product_code,
            "candidate_suppliers": [_clean_text(item.supplier_code) for item in sorted_rows[:10]],
            "candidate_count": len(sorted_rows),
            "winner_supplier_code": _clean_text(winner.supplier_code) or None,
            "winner_effective_price": float(winner.effective_price),
            "winner_priority_used": winner.priority_used,
            "winner_stock": int(winner.stock or 0),
            "stock_priority_override_used": _clean_text(winner.supplier_code).upper() in priority_suppliers,
            "runner_up_supplier_code": _clean_text(runner_up.supplier_code) or None if runner_up is not None else None,
            "runner_up_effective_price": float(runner_up.effective_price) if runner_up is not None else None,
        }
        selected.append(SelectedStoreOffer(store_offer=winner, selection_debug=selection_debug))

    selected.sort(
        key=lambda item: (
            int(item.store_offer.store_id),
            _clean_text(item.store_offer.product_code),
        )
    )
    return selected, {
        "candidate_offers_total": len(offers),
        "selected_offers_total": len(selected),
        "stock_priority_suppliers": sorted(priority_suppliers),
    }

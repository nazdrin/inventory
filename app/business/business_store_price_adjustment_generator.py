from __future__ import annotations

import hashlib
import os
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessStore, BusinessStoreProductPriceAdjustment


FOUR_PLACES = Decimal("0.0001")
TWO_PLACES = Decimal("0.01")
HUNDRED = Decimal("100")


def _adjustment_salt() -> str:
    return str(os.getenv("BUSINESS_STORE_MARKUP_SALT") or os.getenv("BUSINESS_STORE_CODE_SALT") or "").strip()


def _seed_digest(store_id: int, internal_product_code: str) -> str:
    seed = f"{int(store_id)}:{str(internal_product_code).strip()}:{_adjustment_salt()}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


async def _get_store_or_fail(session: AsyncSession, store_id: int) -> BusinessStore:
    store = (
        await session.execute(
            select(BusinessStore).where(BusinessStore.id == int(store_id)).limit(1)
        )
    ).scalar_one_or_none()
    if store is None:
        raise ValueError(f"BusinessStore not found for store_id={store_id}")
    return store


def generate_stable_markup_percent(
    store_id: int,
    internal_product_code: str,
    min_percent: Decimal,
    max_percent: Decimal,
) -> Decimal:
    if max_percent < min_percent:
        raise ValueError("max_percent must be >= min_percent")

    min_value = Decimal(str(min_percent))
    max_value = Decimal(str(max_percent))
    span = max_value - min_value
    if span == 0:
        return min_value.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)

    digest = _seed_digest(store_id, internal_product_code)
    ratio = Decimal(int(digest[:16], 16)) / Decimal(0xFFFFFFFFFFFFFFFF)
    value = min_value + (span * ratio)
    return value.quantize(FOUR_PLACES, rounding=ROUND_HALF_UP)


async def ensure_store_product_price_adjustment(
    session: AsyncSession,
    store_id: int,
    internal_product_code: str,
) -> BusinessStoreProductPriceAdjustment | None:
    normalized_code = str(internal_product_code or "").strip()
    if not normalized_code:
        raise ValueError("internal_product_code is required")

    existing = (
        await session.execute(
            select(BusinessStoreProductPriceAdjustment)
            .where(
                BusinessStoreProductPriceAdjustment.store_id == int(store_id),
                BusinessStoreProductPriceAdjustment.internal_product_code == normalized_code,
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.is_active:
            return existing
        return None

    store = await _get_store_or_fail(session, int(store_id))
    if not bool(store.extra_markup_enabled):
        return None

    min_percent = store.extra_markup_min
    max_percent = store.extra_markup_max
    if min_percent is None or max_percent is None:
        return None

    min_decimal = Decimal(str(min_percent))
    max_decimal = Decimal(str(max_percent))
    if min_decimal < 0 or max_decimal < 0 or max_decimal < min_decimal:
        return None

    obj = BusinessStoreProductPriceAdjustment(
        store_id=int(store.id),
        internal_product_code=normalized_code,
        markup_percent=generate_stable_markup_percent(
            int(store.id),
            normalized_code,
            min_decimal,
            max_decimal,
        ),
        strategy="stable_per_product",
        source="generated",
        is_active=True,
    )
    session.add(obj)
    await session.flush()
    return obj


def apply_extra_markup(base_price: Decimal | None, markup_percent: Decimal | None) -> Decimal | None:
    if base_price is None:
        return None
    if markup_percent is None:
        return base_price.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    price = Decimal(str(base_price)) * (Decimal("1") + (Decimal(str(markup_percent)) / HUNDRED))
    return price.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)


async def generate_missing_store_price_adjustments(
    session: AsyncSession,
    store_id: int,
    internal_product_codes: list[str],
) -> dict[str, object]:
    generated_count = 0
    invalid_count = 0
    sample_adjustments: list[dict[str, str]] = []
    seen_codes: set[str] = set()

    for raw_code in internal_product_codes:
        normalized_code = str(raw_code or "").strip()
        if not normalized_code or normalized_code in seen_codes:
            continue
        seen_codes.add(normalized_code)

        existing = (
            await session.execute(
                select(BusinessStoreProductPriceAdjustment.id)
                .where(
                    BusinessStoreProductPriceAdjustment.store_id == int(store_id),
                    BusinessStoreProductPriceAdjustment.internal_product_code == normalized_code,
                    BusinessStoreProductPriceAdjustment.is_active.is_(True),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if existing is not None:
            continue

        obj = await ensure_store_product_price_adjustment(session, int(store_id), normalized_code)
        if obj is None:
            invalid_count += 1
            continue

        generated_count += 1
        if len(sample_adjustments) < 20:
            sample_adjustments.append(
                {
                    "internal_product_code": normalized_code,
                    "markup_percent": format(Decimal(str(obj.markup_percent)), "f"),
                }
            )

    return {
        "generated_count": generated_count,
        "invalid_count": invalid_count,
        "sample_adjustments": sample_adjustments,
    }

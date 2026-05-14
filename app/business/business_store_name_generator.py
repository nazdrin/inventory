from __future__ import annotations

import hashlib
import os
import re
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    BusinessStore,
    BusinessStoreOffer,
    BusinessStoreProductName,
    CatalogSupplierMapping,
    Offer,
)
from app.business.supplier_identity import resolve_supplier_id_by_code


_WHITESPACE_RE = re.compile(r"\s+")


def normalize_supplier_name(name: str | None) -> str | None:
    normalized = _WHITESPACE_RE.sub(" ", str(name or "").strip())
    return normalized or None


def _name_salt() -> str:
    return str(os.getenv("BUSINESS_STORE_NAME_SALT") or os.getenv("BUSINESS_STORE_CODE_SALT") or "").strip()


def _seed_digest(store_id: int, internal_product_code: str) -> str:
    seed = f"{int(store_id)}:{str(internal_product_code).strip()}:{_name_salt()}"
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


async def get_supplier_name_candidates_for_store_product(
    session: AsyncSession,
    store: BusinessStore,
    internal_product_code: str,
) -> list[dict[str, Any]]:
    normalized_code = str(internal_product_code or "").strip()
    if not normalized_code:
        return []

    offer_rows = (
        await session.execute(
            select(BusinessStoreOffer.supplier_code)
            .where(
                BusinessStoreOffer.store_id == int(store.id),
                BusinessStoreOffer.product_code == normalized_code,
            )
            .distinct()
        )
    ).scalars().all()
    supplier_codes = {
        str(value or "").strip().upper()
        for value in offer_rows
        if str(value or "").strip()
    }

    legacy_scope_key = str(store.legacy_scope_key or "").strip()
    if legacy_scope_key:
        legacy_offer_rows = (
            await session.execute(
                select(Offer.supplier_code)
                .where(
                    Offer.city == legacy_scope_key,
                    Offer.product_code == normalized_code,
                )
                .distinct()
            )
        ).scalars().all()
        supplier_codes.update(
            {
                str(value or "").strip().upper()
                for value in legacy_offer_rows
                if str(value or "").strip()
            }
        )

    supplier_codes = sorted(supplier_codes)
    if not supplier_codes:
        return []

    supplier_id_by_code: dict[str, int] = {}
    for supplier_code in supplier_codes:
        supplier_id = await resolve_supplier_id_by_code(session, supplier_code)
        if supplier_id is not None:
            supplier_id_by_code[supplier_code] = int(supplier_id)

    if not supplier_id_by_code:
        return []

    mapping_rows = (
        await session.execute(
            select(CatalogSupplierMapping)
            .where(
                CatalogSupplierMapping.sku == normalized_code,
                CatalogSupplierMapping.is_active.is_(True),
                CatalogSupplierMapping.supplier_id.in_(list(supplier_id_by_code.values())),
            )
            .order_by(
                CatalogSupplierMapping.supplier_id.asc(),
                CatalogSupplierMapping.supplier_code.asc(),
                CatalogSupplierMapping.id.asc(),
            )
        )
    ).scalars().all()

    seen_names: set[str] = set()
    candidates: list[dict[str, Any]] = []
    for row in mapping_rows:
        supplier_name = normalize_supplier_name(row.supplier_product_name_raw)
        if not supplier_name:
            continue
        dedupe_key = supplier_name.casefold()
        if dedupe_key in seen_names:
            continue
        seen_names.add(dedupe_key)
        candidates.append(
            {
                "external_product_name": supplier_name,
                "name_source": "catalog_supplier_mapping",
                "source_supplier_id": row.supplier_id,
                "source_supplier_code": row.supplier_code,
                "source_supplier_product_id": row.supplier_product_id,
                "source_supplier_product_name_raw": row.supplier_product_name_raw,
            }
        )
    return candidates


def choose_stable_random_name(
    store_id: int,
    internal_product_code: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not candidates:
        return None
    digest = _seed_digest(store_id, internal_product_code)
    index = int(digest, 16) % len(candidates)
    return candidates[index]


async def ensure_store_product_name(
    session: AsyncSession,
    store_id: int,
    internal_product_code: str,
) -> BusinessStoreProductName | None:
    normalized_code = str(internal_product_code or "").strip()
    if not normalized_code:
        raise ValueError("internal_product_code is required")

    existing = (
        await session.execute(
            select(BusinessStoreProductName)
            .where(
                BusinessStoreProductName.store_id == int(store_id),
                BusinessStoreProductName.internal_product_code == normalized_code,
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.is_active:
            return existing
        return None

    store = await _get_store_or_fail(session, store_id)
    if str(store.name_strategy or "base").strip().lower() == "base":
        return None

    selected = choose_stable_random_name(
        int(store.id),
        normalized_code,
        await get_supplier_name_candidates_for_store_product(session, store, normalized_code),
    )
    if selected is None:
        return None

    obj = BusinessStoreProductName(
        store_id=int(store.id),
        internal_product_code=normalized_code,
        external_product_name=str(selected["external_product_name"]),
        name_source=str(selected["name_source"]),
        source_supplier_id=selected.get("source_supplier_id"),
        source_supplier_code=selected.get("source_supplier_code"),
        source_supplier_product_id=selected.get("source_supplier_product_id"),
        source_supplier_product_name_raw=selected.get("source_supplier_product_name_raw"),
        is_active=True,
    )
    session.add(obj)
    await session.flush()
    return obj


async def generate_missing_store_product_names(
    session: AsyncSession,
    store_id: int,
    internal_product_codes: list[str],
) -> dict[str, Any]:
    generated_count = 0
    missing_count = 0
    missing_samples: list[str] = []
    generated_samples: list[dict[str, Any]] = []
    seen_codes: set[str] = set()

    for raw_code in internal_product_codes:
        normalized_code = str(raw_code or "").strip()
        if not normalized_code or normalized_code in seen_codes:
            continue
        seen_codes.add(normalized_code)

        existing = (
            await session.execute(
                select(BusinessStoreProductName.id)
                .where(
                    BusinessStoreProductName.store_id == int(store_id),
                    BusinessStoreProductName.internal_product_code == normalized_code,
                    BusinessStoreProductName.is_active.is_(True),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if existing is not None:
            continue

        obj = await ensure_store_product_name(session, int(store_id), normalized_code)
        if obj is None:
            missing_count += 1
            if len(missing_samples) < 20:
                missing_samples.append(normalized_code)
            continue

        generated_count += 1
        if len(generated_samples) < 20:
            generated_samples.append(
                {
                    "internal_product_code": normalized_code,
                    "external_product_name": obj.external_product_name,
                    "name_source": obj.name_source,
                }
            )

    return {
        "generated_count": generated_count,
        "missing_count": missing_count,
        "missing_samples": missing_samples,
        "generated_samples": generated_samples,
    }


async def cleanup_store_product_names(
    session: AsyncSession,
    store_id: int,
    mode: str = "deactivate",
) -> dict[str, Any]:
    normalized_mode = str(mode or "deactivate").strip().lower()
    if normalized_mode not in {"deactivate", "delete"}:
        raise ValueError("mode must be 'deactivate' or 'delete'")

    rows = (
        await session.execute(
            select(BusinessStoreProductName).where(
                BusinessStoreProductName.store_id == int(store_id)
            )
        )
    ).scalars().all()

    affected = 0
    for row in rows:
        if normalized_mode == "delete":
            await session.delete(row)
            affected += 1
            continue
        if row.is_active:
            row.is_active = False
            affected += 1

    return {
        "store_id": int(store_id),
        "mode": normalized_mode,
        "affected_count": affected,
    }

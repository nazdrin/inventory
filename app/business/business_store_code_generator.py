from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessStore, BusinessStoreProductCode


DEFAULT_CODE_LENGTH = 10
MIN_CODE_LENGTH = 6
MAX_COLLISION_ATTEMPTS = 32


@dataclass(frozen=True)
class _StoreCodeSpec:
    strategy: str
    is_legacy_default: bool
    store_code: str
    code_prefix: str | None


def _normalize_code_length(length: int | None) -> int:
    try:
        normalized = int(length or DEFAULT_CODE_LENGTH)
    except (TypeError, ValueError):
        normalized = DEFAULT_CODE_LENGTH
    return max(MIN_CODE_LENGTH, normalized)


def _default_salt() -> str:
    return str(os.getenv("BUSINESS_STORE_CODE_SALT") or "").strip()


def _default_length() -> int:
    return _normalize_code_length(os.getenv("BUSINESS_STORE_CODE_LENGTH", DEFAULT_CODE_LENGTH))


def _opaque_digest(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest().upper()


def generate_opaque_external_code(
    store_code: str,
    internal_product_code: str,
    salt: str,
    length: int = DEFAULT_CODE_LENGTH,
) -> str:
    normalized_store_code = str(store_code or "").strip().upper()
    normalized_internal_code = str(internal_product_code or "").strip()
    normalized_salt = str(salt or "").strip()
    normalized_length = _normalize_code_length(length)

    if not normalized_store_code:
        raise ValueError("store_code is required")
    if not normalized_internal_code:
        raise ValueError("internal_product_code is required")

    seed = f"{normalized_store_code}:{normalized_internal_code}:{normalized_salt}"
    return _opaque_digest(seed)[:normalized_length]


def _build_code_spec(store: BusinessStore) -> _StoreCodeSpec:
    return _StoreCodeSpec(
        strategy=str(getattr(store, "code_strategy", "") or "opaque_mapping").strip().lower(),
        is_legacy_default=bool(getattr(store, "is_legacy_default", False)),
        store_code=str(getattr(store, "store_code", "") or "").strip(),
        code_prefix=str(getattr(store, "code_prefix", "") or "").strip() or None,
    )


def build_external_code_for_store(
    store: BusinessStore,
    internal_product_code: str,
    salt: str | None = None,
    length: int = DEFAULT_CODE_LENGTH,
) -> str:
    normalized_internal_code = str(internal_product_code or "").strip()
    if not normalized_internal_code:
        raise ValueError("internal_product_code is required")

    spec = _build_code_spec(store)
    resolved_salt = str(salt if salt is not None else _default_salt()).strip()
    resolved_length = _normalize_code_length(length)

    if spec.is_legacy_default or spec.strategy == "legacy_same":
        return normalized_internal_code

    opaque_code = generate_opaque_external_code(
        store_code=spec.store_code,
        internal_product_code=normalized_internal_code,
        salt=resolved_salt,
        length=resolved_length,
    )
    if spec.strategy == "prefix_mapping":
        prefix = spec.code_prefix or spec.store_code
        normalized_prefix = "".join(ch for ch in prefix.upper() if ch.isalnum())
        if not normalized_prefix:
            normalized_prefix = "".join(ch for ch in spec.store_code.upper() if ch.isalnum())
        if not normalized_prefix:
            raise ValueError("prefix_mapping requires code_prefix or store_code with alphanumeric characters")
        return f"{normalized_prefix}{opaque_code}"

    return opaque_code


def _code_source_for_store(store: BusinessStore) -> str:
    spec = _build_code_spec(store)
    if spec.is_legacy_default or spec.strategy == "legacy_same":
        return "legacy_same"
    if spec.strategy == "prefix_mapping":
        return "prefix_mapping"
    return "generated"


async def _get_store_or_fail(session: AsyncSession, store_id: int) -> BusinessStore:
    result = await session.execute(
        select(BusinessStore).where(BusinessStore.id == int(store_id)).limit(1)
    )
    store = result.scalar_one_or_none()
    if store is None:
        raise ValueError(f"BusinessStore not found for store_id={store_id}")
    return store


async def _find_collision(
    session: AsyncSession,
    *,
    store_id: int,
    external_product_code: str,
    internal_product_code: str,
) -> BusinessStoreProductCode | None:
    result = await session.execute(
        select(BusinessStoreProductCode)
        .where(
            BusinessStoreProductCode.store_id == int(store_id),
            BusinessStoreProductCode.external_product_code == external_product_code,
            BusinessStoreProductCode.internal_product_code != internal_product_code,
        )
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _build_unique_external_code(
    session: AsyncSession,
    *,
    store: BusinessStore,
    internal_product_code: str,
    salt: str | None,
    length: int,
) -> tuple[str, str]:
    code_source = _code_source_for_store(store)
    if code_source == "legacy_same":
        return internal_product_code, code_source

    for attempt_no in range(MAX_COLLISION_ATTEMPTS):
        attempt_salt = str(salt if salt is not None else _default_salt()).strip()
        if attempt_no > 0:
            attempt_salt = f"{attempt_salt}:{attempt_no}"

        candidate = build_external_code_for_store(
            store,
            internal_product_code,
            salt=attempt_salt,
            length=length,
        )
        collision = await _find_collision(
            session,
            store_id=int(store.id),
            external_product_code=candidate,
            internal_product_code=internal_product_code,
        )
        if collision is None:
            return candidate, code_source

    raise RuntimeError(
        f"Unable to generate unique external code for store_id={store.id} internal_product_code={internal_product_code}"
    )


async def ensure_store_product_code(
    session: AsyncSession,
    store_id: int,
    internal_product_code: str,
) -> BusinessStoreProductCode:
    normalized_internal_code = str(internal_product_code or "").strip()
    if not normalized_internal_code:
        raise ValueError("internal_product_code is required")

    existing_result = await session.execute(
        select(BusinessStoreProductCode)
        .where(
            BusinessStoreProductCode.store_id == int(store_id),
            BusinessStoreProductCode.internal_product_code == normalized_internal_code,
        )
        .limit(1)
    )
    existing = existing_result.scalar_one_or_none()
    if existing is not None:
        return existing

    store = await _get_store_or_fail(session, store_id)
    external_code, code_source = await _build_unique_external_code(
        session,
        store=store,
        internal_product_code=normalized_internal_code,
        salt=None,
        length=_default_length(),
    )

    obj = BusinessStoreProductCode(
        store_id=int(store.id),
        internal_product_code=normalized_internal_code,
        external_product_code=external_code,
        code_source=code_source,
        is_active=True,
    )
    session.add(obj)
    await session.flush()
    return obj


async def generate_missing_store_product_codes(
    session: AsyncSession,
    store_id: int,
    internal_product_codes: list[str],
) -> int:
    generated = 0
    seen_codes: set[str] = set()

    for raw_code in internal_product_codes:
        normalized_code = str(raw_code or "").strip()
        if not normalized_code or normalized_code in seen_codes:
            continue
        seen_codes.add(normalized_code)

        existing_result = await session.execute(
            select(BusinessStoreProductCode.id)
            .where(
                BusinessStoreProductCode.store_id == int(store_id),
                BusinessStoreProductCode.internal_product_code == normalized_code,
            )
            .limit(1)
        )
        if existing_result.scalar_one_or_none() is not None:
            continue

        await ensure_store_product_code(session, int(store_id), normalized_code)
        generated += 1

    return generated

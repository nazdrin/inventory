from __future__ import annotations

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessStore, BusinessStoreProductCode


async def get_active_business_stores(
    session: AsyncSession,
    enterprise_code: str | None = None,
) -> list[BusinessStore]:
    stmt = select(BusinessStore).where(BusinessStore.is_active.is_(True))
    normalized_enterprise_code = str(enterprise_code or "").strip()
    if normalized_enterprise_code:
        stmt = stmt.where(BusinessStore.enterprise_code == normalized_enterprise_code)

    stmt = stmt.order_by(BusinessStore.store_name.asc(), BusinessStore.store_code.asc())
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_stores_taking_over_legacy_scope(
    session: AsyncSession,
    enterprise_code: str | None = None,
) -> list[BusinessStore]:
    stmt = select(BusinessStore).where(
        BusinessStore.is_active.is_(True),
        BusinessStore.takes_over_legacy_scope.is_(True),
    )
    normalized_enterprise_code = str(enterprise_code or "").strip()
    if normalized_enterprise_code:
        stmt = stmt.where(BusinessStore.enterprise_code == normalized_enterprise_code)

    stmt = stmt.order_by(BusinessStore.store_name.asc(), BusinessStore.store_code.asc())
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_store_by_legacy_scope_key(
    session: AsyncSession,
    legacy_scope_key: str,
) -> BusinessStore | None:
    normalized_key = str(legacy_scope_key or "").strip()
    if not normalized_key:
        return None

    stmt = (
        select(BusinessStore)
        .where(
            BusinessStore.is_active.is_(True),
            BusinessStore.legacy_scope_key == normalized_key,
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_active_store_by_legacy_scope_key(
    session: AsyncSession,
    legacy_scope_key: str,
) -> BusinessStore | None:
    normalized_key = str(legacy_scope_key or "").strip()
    if not normalized_key:
        return None

    stmt = (
        select(BusinessStore)
        .where(
            BusinessStore.is_active.is_(True),
            BusinessStore.legacy_scope_key == normalized_key,
        )
        .order_by(
            BusinessStore.takes_over_legacy_scope.desc(),
            BusinessStore.is_legacy_default.desc(),
            BusinessStore.store_name.asc(),
            BusinessStore.store_code.asc(),
        )
    )
    result = await session.execute(stmt)
    rows = list(result.scalars().all())
    if not rows:
        return None
    if len(rows) > 1:
        raise ValueError(
            f"Multiple active BusinessStore rows found for legacy_scope_key={normalized_key}"
        )
    return rows[0]


async def get_store_by_tabletki_identity(
    session: AsyncSession,
    tabletki_enterprise_code: str,
    tabletki_branch: str,
) -> BusinessStore | None:
    normalized_enterprise_code = str(tabletki_enterprise_code or "").strip()
    normalized_branch = str(tabletki_branch or "").strip()
    if not normalized_enterprise_code or not normalized_branch:
        return None

    stmt = (
        select(BusinessStore)
        .where(
            BusinessStore.is_active.is_(True),
            BusinessStore.tabletki_enterprise_code == normalized_enterprise_code,
            BusinessStore.tabletki_branch == normalized_branch,
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_store_by_salesdrive_enterprise_id(
    session: AsyncSession,
    salesdrive_enterprise_id: int,
) -> BusinessStore | None:
    try:
        normalized_enterprise_id = int(salesdrive_enterprise_id)
    except (TypeError, ValueError):
        return None

    stmt = (
        select(BusinessStore)
        .where(
            BusinessStore.is_active.is_(True),
            BusinessStore.salesdrive_enterprise_id == normalized_enterprise_id,
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_store_by_salesdrive_identity(
    session: AsyncSession,
    salesdrive_enterprise_code: str,
) -> BusinessStore | None:
    normalized_enterprise_code = str(salesdrive_enterprise_code or "").strip()
    if not normalized_enterprise_code:
        return None

    stmt = (
        select(BusinessStore)
        .where(
            BusinessStore.is_active.is_(True),
            BusinessStore.salesdrive_enterprise_code == normalized_enterprise_code,
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def resolve_external_product_code(
    session: AsyncSession,
    store_id: int,
    internal_product_code: str,
) -> str | None:
    normalized_internal_code = str(internal_product_code or "").strip()
    if not normalized_internal_code:
        return None

    stmt = (
        select(BusinessStoreProductCode.external_product_code)
        .join(BusinessStore, BusinessStore.id == BusinessStoreProductCode.store_id)
        .where(
            BusinessStore.is_active.is_(True),
            BusinessStoreProductCode.store_id == int(store_id),
            BusinessStoreProductCode.internal_product_code == normalized_internal_code,
            BusinessStoreProductCode.is_active.is_(True),
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def resolve_internal_product_code(
    session: AsyncSession,
    store_id: int,
    external_product_code: str,
) -> str | None:
    normalized_external_code = str(external_product_code or "").strip()
    if not normalized_external_code:
        return None

    stmt = (
        select(BusinessStoreProductCode.internal_product_code)
        .join(BusinessStore, BusinessStore.id == BusinessStoreProductCode.store_id)
        .where(
            and_(
                BusinessStore.is_active.is_(True),
                BusinessStoreProductCode.store_id == int(store_id),
                BusinessStoreProductCode.external_product_code == normalized_external_code,
                BusinessStoreProductCode.is_active.is_(True),
            )
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()

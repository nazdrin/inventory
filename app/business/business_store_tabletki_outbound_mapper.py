from __future__ import annotations

from copy import deepcopy
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessStore, BusinessStoreProductCode


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


async def resolve_business_store_by_tabletki_branch(
    session: AsyncSession,
    *,
    branch: str,
    enterprise_code: str | None = None,
) -> BusinessStore | None:
    normalized_branch = _clean_text(branch)
    if not normalized_branch:
        return None

    stmt = select(BusinessStore).where(
        BusinessStore.tabletki_branch == normalized_branch,
        BusinessStore.is_active.is_(True),
    )

    normalized_enterprise_code = _clean_text(enterprise_code)
    if normalized_enterprise_code:
        stmt = stmt.where(BusinessStore.enterprise_code == normalized_enterprise_code)

    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        return None
    if len(rows) > 1:
        raise ValueError(
            f"Ambiguous BusinessStore resolution for tabletki_branch={normalized_branch}: "
            f"found {len(rows)} active stores."
        )
    return rows[0]


async def map_internal_code_to_store_external(
    session: AsyncSession,
    *,
    store_id: int,
    internal_product_code: str,
) -> str | None:
    normalized_internal_code = _clean_text(internal_product_code)
    if not normalized_internal_code:
        return None

    row = (
        await session.execute(
            select(BusinessStoreProductCode).where(
                BusinessStoreProductCode.store_id == int(store_id),
                BusinessStoreProductCode.internal_product_code == normalized_internal_code,
                BusinessStoreProductCode.is_active.is_(True),
            ).limit(1)
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    return _clean_text(row.external_product_code)


def _extract_orders(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    if isinstance(payload, dict) and isinstance(payload.get("products"), list):
        return [payload]
    return []


async def restore_salesdrive_products_for_tabletki_outbound(
    session: AsyncSession,
    *,
    payload: dict,
    branch: str | None = None,
    enterprise_code: str | None = None,
    transform_names: bool = False,
) -> dict[str, Any]:
    original_payload = deepcopy(payload)
    transformed_payload = deepcopy(payload)
    warnings: list[str] = []
    errors: list[str] = []

    orders = _extract_orders(transformed_payload)
    if not orders:
        warnings.append("No webhook-like order objects with products were found; payload left unchanged.")
    resolved_branch = _clean_text(branch)
    if not resolved_branch and orders:
        resolved_branch = _clean_text(orders[0].get("branch"))

    if transform_names:
        warnings.append("Product name transformation is not implemented in Stage 1; names left unchanged.")

    try:
        store = await resolve_business_store_by_tabletki_branch(
            session,
            branch=resolved_branch or "",
            enterprise_code=enterprise_code,
        )
    except ValueError as exc:
        errors.append(str(exc))
        return {
            "status": "mapping_error",
            "store_found": False,
            "store_id": None,
            "store_code": None,
            "branch": resolved_branch,
            "mapped_products": 0,
            "missing_mappings": [],
            "warnings": warnings,
            "errors": errors,
            "payload": original_payload,
        }

    if store is None:
        warnings.append("BusinessStore was not resolved by tabletki branch; payload left unchanged.")
        return {
            "status": "legacy_passthrough",
            "store_found": False,
            "store_id": None,
            "store_code": None,
            "branch": resolved_branch,
            "mapped_products": 0,
            "missing_mappings": [],
            "warnings": warnings,
            "errors": errors,
            "payload": original_payload,
        }

    if store.is_legacy_default or _clean_text(store.code_strategy) == "legacy_same":
        warnings.append("BusinessStore uses legacy passthrough code strategy; payload left unchanged.")
        return {
            "status": "legacy_passthrough",
            "store_found": True,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "branch": resolved_branch,
            "mapped_products": 0,
            "missing_mappings": [],
            "warnings": warnings,
            "errors": errors,
            "payload": original_payload,
        }

    missing_mappings: list[dict[str, Any]] = []
    mapped_products = 0

    for order_index, order in enumerate(orders):
        products = order.get("products")
        if not isinstance(products, list):
            continue

        order_branch = _clean_text(order.get("branch"))
        if resolved_branch and order_branch and order_branch != resolved_branch:
            warnings.append(
                f"Order index {order_index} has branch={order_branch}, different from resolved branch={resolved_branch}."
            )

        for product_index, product in enumerate(products):
            if not isinstance(product, dict):
                missing_mappings.append(
                    {
                        "order_index": order_index,
                        "product_index": product_index,
                        "internal_product_code": None,
                        "reason": "invalid_product_payload",
                    }
                )
                continue

            internal_code = _clean_text(product.get("parameter")) or _clean_text(product.get("sku"))
            external_code = await map_internal_code_to_store_external(
                session,
                store_id=int(store.id),
                internal_product_code=internal_code or "",
            )
            if not external_code:
                missing_mappings.append(
                    {
                        "order_index": order_index,
                        "product_index": product_index,
                        "internal_product_code": internal_code,
                        "reason": "missing_external_code_mapping",
                    }
                )
                continue

            if "parameter" in product:
                product["parameter"] = external_code
            if "sku" in product:
                product["sku"] = external_code
            mapped_products += 1

    if missing_mappings:
        errors.append("One or more SalesDrive products could not be mapped to external store codes.")
        return {
            "status": "mapping_error",
            "store_found": True,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "branch": resolved_branch,
            "mapped_products": mapped_products,
            "missing_mappings": missing_mappings,
            "warnings": warnings,
            "errors": errors,
            "payload": transformed_payload,
        }

    return {
        "status": "ok",
        "store_found": True,
        "store_id": int(store.id),
        "store_code": store.store_code,
        "branch": resolved_branch,
        "mapped_products": mapped_products,
        "missing_mappings": [],
        "warnings": warnings,
        "errors": errors,
        "payload": transformed_payload,
    }

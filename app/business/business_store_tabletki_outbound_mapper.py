from __future__ import annotations

from copy import deepcopy
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessEnterpriseProductCode, BusinessStore, EnterpriseSettings


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _uses_external_code_mapping(store: BusinessStore) -> bool:
    return not (
        bool(getattr(store, "is_legacy_default", False))
        or str(_clean_text(getattr(store, "code_strategy", None)) or "").lower() == "legacy_same"
    )


def _code_mapping_mode_for_store(store: BusinessStore | None) -> str:
    if store is None:
        return "enterprise_level"
    return "enterprise_level" if _uses_external_code_mapping(store) else "legacy_same"


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


async def _is_baseline_enterprise_for_store(session: AsyncSession, store: BusinessStore) -> bool:
    enterprise_code = _clean_text(store.enterprise_code)
    if not enterprise_code:
        return True
    runtime_mode = (
        await session.execute(
            select(EnterpriseSettings.business_runtime_mode).where(
                EnterpriseSettings.enterprise_code == enterprise_code
            ).limit(1)
        )
    ).scalar_one_or_none()
    return str(runtime_mode or "baseline").strip().lower() != "custom"


async def map_internal_code_to_store_external(
    session: AsyncSession,
    *,
    store: BusinessStore,
    internal_product_code: str,
) -> dict[str, Any]:
    code_mapping_mode = _code_mapping_mode_for_store(store)
    normalized_internal_code = _clean_text(internal_product_code)
    if not normalized_internal_code:
        return {
            "status": "missing_mapping",
            "code_mapping_mode": code_mapping_mode,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "enterprise_code": _clean_text(store.enterprise_code),
            "internal_product_code": normalized_internal_code,
            "external_product_code": None,
            "reason": "missing_enterprise_internal_code_mapping"
            if code_mapping_mode == "enterprise_level"
            else "missing_internal_code_mapping",
        }

    if code_mapping_mode == "legacy_same":
        return {
            "status": "ok",
            "code_mapping_mode": code_mapping_mode,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "enterprise_code": _clean_text(store.enterprise_code),
            "internal_product_code": normalized_internal_code,
            "external_product_code": normalized_internal_code,
        }

    row = (
        await session.execute(
            select(BusinessEnterpriseProductCode).where(
                BusinessEnterpriseProductCode.enterprise_code == _clean_text(store.enterprise_code),
                BusinessEnterpriseProductCode.internal_product_code == normalized_internal_code,
                BusinessEnterpriseProductCode.is_active.is_(True),
            ).limit(1)
        )
    ).scalar_one_or_none()

    if row is None:
        return {
            "status": "missing_mapping",
            "code_mapping_mode": code_mapping_mode,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "enterprise_code": _clean_text(store.enterprise_code),
            "internal_product_code": normalized_internal_code,
            "external_product_code": None,
            "reason": "missing_enterprise_internal_code_mapping"
            if code_mapping_mode == "enterprise_level"
            else "missing_internal_code_mapping",
        }

    return {
        "status": "ok",
        "code_mapping_mode": code_mapping_mode,
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": _clean_text(store.enterprise_code),
        "internal_product_code": normalized_internal_code,
        "external_product_code": _clean_text(row.external_product_code),
    }


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
    code_mapping_mode = _code_mapping_mode_for_store(None)

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
            "code_mapping_mode": code_mapping_mode,
            "store_found": False,
            "store_id": None,
            "store_code": None,
            "enterprise_code": _clean_text(enterprise_code),
            "branch": resolved_branch,
            "mapped_products": 0,
            "first_parameter_before": None,
            "first_parameter_after": None,
            "first_sku_before": None,
            "first_sku_after": None,
            "missing_mappings": [],
            "warnings": warnings,
            "errors": errors,
            "payload": original_payload,
        }

    if store is None:
        warnings.append("BusinessStore was not resolved by tabletki branch; payload left unchanged.")
        return {
            "status": "legacy_passthrough",
            "code_mapping_mode": code_mapping_mode,
            "store_found": False,
            "store_id": None,
            "store_code": None,
            "enterprise_code": _clean_text(enterprise_code),
            "branch": resolved_branch,
            "mapped_products": 0,
            "first_parameter_before": None,
            "first_parameter_after": None,
            "first_sku_before": None,
            "first_sku_after": None,
            "missing_mappings": [],
            "warnings": warnings,
            "errors": errors,
            "payload": original_payload,
        }

    is_baseline_enterprise = await _is_baseline_enterprise_for_store(session, store)
    if (
        is_baseline_enterprise
        or store.is_legacy_default
        or str(_clean_text(store.code_strategy) or "").lower() == "legacy_same"
    ):
        code_mapping_mode = "legacy_same" if is_baseline_enterprise else _code_mapping_mode_for_store(store)
        warnings.append("BusinessStore uses legacy passthrough code strategy; payload left unchanged.")
        return {
            "status": "legacy_passthrough",
            "code_mapping_mode": code_mapping_mode,
            "store_found": True,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "enterprise_code": _clean_text(store.enterprise_code),
            "branch": resolved_branch,
            "mapped_products": 0,
            "first_parameter_before": None,
            "first_parameter_after": None,
            "first_sku_before": None,
            "first_sku_after": None,
            "missing_mappings": [],
            "warnings": warnings,
            "errors": errors,
            "payload": original_payload,
        }

    code_mapping_mode = _code_mapping_mode_for_store(store)
    missing_mappings: list[dict[str, Any]] = []
    mapped_products = 0
    first_parameter_before: str | None = None
    first_parameter_after: str | None = None
    first_sku_before: str | None = None
    first_sku_after: str | None = None

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

            parameter_before = _clean_text(product.get("parameter"))
            sku_before = _clean_text(product.get("sku"))
            if first_parameter_before is None:
                first_parameter_before = parameter_before
            if first_sku_before is None:
                first_sku_before = sku_before

            parameter_result = None
            sku_result = None
            if parameter_before:
                parameter_result = await map_internal_code_to_store_external(
                    session,
                    store=store,
                    internal_product_code=parameter_before,
                )
            if sku_before and sku_before != parameter_before:
                sku_result = await map_internal_code_to_store_external(
                    session,
                    store=store,
                    internal_product_code=sku_before,
                )
            elif sku_before:
                sku_result = parameter_result

            failed_results = [
                result for result in (parameter_result, sku_result)
                if result is not None and result.get("status") != "ok"
            ]
            if failed_results:
                failure = failed_results[0]
                missing_mappings.append(
                    {
                        "order_index": order_index,
                        "product_index": product_index,
                        "internal_product_code": failure.get("internal_product_code"),
                        "reason": failure.get("reason") or (
                            "missing_enterprise_internal_code_mapping"
                            if code_mapping_mode == "enterprise_level"
                            else "missing_internal_code_mapping"
                        ),
                    }
                )
                continue

            if parameter_result is None and sku_result is None:
                missing_mappings.append(
                    {
                        "order_index": order_index,
                        "product_index": product_index,
                        "internal_product_code": None,
                        "reason": "missing_enterprise_internal_code_mapping"
                        if code_mapping_mode == "enterprise_level"
                        else "missing_internal_code_mapping",
                    }
                )
                continue

            if "parameter" in product and parameter_result is not None:
                product["parameter"] = parameter_result.get("external_product_code")
            if "sku" in product and sku_result is not None:
                product["sku"] = sku_result.get("external_product_code")

            if first_parameter_after is None:
                first_parameter_after = _clean_text(product.get("parameter"))
            if first_sku_after is None:
                first_sku_after = _clean_text(product.get("sku"))

            if parameter_before and sku_before and parameter_before != sku_before:
                warnings.append(
                    f"Order index {order_index} product index {product_index} had different parameter and sku internal codes."
                )

            mapped_products += 1

    if missing_mappings:
        errors.append("One or more SalesDrive products could not be mapped to external store codes.")
        return {
            "status": "mapping_error",
            "code_mapping_mode": code_mapping_mode,
            "store_found": True,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "enterprise_code": _clean_text(store.enterprise_code),
            "branch": resolved_branch,
            "mapped_products": mapped_products,
            "first_parameter_before": first_parameter_before,
            "first_parameter_after": first_parameter_after,
            "first_sku_before": first_sku_before,
            "first_sku_after": first_sku_after,
            "missing_mappings": missing_mappings,
            "warnings": warnings,
            "errors": errors,
            "payload": transformed_payload,
        }

    return {
        "status": "ok",
        "code_mapping_mode": code_mapping_mode,
        "store_found": True,
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": _clean_text(store.enterprise_code),
        "branch": resolved_branch,
        "mapped_products": mapped_products,
        "first_parameter_before": first_parameter_before,
        "first_parameter_after": first_parameter_after,
        "first_sku_before": first_sku_before,
        "first_sku_after": first_sku_after,
        "missing_mappings": [],
        "warnings": warnings,
        "errors": errors,
        "payload": transformed_payload,
    }

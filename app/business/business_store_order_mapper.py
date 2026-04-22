from __future__ import annotations

from copy import deepcopy
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessStore, BusinessStoreProductCode


ORIGINAL_EXTERNAL_GOODS_CODE_FIELD = "originalGoodsCodeExternal"
DEBUG_BUSINESS_STORE_ID_FIELD = "_businessStoreId"


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


async def resolve_business_store_for_order(
    session: AsyncSession,
    *,
    tabletki_branch: str | int | None = None,
    tabletki_enterprise_code: str | int | None = None,
    store_code: str | None = None,
    store_id: int | None = None,
) -> BusinessStore | None:
    if store_id is not None:
        row = (
            await session.execute(
                select(BusinessStore).where(
                    BusinessStore.id == int(store_id),
                    BusinessStore.is_active.is_(True),
                ).limit(1)
            )
        ).scalar_one_or_none()
        return row

    normalized_store_code = _clean_text(store_code)
    if normalized_store_code:
        row = (
            await session.execute(
                select(BusinessStore).where(
                    BusinessStore.store_code == normalized_store_code,
                    BusinessStore.is_active.is_(True),
                ).limit(1)
            )
        ).scalar_one_or_none()
        return row

    normalized_branch = _clean_text(tabletki_branch)
    normalized_enterprise_code = _clean_text(tabletki_enterprise_code)

    if normalized_branch and normalized_enterprise_code:
        row = (
            await session.execute(
                select(BusinessStore).where(
                    BusinessStore.tabletki_branch == normalized_branch,
                    BusinessStore.tabletki_enterprise_code == normalized_enterprise_code,
                    BusinessStore.is_active.is_(True),
                ).limit(1)
            )
        ).scalar_one_or_none()
        return row

    if normalized_branch:
        rows = (
            await session.execute(
                select(BusinessStore).where(
                    BusinessStore.tabletki_branch == normalized_branch,
                    BusinessStore.is_active.is_(True),
                )
            )
        ).scalars().all()
        if not rows:
            return None
        if len(rows) > 1:
            raise ValueError(
                f"Ambiguous BusinessStore resolution for tabletki_branch={normalized_branch}: "
                f"found {len(rows)} active stores."
            )
        return rows[0]

    return None


async def map_external_order_code_to_internal(
    session: AsyncSession,
    *,
    store_id: int,
    external_product_code: str,
) -> dict[str, Any]:
    normalized_external_product_code = _clean_text(external_product_code)
    if not normalized_external_product_code:
        return {
            "status": "missing_mapping",
            "store_id": int(store_id),
            "external_product_code": external_product_code,
            "internal_product_code": None,
            "reason": "missing_external_code_mapping",
        }

    row = (
        await session.execute(
            select(BusinessStoreProductCode).where(
                BusinessStoreProductCode.store_id == int(store_id),
                BusinessStoreProductCode.external_product_code == normalized_external_product_code,
                BusinessStoreProductCode.is_active.is_(True),
            ).limit(1)
        )
    ).scalar_one_or_none()

    if row is None:
        return {
            "status": "missing_mapping",
            "store_id": int(store_id),
            "external_product_code": normalized_external_product_code,
            "internal_product_code": None,
            "reason": "missing_external_code_mapping",
        }

    return {
        "status": "ok",
        "store_id": int(store_id),
        "external_product_code": normalized_external_product_code,
        "internal_product_code": str(row.internal_product_code),
    }


async def normalize_store_order_payload(
    session: AsyncSession,
    *,
    order_payload: dict,
    store_id: int | None = None,
    store_code: str | None = None,
    tabletki_branch: str | int | None = None,
    tabletki_enterprise_code: str | int | None = None,
) -> dict[str, Any]:
    original_payload = deepcopy(order_payload)
    warnings: list[str] = []
    errors: list[str] = []

    store = await resolve_business_store_for_order(
        session,
        tabletki_branch=tabletki_branch,
        tabletki_enterprise_code=tabletki_enterprise_code,
        store_code=store_code,
        store_id=store_id,
    )

    if store is None:
        warnings.append("BusinessStore was not resolved; order payload left unchanged.")
        return {
            "status": "legacy_passthrough",
            "store_found": False,
            "store_id": None,
            "store_code": None,
            "mapped_rows": 0,
            "missing_mappings": [],
            "order": original_payload,
            "warnings": warnings,
            "errors": errors,
        }

    normalized_order = deepcopy(order_payload)
    rows = normalized_order.get("rows")
    if not isinstance(rows, list):
        errors.append("order_payload.rows must be a list")
        return {
            "status": "mapping_error",
            "store_found": True,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "mapped_rows": 0,
            "missing_mappings": [],
            "order": original_payload,
            "warnings": warnings,
            "errors": errors,
        }

    missing_mappings: list[dict[str, Any]] = []
    mapped_rows = 0

    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            missing_mappings.append(
                {
                    "row_index": index,
                    "external_product_code": None,
                    "reason": "invalid_row_payload",
                }
            )
            continue

        original_goods_code = _clean_text(row.get("goodsCode"))
        mapping_result = await map_external_order_code_to_internal(
            session,
            store_id=int(store.id),
            external_product_code=original_goods_code or "",
        )

        if mapping_result["status"] != "ok":
            missing_mappings.append(
                {
                    "row_index": index,
                    "external_product_code": original_goods_code,
                    "reason": mapping_result.get("reason") or "missing_external_code_mapping",
                }
            )
            continue

        row[ORIGINAL_EXTERNAL_GOODS_CODE_FIELD] = mapping_result["external_product_code"]
        row["goodsCode"] = mapping_result["internal_product_code"]
        row[DEBUG_BUSINESS_STORE_ID_FIELD] = int(store.id)
        mapped_rows += 1

    if missing_mappings:
        errors.append("One or more order rows could not be reverse-mapped to internal product codes.")
        return {
            "status": "mapping_error",
            "store_found": True,
            "store_id": int(store.id),
            "store_code": store.store_code,
            "mapped_rows": mapped_rows,
            "missing_mappings": missing_mappings,
            "order": normalized_order,
            "warnings": warnings,
            "errors": errors,
        }

    return {
        "status": "ok",
        "store_found": True,
        "store_id": int(store.id),
        "store_code": store.store_code,
        "mapped_rows": mapped_rows,
        "missing_mappings": [],
        "order": normalized_order,
        "warnings": warnings,
        "errors": errors,
    }


def restore_tabletki_goods_codes_for_status(order: dict) -> dict:
    restored_order = deepcopy(order)
    rows = restored_order.get("rows")
    if not isinstance(rows, list):
        return restored_order

    for row in rows:
        if not isinstance(row, dict):
            continue

        original_external = _clean_text(row.get(ORIGINAL_EXTERNAL_GOODS_CODE_FIELD))
        if original_external:
            row["goodsCode"] = original_external

        row.pop(ORIGINAL_EXTERNAL_GOODS_CODE_FIELD, None)
        row.pop(DEBUG_BUSINESS_STORE_ID_FIELD, None)

    return restored_order

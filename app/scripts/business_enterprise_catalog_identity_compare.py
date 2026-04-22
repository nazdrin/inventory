from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from app.database import get_async_db
from app.models import (
    BusinessEnterpriseProductCode,
    BusinessEnterpriseProductName,
    BusinessStore,
    BusinessStoreProductCode,
    BusinessStoreProductName,
)


DEFAULT_LIMIT_DIFFS = 20


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _print_result(result: dict[str, Any], *, output_json: bool) -> None:
    if output_json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
        return

    print(
        "status={status} store={store_code} enterprise={enterprise_code}".format(
            status=result.get("status"),
            store_code=result.get("store_code"),
            enterprise_code=result.get("enterprise_code"),
        )
    )
    code_counts = result.get("code_counts") or {}
    name_counts = result.get("name_counts") or {}
    print(
        "codes: store_total={store_total} enterprise_total={enterprise_total} matched={matched} missing={missing} different={different} extra={extra}".format(
            store_total=code_counts.get("store_total"),
            enterprise_total=code_counts.get("enterprise_total"),
            matched=code_counts.get("matched"),
            missing=code_counts.get("missing_in_enterprise"),
            different=code_counts.get("different_values"),
            extra=code_counts.get("extra_enterprise"),
        )
    )
    print(
        "names: store_total={store_total} enterprise_total={enterprise_total} matched={matched} missing={missing} different={different} extra={extra}".format(
            store_total=name_counts.get("store_total"),
            enterprise_total=name_counts.get("enterprise_total"),
            matched=name_counts.get("matched"),
            missing=name_counts.get("missing_in_enterprise"),
            different=name_counts.get("different_values"),
            extra=name_counts.get("extra_enterprise"),
        )
    )
    if result.get("warnings"):
        print("warnings:")
        for item in result["warnings"]:
            print(f"- {item}")
    if result.get("errors"):
        print("errors:")
        for item in result["errors"]:
            print(f"- {item}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare store-level and enterprise-level catalog identity mappings.",
    )
    parser.add_argument("--store-id", type=int, default=0)
    parser.add_argument("--store-code", default="")
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--output-json", action="store_true")
    parser.add_argument("--limit-diffs", type=int, default=DEFAULT_LIMIT_DIFFS)
    return parser.parse_args()


def _ensure_selector(args: argparse.Namespace) -> tuple[int | None, str | None]:
    store_id = int(args.store_id or 0)
    store_code = _clean_text(args.store_code)
    if bool(store_id) and store_code:
        raise ValueError("Use either --store-id or --store-code, not both.")
    if not bool(store_id) and not store_code:
        raise ValueError("Comparison requires --store-id or --store-code.")
    return (store_id if store_id > 0 else None), (store_code or None)


async def _resolve_store(session, *, store_id: int | None, store_code: str | None) -> BusinessStore:
    stmt = select(BusinessStore)
    if store_id is not None:
        stmt = stmt.where(BusinessStore.id == int(store_id))
    else:
        stmt = stmt.where(BusinessStore.store_code == str(store_code))
    store = (await session.execute(stmt.limit(1))).scalar_one_or_none()
    if store is None:
        selector = f"store_id={store_id}" if store_id is not None else f"store_code={store_code}"
        raise ValueError(f"BusinessStore not found for {selector}")
    return store


async def _load_store_codes(session, *, store_id: int) -> list[BusinessStoreProductCode]:
    stmt = (
        select(BusinessStoreProductCode)
        .where(BusinessStoreProductCode.store_id == int(store_id))
        .order_by(BusinessStoreProductCode.internal_product_code.asc(), BusinessStoreProductCode.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def _load_enterprise_codes(session, *, enterprise_code: str) -> list[BusinessEnterpriseProductCode]:
    stmt = (
        select(BusinessEnterpriseProductCode)
        .where(BusinessEnterpriseProductCode.enterprise_code == enterprise_code)
        .order_by(BusinessEnterpriseProductCode.internal_product_code.asc(), BusinessEnterpriseProductCode.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def _load_store_names(session, *, store_id: int) -> list[BusinessStoreProductName]:
    stmt = (
        select(BusinessStoreProductName)
        .where(BusinessStoreProductName.store_id == int(store_id))
        .order_by(BusinessStoreProductName.internal_product_code.asc(), BusinessStoreProductName.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


async def _load_enterprise_names(session, *, enterprise_code: str) -> list[BusinessEnterpriseProductName]:
    stmt = (
        select(BusinessEnterpriseProductName)
        .where(BusinessEnterpriseProductName.enterprise_code == enterprise_code)
        .order_by(BusinessEnterpriseProductName.internal_product_code.asc(), BusinessEnterpriseProductName.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


def _limited(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return items[: max(0, int(limit))]


async def _run_compare(*, session, store: BusinessStore, enterprise_code: str, limit_diffs: int) -> dict[str, Any]:
    store_codes = await _load_store_codes(session, store_id=int(store.id))
    enterprise_codes = await _load_enterprise_codes(session, enterprise_code=enterprise_code)
    store_names = await _load_store_names(session, store_id=int(store.id))
    enterprise_names = await _load_enterprise_names(session, enterprise_code=enterprise_code)

    store_codes_by_internal = {
        _clean_text(row.internal_product_code): _clean_text(row.external_product_code)
        for row in store_codes
        if _clean_text(row.internal_product_code)
    }
    enterprise_codes_by_internal = {
        _clean_text(row.internal_product_code): _clean_text(row.external_product_code)
        for row in enterprise_codes
        if _clean_text(row.internal_product_code)
    }
    store_names_by_internal = {
        _clean_text(row.internal_product_code): _clean_text(row.external_product_name)
        for row in store_names
        if _clean_text(row.internal_product_code)
    }
    enterprise_names_by_internal = {
        _clean_text(row.internal_product_code): _clean_text(row.external_product_name)
        for row in enterprise_names
        if _clean_text(row.internal_product_code)
    }

    missing_codes: list[dict[str, Any]] = []
    different_codes: list[dict[str, Any]] = []
    extra_codes: list[dict[str, Any]] = []
    matched_codes = 0

    for internal_code, store_external in store_codes_by_internal.items():
        enterprise_external = enterprise_codes_by_internal.get(internal_code)
        if enterprise_external is None:
            missing_codes.append(
                {
                    "internal_product_code": internal_code,
                    "store_external_product_code": store_external,
                }
            )
        elif enterprise_external != store_external:
            different_codes.append(
                {
                    "internal_product_code": internal_code,
                    "store_external_product_code": store_external,
                    "enterprise_external_product_code": enterprise_external,
                }
            )
        else:
            matched_codes += 1

    for internal_code, enterprise_external in enterprise_codes_by_internal.items():
        if internal_code not in store_codes_by_internal:
            extra_codes.append(
                {
                    "internal_product_code": internal_code,
                    "enterprise_external_product_code": enterprise_external,
                }
            )

    missing_names: list[dict[str, Any]] = []
    different_names: list[dict[str, Any]] = []
    extra_names: list[dict[str, Any]] = []
    matched_names = 0

    for internal_code, store_external_name in store_names_by_internal.items():
        enterprise_external_name = enterprise_names_by_internal.get(internal_code)
        if enterprise_external_name is None:
            missing_names.append(
                {
                    "internal_product_code": internal_code,
                    "store_external_product_name": store_external_name,
                }
            )
        elif enterprise_external_name != store_external_name:
            different_names.append(
                {
                    "internal_product_code": internal_code,
                    "store_external_product_name": store_external_name,
                    "enterprise_external_product_name": enterprise_external_name,
                }
            )
        else:
            matched_names += 1

    for internal_code, enterprise_external_name in enterprise_names_by_internal.items():
        if internal_code not in store_names_by_internal:
            extra_names.append(
                {
                    "internal_product_code": internal_code,
                    "enterprise_external_product_name": enterprise_external_name,
                }
            )

    warnings: list[str] = []
    errors: list[str] = []
    status = "ok"
    if missing_codes or different_codes or extra_codes or missing_names or different_names or extra_names:
        status = "warning"

    return {
        "status": status,
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": enterprise_code,
        "code_counts": {
            "store_total": len(store_codes_by_internal),
            "enterprise_total": len(enterprise_codes_by_internal),
            "matched": matched_codes,
            "missing_in_enterprise": len(missing_codes),
            "different_values": len(different_codes),
            "extra_enterprise": len(extra_codes),
        },
        "name_counts": {
            "store_total": len(store_names_by_internal),
            "enterprise_total": len(enterprise_names_by_internal),
            "matched": matched_names,
            "missing_in_enterprise": len(missing_names),
            "different_values": len(different_names),
            "extra_enterprise": len(extra_names),
        },
        "samples": {
            "missing_codes": _limited(missing_codes, limit_diffs),
            "different_codes": _limited(different_codes, limit_diffs),
            "extra_codes": _limited(extra_codes, limit_diffs),
            "missing_names": _limited(missing_names, limit_diffs),
            "different_names": _limited(different_names, limit_diffs),
            "extra_names": _limited(extra_names, limit_diffs),
        },
        "warnings": warnings,
        "errors": errors,
    }


async def _amain() -> None:
    args = _parse_args()
    store_id, store_code = _ensure_selector(args)

    async with get_async_db(commit_on_exit=False) as session:
        try:
            store = await _resolve_store(session, store_id=store_id, store_code=store_code)
            enterprise_code = _clean_text(args.enterprise_code) or _clean_text(store.enterprise_code)
            if not enterprise_code:
                raise ValueError("enterprise_code is required; pass --enterprise-code or ensure BusinessStore.enterprise_code is filled.")

            result = await _run_compare(
                session=session,
                store=store,
                enterprise_code=enterprise_code,
                limit_diffs=int(args.limit_diffs or DEFAULT_LIMIT_DIFFS),
            )
            await session.rollback()
        except Exception as exc:
            await session.rollback()
            result = {
                "status": "error",
                "store_id": None,
                "store_code": None,
                "enterprise_code": _clean_text(args.enterprise_code) or None,
                "code_counts": {
                    "store_total": 0,
                    "enterprise_total": 0,
                    "matched": 0,
                    "missing_in_enterprise": 0,
                    "different_values": 0,
                    "extra_enterprise": 0,
                },
                "name_counts": {
                    "store_total": 0,
                    "enterprise_total": 0,
                    "matched": 0,
                    "missing_in_enterprise": 0,
                    "different_values": 0,
                    "extra_enterprise": 0,
                },
                "samples": {
                    "missing_codes": [],
                    "different_codes": [],
                    "extra_codes": [],
                    "missing_names": [],
                    "different_names": [],
                    "extra_names": [],
                },
                "warnings": [],
                "errors": [str(exc)],
            }

    _print_result(result, output_json=bool(args.output_json))


if __name__ == "__main__":
    asyncio.run(_amain())

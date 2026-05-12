from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from app.business.business_enterprise_catalog_preview import (
    build_enterprise_catalog_payload_preview,
)
from app.business.business_store_catalog_preview import build_store_catalog_payload_preview
from app.database import get_async_db
from app.models import BusinessStore, DeveloperSettings


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
        "status={status} store={store_code} enterprise={enterprise_code} assortment_mode={assortment_mode}".format(
            status=result.get("status"),
            store_code=result.get("store_code"),
            enterprise_code=result.get("enterprise_code"),
            assortment_mode=result.get("assortment_mode"),
        )
    )
    comparison = result.get("comparison") or {}
    print(
        "candidate_matched={candidate_matched} exportable_matched={exportable_matched} "
        "missing_in_new={missing_in_new} missing_in_old={missing_in_old} "
        "different_codes={different_codes} different_names={different_names} "
        "different_exportable_flags={different_exportable_flags} different_reasons={different_reasons} "
        "branch_same={branch_same}".format(
            candidate_matched=comparison.get("candidate_matched"),
            exportable_matched=comparison.get("exportable_matched"),
            missing_in_new=comparison.get("missing_in_new"),
            missing_in_old=comparison.get("missing_in_old"),
            different_codes=comparison.get("different_codes"),
            different_names=comparison.get("different_names"),
            different_exportable_flags=comparison.get("different_exportable_flags"),
            different_reasons=comparison.get("different_reasons"),
            branch_same=comparison.get("branch_same"),
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
        description="Compare current store-level catalog preview with enterprise-level catalog preview.",
    )
    parser.add_argument("--store-id", type=int, default=0)
    parser.add_argument("--store-code", default="")
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output-json", action="store_true")
    parser.add_argument("--limit-diffs", type=int, default=DEFAULT_LIMIT_DIFFS)
    parser.add_argument(
        "--assortment-mode",
        default="store_compatible",
        choices=["store_compatible", "master_all"],
    )
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


async def _load_old_endpoint_preview(session, *, store: BusinessStore) -> str | None:
    developer_settings = (await session.execute(select(DeveloperSettings).limit(1))).scalar_one_or_none()
    if developer_settings is None:
        return None
    endpoint_catalog = _clean_text(developer_settings.endpoint_catalog)
    branch = _clean_text(store.tabletki_branch)
    if not endpoint_catalog or not branch:
        return None
    return f"{endpoint_catalog}/Import/Ref/{branch}"


def _limited(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return items[: max(0, int(limit))]


def _row_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        internal_code = _clean_text(row.get("internal_product_code"))
        if internal_code:
            result[internal_code] = row
    return result


def _stringify_reasons(row: dict[str, Any]) -> tuple[str, ...]:
    reasons = row.get("reasons") or []
    if not isinstance(reasons, list):
        return tuple()
    return tuple(sorted(_clean_text(item) for item in reasons if _clean_text(item)))


def _normalized_reason(value: str) -> str:
    normalized = _clean_text(value)
    if normalized == "missing_enterprise_code_mapping":
        return "missing_code_mapping"
    if normalized == "missing_enterprise_name_mapping":
        return "missing_name_mapping"
    return normalized


def _normalized_reasons(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(sorted(_normalized_reason(item) for item in _stringify_reasons(row) if _normalized_reason(item)))


async def _run_compare(
    *,
    session,
    store: BusinessStore,
    enterprise_code: str,
    limit: int | None,
    limit_diffs: int,
    assortment_mode: str,
) -> dict[str, Any]:
    old_preview = await build_store_catalog_payload_preview(
        session,
        int(store.id),
        limit=limit,
        include_not_exportable=True,
    )
    new_preview = await build_enterprise_catalog_payload_preview(
        session,
        enterprise_code=enterprise_code,
        limit=limit,
        assortment_mode=assortment_mode,
        store_id=int(store.id),
    )

    old_payload_rows = list(old_preview.get("payload_preview") or [])
    new_payload_rows = list(new_preview.get("payload_preview") or [])
    old_rows_by_internal = _row_map(old_payload_rows)
    new_rows_by_internal = _row_map(new_payload_rows)

    missing_in_new: list[dict[str, Any]] = []
    missing_in_old: list[dict[str, Any]] = []
    different_codes: list[dict[str, Any]] = []
    different_names: list[dict[str, Any]] = []
    different_exportable_flags: list[dict[str, Any]] = []
    reason_mismatch_normalized: list[dict[str, Any]] = []
    raw_reason_differences: list[dict[str, Any]] = []
    different_barcodes: list[dict[str, Any]] = []
    different_manufacturers: list[dict[str, Any]] = []
    candidate_matched = 0
    exportable_matched = 0

    for internal_code, old_row in old_rows_by_internal.items():
        new_row = new_rows_by_internal.get(internal_code)
        if new_row is None:
            missing_in_new.append(
                {
                    "internal_product_code": internal_code,
                    "old_external_product_code": old_row.get("external_product_code"),
                    "old_external_product_name": old_row.get("external_product_name"),
                }
            )
            continue

        candidate_matched += 1

        if _clean_text(old_row.get("external_product_code")) != _clean_text(new_row.get("external_product_code")):
            different_codes.append(
                {
                    "internal_product_code": internal_code,
                    "old_external_product_code": old_row.get("external_product_code"),
                    "new_external_product_code": new_row.get("external_product_code"),
                }
            )
        if _clean_text(old_row.get("external_product_name")) != _clean_text(new_row.get("external_product_name")):
            different_names.append(
                {
                    "internal_product_code": internal_code,
                    "old_external_product_name": old_row.get("external_product_name"),
                    "new_external_product_name": new_row.get("external_product_name"),
                }
            )
        if _clean_text(old_row.get("barcode")) != _clean_text(new_row.get("barcode")):
            different_barcodes.append(
                {
                    "internal_product_code": internal_code,
                    "old_barcode": old_row.get("barcode"),
                    "new_barcode": new_row.get("barcode"),
                }
            )
        if _clean_text(old_row.get("manufacturer")) != _clean_text(new_row.get("manufacturer")):
            different_manufacturers.append(
                {
                    "internal_product_code": internal_code,
                    "old_manufacturer": old_row.get("manufacturer"),
                    "new_manufacturer": new_row.get("manufacturer"),
                }
            )
        if bool(old_row.get("exportable")) != bool(new_row.get("exportable")):
            different_exportable_flags.append(
                {
                    "internal_product_code": internal_code,
                    "old_exportable": bool(old_row.get("exportable")),
                    "new_exportable": bool(new_row.get("exportable")),
                }
            )
        if _normalized_reasons(old_row) != _normalized_reasons(new_row):
            reason_mismatch_normalized.append(
                {
                    "internal_product_code": internal_code,
                    "old_reasons": list(_normalized_reasons(old_row)),
                    "new_reasons": list(_normalized_reasons(new_row)),
                }
            )
        if _stringify_reasons(old_row) != _stringify_reasons(new_row):
            raw_reason_differences.append(
                {
                    "internal_product_code": internal_code,
                    "old_reasons": list(_stringify_reasons(old_row)),
                    "new_reasons": list(_stringify_reasons(new_row)),
                }
            )

        if (
            bool(old_row.get("exportable"))
            and bool(new_row.get("exportable"))
            and _clean_text(old_row.get("external_product_code")) == _clean_text(new_row.get("external_product_code"))
            and _clean_text(old_row.get("external_product_name")) == _clean_text(new_row.get("external_product_name"))
            and _clean_text(old_row.get("barcode")) == _clean_text(new_row.get("barcode"))
            and _clean_text(old_row.get("manufacturer")) == _clean_text(new_row.get("manufacturer"))
            and _normalized_reasons(old_row) == _normalized_reasons(new_row)
        ):
            exportable_matched += 1

    for internal_code, new_row in new_rows_by_internal.items():
        if internal_code not in old_rows_by_internal:
            missing_in_old.append(
                {
                    "internal_product_code": internal_code,
                    "new_external_product_code": new_row.get("external_product_code"),
                    "new_external_product_name": new_row.get("external_product_name"),
                }
            )

    old_endpoint_preview = await _load_old_endpoint_preview(session, store=store)
    new_endpoint_preview = new_preview.get("endpoint_preview")
    old_branch = _clean_text(store.tabletki_branch) or None
    new_branch = _clean_text(new_preview.get("tabletki_branch")) or None
    branch_same = old_branch == new_branch

    warnings = list(old_preview.get("warnings") or []) + list(new_preview.get("warnings") or [])
    errors = list(old_preview.get("errors") or []) + list(new_preview.get("errors") or [])
    if limit is not None:
        warnings.append("Comparison uses limited preview rows because --limit was provided.")
    if not branch_same:
        warnings.append("Store-level catalog branch and enterprise-level catalog branch are different.")

    status = "ok"
    if errors:
        status = "error"
    elif (
        missing_in_new
        or missing_in_old
        or different_codes
        or different_names
        or different_exportable_flags
        or reason_mismatch_normalized
        or different_barcodes
        or different_manufacturers
        or not branch_same
    ):
        status = "warning"

    return {
        "status": status,
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": enterprise_code,
        "assortment_mode": assortment_mode,
        "old_preview": {
            "candidate_products": old_preview.get("summary", {}).get("candidate_products", 0),
            "exportable_products": old_preview.get("summary", {}).get("exportable_products", 0),
            "not_exportable_products": old_preview.get("summary", {}).get("not_exportable_products", 0),
            "tabletki_branch": old_branch,
            "endpoint_preview": old_endpoint_preview,
            "candidate_source": old_preview.get("summary", {}).get("catalog_source"),
        },
        "new_preview": {
            "candidate_products": new_preview.get("candidate_products", 0),
            "exportable_products": new_preview.get("exportable_products", 0),
            "not_exportable_products": new_preview.get("not_exportable_products", 0),
            "tabletki_branch": new_branch,
            "endpoint_preview": new_endpoint_preview,
            "candidate_source": new_preview.get("candidate_source"),
            "store_id_used_for_assortment": new_preview.get("store_id_used_for_assortment"),
        },
        "comparison": {
            "candidate_matched": candidate_matched,
            "exportable_matched": exportable_matched,
            "missing_in_new": len(missing_in_new),
            "missing_in_old": len(missing_in_old),
            "different_codes": len(different_codes),
            "different_names": len(different_names),
            "different_exportable_flags": len(different_exportable_flags),
            "different_reasons": len(reason_mismatch_normalized),
            "reason_mismatch_normalized": len(reason_mismatch_normalized),
            "raw_reason_differences": len(raw_reason_differences),
            "different_barcodes": len(different_barcodes),
            "different_manufacturers": len(different_manufacturers),
            "branch_same": branch_same,
        },
        "samples": {
            "missing_in_new": _limited(missing_in_new, limit_diffs),
            "missing_in_old": _limited(missing_in_old, limit_diffs),
            "different_codes": _limited(different_codes, limit_diffs),
            "different_names": _limited(different_names, limit_diffs),
            "different_exportable_flags": _limited(different_exportable_flags, limit_diffs),
            "different_reasons": _limited(reason_mismatch_normalized, limit_diffs),
            "reason_mismatch_normalized": _limited(reason_mismatch_normalized, limit_diffs),
            "raw_reason_differences": _limited(raw_reason_differences, limit_diffs),
            "different_barcodes": _limited(different_barcodes, limit_diffs),
            "different_manufacturers": _limited(different_manufacturers, limit_diffs),
        },
        "warnings": warnings,
        "errors": errors,
    }


async def _amain() -> None:
    args = _parse_args()
    store_id, store_code = _ensure_selector(args)
    limit = None if int(args.limit or 0) <= 0 else int(args.limit)

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
                limit=limit,
                limit_diffs=int(args.limit_diffs or DEFAULT_LIMIT_DIFFS),
                assortment_mode=_clean_text(args.assortment_mode) or "store_compatible",
            )
            await session.rollback()
        except Exception as exc:
            await session.rollback()
            result = {
                "status": "error",
                "store_id": None,
                "store_code": None,
                "enterprise_code": _clean_text(args.enterprise_code) or None,
                "assortment_mode": _clean_text(args.assortment_mode) or "store_compatible",
                "old_preview": {
                    "candidate_products": 0,
                    "exportable_products": 0,
                    "not_exportable_products": 0,
                    "tabletki_branch": None,
                    "endpoint_preview": None,
                    "candidate_source": None,
                },
                "new_preview": {
                    "candidate_products": 0,
                    "exportable_products": 0,
                    "not_exportable_products": 0,
                    "tabletki_branch": None,
                    "endpoint_preview": None,
                    "candidate_source": None,
                    "store_id_used_for_assortment": None,
                },
                "comparison": {
                    "candidate_matched": 0,
                    "exportable_matched": 0,
                    "missing_in_new": 0,
                    "missing_in_old": 0,
                    "different_codes": 0,
                    "different_names": 0,
                    "different_exportable_flags": 0,
                    "different_reasons": 0,
                    "reason_mismatch_normalized": 0,
                    "raw_reason_differences": 0,
                    "different_barcodes": 0,
                    "different_manufacturers": 0,
                    "branch_same": False,
                },
                "samples": {
                    "missing_in_new": [],
                    "missing_in_old": [],
                    "different_codes": [],
                    "different_names": [],
                    "different_exportable_flags": [],
                    "different_reasons": [],
                    "reason_mismatch_normalized": [],
                    "raw_reason_differences": [],
                    "different_barcodes": [],
                    "different_manufacturers": [],
                },
                "warnings": [],
                "errors": [str(exc)],
            }

    _print_result(result, output_json=bool(args.output_json))


if __name__ == "__main__":
    asyncio.run(_amain())

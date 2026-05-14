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


SAMPLE_LIMIT = 20


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
        "status={status} dry_run={dry_run} store={store_code} enterprise={enterprise_code}".format(
            status=result.get("status"),
            dry_run=result.get("dry_run"),
            store_code=result.get("store_code"),
            enterprise_code=result.get("enterprise_code"),
        )
    )
    print(
        "codes: source={src} insert={ins} same={same} conflicts={conflicts} updated={updated}".format(
            src=result.get("codes_source_total"),
            ins=result.get("codes_to_insert"),
            same=result.get("codes_existing_same"),
            conflicts=result.get("codes_conflicts"),
            updated=result.get("codes_updated"),
        )
    )
    print(
        "names: source={src} insert={ins} same={same} conflicts={conflicts} updated={updated}".format(
            src=result.get("names_source_total"),
            ins=result.get("names_to_insert"),
            same=result.get("names_existing_same"),
            conflicts=result.get("names_conflicts"),
            updated=result.get("names_updated"),
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
        description="Backfill enterprise-level catalog identity from one BusinessStore.",
    )
    parser.add_argument("--store-id", type=int, default=0)
    parser.add_argument("--store-code", default="")
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output-json", action="store_true")
    parser.add_argument("--include-inactive", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def _resolve_modes(args: argparse.Namespace) -> bool:
    if bool(args.dry_run) and bool(args.apply):
        raise ValueError("Use either --dry-run or --apply, not both.")
    if not bool(args.dry_run) and not bool(args.apply):
        return True
    return bool(args.dry_run)


def _ensure_selector(args: argparse.Namespace) -> tuple[int | None, str | None]:
    store_id = int(args.store_id or 0)
    store_code = _clean_text(args.store_code)
    if bool(store_id) and store_code:
        raise ValueError("Use either --store-id or --store-code, not both.")
    if not bool(store_id) and not store_code:
        raise ValueError("Backfill requires --store-id or --store-code.")
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


async def _load_source_codes(session, *, store_id: int, include_inactive: bool) -> list[BusinessStoreProductCode]:
    stmt = select(BusinessStoreProductCode).where(BusinessStoreProductCode.store_id == int(store_id))
    if not include_inactive:
        stmt = stmt.where(BusinessStoreProductCode.is_active.is_(True))
    stmt = stmt.order_by(BusinessStoreProductCode.internal_product_code.asc(), BusinessStoreProductCode.id.asc())
    return list((await session.execute(stmt)).scalars().all())


async def _load_source_names(session, *, store_id: int, include_inactive: bool) -> list[BusinessStoreProductName]:
    stmt = select(BusinessStoreProductName).where(BusinessStoreProductName.store_id == int(store_id))
    if not include_inactive:
        stmt = stmt.where(BusinessStoreProductName.is_active.is_(True))
    stmt = stmt.order_by(BusinessStoreProductName.internal_product_code.asc(), BusinessStoreProductName.id.asc())
    return list((await session.execute(stmt)).scalars().all())


async def _load_target_codes(session, *, enterprise_code: str) -> list[BusinessEnterpriseProductCode]:
    stmt = (
        select(BusinessEnterpriseProductCode)
        .where(BusinessEnterpriseProductCode.enterprise_code == enterprise_code)
        .order_by(
            BusinessEnterpriseProductCode.internal_product_code.asc(),
            BusinessEnterpriseProductCode.id.asc(),
        )
    )
    return list((await session.execute(stmt)).scalars().all())


async def _load_target_names(session, *, enterprise_code: str) -> list[BusinessEnterpriseProductName]:
    stmt = (
        select(BusinessEnterpriseProductName)
        .where(BusinessEnterpriseProductName.enterprise_code == enterprise_code)
        .order_by(
            BusinessEnterpriseProductName.internal_product_code.asc(),
            BusinessEnterpriseProductName.id.asc(),
        )
    )
    return list((await session.execute(stmt)).scalars().all())


def _code_values_equal(source: BusinessStoreProductCode, target: BusinessEnterpriseProductCode) -> bool:
    return (
        _clean_text(source.external_product_code) == _clean_text(target.external_product_code)
        and _clean_text(source.code_source or "backfilled_from_store")
        == _clean_text(target.code_source or "backfilled_from_store")
        and bool(source.is_active) == bool(target.is_active)
    )


def _name_values_equal(source: BusinessStoreProductName, target: BusinessEnterpriseProductName) -> bool:
    return (
        _clean_text(source.external_product_name) == _clean_text(target.external_product_name)
        and _clean_text(source.name_source or "backfilled_from_store")
        == _clean_text(target.name_source or "backfilled_from_store")
        and source.source_supplier_id == target.source_supplier_id
        and _clean_text(source.source_supplier_code) == _clean_text(target.source_supplier_code)
        and _clean_text(source.source_supplier_product_id) == _clean_text(target.source_supplier_product_id)
        and _clean_text(source.source_supplier_product_name_raw) == _clean_text(target.source_supplier_product_name_raw)
        and bool(source.is_active) == bool(target.is_active)
    )


async def _run_backfill(
    *,
    session,
    store: BusinessStore,
    enterprise_code: str,
    dry_run: bool,
    include_inactive: bool,
    overwrite: bool,
) -> dict[str, Any]:
    source_codes = await _load_source_codes(
        session,
        store_id=int(store.id),
        include_inactive=include_inactive,
    )
    source_names = await _load_source_names(
        session,
        store_id=int(store.id),
        include_inactive=include_inactive,
    )
    target_codes = await _load_target_codes(session, enterprise_code=enterprise_code)
    target_names = await _load_target_names(session, enterprise_code=enterprise_code)

    target_codes_by_internal = {
        _clean_text(row.internal_product_code): row
        for row in target_codes
        if _clean_text(row.internal_product_code)
    }
    target_codes_by_external = {
        _clean_text(row.external_product_code): row
        for row in target_codes
        if _clean_text(row.external_product_code)
    }
    target_names_by_internal = {
        _clean_text(row.internal_product_code): row
        for row in target_names
        if _clean_text(row.internal_product_code)
    }

    codes_to_insert = 0
    codes_existing_same = 0
    codes_conflicts = 0
    codes_updated = 0
    names_to_insert = 0
    names_existing_same = 0
    names_conflicts = 0
    names_updated = 0
    warnings: list[str] = []
    errors: list[str] = []
    sample_conflicts: list[dict[str, Any]] = []
    sample_insert_codes: list[dict[str, Any]] = []
    sample_insert_names: list[dict[str, Any]] = []

    for source in source_codes:
        internal_code = _clean_text(source.internal_product_code)
        external_code = _clean_text(source.external_product_code)
        code_source = _clean_text(source.code_source) or "backfilled_from_store"
        existing = target_codes_by_internal.get(internal_code)
        conflicting_external_row = target_codes_by_external.get(external_code)

        if existing is None:
            if conflicting_external_row is not None and _clean_text(conflicting_external_row.internal_product_code) != internal_code:
                codes_conflicts += 1
                if len(sample_conflicts) < SAMPLE_LIMIT:
                    sample_conflicts.append(
                        {
                            "type": "code_external_conflict",
                            "internal_product_code": internal_code,
                            "external_product_code": external_code,
                            "existing_internal_product_code": _clean_text(conflicting_external_row.internal_product_code),
                        }
                    )
                continue

            codes_to_insert += 1
            if len(sample_insert_codes) < SAMPLE_LIMIT:
                sample_insert_codes.append(
                    {
                        "internal_product_code": internal_code,
                        "external_product_code": external_code,
                        "code_source": code_source,
                        "is_active": bool(source.is_active),
                    }
                )
            if not dry_run:
                obj = BusinessEnterpriseProductCode(
                    enterprise_code=enterprise_code,
                    internal_product_code=internal_code,
                    external_product_code=external_code,
                    code_source=code_source,
                    is_active=bool(source.is_active),
                )
                session.add(obj)
                await session.flush()
                target_codes_by_internal[internal_code] = obj
                target_codes_by_external[external_code] = obj
            continue

        if _code_values_equal(source, existing):
            codes_existing_same += 1
            continue

        same_external_other_internal = (
            conflicting_external_row is not None
            and _clean_text(conflicting_external_row.internal_product_code) != internal_code
        )
        if same_external_other_internal:
            codes_conflicts += 1
            if len(sample_conflicts) < SAMPLE_LIMIT:
                sample_conflicts.append(
                    {
                        "type": "code_update_external_conflict",
                        "internal_product_code": internal_code,
                        "external_product_code": external_code,
                        "existing_internal_product_code": _clean_text(conflicting_external_row.internal_product_code),
                    }
                )
            continue

        if not overwrite:
            codes_conflicts += 1
            if len(sample_conflicts) < SAMPLE_LIMIT:
                sample_conflicts.append(
                    {
                        "type": "code_value_conflict",
                        "internal_product_code": internal_code,
                        "store_external_product_code": external_code,
                        "enterprise_external_product_code": _clean_text(existing.external_product_code),
                    }
                )
            continue

        codes_updated += 1
        if not dry_run:
            old_external = _clean_text(existing.external_product_code)
            existing.external_product_code = external_code
            existing.code_source = code_source
            existing.is_active = bool(source.is_active)
            if old_external and target_codes_by_external.get(old_external) is existing:
                target_codes_by_external.pop(old_external, None)
            target_codes_by_external[external_code] = existing

    for source in source_names:
        internal_code = _clean_text(source.internal_product_code)
        name_source = _clean_text(source.name_source) or "backfilled_from_store"
        existing = target_names_by_internal.get(internal_code)

        if existing is None:
            names_to_insert += 1
            if len(sample_insert_names) < SAMPLE_LIMIT:
                sample_insert_names.append(
                    {
                        "internal_product_code": internal_code,
                        "external_product_name": _clean_text(source.external_product_name),
                        "name_source": name_source,
                        "is_active": bool(source.is_active),
                    }
                )
            if not dry_run:
                obj = BusinessEnterpriseProductName(
                    enterprise_code=enterprise_code,
                    internal_product_code=internal_code,
                    external_product_name=_clean_text(source.external_product_name),
                    name_source=name_source,
                    source_supplier_id=source.source_supplier_id,
                    source_supplier_code=_clean_text(source.source_supplier_code) or None,
                    source_supplier_product_id=_clean_text(source.source_supplier_product_id) or None,
                    source_supplier_product_name_raw=_clean_text(source.source_supplier_product_name_raw) or None,
                    is_active=bool(source.is_active),
                )
                session.add(obj)
                await session.flush()
                target_names_by_internal[internal_code] = obj
            continue

        if _name_values_equal(source, existing):
            names_existing_same += 1
            continue

        if not overwrite:
            names_conflicts += 1
            if len(sample_conflicts) < SAMPLE_LIMIT:
                sample_conflicts.append(
                    {
                        "type": "name_value_conflict",
                        "internal_product_code": internal_code,
                        "store_external_product_name": _clean_text(source.external_product_name),
                        "enterprise_external_product_name": _clean_text(existing.external_product_name),
                    }
                )
            continue

        names_updated += 1
        if not dry_run:
            existing.external_product_name = _clean_text(source.external_product_name)
            existing.name_source = name_source
            existing.source_supplier_id = source.source_supplier_id
            existing.source_supplier_code = _clean_text(source.source_supplier_code) or None
            existing.source_supplier_product_id = _clean_text(source.source_supplier_product_id) or None
            existing.source_supplier_product_name_raw = _clean_text(source.source_supplier_product_name_raw) or None
            existing.is_active = bool(source.is_active)

    if codes_conflicts or names_conflicts:
        warnings.append("Conflicts detected; runtime readers remain unchanged and conflicting rows were not auto-switched.")

    status = "ok"
    if errors:
        status = "error"
    elif codes_conflicts or names_conflicts:
        status = "partial"

    return {
        "status": status,
        "dry_run": bool(dry_run),
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": enterprise_code,
        "codes_source_total": len(source_codes),
        "codes_to_insert": codes_to_insert,
        "codes_existing_same": codes_existing_same,
        "codes_conflicts": codes_conflicts,
        "codes_updated": codes_updated,
        "names_source_total": len(source_names),
        "names_to_insert": names_to_insert,
        "names_existing_same": names_existing_same,
        "names_conflicts": names_conflicts,
        "names_updated": names_updated,
        "warnings": warnings,
        "errors": errors,
        "sample_conflicts": sample_conflicts,
        "sample_insert_codes": sample_insert_codes,
        "sample_insert_names": sample_insert_names,
    }


async def _amain() -> None:
    args = _parse_args()
    dry_run = _resolve_modes(args)
    store_id, store_code = _ensure_selector(args)

    async with get_async_db(commit_on_exit=False) as session:
        try:
            store = await _resolve_store(session, store_id=store_id, store_code=store_code)
            enterprise_code = _clean_text(args.enterprise_code) or _clean_text(store.enterprise_code)
            if not enterprise_code:
                raise ValueError("enterprise_code is required; pass --enterprise-code or ensure BusinessStore.enterprise_code is filled.")

            result = await _run_backfill(
                session=session,
                store=store,
                enterprise_code=enterprise_code,
                dry_run=bool(dry_run),
                include_inactive=bool(args.include_inactive),
                overwrite=bool(args.overwrite),
            )

            if dry_run:
                await session.rollback()
            else:
                await session.commit()
        except Exception as exc:
            await session.rollback()
            result = {
                "status": "error",
                "dry_run": bool(dry_run),
                "store_id": None,
                "store_code": None,
                "enterprise_code": _clean_text(args.enterprise_code) or None,
                "codes_source_total": 0,
                "codes_to_insert": 0,
                "codes_existing_same": 0,
                "codes_conflicts": 0,
                "codes_updated": 0,
                "names_source_total": 0,
                "names_to_insert": 0,
                "names_existing_same": 0,
                "names_conflicts": 0,
                "names_updated": 0,
                "warnings": [],
                "errors": [str(exc)],
                "sample_conflicts": [],
                "sample_insert_codes": [],
                "sample_insert_names": [],
            }

    _print_result(result, output_json=bool(args.output_json))


if __name__ == "__main__":
    asyncio.run(_amain())

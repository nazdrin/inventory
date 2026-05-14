from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import BusinessStore, EnterpriseSettings, MappingBranch


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _build_store_code(enterprise_code: str, branch: str) -> str:
    return f"business_{enterprise_code}_{branch}"


def _build_store_name(enterprise_name: str, branch: str) -> str:
    normalized_enterprise_name = _clean_text(enterprise_name) or "Business"
    return f"{normalized_enterprise_name} / {branch}"


async def _load_enterprises(
    session: AsyncSession,
    *,
    enterprise_code: str | None = None,
) -> list[EnterpriseSettings]:
    stmt = (
        select(EnterpriseSettings)
        .where(EnterpriseSettings.data_format == "Business")
        .order_by(EnterpriseSettings.enterprise_code.asc())
    )
    normalized_enterprise_code = _clean_text(enterprise_code)
    if normalized_enterprise_code:
        stmt = stmt.where(EnterpriseSettings.enterprise_code == normalized_enterprise_code)
    return list((await session.execute(stmt)).scalars().all())


async def _load_mapping_rows(
    session: AsyncSession,
    *,
    enterprise_code: str | None = None,
) -> list[MappingBranch]:
    stmt = (
        select(MappingBranch)
        .order_by(MappingBranch.enterprise_code.asc(), MappingBranch.branch.asc())
    )
    normalized_enterprise_code = _clean_text(enterprise_code)
    if normalized_enterprise_code:
        stmt = stmt.where(MappingBranch.enterprise_code == normalized_enterprise_code)
    return list((await session.execute(stmt)).scalars().all())


async def _load_store_rows(
    session: AsyncSession,
    *,
    enterprise_code: str | None = None,
) -> list[BusinessStore]:
    stmt = (
        select(BusinessStore)
        .order_by(BusinessStore.enterprise_code.asc(), BusinessStore.store_code.asc(), BusinessStore.id.asc())
    )
    normalized_enterprise_code = _clean_text(enterprise_code)
    if normalized_enterprise_code:
        stmt = stmt.where(BusinessStore.enterprise_code == normalized_enterprise_code)
    return list((await session.execute(stmt)).scalars().all())


async def _collect_sync_state(
    session: AsyncSession,
    *,
    enterprise_code: str | None = None,
) -> dict[str, Any]:
    normalized_enterprise_code = _clean_text(enterprise_code) or None
    warnings: list[str] = []

    enterprises = await _load_enterprises(session, enterprise_code=normalized_enterprise_code)
    mapping_rows = await _load_mapping_rows(session, enterprise_code=normalized_enterprise_code)
    stores = await _load_store_rows(session, enterprise_code=normalized_enterprise_code)

    enterprise_by_code = {
        _clean_text(item.enterprise_code): item
        for item in enterprises
        if _clean_text(item.enterprise_code)
    }

    mapping_by_key: dict[tuple[str, str], MappingBranch] = {}
    duplicate_mapping_keys: set[tuple[str, str]] = set()
    for row in mapping_rows:
        key = (_clean_text(row.enterprise_code), _clean_text(row.branch))
        if not key[0] or not key[1]:
            warnings.append("Skipped mapping_branch row with empty enterprise_code or branch.")
            continue
        if key in mapping_by_key:
            duplicate_mapping_keys.add(key)
            continue
        mapping_by_key[key] = row

    stores_by_key: dict[tuple[str, str], list[BusinessStore]] = defaultdict(list)
    for store in stores:
        key = (_clean_text(store.enterprise_code), _clean_text(store.tabletki_branch))
        if key[0] and key[1]:
            stores_by_key[key].append(store)

    duplicate_groups: list[dict[str, Any]] = []
    for key, items in stores_by_key.items():
        if len(items) > 1:
            duplicate_groups.append(_sample_duplicate_rows(key=key, stores=items))

    missing_stores_to_create: list[dict[str, Any]] = []
    for key, mapping_row in mapping_by_key.items():
        enterprise = enterprise_by_code.get(key[0])
        if enterprise is None:
            warnings.append(
                f"mapping_branch row skipped because EnterpriseSettings was not found for enterprise_code={key[0]}"
            )
            continue
        if key not in stores_by_key:
            missing_stores_to_create.append(
                _sample_missing_row(enterprise=enterprise, mapping_row=mapping_row)
            )

    orphan_stores_to_deactivate: list[dict[str, Any]] = []
    for key, items in stores_by_key.items():
        if key in mapping_by_key:
            continue
        for store in items:
            if bool(store.is_active):
                orphan_stores_to_deactivate.append(_sample_orphan_row(store))

    return {
        "enterprise_code_filter": normalized_enterprise_code,
        "enterprise_by_code": enterprise_by_code,
        "mapping_by_key": mapping_by_key,
        "stores": stores,
        "stores_by_key": stores_by_key,
        "duplicate_mapping_keys": duplicate_mapping_keys,
        "duplicate_groups": duplicate_groups,
        "missing_stores_to_create_full": missing_stores_to_create,
        "orphan_stores_to_deactivate_full": orphan_stores_to_deactivate,
        "warnings": warnings,
    }


def _sample_missing_row(*, enterprise: EnterpriseSettings, mapping_row: MappingBranch) -> dict[str, Any]:
    branch = _clean_text(mapping_row.branch)
    enterprise_code = _clean_text(enterprise.enterprise_code)
    return {
        "enterprise_code": enterprise_code,
        "enterprise_name": _clean_text(enterprise.enterprise_name) or None,
        "branch": branch,
        "suggested_store_code": _build_store_code(enterprise_code, branch),
        "suggested_store_name": _build_store_name(enterprise.enterprise_name, branch),
        "tabletki_enterprise_code": enterprise_code,
        "tabletki_branch": branch,
        "legacy_scope_key": None,
        "note": "legacy_scope_key is intentionally not auto-derived from mapping_branch.store_id.",
    }


def _sample_orphan_row(store: BusinessStore) -> dict[str, Any]:
    return {
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": _clean_text(store.enterprise_code) or None,
        "tabletki_branch": _clean_text(store.tabletki_branch) or None,
        "is_active": bool(store.is_active),
        "action": "deactivate" if bool(store.is_active) else "no_change_inactive",
    }


def _sample_duplicate_rows(
    *,
    key: tuple[str, str],
    stores: list[BusinessStore],
) -> dict[str, Any]:
    enterprise_code, branch = key
    return {
        "enterprise_code": enterprise_code or None,
        "branch": branch or None,
        "store_ids": [int(item.id) for item in stores],
        "store_codes": [item.store_code for item in stores],
        "active_store_ids": [int(item.id) for item in stores if bool(item.is_active)],
    }


async def build_business_store_branch_sync_report(
    session: AsyncSession,
    enterprise_code: str | None = None,
) -> dict[str, Any]:
    state = await _collect_sync_state(session, enterprise_code=enterprise_code)
    warnings = list(state["warnings"])
    errors: list[str] = []
    duplicate_groups = list(state["duplicate_groups"])
    duplicate_mapping_keys = set(state["duplicate_mapping_keys"])
    missing_stores_to_create = list(state["missing_stores_to_create_full"])
    orphan_stores_to_deactivate = list(state["orphan_stores_to_deactivate_full"])

    if duplicate_mapping_keys:
        warnings.append(
            "mapping_branch contains duplicate enterprise_code+branch keys; sync create/apply ignores duplicate mapping rows."
        )
    if duplicate_groups:
        warnings.append("Duplicate BusinessStore rows by enterprise_code+tabletki_branch were found; apply mode will not auto-fix duplicates.")

    return {
        "status": "ok" if not errors else "error",
        "enterprise_code_filter": state["enterprise_code_filter"],
        "enterprises_scanned": len(state["enterprise_by_code"]),
        "mapping_branch_rows": len(state["mapping_by_key"]),
        "stores_found": len(state["stores"]),
        "duplicates": len(duplicate_groups),
        "missing_stores_to_create": len(missing_stores_to_create),
        "orphan_stores_to_deactivate": len(orphan_stores_to_deactivate),
        "sample_missing": missing_stores_to_create[:50],
        "sample_orphan": orphan_stores_to_deactivate[:50],
        "sample_duplicates": duplicate_groups[:50],
        "warnings": warnings,
        "errors": errors,
    }


async def apply_business_store_branch_sync(
    session: AsyncSession,
    enterprise_code: str | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    state = await _collect_sync_state(session, enterprise_code=enterprise_code)
    report = await build_business_store_branch_sync_report(session, enterprise_code=enterprise_code)
    warnings = list(report.get("warnings") or [])
    errors = list(report.get("errors") or [])

    created_rows: list[dict[str, Any]] = []
    deactivated_rows: list[dict[str, Any]] = []

    if errors:
        return {
            **report,
            "dry_run": bool(dry_run),
            "created": 0,
            "deactivated": 0,
            "sample_created": [],
            "sample_deactivated": [],
        }

    enterprise_by_code = state["enterprise_by_code"]
    stores_by_key = state["stores_by_key"]
    stores_by_id = {int(item.id): item for item in state["stores"]}

    for item in state["missing_stores_to_create_full"]:
        enterprise = enterprise_by_code.get(_clean_text(item.get("enterprise_code")))
        if enterprise is None:
            continue
        payload = {
            "store_code": _clean_text(item.get("suggested_store_code")),
            "store_name": _clean_text(item.get("suggested_store_name")),
            "enterprise_code": _clean_text(item.get("enterprise_code")),
            "tabletki_enterprise_code": _clean_text(item.get("tabletki_enterprise_code")),
            "tabletki_branch": _clean_text(item.get("branch")),
            "is_active": True,
            "stock_enabled": False,
            "orders_enabled": False,
            "migration_status": "draft",
            "salesdrive_enterprise_id": None,
            "legacy_scope_key": None,
            # Deprecated compatibility field is still required by current model/runtime shape.
            "catalog_enabled": True,
        }
        created_rows.append(payload)
        if bool(dry_run):
            continue

        key = (payload["enterprise_code"], payload["tabletki_branch"])
        if key in stores_by_key:
            warnings.append(
                f"Skipped create for enterprise_code={key[0]} branch={key[1]} because a store appeared before apply commit."
            )
            continue

        obj = BusinessStore(
            store_code=payload["store_code"],
            store_name=payload["store_name"],
            enterprise_code=payload["enterprise_code"],
            tabletki_enterprise_code=payload["tabletki_enterprise_code"],
            tabletki_branch=payload["tabletki_branch"],
            is_active=True,
            stock_enabled=False,
            orders_enabled=False,
            migration_status="draft",
            salesdrive_enterprise_id=None,
            legacy_scope_key=None,
            catalog_enabled=True,
        )
        session.add(obj)
        await session.flush()
        stores_by_key[key].append(obj)

    for item in state["orphan_stores_to_deactivate_full"]:
        store_id = int(item.get("store_id"))
        store = stores_by_id.get(store_id)
        if store is None:
            continue
        deactivated_rows.append(
            {
                "store_id": int(store.id),
                "store_code": store.store_code,
                "enterprise_code": _clean_text(store.enterprise_code) or None,
                "tabletki_branch": _clean_text(store.tabletki_branch) or None,
            }
        )
        if bool(dry_run):
            continue
        if bool(store.is_active):
            store.is_active = False

    if not bool(dry_run):
        await session.commit()

    status = "ok"
    if report.get("duplicates"):
        status = "warning"

    return {
        **report,
        "status": status,
        "dry_run": bool(dry_run),
        "created": len(created_rows),
        "deactivated": len(deactivated_rows),
        "sample_created": created_rows[:50],
        "sample_deactivated": deactivated_rows[:50],
        "warnings": warnings,
        "errors": errors,
    }

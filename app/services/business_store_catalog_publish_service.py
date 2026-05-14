from __future__ import annotations

import os
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_baseline_catalog_exporter import export_business_baseline_catalog
from app.business.business_enterprise_catalog_exporter import export_business_enterprise_catalog
from app.models import BusinessStore, EnterpriseSettings
from app.services.business_custom_catalog_identity_refresh_service import (
    refresh_custom_catalog_identity_mappings,
)
from app.services.business_runtime_mode_service import (
    BASELINE_BUSINESS_RUNTIME_MODE,
    CUSTOM_BUSINESS_RUNTIME_MODE,
    resolve_business_runtime_mode_from_db,
)
from app.services.business_store_offers_builder import build_business_store_offers


PUBLISH_READY_DRY_RUN_STATES = {"dry_run"}
PUBLISH_READY_LIVE_STATES = {"catalog_stock_live", "orders_live"}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _should_refresh_store_native_offers_before_catalog() -> bool:
    raw_value = _clean_text(os.getenv("BUSINESS_STORE_NATIVE_REFRESH_OFFERS_BEFORE_CATALOG")).lower()
    if raw_value in {"0", "false", "no", "off"}:
        return False
    return True


async def _refresh_store_native_offers_for_catalog_runtime(
    session: AsyncSession,
    *,
    store_id: int,
    dry_run: bool,
) -> dict[str, Any]:
    store = (
        await session.execute(
            select(BusinessStore).where(BusinessStore.id == int(store_id)).limit(1)
        )
    ).scalar_one_or_none()
    if store is None:
        return {
            "status": "error",
            "store_id": int(store_id),
            "errors": [f"BusinessStore not found for store_id={store_id}"],
        }
    if not _should_refresh_store_native_offers_before_catalog():
        return {
            "status": "skipped",
            "store_id": int(store.id),
            "store_code": store.store_code,
            "enterprise_code": store.enterprise_code,
            "skip_reason": "disabled_by_env",
        }
    result = await build_business_store_offers(
        session,
        dry_run=False,
        store_id=int(store.id),
        enterprise_code=_clean_text(store.enterprise_code),
        compare_legacy=False,
    )
    if not bool(dry_run):
        offers_changes = int(result.get("upsert_rows", 0) or 0) + int(
            result.get("stale_rows_deleted", 0) or 0
        )
        if offers_changes > 0:
            await session.commit()
    return result


def _empty_store_report_row(
    store: BusinessStore,
    *,
    enterprise_catalog_enabled: bool | None,
    store_catalog_enabled_deprecated: bool,
    catalog_gate_source: str,
    eligible: bool,
    skip_reason: str | None,
    status: str,
    errors: list[str] | None = None,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": store.enterprise_code,
        "tabletki_enterprise_code": store.tabletki_enterprise_code,
        "tabletki_branch": store.tabletki_branch,
        "migration_status": store.migration_status,
        "enterprise_catalog_enabled": enterprise_catalog_enabled,
        "store_catalog_enabled_deprecated": bool(store_catalog_enabled_deprecated),
        "catalog_gate_source": catalog_gate_source,
        "eligible": bool(eligible),
        "skip_reason": skip_reason,
        "status": status,
        "business_runtime_mode": CUSTOM_BUSINESS_RUNTIME_MODE,
        "runtime_mode_source": "enterprise_settings",
        "catalog_runtime_path": "enterprise_identity",
        "identity_mode": "enterprise_level",
        "assortment_mode": None,
        "target_branch": None,
        "target_branch_source": None,
        "catalog_scope_store_id": None,
        "catalog_scope_store_code": None,
        "catalog_scope_branch": None,
        "catalog_scope_key": None,
        "catalog_scope_source": None,
        "catalog_scope_error_reason": None,
        "catalog_only_in_stock_source": None,
        "catalog_only_in_stock": None,
        "candidate_products": 0,
        "exportable_products": 0,
        "skipped_products": 0,
        "sent_products": 0,
        "endpoint_preview": None,
        "warnings": list(warnings or []),
        "errors": list(errors or []),
        "identity_refresh_status": None,
        "offers_refresh_status": None,
        "offers_refresh_candidate_products": 0,
        "offers_refresh_upsert_rows": 0,
        "offers_refresh_stale_rows_deleted": 0,
        "offers_refresh_supplier_links_processed": 0,
        "offers_refresh_supplier_links_skipped": 0,
        "identity_refresh_candidate_source": None,
        "identity_refresh_candidate_products": 0,
        "identity_refresh_created_store_codes": 0,
        "identity_refresh_created_store_names": 0,
        "identity_refresh_created_enterprise_codes": 0,
        "identity_refresh_created_enterprise_names": 0,
        "identity_refresh_skipped_enterprise_codes": 0,
        "identity_refresh_skipped_enterprise_names": 0,
    }


def _allowed_migration_states(
    *,
    include_dry_run_states: bool,
    include_live_states: bool,
) -> set[str]:
    states: set[str] = set()
    if include_dry_run_states:
        states.update(PUBLISH_READY_DRY_RUN_STATES)
    if include_live_states:
        states.update(PUBLISH_READY_LIVE_STATES)
    return states


def _check_store_catalog_eligibility(
    store: BusinessStore,
    enterprise: EnterpriseSettings | None,
    *,
    allowed_states: set[str],
    include_legacy_default: bool,
    runtime_mode_report: dict[str, Any],
) -> tuple[bool, str | None]:
    business_runtime_mode = runtime_mode_report.get("business_runtime_mode")
    if enterprise is None:
        return False, "missing_enterprise_settings"
    if not bool(enterprise.catalog_enabled):
        return False, "enterprise_catalog_disabled"
    if business_runtime_mode == BASELINE_BUSINESS_RUNTIME_MODE:
        if not _clean_text(getattr(enterprise, "branch_id", None)):
            return False, "missing_enterprise_branch"
        return True, None

    if not bool(store.is_active):
        return False, "inactive_store"
    if bool(store.is_legacy_default) and not include_legacy_default:
        return False, "legacy_default_excluded"

    migration_status = _clean_text(store.migration_status)
    if migration_status not in allowed_states:
        return False, "migration_status_not_publish_ready"
    if not _clean_text(store.tabletki_branch):
        return False, "missing_tabletki_branch"
    if not _clean_text(store.tabletki_enterprise_code):
        return False, "missing_tabletki_enterprise_code"
    return True, None


async def _load_store_candidates(
    session: AsyncSession,
    *,
    store_id: int | None = None,
    store_code: str | None = None,
) -> list[tuple[BusinessStore, EnterpriseSettings | None]]:
    stmt = (
        select(BusinessStore, EnterpriseSettings)
        .outerjoin(
            EnterpriseSettings,
            EnterpriseSettings.enterprise_code == BusinessStore.enterprise_code,
        )
        .order_by(BusinessStore.store_code.asc(), BusinessStore.id.asc())
    )
    if store_id is not None:
        stmt = stmt.where(BusinessStore.id == int(store_id))
    if store_code is not None:
        stmt = stmt.where(BusinessStore.store_code == _clean_text(store_code))

    rows = await session.execute(stmt)
    return [(store, enterprise) for store, enterprise in rows.all()]


async def get_eligible_business_store_catalogs(
    session: AsyncSession,
    *,
    include_dry_run_states: bool = True,
    include_live_states: bool = True,
    include_legacy_default: bool = False,
    store_id: int | None = None,
    store_code: str | None = None,
) -> dict[str, Any]:
    allowed_states = _allowed_migration_states(
        include_dry_run_states=bool(include_dry_run_states),
        include_live_states=bool(include_live_states),
    )
    store_rows = await _load_store_candidates(
        session,
        store_id=store_id,
        store_code=store_code,
    )

    report_rows: list[dict[str, Any]] = []
    eligible_count = 0
    skipped_count = 0
    processed_baseline_enterprises: set[str] = set()

    for store, enterprise in store_rows:
        runtime_mode_report = await resolve_business_runtime_mode_from_db(session, str(store.enterprise_code or ""))
        business_runtime_mode = runtime_mode_report.get("business_runtime_mode")
        catalog_runtime_path = runtime_mode_report.get("catalog_runtime_path")
        catalog_gate_source = "enterprise_settings"
        eligible, skip_reason = _check_store_catalog_eligibility(
            store,
            enterprise,
            allowed_states=allowed_states,
            include_legacy_default=bool(include_legacy_default),
            runtime_mode_report=runtime_mode_report,
        )
        if eligible and business_runtime_mode == BASELINE_BUSINESS_RUNTIME_MODE:
            normalized_enterprise_code = _clean_text(store.enterprise_code)
            if normalized_enterprise_code in processed_baseline_enterprises:
                eligible = False
                skip_reason = "baseline_enterprise_already_processed"
            else:
                processed_baseline_enterprises.add(normalized_enterprise_code)
        enterprise_catalog_enabled = None if enterprise is None else bool(enterprise.catalog_enabled)
        row = _empty_store_report_row(
            store,
            enterprise_catalog_enabled=enterprise_catalog_enabled,
            store_catalog_enabled_deprecated=bool(store.catalog_enabled),
            catalog_gate_source=catalog_gate_source,
            eligible=eligible,
            skip_reason=skip_reason,
            status="eligible" if eligible else "skipped",
        )
        row["business_runtime_mode"] = business_runtime_mode
        row["runtime_mode_source"] = runtime_mode_report.get("runtime_mode_source")
        row["catalog_runtime_path"] = catalog_runtime_path
        row["identity_mode"] = "baseline_legacy" if business_runtime_mode == BASELINE_BUSINESS_RUNTIME_MODE else "enterprise_level"
        row["target_branch_source"] = "enterprise_settings"
        row["target_branch"] = _clean_text(getattr(enterprise, "branch_id", None)) or None
        row["assortment_mode"] = "baseline_legacy" if business_runtime_mode == BASELINE_BUSINESS_RUNTIME_MODE else "store_compatible"
        report_rows.append(row)
        if eligible:
            eligible_count += 1
        else:
            skipped_count += 1

    return {
        "status": "ok",
        "total_stores_found": len(store_rows),
        "eligible_stores": eligible_count,
        "skipped_stores": skipped_count,
        "stores": report_rows,
        "warnings": [],
        "errors": [],
        "allowed_migration_states": sorted(allowed_states),
    }


async def publish_enabled_business_store_catalogs(
    session: AsyncSession,
    *,
    dry_run: bool = True,
    limit: int | None = None,
    include_legacy_default: bool = False,
    store_code: str | None = None,
    store_id: int | None = None,
    require_confirm: bool = True,
    confirm: bool = False,
) -> dict[str, Any]:
    if not bool(dry_run) and bool(require_confirm) and not bool(confirm):
        raise ValueError("Live send requires explicit confirm.")

    eligibility = await get_eligible_business_store_catalogs(
        session,
        include_dry_run_states=True,
        include_live_states=True,
        include_legacy_default=bool(include_legacy_default),
        store_id=int(store_id) if store_id is not None else None,
        store_code=_clean_text(store_code) or None,
    )

    top_errors: list[str] = []
    top_warnings: list[str] = []
    report_rows: list[dict[str, Any]] = []
    published_stores = 0
    failed_stores = 0
    skipped_stores = 0

    if int(eligibility["total_stores_found"]) == 0:
        selector_desc = f"store_id={store_id}" if store_id is not None else f"store_code={store_code}"
        if store_id is not None or _clean_text(store_code):
            top_errors.append(f"No BusinessStore found for selector {selector_desc}.")
        else:
            top_errors.append("No BusinessStore rows found.")

    effective_require_confirm = bool(require_confirm) and not bool(confirm)

    for entry in eligibility["stores"]:
        if not bool(entry["eligible"]):
            skipped_stores += 1
            report_rows.append(entry)
            continue

        if entry.get("business_runtime_mode") == BASELINE_BUSINESS_RUNTIME_MODE:
            result = await export_business_baseline_catalog(
                session,
                store_id=int(entry["store_id"]),
                dry_run=bool(dry_run),
                limit=limit,
                require_confirm=effective_require_confirm,
            )
            offers_refresh = None
            identity_refresh = None
        else:
            offers_refresh = await _refresh_store_native_offers_for_catalog_runtime(
                session,
                store_id=int(entry["store_id"]),
                dry_run=bool(dry_run),
            )
            if offers_refresh.get("status") == "error":
                result = {
                    "status": "error",
                    "identity_mode": "enterprise_level",
                    "assortment_mode": "store_compatible",
                    "catalog_runtime_path": "enterprise_identity",
                    "candidate_source": None,
                    "total_candidates": int(offers_refresh.get("candidate_products", 0) or 0),
                    "exportable_products": 0,
                    "skipped_products": int(offers_refresh.get("candidate_products", 0) or 0),
                    "sent_products": 0,
                    "warnings": list(offers_refresh.get("warnings") or []),
                    "errors": list(offers_refresh.get("errors") or []),
                    "endpoint_preview": None,
                    "catalog_scope_store_id": int(entry["store_id"]),
                    "catalog_scope_store_code": entry.get("store_code"),
                    "catalog_scope_branch": entry.get("tabletki_branch"),
                    "catalog_scope_key": None,
                    "catalog_scope_source": "enterprise_branch_match",
                    "catalog_scope_error_reason": None,
                    "catalog_only_in_stock_source": "catalog_scope_store",
                    "catalog_only_in_stock": None,
                    "target_branch": entry.get("target_branch"),
                    "target_branch_source": "enterprise_settings",
                }
                identity_refresh = None
            else:
                identity_refresh = await refresh_custom_catalog_identity_mappings(
                    session,
                    store_id=int(entry["store_id"]),
                    dry_run=bool(dry_run),
                )
                if identity_refresh.get("status") == "error":
                    result = {
                        "status": "error",
                        "identity_mode": "enterprise_level",
                        "assortment_mode": "store_compatible",
                        "catalog_runtime_path": "enterprise_identity",
                        "candidate_source": identity_refresh.get("candidate_source"),
                        "total_candidates": identity_refresh.get("candidate_products", 0),
                        "exportable_products": 0,
                        "skipped_products": int(identity_refresh.get("candidate_products", 0) or 0),
                        "sent_products": 0,
                        "warnings": list(identity_refresh.get("warnings") or []),
                        "errors": list(identity_refresh.get("errors") or []),
                        "endpoint_preview": None,
                        "catalog_scope_store_id": int(entry["store_id"]),
                        "catalog_scope_store_code": entry.get("store_code"),
                        "catalog_scope_branch": entry.get("tabletki_branch"),
                        "catalog_scope_key": None,
                        "catalog_scope_source": "enterprise_branch_match",
                        "catalog_scope_error_reason": None,
                        "catalog_only_in_stock_source": "catalog_scope_store",
                        "catalog_only_in_stock": None,
                        "target_branch": entry.get("target_branch"),
                        "target_branch_source": "enterprise_settings",
                    }
                else:
                    if not bool(dry_run):
                        created_total = (
                            int(offers_refresh.get("upsert_rows", 0) or 0)
                            + int(offers_refresh.get("stale_rows_deleted", 0) or 0)
                            + int(identity_refresh.get("created_store_codes", 0) or 0)
                            + int(identity_refresh.get("created_store_names", 0) or 0)
                            + int(identity_refresh.get("created_enterprise_codes", 0) or 0)
                            + int(identity_refresh.get("created_enterprise_names", 0) or 0)
                        )
                        if created_total > 0:
                            await session.commit()
                    result = await export_business_enterprise_catalog(
                        session,
                        store_id=int(entry["store_id"]),
                        dry_run=bool(dry_run),
                        limit=limit,
                        require_confirm=effective_require_confirm,
                    )
        merged = {
            **entry,
            "status": result.get("status"),
            "business_runtime_mode": entry.get("business_runtime_mode"),
            "runtime_mode_source": entry.get("runtime_mode_source"),
            "catalog_runtime_path": result.get("catalog_runtime_path", entry.get("catalog_runtime_path")),
            "identity_mode": result.get("identity_mode"),
            "assortment_mode": result.get("assortment_mode"),
            "catalog_gate_source": result.get("catalog_gate_source", entry.get("catalog_gate_source")),
            "enterprise_catalog_enabled": result.get("enterprise_catalog_enabled", entry.get("enterprise_catalog_enabled")),
            "store_catalog_enabled_deprecated": result.get(
                "store_catalog_enabled_deprecated",
                entry.get("store_catalog_enabled_deprecated"),
            ),
            "catalog_scope_store_id": result.get("catalog_scope_store_id", entry.get("catalog_scope_store_id")),
            "catalog_scope_store_code": result.get("catalog_scope_store_code", entry.get("catalog_scope_store_code")),
            "catalog_scope_branch": result.get("catalog_scope_branch", entry.get("catalog_scope_branch")),
            "catalog_scope_key": result.get("catalog_scope_key", entry.get("catalog_scope_key")),
            "catalog_scope_source": result.get("catalog_scope_source", entry.get("catalog_scope_source")),
            "catalog_scope_error_reason": result.get(
                "catalog_scope_error_reason",
                entry.get("catalog_scope_error_reason"),
            ),
            "catalog_only_in_stock_source": result.get(
                "catalog_only_in_stock_source",
                entry.get("catalog_only_in_stock_source"),
            ),
            "catalog_only_in_stock": result.get("catalog_only_in_stock", entry.get("catalog_only_in_stock")),
            "target_branch": result.get("target_branch"),
            "target_branch_source": result.get("target_branch_source"),
            "candidate_products": int(result.get("total_candidates", 0) or 0),
            "exportable_products": int(result.get("exportable_products", 0) or 0),
            "skipped_products": int(result.get("skipped_products", 0) or 0),
            "sent_products": int(result.get("sent_products", 0) or 0),
            "endpoint_preview": result.get("endpoint_preview"),
            "warnings": list(result.get("warnings") or []),
            "errors": list(result.get("errors") or []),
            "offers_refresh_status": None if offers_refresh is None else offers_refresh.get("status"),
            "offers_refresh_candidate_products": 0 if offers_refresh is None else int(offers_refresh.get("candidate_products", 0) or 0),
            "offers_refresh_upsert_rows": 0 if offers_refresh is None else int(offers_refresh.get("upsert_rows", 0) or 0),
            "offers_refresh_stale_rows_deleted": 0 if offers_refresh is None else int(offers_refresh.get("stale_rows_deleted", 0) or 0),
            "offers_refresh_supplier_links_processed": 0 if offers_refresh is None else int(offers_refresh.get("supplier_links_processed", 0) or 0),
            "offers_refresh_supplier_links_skipped": 0 if offers_refresh is None else int(offers_refresh.get("supplier_links_skipped", 0) or 0),
            "identity_refresh_status": None if identity_refresh is None else identity_refresh.get("status"),
            "identity_refresh_candidate_source": None if identity_refresh is None else identity_refresh.get("candidate_source"),
            "identity_refresh_candidate_products": 0 if identity_refresh is None else int(identity_refresh.get("candidate_products", 0) or 0),
            "identity_refresh_created_store_codes": 0 if identity_refresh is None else int(identity_refresh.get("created_store_codes", 0) or 0),
            "identity_refresh_created_store_names": 0 if identity_refresh is None else int(identity_refresh.get("created_store_names", 0) or 0),
            "identity_refresh_created_enterprise_codes": 0 if identity_refresh is None else int(identity_refresh.get("created_enterprise_codes", 0) or 0),
            "identity_refresh_created_enterprise_names": 0 if identity_refresh is None else int(identity_refresh.get("created_enterprise_names", 0) or 0),
            "identity_refresh_skipped_enterprise_codes": 0 if identity_refresh is None else int(identity_refresh.get("skipped_enterprise_codes", 0) or 0),
            "identity_refresh_skipped_enterprise_names": 0 if identity_refresh is None else int(identity_refresh.get("skipped_enterprise_names", 0) or 0),
        }
        report_rows.append(merged)
        if result.get("status") in {"ok", "sent"}:
            published_stores += 1
        else:
            failed_stores += 1

    eligible_stores = int(eligibility["eligible_stores"])
    total_found = int(eligibility["total_stores_found"])

    if top_errors:
        overall_status = "error"
    elif failed_stores > 0 and published_stores > 0:
        overall_status = "partial"
    elif failed_stores > 0:
        overall_status = "error"
    elif eligible_stores == 0:
        overall_status = "error"
    else:
        overall_status = "ok"

    if not dry_run:
        top_warnings.append("Scheduler is not connected; this service performs only manual multi-store publish.")
    else:
        top_warnings.append("Dry-run mode: no external API calls were performed.")

    distinct_runtime_modes = {
        _clean_text(row.get("business_runtime_mode"))
        for row in report_rows
        if _clean_text(row.get("business_runtime_mode"))
    }
    distinct_catalog_paths = {
        _clean_text(row.get("catalog_runtime_path"))
        for row in report_rows
        if _clean_text(row.get("catalog_runtime_path"))
    }
    distinct_identity_modes = {
        _clean_text(row.get("identity_mode"))
        for row in report_rows
        if _clean_text(row.get("identity_mode"))
    }

    return {
        "status": overall_status,
        "dry_run": bool(dry_run),
        "business_runtime_mode": next(iter(distinct_runtime_modes)) if len(distinct_runtime_modes) == 1 else None,
        "runtime_mode_source": "enterprise_settings" if distinct_runtime_modes else None,
        "catalog_runtime_path": next(iter(distinct_catalog_paths)) if len(distinct_catalog_paths) == 1 else None,
        "identity_mode": next(iter(distinct_identity_modes)) if len(distinct_identity_modes) == 1 else None,
        "total_stores_found": total_found,
        "eligible_stores": eligible_stores,
        "skipped_stores": skipped_stores,
        "published_stores": published_stores,
        "failed_stores": failed_stores,
        "stores": report_rows,
        "warnings": top_warnings,
        "errors": top_errors,
    }

from __future__ import annotations

import os
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_stock_exporter import export_business_store_stock
from app.business.business_store_stock_preview import build_store_stock_payload_preview
from app.models import BusinessStore, EnterpriseSettings
from app.services.business_runtime_mode_service import (
    CUSTOM_BUSINESS_RUNTIME_MODE,
    resolve_business_runtime_mode_from_db,
)


PUBLISH_READY_STOCK_STATES = {"dry_run", "catalog_stock_live", "orders_live"}
OFFERS_FRESHNESS_WARNING = "Store-aware stock publish relies on current offers state and does not refresh offers."


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _enterprise_stock_code_mapping_enabled() -> bool:
    return (os.getenv("BUSINESS_ENTERPRISE_STOCK_CODE_MAPPING_ENABLED", "0") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _empty_store_report_row(
    store: BusinessStore,
    *,
    enterprise_stock_enabled: bool | None,
    eligible: bool,
    skip_reason: str | None,
    status: str,
    warnings: list[str] | None = None,
    errors: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": store.enterprise_code,
        "tabletki_enterprise_code": store.tabletki_enterprise_code,
        "tabletki_branch": store.tabletki_branch,
        "migration_status": store.migration_status,
        "enterprise_stock_enabled": enterprise_stock_enabled,
        "eligible": bool(eligible),
        "skip_reason": skip_reason,
        "status": status,
        "code_mapping_mode": "store_level",
        "identity_mode": "store_level",
        "target_branch": store.tabletki_branch,
        "target_branch_source": "business_store",
        "candidate_products": 0,
        "exportable_products": 0,
        "skipped_products": 0,
        "sent_products": 0,
        "endpoint_preview": None,
        "business_runtime_mode": CUSTOM_BUSINESS_RUNTIME_MODE,
        "runtime_mode_source": "enterprise_settings",
        "stock_mode": "store_aware",
        "stock_mode_source": "enterprise_settings",
        "warnings": list(warnings or []),
        "errors": list(errors or []),
    }


def _detect_preview_skip_reason(preview: dict[str, Any]) -> str:
    summary = preview.get("summary", {}) or {}
    exportable_products = int(summary.get("exportable_products", 0) or 0)
    missing_code_mapping = int(summary.get("missing_code_mapping", 0) or 0)
    missing_price_adjustment = int(summary.get("missing_price_adjustment", 0) or 0)

    if exportable_products > 0:
        return ""
    if missing_code_mapping > 0:
        return "missing_enterprise_code_mapping" if preview.get("code_mapping_mode") == "enterprise_level" else "missing_code_mapping"
    if missing_price_adjustment > 0:
        return "missing_price_adjustment"
    return "missing_exportable_rows"


def _check_store_stock_eligibility(
    store: BusinessStore,
    enterprise: EnterpriseSettings | None,
    *,
    include_legacy_default: bool,
    stock_mode_report: dict[str, Any],
) -> tuple[bool, str | None]:
    if not bool(store.is_active):
        return False, "inactive_store"
    if not bool(store.stock_enabled):
        return False, "store_stock_disabled"
    if enterprise is None:
        return False, "missing_enterprise_settings"
    if stock_mode_report.get("business_runtime_mode") != CUSTOM_BUSINESS_RUNTIME_MODE:
        return False, "baseline_legacy_mode_enabled"
    if not bool(enterprise.stock_enabled):
        return False, "enterprise_stock_disabled"
    if bool(store.is_legacy_default) and not include_legacy_default:
        return False, "legacy_default_excluded"
    if _clean_text(store.migration_status) not in PUBLISH_READY_STOCK_STATES:
        return False, "migration_status_not_stock_ready"
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
    enterprise_code: str | None = None,
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
    if enterprise_code is not None:
        stmt = stmt.where(BusinessStore.enterprise_code == _clean_text(enterprise_code))

    rows = await session.execute(stmt)
    return [(store, enterprise) for store, enterprise in rows.all()]


async def get_eligible_business_store_stocks(
    session: AsyncSession,
    *,
    include_legacy_default: bool = False,
    store_id: int | None = None,
    store_code: str | None = None,
    enterprise_code: str | None = None,
) -> dict[str, Any]:
    code_mapping_mode = "enterprise_level" if _enterprise_stock_code_mapping_enabled() else "store_level"
    store_rows = await _load_store_candidates(
        session,
        store_id=store_id,
        store_code=store_code,
        enterprise_code=enterprise_code,
    )

    report_rows: list[dict[str, Any]] = []
    eligible_count = 0
    skipped_count = 0

    for store, enterprise in store_rows:
        enterprise_stock_enabled = None if enterprise is None else bool(enterprise.stock_enabled)
        stock_mode_report = await resolve_business_runtime_mode_from_db(session, str(store.enterprise_code or ""))
        eligible, skip_reason = _check_store_stock_eligibility(
            store,
            enterprise,
            include_legacy_default=bool(include_legacy_default),
            stock_mode_report=stock_mode_report,
        )

        warnings = [OFFERS_FRESHNESS_WARNING]
        errors: list[str] = []
        candidate_products = 0
        exportable_products = 0
        skipped_products = 0

        if eligible:
            preview = await build_store_stock_payload_preview(
                session,
                int(store.id),
                limit=None,
                include_not_exportable=True,
            )
            summary = preview.get("summary", {}) or {}
            candidate_products = int(summary.get("candidate_products", 0) or 0)
            exportable_products = int(summary.get("exportable_products", 0) or 0)
            skipped_products = int(summary.get("not_exportable_products", 0) or 0)
            warnings.extend(list(preview.get("warnings") or []))

            preview_skip_reason = _detect_preview_skip_reason(preview)
            if preview_skip_reason:
                eligible = False
                skip_reason = preview_skip_reason

        row = _empty_store_report_row(
            store,
            enterprise_stock_enabled=enterprise_stock_enabled,
            eligible=eligible,
            skip_reason=skip_reason,
            status="eligible" if eligible else "skipped",
            warnings=warnings,
            errors=errors,
        )
        row["code_mapping_mode"] = code_mapping_mode
        row["identity_mode"] = code_mapping_mode
        row["business_runtime_mode"] = stock_mode_report.get("business_runtime_mode")
        row["runtime_mode_source"] = stock_mode_report.get("runtime_mode_source")
        row["stock_mode"] = stock_mode_report.get("stock_runtime_path")
        row["stock_mode_source"] = stock_mode_report.get("runtime_mode_source")
        row["candidate_products"] = candidate_products
        row["exportable_products"] = exportable_products
        row["skipped_products"] = skipped_products
        report_rows.append(row)
        if eligible:
            eligible_count += 1
        else:
            skipped_count += 1

    return {
        "status": "ok",
        "business_runtime_mode": CUSTOM_BUSINESS_RUNTIME_MODE,
        "runtime_mode_source": "enterprise_settings",
        "stock_mode": "store_aware",
        "stock_mode_source": "enterprise_settings",
        "enterprise_code": _clean_text(enterprise_code) or None,
        "total_stores_found": len(store_rows),
        "eligible_stores": eligible_count,
        "skipped_stores": skipped_count,
        "stores": report_rows,
        "warnings": [OFFERS_FRESHNESS_WARNING],
        "errors": [],
        "allowed_migration_states": sorted(PUBLISH_READY_STOCK_STATES),
    }


async def publish_enabled_business_store_stocks(
    session: AsyncSession,
    *,
    dry_run: bool = True,
    limit: int | None = None,
    include_legacy_default: bool = False,
    store_code: str | None = None,
    store_id: int | None = None,
    enterprise_code: str | None = None,
    require_confirm: bool = True,
    confirm: bool = False,
) -> dict[str, Any]:
    if not bool(dry_run) and bool(require_confirm) and not bool(confirm):
        raise ValueError("Live send requires explicit confirm.")

    code_mapping_mode = "enterprise_level" if _enterprise_stock_code_mapping_enabled() else "store_level"

    eligibility = await get_eligible_business_store_stocks(
        session,
        include_legacy_default=bool(include_legacy_default),
        store_id=int(store_id) if store_id is not None else None,
        store_code=_clean_text(store_code) or None,
        enterprise_code=_clean_text(enterprise_code) or None,
    )

    top_errors: list[str] = []
    top_warnings: list[str] = list(eligibility.get("warnings") or [])
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

        result = await export_business_store_stock(
            session,
            store_id=int(entry["store_id"]),
            dry_run=bool(dry_run),
            limit=limit,
            require_confirm=effective_require_confirm,
            confirm=bool(confirm),
        )
        merged = {
            **entry,
            "status": result.get("status"),
            "code_mapping_mode": result.get("code_mapping_mode"),
            "identity_mode": result.get("identity_mode"),
            "target_branch": result.get("target_branch"),
            "target_branch_source": result.get("target_branch_source"),
            "candidate_products": int(result.get("total_candidates", 0) or 0),
            "exportable_products": int(result.get("exportable_products", 0) or 0),
            "skipped_products": int(result.get("skipped_products", 0) or 0),
            "sent_products": int(result.get("sent_products", 0) or 0),
            "endpoint_preview": result.get("endpoint_preview"),
            "warnings": list(dict.fromkeys(list(entry.get("warnings") or []) + list(result.get("warnings") or []))),
            "errors": list(result.get("errors") or []),
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
        top_warnings.append("Scheduler is not connected; this service performs only manual multi-store stock publish.")
    else:
        top_warnings.append("Dry-run mode: no external API calls were performed.")
    top_warnings.append(
        "Enterprise-level stock code mapping is {}.".format(
            "enabled" if code_mapping_mode == "enterprise_level" else "disabled"
        )
    )

    return {
        "status": overall_status,
        "dry_run": bool(dry_run),
        "stock_mode": "store_aware",
        "stock_mode_source": "default",
        "enterprise_code": _clean_text(enterprise_code) or None,
        "code_mapping_mode": code_mapping_mode,
        "identity_mode": code_mapping_mode,
        "total_stores_found": total_found,
        "eligible_stores": eligible_stores,
        "skipped_stores": skipped_stores,
        "published_stores": published_stores,
        "failed_stores": failed_stores,
        "stores": report_rows,
        "warnings": list(dict.fromkeys(top_warnings)),
        "errors": top_errors,
    }

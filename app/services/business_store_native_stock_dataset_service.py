from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_code_generator import ensure_store_product_code
from app.business.business_store_stock_preview import build_store_stock_payload_preview
from app.models import BusinessStore, EnterpriseSettings
from app.services.business_runtime_mode_service import (
    CUSTOM_BUSINESS_RUNTIME_MODE,
    resolve_business_runtime_mode_from_db,
)
from app.services.business_store_native_stock_mapping_service import (
    load_stock_mapping_context,
    resolve_stock_external_code,
)
from app.services.business_store_native_stock_selection_service import (
    load_store_native_offers,
    select_best_store_native_offers,
)


PUBLISH_READY_STOCK_STATES = {"dry_run", "catalog_stock_live", "orders_live"}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


async def _load_store_candidates(
    session: AsyncSession,
    *,
    enterprise_code: str,
    store_id: int | None = None,
) -> list[tuple[BusinessStore, EnterpriseSettings | None]]:
    stmt = (
        select(BusinessStore, EnterpriseSettings)
        .outerjoin(
            EnterpriseSettings,
            EnterpriseSettings.enterprise_code == BusinessStore.enterprise_code,
        )
        .where(BusinessStore.enterprise_code == _clean_text(enterprise_code))
        .order_by(BusinessStore.store_code.asc(), BusinessStore.id.asc())
    )
    if store_id is not None:
        stmt = stmt.where(BusinessStore.id == int(store_id))
    rows = await session.execute(stmt)
    return [(store, enterprise) for store, enterprise in rows.all()]


def _check_store_eligibility(
    store: BusinessStore,
    enterprise: EnterpriseSettings | None,
    *,
    business_runtime_mode: str,
    allow_baseline_runtime_override: bool = False,
) -> tuple[bool, str | None]:
    if not bool(store.is_active):
        return False, "inactive_store"
    if not bool(store.stock_enabled) and not bool(allow_baseline_runtime_override):
        return False, "store_stock_disabled"
    if enterprise is None:
        return False, "missing_enterprise_settings"
    if business_runtime_mode != CUSTOM_BUSINESS_RUNTIME_MODE and not bool(allow_baseline_runtime_override):
        return False, "baseline_runtime_mode"
    if not bool(enterprise.stock_enabled):
        return False, "enterprise_stock_disabled"
    if _clean_text(store.migration_status) not in PUBLISH_READY_STOCK_STATES and not bool(allow_baseline_runtime_override):
        return False, "migration_status_not_stock_ready"
    if not _clean_text(store.tabletki_branch):
        return False, "missing_tabletki_branch"
    return True, None


def _build_preview_row(
    *,
    store: BusinessStore,
    selected_row: dict[str, Any],
    branch: str,
    code: str,
    mapping_mode: str,
    identity_mode: str,
    price: int,
    qty: int,
    price_reserve: int,
    mapping_source: str,
) -> dict[str, Any]:
    return {
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": store.enterprise_code,
        "tabletki_branch": branch,
        "internal_product_code": selected_row.get("internal_product_code"),
        "external_product_code": code,
        "supplier_code": selected_row.get("supplier_code"),
        "qty": qty,
        "price": price,
        "price_reserve": price_reserve,
        "mapping_mode": mapping_mode,
        "identity_mode": identity_mode,
        "mapping_source": mapping_source,
        "selection_debug": selected_row.get("selection_debug") or {},
        "pricing_context": selected_row.get("pricing_context") or {},
    }


async def _ensure_store_level_stock_code_mapping(
    session: AsyncSession,
    *,
    mapping_context: dict[str, Any],
    store: BusinessStore,
    internal_product_code: str,
) -> bool:
    if bool(mapping_context.get("enterprise_mapping_enabled")):
        return False

    normalized_code = _clean_text(internal_product_code)
    if not normalized_code:
        return False

    key = (int(store.id), normalized_code)
    existing = (mapping_context.get("store_map") or {}).get(key)
    if existing is not None:
        return False

    generated = await ensure_store_product_code(
        session,
        int(store.id),
        normalized_code,
    )
    mapping_context.setdefault("store_map", {})[key] = generated
    return True


def _compare_new_vs_legacy(
    *,
    new_rows: list[dict[str, Any]],
    legacy_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    new_map = {_clean_text(row.get("external_product_code")): row for row in new_rows if _clean_text(row.get("external_product_code"))}
    legacy_map = {
        _clean_text(row.get("external_product_code")): row
        for row in legacy_rows
        if bool(row.get("exportable")) and _clean_text(row.get("external_product_code"))
    }
    keys = sorted(set(new_map) | set(legacy_map))
    matched = 0
    price_diff = 0
    qty_diff = 0
    new_only = 0
    legacy_only = 0
    samples: list[dict[str, Any]] = []
    for key in keys:
        new_row = new_map.get(key)
        legacy_row = legacy_map.get(key)
        if new_row is None:
            legacy_only += 1
            if len(samples) < 20:
                samples.append({"code": key, "status": "legacy_only"})
            continue
        if legacy_row is None:
            new_only += 1
            if len(samples) < 20:
                samples.append({"code": key, "status": "new_only"})
            continue
        matched += 1
        old_price = int(float(legacy_row.get("final_store_price_preview") or 0))
        new_price = int(new_row.get("price") or 0)
        old_qty = int(legacy_row.get("qty") or 0)
        new_qty = int(new_row.get("qty") or 0)
        if old_price != new_price:
            price_diff += 1
        if old_qty != new_qty:
            qty_diff += 1
        if len(samples) < 20 and (old_price != new_price or old_qty != new_qty):
            samples.append(
                {
                    "code": key,
                    "status": "delta",
                    "legacy_price": old_price,
                    "new_price": new_price,
                    "legacy_qty": old_qty,
                    "new_qty": new_qty,
                }
            )
    return {
        "matched": matched,
        "new_only": new_only,
        "legacy_only": legacy_only,
        "price_diff": price_diff,
        "qty_diff": qty_diff,
        "samples": samples,
    }


async def build_business_store_native_stock_dataset(
    session: AsyncSession,
    *,
    enterprise_code: str,
    store_id: int | None = None,
    limit: int | None = None,
    compare_legacy: bool = False,
    allow_baseline_runtime_override: bool = False,
) -> dict[str, Any]:
    normalized_enterprise_code = _clean_text(enterprise_code)
    mode_report = await resolve_business_runtime_mode_from_db(session, normalized_enterprise_code)
    business_runtime_mode = _clean_text(mode_report.get("business_runtime_mode"))

    store_rows = await _load_store_candidates(
        session,
        enterprise_code=normalized_enterprise_code,
        store_id=store_id,
    )

    if not store_rows:
        return {
            "status": "error",
            "enterprise_code": normalized_enterprise_code,
            "business_runtime_mode": business_runtime_mode,
            "runtime_mode_source": mode_report.get("runtime_mode_source"),
            "stock_mode": "store_native_permanent",
            "stock_mode_source": "business_store_offers",
            "baseline_runtime_override_used": bool(allow_baseline_runtime_override),
            "stores_total": 0,
            "stores_processed": 0,
            "stores_skipped": 0,
            "selected_offers_count": 0,
            "payload_rows_count": 0,
            "warnings": [],
            "errors": ["No BusinessStore rows found for enterprise."],
            "stores": [],
            "payload_preview": [],
        }

    candidate_stores = [store for store, _ in store_rows]
    mapping_context = await load_stock_mapping_context(session, stores=candidate_stores)

    stores_report: list[dict[str, Any]] = []
    payload_rows: list[dict[str, Any]] = []
    branch_counter: Counter[str] = Counter()
    warnings: list[str] = []
    errors: list[str] = []
    total_selected = 0
    total_candidate_offers = 0
    stores_processed = 0
    stores_skipped = 0

    for store, enterprise in store_rows:
        eligible, skip_reason = _check_store_eligibility(
            store,
            enterprise,
            business_runtime_mode=business_runtime_mode,
            allow_baseline_runtime_override=bool(allow_baseline_runtime_override),
        )
        store_report: dict[str, Any] = {
            "store_id": int(store.id),
            "store_code": store.store_code,
            "enterprise_code": store.enterprise_code,
            "tabletki_branch": store.tabletki_branch,
            "eligible": eligible,
            "skip_reason": skip_reason,
            "candidate_offers_count": 0,
            "selected_offers_count": 0,
            "payload_rows_count": 0,
            "warnings": [],
            "errors": [],
        }
        if not eligible:
            stores_skipped += 1
            stores_report.append(store_report)
            continue

        source_offers = await load_store_native_offers(session, store_ids=[int(store.id)])
        selected_offers, selection_summary = select_best_store_native_offers(source_offers)
        total_candidate_offers += int(selection_summary.get("candidate_offers_total", 0) or 0)
        total_selected += int(selection_summary.get("selected_offers_total", 0) or 0)

        branch_rows: list[dict[str, Any]] = []
        mapping_warnings = 0
        auto_generated_mappings = 0
        warning_counter: Counter[str] = Counter()
        for selected in selected_offers:
            row = selected.store_offer
            resolution = resolve_stock_external_code(
                mapping_context=mapping_context,
                store=store,
                internal_product_code=_clean_text(row.product_code),
                business_runtime_mode=business_runtime_mode,
            )
            if resolution.external_code is None and resolution.source == "store_mapping_missing":
                generated_now = await _ensure_store_level_stock_code_mapping(
                    session,
                    mapping_context=mapping_context,
                    store=store,
                    internal_product_code=_clean_text(row.product_code),
                )
                if generated_now:
                    auto_generated_mappings += 1
                    resolution = resolve_stock_external_code(
                        mapping_context=mapping_context,
                        store=store,
                        internal_product_code=_clean_text(row.product_code),
                        business_runtime_mode=business_runtime_mode,
                    )
            if resolution.external_code is None:
                mapping_warnings += 1
                if resolution.warning:
                    warning_counter[resolution.warning] += 1
                continue

            price = int(row.effective_price)
            qty = int(row.stock or 0)
            price_reserve = price
            preview_row = _build_preview_row(
                store=store,
                selected_row={
                    "internal_product_code": _clean_text(row.product_code),
                    "supplier_code": _clean_text(row.supplier_code),
                    "selection_debug": selected.selection_debug,
                    "pricing_context": row.pricing_context,
                },
                branch=_clean_text(store.tabletki_branch),
                code=_clean_text(resolution.external_code),
                mapping_mode=resolution.mapping_mode,
                identity_mode=resolution.identity_mode,
                price=price,
                qty=qty,
                price_reserve=price_reserve,
                mapping_source=resolution.source,
            )
            branch_rows.append(preview_row)
            branch_counter[_clean_text(store.tabletki_branch)] += 1

        if limit is not None:
            branch_rows = branch_rows[: max(0, int(limit))]

        legacy_compare_summary = None
        if compare_legacy:
            legacy_preview = await build_store_stock_payload_preview(
                session,
                int(store.id),
                limit=None,
                include_not_exportable=True,
            )
            legacy_compare_summary = _compare_new_vs_legacy(
                new_rows=branch_rows,
                legacy_rows=list(legacy_preview.get("payload_preview") or []),
            )

        stores_processed += 1
        if mapping_warnings > 0:
            warnings.append(
                f"Store {store.store_code}: {mapping_warnings} rows skipped because stock code mapping is missing."
            )
        store_report["candidate_offers_count"] = int(selection_summary.get("candidate_offers_total", 0) or 0)
        store_report["selected_offers_count"] = len(selected_offers)
        store_report["payload_rows_count"] = len(branch_rows)
        store_report["mapping_warnings_count"] = mapping_warnings
        store_report["auto_generated_store_code_mappings"] = auto_generated_mappings
        if warning_counter:
            store_report["warning_counts"] = dict(sorted(warning_counter.items()))
            store_report["warnings"] = sorted(warning_counter.keys())
        store_report["selection_summary"] = selection_summary
        if legacy_compare_summary is not None:
            store_report["legacy_compare"] = legacy_compare_summary

        stores_report.append(store_report)
        payload_rows.extend(branch_rows)

    status = "ok"
    if errors:
        status = "error"
    elif warnings or stores_skipped > 0:
        status = "warning"

    payload_preview = payload_rows[:20] if limit is None else payload_rows[: max(0, int(limit))]
    compare_totals = defaultdict(int)
    compare_samples: list[dict[str, Any]] = []
    if compare_legacy:
        for row in stores_report:
            compare = row.get("legacy_compare") or {}
            for key in ("matched", "new_only", "legacy_only", "price_diff", "qty_diff"):
                compare_totals[key] += int(compare.get(key, 0) or 0)
            for item in list(compare.get("samples") or []):
                if len(compare_samples) < 20:
                    compare_samples.append(
                        {
                            "store_code": row.get("store_code"),
                            **item,
                        }
                    )

    return {
        "status": status,
        "enterprise_code": normalized_enterprise_code,
        "business_runtime_mode": business_runtime_mode,
        "runtime_mode_source": mode_report.get("runtime_mode_source"),
        "stock_mode": "store_native_permanent",
        "stock_mode_source": "business_store_offers",
        "baseline_runtime_override_used": bool(allow_baseline_runtime_override),
        "runtime_path": "business_store_native_stock",
        "stores_total": len(store_rows),
        "stores_processed": stores_processed,
        "stores_skipped": stores_skipped,
        "candidate_offers_count": total_candidate_offers,
        "selected_offers_count": total_selected,
        "payload_rows_count": len(payload_rows),
        "branch_distribution": dict(sorted(branch_counter.items())),
        "code_mapping_mode": "enterprise_level" if mapping_context.get("enterprise_mapping_enabled") else "store_level",
        "identity_mode": "enterprise_level" if mapping_context.get("enterprise_mapping_enabled") else "store_level",
        "stores": stores_report,
        "payload_preview": payload_preview,
        "payload_rows": payload_rows if limit is None else payload_rows[: max(0, int(limit))],
        "warnings": warnings,
        "errors": errors,
        "legacy_compare": {
            **compare_totals,
            "samples": compare_samples,
        } if compare_legacy else None,
    }

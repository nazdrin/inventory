from __future__ import annotations

import os
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.business.dropship_pipeline import run_pipeline
from app.services.business_baseline_stock_preview_service import (
    build_business_baseline_stock_preview,
)
from app.services.business_runtime_mode_service import (
    BASELINE_BUSINESS_RUNTIME_MODE,
    CUSTOM_BUSINESS_RUNTIME_MODE,
    resolve_business_runtime_mode_from_db,
)
from app.services.business_store_stock_publish_service import (
    publish_enabled_business_store_stocks,
)
from app.services.business_store_native_stock_publish_service import (
    publish_business_store_native_stock,
)
from app.services.business_store_offers_builder import build_business_store_offers


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


CUSTOM_STOCK_RUNTIME_PATH_AUTO = "auto"
CUSTOM_STOCK_RUNTIME_PATH_LEGACY = "legacy"
CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE = "store_native"


def _baseline_stock_live_path_default() -> str:
    selector = _clean_text(os.getenv("BUSINESS_BASELINE_STOCK_LIVE_PATH")).lower()
    if selector in {CUSTOM_STOCK_RUNTIME_PATH_LEGACY, CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE}:
        return selector
    return CUSTOM_STOCK_RUNTIME_PATH_LEGACY


def _custom_stock_live_path_default() -> str:
    selector = _clean_text(os.getenv("BUSINESS_CUSTOM_STOCK_LIVE_PATH")).lower()
    if selector in {CUSTOM_STOCK_RUNTIME_PATH_LEGACY, CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE}:
        return selector
    return CUSTOM_STOCK_RUNTIME_PATH_LEGACY


def _resolve_custom_stock_runtime_path(
    *,
    dry_run: bool,
    requested_runtime_path: str | None,
) -> str:
    selector = _clean_text(requested_runtime_path).lower()
    if selector == CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE:
        return CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE
    if selector == CUSTOM_STOCK_RUNTIME_PATH_LEGACY:
        return CUSTOM_STOCK_RUNTIME_PATH_LEGACY
    if bool(dry_run):
        return CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE
    return _custom_stock_live_path_default()


def _resolve_baseline_stock_runtime_path(
    *,
    dry_run: bool,
    requested_runtime_path: str | None,
) -> str:
    selector = _clean_text(requested_runtime_path).lower()
    if selector == CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE:
        return CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE
    if selector == CUSTOM_STOCK_RUNTIME_PATH_LEGACY:
        return CUSTOM_STOCK_RUNTIME_PATH_LEGACY
    if bool(dry_run):
        return _baseline_stock_live_path_default()
    return _baseline_stock_live_path_default()


def _resolve_requested_stock_runtime_path(value: str | None) -> str:
    selector = _clean_text(value).lower()
    if selector in {
        CUSTOM_STOCK_RUNTIME_PATH_AUTO,
        CUSTOM_STOCK_RUNTIME_PATH_LEGACY,
        CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE,
    }:
        return selector
    return CUSTOM_STOCK_RUNTIME_PATH_AUTO


def _should_refresh_store_native_offers_before_stock() -> bool:
    raw_value = _clean_text(os.getenv("BUSINESS_STORE_NATIVE_REFRESH_OFFERS_BEFORE_STOCK")).lower()
    if raw_value in {"0", "false", "no", "off"}:
        return False
    return True


async def _refresh_store_native_offers_for_stock_runtime(
    session: AsyncSession,
    *,
    enterprise_code: str,
    dry_run: bool,
) -> dict[str, Any] | None:
    if not _should_refresh_store_native_offers_before_stock():
        return {
            "status": "skipped",
            "enterprise_code": enterprise_code,
            "skip_reason": "disabled_by_env",
        }
    result = await build_business_store_offers(
        session,
        dry_run=False,
        enterprise_code=enterprise_code,
        compare_legacy=False,
    )
    if not bool(dry_run):
        offers_changes = int(result.get("upsert_rows", 0) or 0) + int(
            result.get("stale_rows_deleted", 0) or 0
        )
        if offers_changes > 0:
            await session.commit()
    return result


async def publish_business_stock_for_enterprise(
    session: AsyncSession,
    *,
    enterprise_code: str,
    dry_run: bool = True,
    limit: int | None = None,
    include_legacy_default: bool = False,
    require_confirm: bool = True,
    confirm: bool = False,
    runtime_path: str | None = None,
    compare_legacy: bool = False,
) -> dict[str, Any]:
    """Mode-aware Business stock publish entrypoint.

    `baseline_legacy` deliberately delegates to the existing dropship pipeline
    for live sends, so pricing, jitter and mapping_branch routing stay unchanged.
    Dry-run uses the read-only baseline preview and does not call external APIs.
    """

    normalized_enterprise_code = _clean_text(enterprise_code)
    mode_report = await resolve_business_runtime_mode_from_db(session, normalized_enterprise_code)
    business_runtime_mode = mode_report.get("business_runtime_mode")
    stock_mode = mode_report.get("stock_runtime_path")
    requested_runtime_path = _resolve_requested_stock_runtime_path(runtime_path)

    if not normalized_enterprise_code:
        return {
            "status": "error",
            "dry_run": bool(dry_run),
            "business_runtime_mode": business_runtime_mode,
            "runtime_mode_source": mode_report.get("runtime_mode_source"),
            "stock_mode": stock_mode,
            "stock_mode_source": mode_report.get("stock_mode_source"),
            "enterprise_code": normalized_enterprise_code,
            "requested_runtime_path": requested_runtime_path,
            "effective_runtime_path": None,
            "warnings": [],
            "errors": ["enterprise_code is required"],
        }

    if business_runtime_mode == BASELINE_BUSINESS_RUNTIME_MODE:
        resolved_runtime_path = _resolve_baseline_stock_runtime_path(
            dry_run=bool(dry_run),
            requested_runtime_path=runtime_path,
        )
        if resolved_runtime_path == CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE:
            offers_build_result = await _refresh_store_native_offers_for_stock_runtime(
                session,
                enterprise_code=normalized_enterprise_code,
                dry_run=bool(dry_run),
            )
            result = await publish_business_store_native_stock(
                session,
                enterprise_code=normalized_enterprise_code,
                dry_run=bool(dry_run),
                limit=limit,
                require_confirm=require_confirm,
                confirm=confirm,
                compare_legacy=bool(compare_legacy),
                allow_baseline_runtime_override=True,
            )
            return {
                **result,
                "dry_run": bool(dry_run),
                "business_runtime_mode": business_runtime_mode,
                "runtime_mode_source": mode_report.get("runtime_mode_source"),
                "stock_mode": stock_mode,
                "stock_mode_source": mode_report.get("stock_mode_source"),
                "enterprise_code": normalized_enterprise_code,
                "requested_runtime_path": requested_runtime_path,
                "effective_runtime_path": CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE,
                "runtime_path": "business_store_native_stock_publish",
                "stock_runtime_path": "business_store_native_stock_publish",
                "catalog_runtime_path": mode_report.get("catalog_runtime_path"),
                "baseline_runtime_override_used": True,
                "runtime_path_resolution_reason": (
                    "explicit_store_native_override_for_baseline_enterprise"
                    if requested_runtime_path == CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE
                    else "baseline_runtime_auto_selector"
                ),
                "store_native_offers_build": offers_build_result,
            }
        if bool(dry_run):
            preview = await build_business_baseline_stock_preview(
                session,
                enterprise_code=normalized_enterprise_code,
                limit=limit,
            )
            return {
                **preview,
                "dry_run": True,
                "business_runtime_mode": business_runtime_mode,
                "runtime_mode_source": mode_report.get("runtime_mode_source"),
                "stock_mode": stock_mode,
                "stock_mode_source": mode_report.get("stock_mode_source"),
                "sent_products": 0,
                "requested_runtime_path": requested_runtime_path,
                "effective_runtime_path": CUSTOM_STOCK_RUNTIME_PATH_LEGACY,
                "runtime_path": "legacy_dropship_pipeline_preview",
                "stock_runtime_path": "legacy_dropship_pipeline_preview",
                "catalog_runtime_path": mode_report.get("catalog_runtime_path"),
                "baseline_runtime_override_used": False,
                "runtime_path_resolution_reason": (
                    "explicit_legacy_override_for_baseline_enterprise"
                    if requested_runtime_path == CUSTOM_STOCK_RUNTIME_PATH_LEGACY
                    else "baseline_default_legacy_preview"
                ),
            }

        if bool(require_confirm) and not bool(confirm):
            raise ValueError("Live baseline stock send requires explicit confirm.")

        pipeline_result = await run_pipeline(normalized_enterprise_code, "stock")
        store_markup_warnings = list((pipeline_result or {}).get("store_markup_warnings") or [])
        return {
            "status": "sent",
            "dry_run": False,
            "business_runtime_mode": business_runtime_mode,
            "runtime_mode_source": mode_report.get("runtime_mode_source"),
            "stock_mode": stock_mode,
            "stock_mode_source": mode_report.get("stock_mode_source"),
            "enterprise_code": normalized_enterprise_code,
            "requested_runtime_path": requested_runtime_path,
            "effective_runtime_path": CUSTOM_STOCK_RUNTIME_PATH_LEGACY,
            "runtime_path": "legacy_dropship_pipeline",
            "stock_runtime_path": "legacy_dropship_pipeline",
            "catalog_runtime_path": mode_report.get("catalog_runtime_path"),
            "baseline_runtime_override_used": False,
            "runtime_path_resolution_reason": (
                "explicit_legacy_override_for_baseline_enterprise"
                if requested_runtime_path == CUSTOM_STOCK_RUNTIME_PATH_LEGACY
                else "baseline_default_legacy_live"
            ),
            "store_markup_overlay_applied": bool((pipeline_result or {}).get("store_markup_overlay_applied")),
            "store_markup_rows_changed": int((pipeline_result or {}).get("store_markup_rows_changed", 0) or 0),
            "store_markup_branches_used": list((pipeline_result or {}).get("store_markup_branches_used") or []),
            "store_markup_branches_skipped": list((pipeline_result or {}).get("store_markup_branches_skipped") or []),
            "store_markup_warnings": store_markup_warnings,
            "warnings": store_markup_warnings,
            "errors": [],
        }

    resolved_runtime_path = _resolve_custom_stock_runtime_path(
        dry_run=bool(dry_run),
        requested_runtime_path=runtime_path,
    )
    if resolved_runtime_path == CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE:
        offers_build_result = await _refresh_store_native_offers_for_stock_runtime(
            session,
            enterprise_code=normalized_enterprise_code,
            dry_run=bool(dry_run),
        )
        result = await publish_business_store_native_stock(
            session,
            enterprise_code=normalized_enterprise_code,
            dry_run=bool(dry_run),
            limit=limit,
            require_confirm=require_confirm,
            confirm=confirm,
            compare_legacy=bool(compare_legacy),
        )
        runtime_path_label = "business_store_native_stock_publish"
    else:
        offers_build_result = None
        result = await publish_enabled_business_store_stocks(
            session,
            dry_run=bool(dry_run),
            limit=limit,
            include_legacy_default=bool(include_legacy_default),
            enterprise_code=normalized_enterprise_code,
            require_confirm=require_confirm,
            confirm=confirm,
        )
        runtime_path_label = "business_store_stock_publish_legacy"
    return {
        **result,
        "business_runtime_mode": business_runtime_mode,
        "runtime_mode_source": mode_report.get("runtime_mode_source"),
        "stock_mode": stock_mode,
        "stock_mode_source": mode_report.get("stock_mode_source"),
        "enterprise_code": normalized_enterprise_code,
        "requested_runtime_path": requested_runtime_path,
        "effective_runtime_path": resolved_runtime_path,
        "runtime_path": runtime_path_label,
        "stock_runtime_path": runtime_path_label,
        "catalog_runtime_path": mode_report.get("catalog_runtime_path"),
        "custom_stock_runtime_path_selected": resolved_runtime_path,
        "baseline_runtime_override_used": False,
        "runtime_path_resolution_reason": (
            "explicit_custom_runtime_override"
            if requested_runtime_path in {CUSTOM_STOCK_RUNTIME_PATH_LEGACY, CUSTOM_STOCK_RUNTIME_PATH_STORE_NATIVE}
            else "custom_runtime_auto_selector"
        ),
        "store_native_offers_build": offers_build_result,
    }

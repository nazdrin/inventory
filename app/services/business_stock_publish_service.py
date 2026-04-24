from __future__ import annotations

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


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


async def publish_business_stock_for_enterprise(
    session: AsyncSession,
    *,
    enterprise_code: str,
    dry_run: bool = True,
    limit: int | None = None,
    include_legacy_default: bool = False,
    require_confirm: bool = True,
    confirm: bool = False,
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

    if not normalized_enterprise_code:
        return {
            "status": "error",
            "dry_run": bool(dry_run),
            "business_runtime_mode": business_runtime_mode,
            "runtime_mode_source": mode_report.get("runtime_mode_source"),
            "stock_mode": stock_mode,
            "stock_mode_source": mode_report.get("stock_mode_source"),
            "enterprise_code": normalized_enterprise_code,
            "warnings": [],
            "errors": ["enterprise_code is required"],
        }

    if business_runtime_mode == BASELINE_BUSINESS_RUNTIME_MODE:
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
                "runtime_path": "legacy_dropship_pipeline_preview",
                "stock_runtime_path": "legacy_dropship_pipeline_preview",
                "catalog_runtime_path": mode_report.get("catalog_runtime_path"),
            }

        if bool(require_confirm) and not bool(confirm):
            raise ValueError("Live baseline stock send requires explicit confirm.")

        await run_pipeline(normalized_enterprise_code, "stock")
        return {
            "status": "sent",
            "dry_run": False,
            "business_runtime_mode": business_runtime_mode,
            "runtime_mode_source": mode_report.get("runtime_mode_source"),
            "stock_mode": stock_mode,
            "stock_mode_source": mode_report.get("stock_mode_source"),
            "enterprise_code": normalized_enterprise_code,
            "runtime_path": "legacy_dropship_pipeline",
            "stock_runtime_path": "legacy_dropship_pipeline",
            "catalog_runtime_path": mode_report.get("catalog_runtime_path"),
            "warnings": [],
            "errors": [],
        }

    result = await publish_enabled_business_store_stocks(
        session,
        dry_run=bool(dry_run),
        limit=limit,
        include_legacy_default=bool(include_legacy_default),
        enterprise_code=normalized_enterprise_code,
        require_confirm=require_confirm,
        confirm=confirm,
    )
    return {
        **result,
        "business_runtime_mode": business_runtime_mode,
        "runtime_mode_source": mode_report.get("runtime_mode_source"),
        "stock_mode": stock_mode,
        "stock_mode_source": mode_report.get("stock_mode_source"),
        "enterprise_code": normalized_enterprise_code,
        "runtime_path": "business_store_stock_publish",
        "stock_runtime_path": "business_store_stock_publish",
        "catalog_runtime_path": mode_report.get("catalog_runtime_path"),
    }

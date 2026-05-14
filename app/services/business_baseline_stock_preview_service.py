from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.dropship_pipeline import (
    _load_branch_mapping,
    build_best_offers_by_city,
    build_stock_payload_with_markup_overlay_report,
)
from app.database import EnterpriseSettings, MappingBranch


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _limit_rows(rows: list[dict[str, Any]], limit: int | None) -> list[dict[str, Any]]:
    if limit is None:
        return list(rows)
    return rows[: max(0, int(limit))]


async def _load_enterprise(
    session: AsyncSession,
    enterprise_code: str,
) -> EnterpriseSettings | None:
    return (
        await session.execute(
            select(EnterpriseSettings)
            .where(EnterpriseSettings.enterprise_code == str(enterprise_code))
            .limit(1)
        )
    ).scalar_one_or_none()


async def _load_mapping_branch_rows(
    session: AsyncSession,
    enterprise_code: str,
) -> list[MappingBranch]:
    return list(
        (
            await session.execute(
                select(MappingBranch)
                .where(MappingBranch.enterprise_code == str(enterprise_code))
                .order_by(MappingBranch.branch.asc(), MappingBranch.store_id.asc())
            )
        ).scalars().all()
    )


async def _collect_missing_branch_mappings(
    session: AsyncSession,
    enterprise_code: str,
    *,
    sample_limit: int = 20,
) -> tuple[list[str], int]:
    best_rows = await build_best_offers_by_city(session)
    city_to_branch = await _load_branch_mapping(session, enterprise_code)
    missing_cities: set[str] = set()
    missing_rows_count = 0

    for row in best_rows:
        city = _clean_text(row.get("city"))
        if not city:
            continue
        if city not in city_to_branch:
            missing_rows_count += 1
            missing_cities.add(city)

    return sorted(missing_cities)[: max(0, int(sample_limit))], missing_rows_count


async def build_business_baseline_stock_preview(
    session: AsyncSession,
    enterprise_code: str,
    limit: int | None = None,
) -> dict[str, Any]:
    """Build a read-only preview of the legacy enterprise-level stock payload.

    This intentionally reuses the current legacy stock payload builder and does
    not call process_database_service, scheduler refreshes, or external APIs.
    """

    normalized_enterprise_code = _clean_text(enterprise_code)
    warnings: list[str] = []
    errors: list[str] = []

    if not normalized_enterprise_code:
        return {
            "status": "error",
            "stock_mode": "baseline_legacy",
            "enterprise_code": normalized_enterprise_code,
            "mapping_branch_rows": 0,
            "output_branches_count": 0,
            "rows_total": 0,
            "limited_rows": 0,
            "sample_rows": [],
            "warnings": [],
            "errors": ["enterprise_code is required"],
            "price_source": "legacy_algorithm",
            "depends_on_business_stores": False,
            "store_markup_overlay_applied": False,
            "store_markup_rows_changed": 0,
            "store_markup_branches_used": [],
            "store_markup_branches_skipped": [],
            "store_markup_warnings": [],
            "store_markup_sample_changes": [],
        }

    enterprise = await _load_enterprise(session, normalized_enterprise_code)
    if enterprise is None:
        return {
            "status": "error",
            "stock_mode": "baseline_legacy",
            "enterprise_code": normalized_enterprise_code,
            "mapping_branch_rows": 0,
            "output_branches_count": 0,
            "rows_total": 0,
            "limited_rows": 0,
            "sample_rows": [],
            "warnings": [],
            "errors": [f"EnterpriseSettings not found for enterprise_code={normalized_enterprise_code}"],
            "price_source": "legacy_algorithm",
            "depends_on_business_stores": False,
            "store_markup_overlay_applied": False,
            "store_markup_rows_changed": 0,
            "store_markup_branches_used": [],
            "store_markup_branches_skipped": [],
            "store_markup_warnings": [],
            "store_markup_sample_changes": [],
        }

    mapping_branch_rows = await _load_mapping_branch_rows(session, normalized_enterprise_code)
    if not mapping_branch_rows:
        warnings.append("No mapping_branch rows found for enterprise.")

    payload_rows, markup_overlay_report = await build_stock_payload_with_markup_overlay_report(
        session,
        normalized_enterprise_code,
    )
    rows_total = len(payload_rows)
    limited_payload_rows = _limit_rows(payload_rows, limit)
    preview_rows = limited_payload_rows if limit is not None else payload_rows[:20]
    branches_in_payload = sorted({_clean_text(row.get("branch")) for row in payload_rows if _clean_text(row.get("branch"))})
    branches_from_mapping = sorted({_clean_text(row.branch) for row in mapping_branch_rows if _clean_text(row.branch)})
    branches_without_payload_rows = sorted(set(branches_from_mapping) - set(branches_in_payload))
    missing_mapping_cities, missing_mapping_rows_count = await _collect_missing_branch_mappings(
        session,
        normalized_enterprise_code,
    )

    if rows_total == 0:
        warnings.append("Legacy algorithm produced zero stock rows.")
    if missing_mapping_rows_count:
        warnings.append(
            f"Legacy algorithm skipped {missing_mapping_rows_count} best-offer rows without mapping_branch city mapping."
        )
    warnings.extend(list(markup_overlay_report.get("store_markup_warnings") or []))

    status = "ok"
    if errors:
        status = "error"
    elif warnings:
        status = "warning"

    return {
        "status": status,
        "stock_mode": "baseline_legacy",
        "enterprise_code": normalized_enterprise_code,
        "enterprise_name": enterprise.enterprise_name,
        "mapping_branch_rows": len(mapping_branch_rows),
        "mapping_branches": [
            {
                "branch": row.branch,
                "store_id": row.store_id,
            }
            for row in mapping_branch_rows
        ],
        "output_branches_count": len(branches_in_payload),
        "branches_in_payload": branches_in_payload,
        "branches_without_payload_rows": branches_without_payload_rows,
        "branches_missing_mapping": missing_mapping_cities,
        "missing_mapping_rows_count": missing_mapping_rows_count,
        "rows_total": rows_total,
        "limited_rows": len(limited_payload_rows) if limit is not None else None,
        "limit": limit,
        "is_limited": bool(limit is not None and rows_total > len(limited_payload_rows)),
        "sample_rows": preview_rows[:20],
        "payload_preview": preview_rows,
        "price_source": "legacy_algorithm_plus_store_markup_overlay",
        "depends_on_business_stores": True,
        "uses_process_database_service": False,
        "external_api_calls": False,
        "store_markup_overlay_applied": bool(markup_overlay_report.get("store_markup_overlay_applied")),
        "store_markup_rows_changed": int(markup_overlay_report.get("store_markup_rows_changed", 0) or 0),
        "store_markup_branches_used": list(markup_overlay_report.get("store_markup_branches_used") or []),
        "store_markup_branches_skipped": list(markup_overlay_report.get("store_markup_branches_skipped") or []),
        "store_markup_warnings": list(markup_overlay_report.get("store_markup_warnings") or []),
        "store_markup_sample_changes": list(markup_overlay_report.get("store_markup_sample_changes") or []),
        "warnings": warnings,
        "errors": errors,
    }

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DeveloperSettings, EnterpriseSettings, MasterCatalog


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _base_product_name(row: MasterCatalog) -> str:
    return _clean_text(row.name_ua) or _clean_text(row.name_ru) or _clean_text(row.sku)


def _preview_brand(row: MasterCatalog) -> str | None:
    normalized = _clean_text(getattr(row, "brand", None))
    return normalized or None


def _preview_barcode(row: MasterCatalog) -> str | None:
    normalized = _clean_text(getattr(row, "barcode", None))
    return normalized or None


def _preview_manufacturer(row: MasterCatalog) -> str | None:
    normalized = _clean_text(getattr(row, "manufacturer", None))
    return normalized or None


async def _get_enterprise_or_fail(session: AsyncSession, enterprise_code: str) -> EnterpriseSettings:
    normalized_enterprise_code = _clean_text(enterprise_code)
    if not normalized_enterprise_code:
        raise ValueError("enterprise_code is required")

    row = (
        await session.execute(
            select(EnterpriseSettings)
            .where(EnterpriseSettings.enterprise_code == normalized_enterprise_code)
            .limit(1)
        )
    ).scalar_one_or_none()
    if row is None:
        raise ValueError(f"EnterpriseSettings not found for enterprise_code={normalized_enterprise_code}")
    return row


async def _get_developer_settings(session: AsyncSession) -> DeveloperSettings | None:
    return (await session.execute(select(DeveloperSettings).limit(1))).scalar_one_or_none()


async def build_baseline_business_catalog_payload_preview(
    session: AsyncSession,
    enterprise_code: str,
    limit: int | None = None,
) -> dict[str, Any]:
    warnings: list[str] = []
    errors: list[str] = []

    try:
        enterprise = await _get_enterprise_or_fail(session, enterprise_code)
    except ValueError as exc:
        return {
            "status": "error",
            "enterprise_code": _clean_text(enterprise_code),
            "enterprise_name": None,
            "enterprise_catalog_enabled": None,
            "catalog_gate_source": "enterprise_settings",
            "assortment_mode": "baseline_legacy",
            "candidate_source": "master_all",
            "catalog_runtime_path": "baseline_legacy",
            "tabletki_branch": None,
            "master_catalog_total": 0,
            "candidate_products": 0,
            "exportable_products": 0,
            "not_exportable_products": 0,
            "missing_code_mapping": 0,
            "missing_name_mapping": 0,
            "endpoint_preview": None,
            "sample_rows": [],
            "payload_preview": [],
            "warnings": warnings,
            "errors": [str(exc)],
        }

    normalized_enterprise_code = _clean_text(enterprise.enterprise_code)
    branch_id = _clean_text(enterprise.branch_id)
    if _clean_text(enterprise.data_format).lower() != "business":
        warnings.append(
            f"EnterpriseSettings.data_format={enterprise.data_format!r}; expected 'Business' for baseline Business catalog preview."
        )
    if not bool(enterprise.catalog_enabled):
        warnings.append("EnterpriseSettings.catalog_enabled is false; preview is read-only and does not switch runtime.")
    if not branch_id:
        errors.append("EnterpriseSettings.branch_id is required for baseline catalog preview.")

    developer_settings = await _get_developer_settings(session)
    endpoint_preview = None
    if developer_settings is None:
        warnings.append("DeveloperSettings not found; endpoint_preview is unavailable.")
    elif not _clean_text(developer_settings.endpoint_catalog):
        warnings.append("DeveloperSettings.endpoint_catalog is empty; endpoint_preview is unavailable.")
    elif branch_id:
        endpoint_preview = f"{_clean_text(developer_settings.endpoint_catalog)}/Import/Ref/{branch_id}"

    stmt = select(MasterCatalog)
    if hasattr(MasterCatalog, "is_archived"):
        stmt = stmt.where(MasterCatalog.is_archived.is_(False))
    stmt = stmt.order_by(MasterCatalog.sku.asc())
    master_rows = list((await session.execute(stmt)).scalars().all())

    payload_rows: list[dict[str, Any]] = []
    for row in master_rows:
        internal_product_code = _clean_text(row.sku)
        if not internal_product_code:
            continue
        base_name = _base_product_name(row)
        payload_rows.append(
            {
                "internal_product_code": internal_product_code,
                "external_product_code": internal_product_code,
                "base_name": base_name,
                "external_product_name": base_name,
                "barcode": _preview_barcode(row),
                "manufacturer": _preview_manufacturer(row),
                "brand": _preview_brand(row),
                "tabletki_enterprise_code": normalized_enterprise_code,
                "tabletki_branch": branch_id or None,
                "exportable": not errors,
                "reasons": [],
            }
        )

    limited_payload_rows = payload_rows if limit is None else payload_rows[: max(0, int(limit))]
    status = "ok"
    if errors:
        status = "error"
    elif warnings:
        status = "warning"

    return {
        "status": status,
        "enterprise_code": normalized_enterprise_code,
        "enterprise_name": _clean_text(enterprise.enterprise_name) or None,
        "enterprise_catalog_enabled": bool(enterprise.catalog_enabled),
        "catalog_gate_source": "enterprise_settings",
        "assortment_mode": "baseline_legacy",
        "candidate_source": "master_all",
        "catalog_runtime_path": "baseline_legacy",
        "tabletki_branch": branch_id or None,
        "master_catalog_total": len(master_rows),
        "candidate_products": len(payload_rows),
        "exportable_products": len(payload_rows) if not errors else 0,
        "not_exportable_products": 0 if not errors else len(payload_rows),
        "missing_code_mapping": 0,
        "missing_name_mapping": 0,
        "endpoint_preview": endpoint_preview,
        "sample_rows": limited_payload_rows[:20],
        "payload_preview": limited_payload_rows,
        "warnings": warnings,
        "errors": errors,
    }

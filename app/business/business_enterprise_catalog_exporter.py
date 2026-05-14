from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_enterprise_catalog_preview import (
    build_enterprise_catalog_payload_preview,
)
from app.business.business_store_catalog_exporter import (
    _build_offer_payload,
    _build_preview_document,
    _build_suppliers_block,
    _clean_text,
    _get_store_or_fail,
    _load_export_dependencies,
    _save_preview_file,
)


async def export_business_enterprise_catalog(
    session: AsyncSession,
    store_id: int,
    dry_run: bool = True,
    limit: int | None = None,
    require_confirm: bool = True,
) -> dict[str, Any]:
    if not dry_run and require_confirm:
        raise ValueError("Live send requires explicit confirm.")

    store = await _get_store_or_fail(session, int(store_id))
    preview = await build_enterprise_catalog_payload_preview(
        session,
        enterprise_code=str(store.enterprise_code or ""),
        limit=None,
        assortment_mode="store_compatible",
        store_id=int(store.id),
    )

    warnings = list(preview.get("warnings") or [])
    errors = list(preview.get("errors") or [])
    exportable_rows = [
        row for row in (preview.get("payload_preview") or [])
        if bool(row.get("exportable"))
    ]

    if limit is not None:
        exportable_rows = exportable_rows[: max(0, int(limit))]

    developer_settings = None
    enterprise_settings = None
    payload = None
    endpoint = None
    target_branch = _clean_text(preview.get("tabletki_branch"))
    target_enterprise_code = _clean_text(store.tabletki_enterprise_code or store.enterprise_code)

    if not target_branch:
        errors.append("EnterpriseSettings.branch_id is required for enterprise-level catalog export.")
    if not target_enterprise_code:
        errors.append("BusinessStore enterprise_code or tabletki_enterprise_code is required for catalog export.")
    if not exportable_rows:
        errors.append("No exportable products found for this BusinessStore.")

    if not errors:
        developer_settings, enterprise_settings = await _load_export_dependencies(session, store)
        if not _clean_text(developer_settings.endpoint_catalog):
            errors.append("DeveloperSettings.endpoint_catalog is required for catalog export.")
        if not _clean_text(enterprise_settings.tabletki_login):
            errors.append("EnterpriseSettings.tabletki_login is required for catalog export.")
        if not _clean_text(enterprise_settings.tabletki_password):
            errors.append("EnterpriseSettings.tabletki_password is required for catalog export.")

    if not errors:
        payload = {
            "Suppliers": _build_suppliers_block(developer_settings),
            "Offers": [_build_offer_payload(row) for row in exportable_rows],
        }
        endpoint = f"{developer_settings.endpoint_catalog}/Import/Ref/{target_branch}"

    sample_payload = []
    if payload is not None:
        sample_payload = payload["Offers"][:20]

    result = {
        "status": "error" if errors else "ok",
        "dry_run": bool(dry_run),
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_catalog_enabled": bool(enterprise_settings.catalog_enabled) if enterprise_settings is not None else None,
        "store_catalog_enabled_deprecated": bool(store.catalog_enabled),
        "catalog_gate_source": "enterprise_settings",
        "tabletki_enterprise_code": target_enterprise_code,
        "tabletki_branch": store.tabletki_branch,
        "identity_mode": "enterprise_level",
        "assortment_mode": preview.get("assortment_mode") or "store_compatible",
        "catalog_scope_store_id": preview.get("catalog_scope_store_id"),
        "catalog_scope_store_code": preview.get("catalog_scope_store_code"),
        "catalog_scope_branch": preview.get("catalog_scope_branch"),
        "catalog_scope_key": preview.get("catalog_scope_key"),
        "catalog_scope_source": preview.get("catalog_scope_source"),
        "catalog_scope_error_reason": preview.get("catalog_scope_error_reason"),
        "catalog_only_in_stock_source": preview.get("catalog_only_in_stock_source"),
        "catalog_only_in_stock": preview.get("catalog_only_in_stock"),
        "target_branch": target_branch or None,
        "target_branch_source": "enterprise_settings",
        "candidate_source": preview.get("candidate_source"),
        "total_candidates": preview.get("candidate_products", 0),
        "exportable_products": len(exportable_rows),
        "skipped_products": max(0, int(preview.get("candidate_products", 0) or 0) - len(exportable_rows)),
        "sent_products": 0,
        "sample_payload": sample_payload,
        "warnings": warnings,
        "errors": errors,
        "tabletki_response": None,
        "endpoint_preview": endpoint,
        "preview_file": None,
        "catalog_runtime_path": "enterprise_identity",
    }

    if payload is not None:
        preview_document = _build_preview_document(
            store=store,
            payload=payload,
            payload_preview_rows=sample_payload,
            result_summary=result,
        )
        result["preview_file"] = _save_preview_file(store, preview_document)

    if errors or dry_run:
        return result

    from app.services.catalog_export_service import post_data_to_endpoint

    response_status, response_text = await post_data_to_endpoint(
        endpoint,
        payload,
        _clean_text(enterprise_settings.tabletki_login),
        _clean_text(enterprise_settings.tabletki_password),
        target_enterprise_code,
    )
    result["sent_products"] = len(exportable_rows)
    result["tabletki_response"] = {
        "status_code": int(response_status),
        "body_preview": str(response_text or "")[:2000],
    }
    result["status"] = "sent" if int(response_status) < 400 else "send_failed"
    return result

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_catalog_preview import build_store_catalog_payload_preview
from app.models import BusinessStore, DeveloperSettings, EnterpriseSettings
from app.services.catalog_export_service import SUPPLIER_MAPPING, post_data_to_endpoint
from app.services.business_runtime_mode_service import (
    BASELINE_BUSINESS_RUNTIME_MODE,
    resolve_business_runtime_mode_from_db,
)


VAT_VALUE = 20.0
EXPORTS_DIR = Path("exports")


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


async def _get_store_or_fail(session: AsyncSession, store_id: int) -> BusinessStore:
    store = (
        await session.execute(
            select(BusinessStore).where(BusinessStore.id == int(store_id)).limit(1)
        )
    ).scalar_one_or_none()
    if store is None:
        raise ValueError(f"BusinessStore not found for store_id={store_id}")
    return store


async def _get_store_by_code_or_fail(session: AsyncSession, store_code: str) -> BusinessStore:
    normalized_store_code = str(store_code or "").strip()
    if not normalized_store_code:
        raise ValueError("store_code is required")
    store = (
        await session.execute(
            select(BusinessStore).where(BusinessStore.store_code == normalized_store_code).limit(1)
        )
    ).scalar_one_or_none()
    if store is None:
        raise ValueError(f"BusinessStore not found for store_code={normalized_store_code}")
    return store


async def _load_export_dependencies(
    session: AsyncSession,
    store: BusinessStore,
) -> tuple[DeveloperSettings, EnterpriseSettings]:
    developer_settings = (
        await session.execute(select(DeveloperSettings).limit(1))
    ).scalar_one_or_none()
    if developer_settings is None:
        raise ValueError("DeveloperSettings not found")

    enterprise_code = str(store.enterprise_code or "").strip()
    if not enterprise_code:
        raise ValueError(f"BusinessStore {store.store_code} has empty enterprise_code")

    enterprise_settings = (
        await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code).limit(1)
        )
    ).scalar_one_or_none()
    if enterprise_settings is None:
        raise ValueError(f"EnterpriseSettings not found for enterprise_code={enterprise_code}")
    return developer_settings, enterprise_settings


def _build_suppliers_block(developer_settings: DeveloperSettings) -> list[dict[str, str]]:
    suppliers: list[dict[str, str]] = []
    for supplier_key, supplier_id in SUPPLIER_MAPPING.items():
        suppliers.append(
            {
                "ID": str(supplier_id),
                "Name": supplier_key.capitalize(),
                "Edrpo": _clean_text(getattr(developer_settings, supplier_key, None)),
            }
        )
    return suppliers


def _build_supplier_codes(barcode: str) -> list[dict[str, str]]:
    normalized_barcode = _clean_text(barcode)
    if not normalized_barcode:
        return []
    return [{"ID": str(SUPPLIER_MAPPING["barcode"]), "Code": normalized_barcode}]


def _build_offer_payload(preview_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "Code": _clean_text(preview_row.get("external_product_code")),
        "Name": _clean_text(preview_row.get("external_product_name")),
        "Producer": _clean_text(preview_row.get("manufacturer")),
        "VAT": VAT_VALUE,
        "SupplierCodes": _build_supplier_codes(_clean_text(preview_row.get("barcode"))),
    }


def _build_preview_document(
    *,
    store: BusinessStore,
    payload: dict[str, Any],
    payload_preview_rows: list[dict[str, Any]],
    result_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "store_id": int(store.id),
        "store_code": store.store_code,
        "enterprise_code": store.enterprise_code,
        "tabletki_enterprise_code": store.tabletki_enterprise_code,
        "tabletki_branch": store.tabletki_branch,
        "result": result_summary,
        "payload": payload,
        "payload_preview": payload_preview_rows,
    }


def _save_preview_file(store: BusinessStore, document: dict[str, Any]) -> str:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORTS_DIR / f"business_store_catalog_{store.store_code}.json"
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


async def export_business_store_catalog(
    session: AsyncSession,
    store_id: int,
    dry_run: bool = True,
    limit: int | None = None,
    require_confirm: bool = True,
) -> dict[str, Any]:
    if not dry_run and require_confirm:
        raise ValueError("Live send requires explicit confirm.")

    store = await _get_store_or_fail(session, int(store_id))
    preview = await build_store_catalog_payload_preview(
        session,
        int(store.id),
        limit=None,
        include_not_exportable=True,
    )

    warnings = list(preview.get("warnings") or [])
    errors: list[str] = []
    exportable_rows = [
        row for row in (preview.get("payload_preview") or [])
        if bool(row.get("exportable"))
    ]

    if limit is not None:
        exportable_rows = exportable_rows[: max(0, int(limit))]

    if not str(store.tabletki_enterprise_code or "").strip():
        errors.append("BusinessStore tabletki_enterprise_code is required for catalog export.")
    if not str(store.tabletki_branch or "").strip():
        errors.append("BusinessStore tabletki_branch is required for catalog export.")
    if not exportable_rows:
        errors.append("No exportable products found for this BusinessStore.")

    developer_settings = None
    enterprise_settings = None
    payload = None
    endpoint = None
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
        endpoint = f"{developer_settings.endpoint_catalog}/Import/Ref/{_clean_text(store.tabletki_branch)}"

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
        "catalog_gate_source": "store_and_enterprise_legacy",
        "tabletki_enterprise_code": store.tabletki_enterprise_code,
        "tabletki_branch": store.tabletki_branch,
        "identity_mode": "store_level",
        "assortment_mode": "store_native",
        "catalog_scope_store_id": int(store.id),
        "catalog_scope_store_code": store.store_code,
        "catalog_scope_branch": store.tabletki_branch,
        "catalog_scope_key": store.legacy_scope_key,
        "catalog_scope_source": "selected_store_legacy",
        "catalog_only_in_stock_source": "selected_store_legacy",
        "catalog_only_in_stock": bool(store.catalog_only_in_stock),
        "target_branch": store.tabletki_branch,
        "target_branch_source": "business_store",
        "candidate_source": preview.get("summary", {}).get("catalog_source"),
        "total_candidates": preview.get("summary", {}).get("candidate_products", 0),
        "exportable_products": len(exportable_rows),
        "skipped_products": max(0, int(preview.get("summary", {}).get("candidate_products", 0)) - len(exportable_rows)),
        "sent_products": 0,
        "sample_payload": sample_payload,
        "warnings": warnings,
        "errors": errors,
        "tabletki_response": None,
        "endpoint_preview": endpoint,
        "preview_file": None,
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

    response_status, response_text = await post_data_to_endpoint(
        endpoint,
        payload,
        _clean_text(enterprise_settings.tabletki_login),
        _clean_text(enterprise_settings.tabletki_password),
        _clean_text(store.tabletki_enterprise_code or store.enterprise_code),
    )
    result["sent_products"] = len(exportable_rows)
    result["tabletki_response"] = {
        "status_code": int(response_status),
        "body_preview": str(response_text or "")[:2000],
    }
    result["status"] = "sent" if int(response_status) < 400 else "send_failed"
    return result


async def export_business_store_catalog_by_selector(
    session: AsyncSession,
    *,
    store_id: int | None = None,
    store_code: str | None = None,
    dry_run: bool = True,
    limit: int | None = None,
    require_confirm: bool = True,
) -> dict[str, Any]:
    if store_id:
        store = await _get_store_or_fail(session, int(store_id))
    elif store_code:
        store = await _get_store_by_code_or_fail(session, str(store_code))
    else:
        raise ValueError("store_id or store_code is required")

    runtime_mode_report = await resolve_business_runtime_mode_from_db(session, str(store.enterprise_code or ""))

    if runtime_mode_report.get("business_runtime_mode") == BASELINE_BUSINESS_RUNTIME_MODE:
        from app.business.business_baseline_catalog_exporter import (
            export_business_baseline_catalog,
        )

        return await export_business_baseline_catalog(
            session,
            store_id=int(store.id),
            dry_run=dry_run,
            limit=limit,
            require_confirm=require_confirm,
        )

    if runtime_mode_report.get("catalog_runtime_path") == "enterprise_identity":
        from app.business.business_enterprise_catalog_exporter import (
            export_business_enterprise_catalog,
        )

        return await export_business_enterprise_catalog(
            session,
            store_id=int(store.id),
            dry_run=dry_run,
            limit=limit,
            require_confirm=require_confirm,
        )

    return await export_business_store_catalog(
        session,
        store_id=int(store.id),
        dry_run=dry_run,
        limit=limit,
        require_confirm=require_confirm,
    )

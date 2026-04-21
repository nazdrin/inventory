from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.business.business_store_stock_preview import build_store_stock_payload_preview
from app.models import BusinessStore, DeveloperSettings, EnterpriseSettings


EXPORTS_DIR = Path("exports")


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    normalized = _clean_text(value)
    if not normalized:
        return None
    return Decimal(normalized)


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
    normalized_store_code = _clean_text(store_code)
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

    enterprise_code = _clean_text(store.enterprise_code)
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


def _build_rest_item(preview_row: dict[str, Any]) -> dict[str, Any]:
    final_price = _decimal_or_none(preview_row.get("final_store_price_preview"))
    qty = int(preview_row.get("qty") or 0)
    return {
        "Code": _clean_text(preview_row.get("external_product_code")),
        "Price": int(final_price) if final_price is not None else 0,
        "Qty": qty,
        "PriceReserve": int(final_price) if final_price is not None else 0,
    }


def _build_payload(store: BusinessStore, exportable_rows: list[dict[str, Any]]) -> dict[str, Any]:
    branch_code = _clean_text(store.tabletki_branch)
    return {
        "Branches": [
            {
                "Code": branch_code,
                "Rests": [_build_rest_item(row) for row in exportable_rows],
            }
        ]
    }


def _build_sample_debug_rows(exportable_rows: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    sample: list[dict[str, Any]] = []
    for row in exportable_rows[: max(0, int(limit))]:
        sample.append(
            {
                "internal_product_code": row.get("internal_product_code"),
                "external_product_code": row.get("external_product_code"),
                "qty": row.get("qty"),
                "base_price": row.get("base_price"),
                "markup_percent": row.get("markup_percent"),
                "final_store_price_preview": row.get("final_store_price_preview"),
            }
        )
    return sample


def _build_preview_document(
    *,
    store: BusinessStore,
    payload: dict[str, Any],
    sample_payload: list[dict[str, Any]],
    sample_debug_rows: list[dict[str, Any]],
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
        "sample_payload": sample_payload,
        "sample_debug_rows": sample_debug_rows,
    }


def _save_preview_file(store: BusinessStore, document: dict[str, Any]) -> str:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORTS_DIR / f"business_store_stock_{store.store_code}.json"
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    return str(path)


async def _send_payload(endpoint: str, payload: dict[str, Any], login: str, password: str) -> tuple[int, str]:
    headers = {"Content-Type": "application/json"}
    auth = aiohttp.BasicAuth(login, password)
    async with aiohttp.ClientSession() as session:
        async with session.post(endpoint, json=payload, headers=headers, auth=auth) as response:
            response_text = await response.text()
            return response.status, response_text


async def export_business_store_stock(
    session: AsyncSession,
    store_id: int,
    dry_run: bool = True,
    limit: int | None = None,
    require_confirm: bool = True,
    confirm: bool = False,
) -> dict[str, Any]:
    if not dry_run and require_confirm and not bool(confirm):
        raise ValueError("Live send requires explicit confirm.")

    store = await _get_store_or_fail(session, int(store_id))
    preview = await build_store_stock_payload_preview(
        session,
        int(store.id),
        limit=None,
        include_not_exportable=True,
    )

    warnings = list(preview.get("warnings") or [])
    errors: list[str] = []
    all_rows = list(preview.get("payload_preview") or [])
    exportable_rows_all = [row for row in all_rows if bool(row.get("exportable"))]
    exportable_rows = exportable_rows_all if limit is None else exportable_rows_all[: max(0, int(limit))]

    if not _clean_text(store.tabletki_enterprise_code):
        errors.append("BusinessStore tabletki_enterprise_code is required for stock export.")
    if not _clean_text(store.tabletki_branch):
        errors.append("BusinessStore tabletki_branch is required for stock export.")
    if not exportable_rows:
        errors.append("No exportable stock rows found for this BusinessStore.")

    developer_settings = None
    enterprise_settings = None
    payload = None
    endpoint = None
    if not errors:
        developer_settings, enterprise_settings = await _load_export_dependencies(session, store)
        if not _clean_text(developer_settings.endpoint_stock):
            errors.append("DeveloperSettings.endpoint_stock is required for stock export.")
        if not _clean_text(enterprise_settings.tabletki_login):
            errors.append("EnterpriseSettings.tabletki_login is required for stock export.")
        if not _clean_text(enterprise_settings.tabletki_password):
            errors.append("EnterpriseSettings.tabletki_password is required for stock export.")
    if not errors:
        payload = _build_payload(store, exportable_rows)
        endpoint = f"{_clean_text(developer_settings.endpoint_stock)}/Import/Rests"

    sample_payload = []
    sample_debug_rows = _build_sample_debug_rows(exportable_rows)
    if payload is not None:
        sample_payload = list((payload.get("Branches") or [{}])[0].get("Rests") or [])[:20]

    result = {
        "status": "error" if errors else "ok",
        "dry_run": bool(dry_run),
        "store_id": int(store.id),
        "store_code": store.store_code,
        "tabletki_enterprise_code": store.tabletki_enterprise_code,
        "tabletki_branch": store.tabletki_branch,
        "total_candidates": int(preview.get("summary", {}).get("candidate_products", 0) or 0),
        "exportable_products_total": len(exportable_rows_all),
        "exportable_products": len(exportable_rows),
        "skipped_products": max(0, int(preview.get("summary", {}).get("candidate_products", 0) or 0) - len(exportable_rows_all)),
        "sent_products": 0,
        "sample_payload": sample_payload,
        "sample_debug_rows": sample_debug_rows,
        "warnings": warnings,
        "errors": errors,
        "tabletki_response": None,
        "endpoint_preview": endpoint,
        "preview_file": None,
        "payload_branches_count": len(payload.get("Branches") or []) if payload is not None else 0,
    }

    if payload is not None:
        preview_document = _build_preview_document(
            store=store,
            payload=payload,
            sample_payload=sample_payload,
            sample_debug_rows=sample_debug_rows,
            result_summary=result,
        )
        result["preview_file"] = _save_preview_file(store, preview_document)

    if errors or dry_run:
        return result

    response_status, response_text = await _send_payload(
        endpoint,
        payload,
        _clean_text(enterprise_settings.tabletki_login),
        _clean_text(enterprise_settings.tabletki_password),
    )
    result["sent_products"] = len(exportable_rows)
    result["tabletki_response"] = {
        "status_code": int(response_status),
        "body_preview": str(response_text or "")[:2000],
    }
    result["status"] = "sent" if int(response_status) < 400 else "send_failed"
    return result


async def export_business_store_stock_by_selector(
    session: AsyncSession,
    *,
    store_id: int | None = None,
    store_code: str | None = None,
    dry_run: bool = True,
    limit: int | None = None,
    require_confirm: bool = True,
    confirm: bool = False,
) -> dict[str, Any]:
    if store_id:
        store = await _get_store_or_fail(session, int(store_id))
    elif store_code:
        store = await _get_store_by_code_or_fail(session, str(store_code))
    else:
        raise ValueError("store_id or store_code is required")

    return await export_business_store_stock(
        session,
        store_id=int(store.id),
        dry_run=dry_run,
        limit=limit,
        require_confirm=require_confirm,
        confirm=confirm,
    )

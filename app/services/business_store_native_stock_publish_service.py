from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DeveloperSettings, EnterpriseSettings
from app.services.business_store_native_stock_dataset_service import (
    build_business_store_native_stock_dataset,
)
from app.services.stock_export_service import send_to_endpoint


EXPORTS_DIR = Path("exports")


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


async def _load_export_dependencies(
    session: AsyncSession,
    *,
    enterprise_code: str,
) -> tuple[DeveloperSettings | None, EnterpriseSettings | None]:
    developer_settings = (
        await session.execute(select(DeveloperSettings).limit(1))
    ).scalar_one_or_none()
    enterprise_settings = (
        await session.execute(
            select(EnterpriseSettings)
            .where(EnterpriseSettings.enterprise_code == _clean_text(enterprise_code))
            .limit(1)
        )
    ).scalar_one_or_none()
    return developer_settings, enterprise_settings


def _build_payload_rows_by_branch(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        branch = _clean_text(row.get("tabletki_branch"))
        if not branch:
            continue
        grouped.setdefault(branch, []).append(
            {
                "Code": _clean_text(row.get("external_product_code")),
                "Price": int(row.get("price") or 0),
                "Qty": int(row.get("qty") or 0),
                "PriceReserve": int(row.get("price_reserve") or 0),
            }
        )
    return grouped


def _build_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped = _build_payload_rows_by_branch(rows)
    return {
        "Branches": [
            {
                "Code": branch,
                "Rests": rests,
            }
            for branch, rests in sorted(grouped.items())
        ]
    }


def _save_preview_file(*, enterprise_code: str, document: dict[str, Any]) -> str:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORTS_DIR / f"business_store_native_stock_{enterprise_code}.json"
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    return str(path)


async def publish_business_store_native_stock(
    session: AsyncSession,
    *,
    enterprise_code: str,
    dry_run: bool = True,
    limit: int | None = None,
    store_id: int | None = None,
    require_confirm: bool = True,
    confirm: bool = False,
    compare_legacy: bool = False,
    allow_baseline_runtime_override: bool = False,
) -> dict[str, Any]:
    if not dry_run and require_confirm and not bool(confirm):
        raise ValueError("Live send requires explicit confirm.")

    dataset = await build_business_store_native_stock_dataset(
        session,
        enterprise_code=enterprise_code,
        store_id=store_id,
        limit=limit,
        compare_legacy=compare_legacy,
        allow_baseline_runtime_override=bool(allow_baseline_runtime_override),
    )

    rows = list(dataset.get("payload_rows") or [])
    external_dataset = {key: value for key, value in dataset.items() if key != "payload_rows"}
    warnings = list(dataset.get("warnings") or [])
    errors = list(dataset.get("errors") or [])
    payload = _build_payload(rows) if rows else {"Branches": []}
    endpoint = None
    preview_file = _save_preview_file(
        enterprise_code=_clean_text(enterprise_code),
        document={
            "result": dataset,
            "payload": payload,
            "sample_payload": list((payload.get("Branches") or [{}])[0].get("Rests") or [])[:20] if payload.get("Branches") else [],
        },
    )

    developer_settings = None
    enterprise_settings = None
    if not dry_run and not errors:
        developer_settings, enterprise_settings = await _load_export_dependencies(
            session,
            enterprise_code=_clean_text(enterprise_code),
        )
        if developer_settings is None:
            errors.append("DeveloperSettings not found.")
        if enterprise_settings is None:
            errors.append("EnterpriseSettings not found for stock export.")
        if developer_settings is not None and not _clean_text(developer_settings.endpoint_stock):
            errors.append("DeveloperSettings.endpoint_stock is required for stock export.")
        if enterprise_settings is not None and not _clean_text(enterprise_settings.tabletki_login):
            errors.append("EnterpriseSettings.tabletki_login is required for stock export.")
        if enterprise_settings is not None and not _clean_text(enterprise_settings.tabletki_password):
            errors.append("EnterpriseSettings.tabletki_password is required for stock export.")
        if not rows:
            errors.append("No payload rows available for stock export.")
        if not errors:
            endpoint = f"{_clean_text(developer_settings.endpoint_stock)}/Import/Rests"

    result = {
        **external_dataset,
        "dry_run": bool(dry_run),
        "endpoint_preview": endpoint,
        "preview_file": preview_file,
        "payload_branches_count": len(payload.get("Branches") or []),
        "sample_payload": list((payload.get("Branches") or [{}])[0].get("Rests") or [])[:20] if payload.get("Branches") else [],
        "sent_products": 0,
        "status": external_dataset.get("status"),
        "warnings": warnings,
        "errors": errors,
    }

    if dry_run or errors:
        if errors:
            result["status"] = "error"
        return result

    response_status, response_text = await send_to_endpoint(
        endpoint,
        payload,
        _clean_text(enterprise_settings.tabletki_login),
        _clean_text(enterprise_settings.tabletki_password),
        _clean_text(enterprise_code),
    )
    result["status"] = "sent" if 200 <= int(response_status) < 300 else "error"
    result["tabletki_response"] = {
        "status_code": int(response_status),
        "body": response_text,
    }
    result["sent_products"] = len(rows)
    return result

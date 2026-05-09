from __future__ import annotations

import logging
import os
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.integrations.checkbox.client import CheckboxClient
from app.integrations.checkbox.config import load_checkbox_settings
from app.integrations.checkbox.mapper import map_salesdrive_order_to_receipt
from app.integrations.checkbox.notifications import notify_receipt_fiscalized
from app.integrations.checkbox.repository import (
    get_or_create_receipt,
    mark_receipt_failed,
    mark_receipt_fiscalized,
    mark_receipt_pending,
    mark_receipt_skipped,
)
from app.integrations.checkbox.shift_service import ensure_open_shift
from app.integrations.salesdrive.client import get_salesdrive_api_key, update_order_field
from app.models import CheckboxReceipt


logger = logging.getLogger("checkbox.service")


def _extract_receipt_id(response: dict[str, Any]) -> str | None:
    value = response.get("id")
    return str(value).strip() if value else None


def _extract_shift_id(response: dict[str, Any]) -> str | None:
    shift = response.get("shift")
    if isinstance(shift, dict) and shift.get("id"):
        return str(shift["id"]).strip()
    value = response.get("shift_id")
    return str(value).strip() if value else None


def _extract_receipt_url(response: dict[str, Any]) -> str | None:
    receipt_id = response.get("id")
    if receipt_id:
        base_url = os.getenv("CHECKBOX_API_BASE_URL", "https://api.checkbox.ua").rstrip("/")
        return f"{base_url}/api/v1/receipts/{receipt_id}/png"
    for key in ("receipt_url", "html_url", "url", "tax_url"):
        value = response.get(key)
        if value:
            return str(value)
    return None


def _extract_fiscal_code(response: dict[str, Any]) -> str | None:
    value = response.get("fiscal_code")
    return str(value).strip() if value else None


def _normalize_supplier_token(value: Any) -> str:
    return str(value or "").strip().lower()


def _collect_supplier_tokens(data: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for key in (
        "supplier",
        "supplier_code",
        "supplierCode",
        "supplierlist",
        "supplierList",
        "supplier_id",
        "supplierId",
    ):
        value = data.get(key)
        if isinstance(value, list):
            for item in value:
                tokens.add(_normalize_supplier_token(item))
        else:
            tokens.add(_normalize_supplier_token(value))

    products = data.get("products") or []
    if isinstance(products, list):
        for product in products:
            if not isinstance(product, dict):
                continue
            for key in (
                "supplier",
                "supplier_code",
                "supplierCode",
                "supplier_id",
                "supplierId",
                "supplierlist",
                "supplierList",
            ):
                value = product.get(key)
                if isinstance(value, list):
                    for item in value:
                        tokens.add(_normalize_supplier_token(item))
                else:
                    tokens.add(_normalize_supplier_token(value))
    return {token for token in tokens if token}


def _excluded_supplier_match(data: dict[str, Any], excluded_suppliers: set[str]) -> str | None:
    if not excluded_suppliers:
        return None
    tokens = _collect_supplier_tokens(data)
    normalized_excluded = {_normalize_supplier_token(item) for item in excluded_suppliers if str(item).strip()}
    for token in tokens:
        for excluded in normalized_excluded:
            if token == excluded or excluded in token:
                return excluded
    return None


async def _update_salesdrive_check(
    session: AsyncSession,
    *,
    settings,
    row: CheckboxReceipt,
) -> bool:
    if not (settings.salesdrive_update_check_enabled and row.receipt_url):
        return True
    api_key = await get_salesdrive_api_key(session, row.enterprise_code)
    if not api_key:
        logger.warning("SalesDrive check update skipped: no API key enterprise_code=%s", row.enterprise_code)
        await mark_receipt_failed(row, error_message="SalesDrive API key is not configured")
        return False
    updated = await update_order_field(
        api_key=api_key,
        order_id=row.salesdrive_order_id,
        external_id=row.salesdrive_external_id,
        field_name=settings.salesdrive_check_field,
        value=row.receipt_url,
    )
    if not updated:
        await mark_receipt_failed(row, error_message="SalesDrive check field update failed")
        return False
    return True


async def handle_salesdrive_webhook_order(
    session: AsyncSession,
    *,
    data: dict[str, Any],
    enterprise_code: str,
) -> None:
    settings = load_checkbox_settings()
    if not settings.is_enabled_for_enterprise(enterprise_code):
        return

    status_id = data.get("statusId")
    try:
        status_id_int = int(status_id)
    except (TypeError, ValueError):
        return
    if status_id_int not in (4, 5):
        return

    mapped = map_salesdrive_order_to_receipt(data, enterprise_code=enterprise_code, settings=settings)
    salesdrive_order_id = str(data.get("id") or "").strip()
    salesdrive_external_id = str(data.get("externalId") or "").strip() or None
    row = await get_or_create_receipt(
        session,
        enterprise_code=enterprise_code,
        salesdrive_order_id=salesdrive_order_id,
        salesdrive_external_id=salesdrive_external_id,
        salesdrive_status_id=status_id_int,
        cash_register_code=settings.default_cash_register_code,
        checkbox_order_id=mapped.checkbox_order_id,
        payload_json=mapped.payload,
        total_amount=mapped.total_amount,
        items_count=mapped.items_count,
    )

    excluded_supplier = _excluded_supplier_match(data, settings.excluded_suppliers)
    if excluded_supplier:
        await mark_receipt_skipped(row, reason=f"Checkbox fiscalization skipped for excluded supplier: {excluded_supplier}")
        logger.info(
            "Checkbox fiscalization skipped: enterprise_code=%s salesdrive_order_id=%s excluded_supplier=%s",
            enterprise_code,
            salesdrive_order_id,
            excluded_supplier,
        )
        return

    if status_id_int == 4:
        logger.info(
            "Checkbox draft saved: enterprise_code=%s salesdrive_order_id=%s",
            enterprise_code,
            salesdrive_order_id,
        )
        return

    if row.checkbox_status == "fiscalized" and row.receipt_url:
        await _update_salesdrive_check(session, settings=settings, row=row)
        return

    client = CheckboxClient(settings)
    try:
        token = await client.signin()
        if not row.checkbox_receipt_id:
            shift = await ensure_open_shift(
                session,
                client=client,
                settings=settings,
                token=token,
                enterprise_code=enterprise_code,
                cash_register_code=settings.default_cash_register_code,
            )
            create_response = await client.create_sell_receipt(token, mapped.payload)
            await mark_receipt_pending(
                row,
                response_json=create_response,
                checkbox_receipt_id=_extract_receipt_id(create_response),
                checkbox_shift_id=_extract_shift_id(create_response) or (shift.checkbox_shift_id if shift else None),
            )
        receipt_id = row.checkbox_receipt_id
        if not receipt_id:
            raise RuntimeError("Checkbox receipt id is missing")
        final_response = await client.wait_receipt_done(token, receipt_id)
        await mark_receipt_fiscalized(
            row,
            response_json=final_response,
            receipt_url=_extract_receipt_url(final_response),
            fiscal_code=_extract_fiscal_code(final_response),
        )
        salesdrive_updated = await _update_salesdrive_check(session, settings=settings, row=row)
        if not salesdrive_updated:
            return
        notify_receipt_fiscalized(settings, row)
    except Exception as exc:
        logger.exception(
            "Checkbox fiscalization failed: enterprise_code=%s salesdrive_order_id=%s",
            enterprise_code,
            salesdrive_order_id,
        )
        await mark_receipt_failed(row, error_message=str(exc))

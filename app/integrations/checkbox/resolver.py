from __future__ import annotations

from dataclasses import replace
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.integrations.checkbox.config import CheckboxSettings
from app.models import BusinessStore, CheckboxCashRegister, CheckboxReceiptExclusion


def _payload_branch(data: dict[str, Any]) -> str | None:
    for key in ("branch", "sajt", "utmSource"):
        value = data.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


async def resolve_business_store(
    session: AsyncSession,
    *,
    data: dict[str, Any],
    enterprise_code: str,
) -> BusinessStore | None:
    branch = _payload_branch(data)
    if branch:
        row = await session.scalar(
            select(BusinessStore)
            .where(
                BusinessStore.enterprise_code == enterprise_code,
                BusinessStore.tabletki_branch == branch,
                BusinessStore.is_active.is_(True),
            )
            .order_by(BusinessStore.id.asc())
            .limit(1)
        )
        if row is not None:
            return row

    return await session.scalar(
        select(BusinessStore)
        .where(
            BusinessStore.enterprise_code == enterprise_code,
            BusinessStore.is_active.is_(True),
            BusinessStore.business_organization_id.is_not(None),
        )
        .order_by(BusinessStore.is_legacy_default.desc(), BusinessStore.id.asc())
        .limit(1)
    )


async def resolve_cash_register(
    session: AsyncSession,
    *,
    store: BusinessStore | None,
    enterprise_code: str,
) -> CheckboxCashRegister | None:
    organization_id = int(store.business_organization_id) if store and store.business_organization_id else None
    if organization_id and store:
        row = await session.scalar(
            select(CheckboxCashRegister)
            .where(
                CheckboxCashRegister.business_organization_id == organization_id,
                CheckboxCashRegister.business_store_id == int(store.id),
                CheckboxCashRegister.is_active.is_(True),
            )
            .order_by(CheckboxCashRegister.is_default.desc(), CheckboxCashRegister.id.asc())
            .limit(1)
        )
        if row is not None:
            return row

    if organization_id:
        row = await session.scalar(
            select(CheckboxCashRegister)
            .where(
                CheckboxCashRegister.business_organization_id == organization_id,
                CheckboxCashRegister.is_active.is_(True),
            )
            .order_by(CheckboxCashRegister.is_default.desc(), CheckboxCashRegister.id.asc())
            .limit(1)
        )
        if row is not None:
            return row

    return await session.scalar(
        select(CheckboxCashRegister)
        .where(
            CheckboxCashRegister.enterprise_code == enterprise_code,
            CheckboxCashRegister.is_active.is_(True),
        )
        .order_by(CheckboxCashRegister.is_default.desc(), CheckboxCashRegister.id.asc())
        .limit(1)
    )


def settings_for_register(
    base: CheckboxSettings,
    register: CheckboxCashRegister | None,
) -> CheckboxSettings:
    if register is None:
        return base
    return replace(
        base,
        api_base_url=(register.api_base_url or base.api_base_url).rstrip("/"),
        license_key=register.checkbox_license_key or base.license_key,
        cashier_login=register.cashier_login or base.cashier_login,
        cashier_password=register.cashier_password or base.cashier_password,
        cashier_pin=register.cashier_pin or base.cashier_pin,
        test_mode=bool(register.is_test_mode),
        default_cash_register_code=register.cash_register_code or base.default_cash_register_code,
        shift_open_on_demand=str(register.shift_open_mode or "") in {"first_status_4", "on_fiscalization", "scheduled"},
        telegram_receipt_notifications_enabled=bool(register.receipt_notifications_enabled),
        telegram_shift_notifications_enabled=bool(register.shift_notifications_enabled),
    )


async def receipt_excluded_suppliers(
    session: AsyncSession,
    *,
    organization_id: int | None,
    cash_register_id: int | None,
    fallback: set[str],
) -> set[str]:
    values = {str(item).strip() for item in fallback if str(item).strip()}
    if not organization_id:
        return values
    rows = (
        await session.execute(
            select(CheckboxReceiptExclusion.supplier_code).where(
                CheckboxReceiptExclusion.business_organization_id == int(organization_id),
                CheckboxReceiptExclusion.is_active.is_(True),
                CheckboxReceiptExclusion.cash_register_id.is_(None)
                if cash_register_id is None
                else or_(
                    CheckboxReceiptExclusion.cash_register_id.is_(None),
                    CheckboxReceiptExclusion.cash_register_id == int(cash_register_id),
                ),
            )
        )
    ).scalars().all()
    values.update(str(item).strip() for item in rows if str(item).strip())
    return values

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.integrations.checkbox.client import CheckboxClient, CheckboxClientError
from app.integrations.checkbox.config import CheckboxSettings
from app.integrations.checkbox.notifications import notify_shift_closed, notify_shift_opened
from app.integrations.checkbox.repository import (
    get_open_shift,
    update_shift_summary,
    upsert_shift_from_response,
)
from app.models import CheckboxShift


logger = logging.getLogger("checkbox.shift_service")


def _is_shift_already_open_error(exc: CheckboxClientError) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "вже працює",
            "уже работает",
            "already works",
            "already working",
        )
    )


async def ensure_open_shift(
    session: AsyncSession,
    *,
    client: CheckboxClient,
    settings: CheckboxSettings,
    token: str,
    enterprise_code: str,
    cash_register_code: str,
    business_organization_id: int | None = None,
    cash_register_id: int | None = None,
) -> CheckboxShift | None:
    existing = await get_open_shift(
        session,
        enterprise_code=enterprise_code,
        cash_register_code=cash_register_code,
        cash_register_id=cash_register_id,
    )
    if existing and existing.status == "opened":
        return existing
    if existing and existing.checkbox_shift_id:
        response = await client.get_shift(token, existing.checkbox_shift_id)
        shift = await upsert_shift_from_response(
            session,
            enterprise_code=enterprise_code,
            cash_register_code=cash_register_code,
            business_organization_id=business_organization_id,
            cash_register_id=cash_register_id,
            response_json=response,
        )
        if shift.status == "opened":
            return shift

    if not settings.shift_open_on_demand:
        return existing

    try:
        response = await client.open_shift(token)
    except CheckboxClientError as exc:
        if not _is_shift_already_open_error(exc):
            raise
        logger.info(
            "Checkbox shift is already open in Checkbox: enterprise_code=%s cash_register_code=%s cash_register_id=%s",
            enterprise_code,
            cash_register_code,
            cash_register_id,
        )
        if existing:
            existing.status = "opened"
            existing.error_message = None
            existing.business_organization_id = business_organization_id or existing.business_organization_id
            existing.cash_register_id = cash_register_id or existing.cash_register_id
            await session.flush()
            return existing
        shift = CheckboxShift(
            enterprise_code=enterprise_code,
            cash_register_code=cash_register_code,
            business_organization_id=business_organization_id,
            cash_register_id=cash_register_id,
            status="opened",
            opened_at=datetime.now(timezone.utc),
            error_message=None,
        )
        session.add(shift)
        await session.flush()
        return shift
    shift = await upsert_shift_from_response(
        session,
        enterprise_code=enterprise_code,
        cash_register_code=cash_register_code,
        business_organization_id=business_organization_id,
        cash_register_id=cash_register_id,
        response_json=response,
    )
    await session.flush()
    notify_shift_opened(settings, shift)
    return shift


async def close_current_shift(
    session: AsyncSession,
    *,
    client: CheckboxClient,
    settings: CheckboxSettings,
    token: str,
    enterprise_code: str,
    cash_register_code: str,
    business_organization_id: int | None = None,
    cash_register_id: int | None = None,
) -> CheckboxShift | None:
    shift = await get_open_shift(
        session,
        enterprise_code=enterprise_code,
        cash_register_code=cash_register_code,
        cash_register_id=cash_register_id,
    )
    if not shift:
        return None

    shift.status = "closing"
    shift.business_organization_id = business_organization_id or shift.business_organization_id
    shift.cash_register_id = cash_register_id or shift.cash_register_id
    await update_shift_summary(session, shift=shift)
    response = await client.close_shift(token)
    shift.response_json = response
    shift.status = "closed"
    shift.closed_at = datetime.now(timezone.utc)
    await update_shift_summary(session, shift=shift)
    notify_shift_closed(settings, shift)
    return shift

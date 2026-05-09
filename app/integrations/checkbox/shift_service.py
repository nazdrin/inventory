from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.integrations.checkbox.client import CheckboxClient
from app.integrations.checkbox.config import CheckboxSettings
from app.integrations.checkbox.notifications import notify_shift_closed, notify_shift_opened
from app.integrations.checkbox.repository import (
    get_open_shift,
    update_shift_summary,
    upsert_shift_from_response,
)
from app.models import CheckboxShift


logger = logging.getLogger("checkbox.shift_service")


async def ensure_open_shift(
    session: AsyncSession,
    *,
    client: CheckboxClient,
    settings: CheckboxSettings,
    token: str,
    enterprise_code: str,
    cash_register_code: str,
) -> CheckboxShift | None:
    existing = await get_open_shift(
        session,
        enterprise_code=enterprise_code,
        cash_register_code=cash_register_code,
    )
    if existing and existing.status == "opened":
        return existing
    if existing and existing.checkbox_shift_id:
        response = await client.get_shift(token, existing.checkbox_shift_id)
        shift = await upsert_shift_from_response(
            session,
            enterprise_code=enterprise_code,
            cash_register_code=cash_register_code,
            response_json=response,
        )
        if shift.status == "opened":
            return shift

    if not settings.shift_open_on_demand:
        return existing

    response = await client.open_shift(token)
    shift = await upsert_shift_from_response(
        session,
        enterprise_code=enterprise_code,
        cash_register_code=cash_register_code,
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
) -> CheckboxShift | None:
    shift = await get_open_shift(
        session,
        enterprise_code=enterprise_code,
        cash_register_code=cash_register_code,
    )
    if not shift:
        return None

    shift.status = "closing"
    await update_shift_summary(session, shift=shift)
    response = await client.close_shift(token)
    shift.response_json = response
    shift.status = "closed"
    shift.closed_at = datetime.now(timezone.utc)
    await update_shift_summary(session, shift=shift)
    notify_shift_closed(settings, shift)
    return shift

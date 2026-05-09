from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import CheckboxReceipt, CheckboxShift


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def get_receipt(
    session: AsyncSession,
    *,
    enterprise_code: str,
    salesdrive_order_id: str,
) -> CheckboxReceipt | None:
    result = await session.execute(
        select(CheckboxReceipt).where(
            CheckboxReceipt.enterprise_code == enterprise_code,
            CheckboxReceipt.salesdrive_order_id == salesdrive_order_id,
        )
    )
    return result.scalars().first()


async def get_or_create_receipt(
    session: AsyncSession,
    *,
    enterprise_code: str,
    salesdrive_order_id: str,
    salesdrive_external_id: str | None,
    salesdrive_status_id: int | None,
    cash_register_code: str,
    checkbox_order_id: str,
    payload_json: dict[str, Any],
    total_amount: Decimal,
    items_count: int,
) -> CheckboxReceipt:
    row = await get_receipt(
        session,
        enterprise_code=enterprise_code,
        salesdrive_order_id=salesdrive_order_id,
    )
    if row is not None:
        row.salesdrive_status_id = salesdrive_status_id
        row.salesdrive_external_id = salesdrive_external_id or row.salesdrive_external_id
        row.cash_register_code = cash_register_code or row.cash_register_code
        row.checkbox_order_id = row.checkbox_order_id or checkbox_order_id
        row.payload_json = payload_json
        row.total_amount = total_amount
        row.items_count = items_count
        return row

    row = CheckboxReceipt(
        enterprise_code=enterprise_code,
        salesdrive_order_id=salesdrive_order_id,
        salesdrive_external_id=salesdrive_external_id,
        salesdrive_status_id=salesdrive_status_id,
        cash_register_code=cash_register_code,
        checkbox_order_id=checkbox_order_id,
        checkbox_status="draft",
        payload_json=payload_json,
        total_amount=total_amount,
        items_count=items_count,
    )
    try:
        async with session.begin_nested():
            session.add(row)
            await session.flush()
        return row
    except IntegrityError:
        existing = await get_receipt(
            session,
            enterprise_code=enterprise_code,
            salesdrive_order_id=salesdrive_order_id,
        )
        if existing is None:
            raise
        return existing


async def mark_receipt_pending(
    row: CheckboxReceipt,
    *,
    response_json: dict[str, Any],
    checkbox_receipt_id: str | None,
    checkbox_shift_id: str | None,
) -> None:
    row.checkbox_status = "pending"
    row.checkbox_receipt_id = checkbox_receipt_id or row.checkbox_receipt_id
    row.checkbox_shift_id = checkbox_shift_id or row.checkbox_shift_id
    row.response_json = response_json
    row.error_message = None


async def mark_receipt_fiscalized(
    row: CheckboxReceipt,
    *,
    response_json: dict[str, Any],
    receipt_url: str | None,
    fiscal_code: str | None,
) -> None:
    row.checkbox_status = "fiscalized"
    row.response_json = response_json
    row.receipt_url = receipt_url or row.receipt_url
    row.fiscal_code = fiscal_code or row.fiscal_code
    row.fiscalized_at = utcnow()
    row.next_retry_at = None
    row.error_message = None


async def mark_receipt_failed(
    row: CheckboxReceipt,
    *,
    error_message: str,
    retry_delay_seconds: int = 300,
) -> None:
    row.checkbox_status = "failed"
    row.error_message = error_message[:4000]
    row.retry_count = int(row.retry_count or 0) + 1
    row.next_retry_at = utcnow() + timedelta(seconds=retry_delay_seconds)


async def get_open_shift(
    session: AsyncSession,
    *,
    enterprise_code: str,
    cash_register_code: str,
) -> CheckboxShift | None:
    result = await session.execute(
        select(CheckboxShift)
        .where(
            CheckboxShift.enterprise_code == enterprise_code,
            CheckboxShift.cash_register_code == cash_register_code,
            CheckboxShift.status.in_(("opening", "opened")),
        )
        .order_by(CheckboxShift.id.desc())
        .limit(1)
    )
    return result.scalars().first()


async def upsert_shift_from_response(
    session: AsyncSession,
    *,
    enterprise_code: str,
    cash_register_code: str,
    response_json: dict[str, Any],
) -> CheckboxShift:
    checkbox_shift_id = str(response_json.get("id") or "").strip()
    row = None
    if checkbox_shift_id:
        result = await session.execute(
            select(CheckboxShift).where(
                CheckboxShift.enterprise_code == enterprise_code,
                CheckboxShift.cash_register_code == cash_register_code,
                CheckboxShift.checkbox_shift_id == checkbox_shift_id,
            )
        )
        row = result.scalars().first()

    if row is None:
        row = CheckboxShift(
            enterprise_code=enterprise_code,
            cash_register_code=cash_register_code,
            checkbox_shift_id=checkbox_shift_id or None,
        )
        session.add(row)

    status = str(response_json.get("status") or "").lower()
    row.status = "opened" if status == "opened" else "opening"
    opened_at = response_json.get("opened_at")
    if isinstance(opened_at, datetime):
        row.opened_at = opened_at
    row.response_json = response_json
    row.error_message = None
    await session.flush()
    return row


async def update_shift_summary(
    session: AsyncSession,
    *,
    shift: CheckboxShift,
) -> None:
    if not shift.checkbox_shift_id:
        return
    result = await session.execute(
        select(
            func.count(CheckboxReceipt.id),
            func.coalesce(func.sum(CheckboxReceipt.total_amount), 0),
        ).where(
            CheckboxReceipt.checkbox_shift_id == shift.checkbox_shift_id,
            CheckboxReceipt.checkbox_status == "fiscalized",
        )
    )
    count, total = result.first() or (0, 0)
    shift.receipts_count = int(count or 0)
    shift.receipts_total_amount = total or Decimal("0")


async def due_receipts(session: AsyncSession, *, limit: int, max_attempts: int) -> list[CheckboxReceipt]:
    result = await session.execute(
        select(CheckboxReceipt)
        .where(
            CheckboxReceipt.checkbox_status.in_(("pending", "failed")),
            CheckboxReceipt.next_retry_at <= utcnow(),
            CheckboxReceipt.retry_count < max_attempts,
        )
        .order_by(CheckboxReceipt.next_retry_at.asc(), CheckboxReceipt.id.asc())
        .limit(limit)
    )
    return list(result.scalars().all())

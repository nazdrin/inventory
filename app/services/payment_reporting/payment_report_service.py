from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    AccountBalanceAdjustment,
    DropshipEnterprise,
    InternalTransferPair,
    PaymentBusinessAccount,
    PaymentBusinessEntity,
    PaymentCounterpartySupplierMapping,
    PaymentImportRun,
    SalesDrivePayment,
)


def _amount(value: Any) -> str:
    if value is None:
        value = Decimal("0")
    return format(Decimal(value).quantize(Decimal("0.01")), "f")


def _short_text(value: str | None, max_len: int = 500) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _next_month_start(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _date_start(value: date) -> datetime:
    return datetime.combine(value, datetime.min.time())


def _date_end(value: date) -> datetime:
    return datetime.combine(value, datetime.max.time()).replace(microsecond=999999)


async def _sum_account_payments(
    session: AsyncSession,
    *,
    account_id: int,
    period_from: datetime,
    period_to: datetime,
) -> tuple[int, Decimal, int, Decimal]:
    incoming_row = await session.execute(
        select(func.count(SalesDrivePayment.id), func.coalesce(func.sum(SalesDrivePayment.amount), 0)).where(
            SalesDrivePayment.payment_type == "incoming",
            SalesDrivePayment.business_account_id == account_id,
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
    )
    outcoming_row = await session.execute(
        select(func.count(SalesDrivePayment.id), func.coalesce(func.sum(SalesDrivePayment.amount), 0)).where(
            SalesDrivePayment.payment_type == "outcoming",
            SalesDrivePayment.business_account_id == account_id,
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
    )
    incoming_count, incoming_amount = incoming_row.one()
    outcoming_count, outcoming_amount = outcoming_row.one()
    return (
        int(incoming_count or 0),
        Decimal(incoming_amount or 0),
        int(outcoming_count or 0),
        Decimal(outcoming_amount or 0),
    )


async def _latest_balance_checkpoint(
    session: AsyncSession,
    *,
    account_id: int,
    period_to: datetime,
) -> AccountBalanceAdjustment | None:
    return await session.scalar(
        select(AccountBalanceAdjustment)
        .where(
            AccountBalanceAdjustment.account_id == account_id,
            AccountBalanceAdjustment.balance_date.is_not(None),
            AccountBalanceAdjustment.actual_balance.is_not(None),
            AccountBalanceAdjustment.balance_date <= period_to.date(),
        )
        .order_by(AccountBalanceAdjustment.balance_date.desc(), AccountBalanceAdjustment.id.desc())
        .limit(1)
    )


async def _previous_balance_checkpoint(
    session: AsyncSession,
    *,
    account_id: int,
    balance_date: date,
) -> AccountBalanceAdjustment | None:
    return await session.scalar(
        select(AccountBalanceAdjustment)
        .where(
            AccountBalanceAdjustment.account_id == account_id,
            AccountBalanceAdjustment.balance_date.is_not(None),
            AccountBalanceAdjustment.actual_balance.is_not(None),
            AccountBalanceAdjustment.balance_date < balance_date,
        )
        .order_by(AccountBalanceAdjustment.balance_date.desc(), AccountBalanceAdjustment.id.desc())
        .limit(1)
    )


async def _derive_opening_balance(
    session: AsyncSession,
    *,
    account_id: int,
    period_from: datetime,
    period_month: date,
    current_adjustment: AccountBalanceAdjustment | None,
    balance_checkpoint: AccountBalanceAdjustment | None,
) -> tuple[Decimal | None, str | None, str | None]:
    if current_adjustment is not None and current_adjustment.actual_opening_balance is not None:
        return Decimal(current_adjustment.actual_opening_balance), "manual_current_period", period_month.isoformat()

    if (
        balance_checkpoint is not None
        and balance_checkpoint.balance_date is not None
        and balance_checkpoint.actual_balance is not None
        and balance_checkpoint.balance_date < period_from.date()
    ):
        _, incoming_amount, _, outcoming_amount = await _sum_account_payments(
            session,
            account_id=account_id,
            period_from=_date_end(balance_checkpoint.balance_date) + timedelta(microseconds=1),
            period_to=period_from - timedelta(microseconds=1),
        )
        return (
            Decimal(balance_checkpoint.actual_balance) + incoming_amount - outcoming_amount,
            "balance_checkpoint",
            balance_checkpoint.balance_date.isoformat(),
        )

    anchor = await session.scalar(
        select(AccountBalanceAdjustment)
        .where(
            AccountBalanceAdjustment.account_id == account_id,
            AccountBalanceAdjustment.period_month < period_month,
            AccountBalanceAdjustment.actual_closing_balance.is_not(None),
        )
        .order_by(AccountBalanceAdjustment.period_month.desc())
        .limit(1)
    )
    if anchor is None or anchor.actual_closing_balance is None:
        return None, None, None

    opening_balance = Decimal(anchor.actual_closing_balance)
    carry_from = datetime.combine(_next_month_start(anchor.period_month), datetime.min.time())
    if carry_from < period_from:
        _, incoming_amount, _, outcoming_amount = await _sum_account_payments(
            session,
            account_id=account_id,
            period_from=carry_from,
            period_to=period_from - timedelta(microseconds=1),
        )
        boundary_row = await session.execute(
            select(func.coalesce(func.sum(AccountBalanceAdjustment.opening_balance_adjustment), 0)).where(
                AccountBalanceAdjustment.account_id == account_id,
                AccountBalanceAdjustment.period_month > anchor.period_month,
                AccountBalanceAdjustment.period_month < period_month,
            )
        )
        opening_balance = opening_balance + incoming_amount - outcoming_amount + Decimal(boundary_row.scalar() or 0)

    return opening_balance, "carried_forward", anchor.period_month.isoformat()


async def _calculate_balance_from_checkpoint(
    session: AsyncSession,
    *,
    account_id: int,
    checkpoint: AccountBalanceAdjustment | None,
    period_to: datetime,
) -> Decimal | None:
    if checkpoint is None or checkpoint.balance_date is None or checkpoint.actual_balance is None:
        return None
    period_from = _date_end(checkpoint.balance_date) + timedelta(microseconds=1)
    if period_from > period_to:
        return Decimal(checkpoint.actual_balance)
    _, incoming_amount, _, outcoming_amount = await _sum_account_payments(
        session,
        account_id=account_id,
        period_from=period_from,
        period_to=period_to,
    )
    return Decimal(checkpoint.actual_balance) + incoming_amount - outcoming_amount


async def _calculate_checkpoint_difference(
    session: AsyncSession,
    *,
    account_id: int,
    checkpoint: AccountBalanceAdjustment | None,
) -> tuple[Decimal | None, Decimal | None]:
    if checkpoint is None or checkpoint.balance_date is None or checkpoint.actual_balance is None:
        return None, None
    previous = await _previous_balance_checkpoint(
        session,
        account_id=account_id,
        balance_date=checkpoint.balance_date,
    )
    calculated = await _calculate_balance_from_checkpoint(
        session,
        account_id=account_id,
        checkpoint=previous,
        period_to=_date_end(checkpoint.balance_date),
    )
    if calculated is None:
        return None, None
    return calculated, Decimal(checkpoint.actual_balance) - calculated


async def build_payment_summary(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
) -> dict[str, Any]:
    incoming_total = await session.execute(
        select(func.count(SalesDrivePayment.id), func.coalesce(func.sum(SalesDrivePayment.amount), 0)).where(
            SalesDrivePayment.payment_type == "incoming",
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
    )
    outcoming_total = await session.execute(
        select(func.count(SalesDrivePayment.id), func.coalesce(func.sum(SalesDrivePayment.amount), 0)).where(
            SalesDrivePayment.payment_type == "outcoming",
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
    )
    incoming_count, incoming_amount = incoming_total.one()
    outcoming_count, outcoming_amount = outcoming_total.one()

    incoming_rows = await session.execute(
        select(
            SalesDrivePayment.incoming_category,
            func.count(SalesDrivePayment.id),
            func.coalesce(func.sum(SalesDrivePayment.amount), 0),
        )
        .where(
            SalesDrivePayment.payment_type == "incoming",
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
        .group_by(SalesDrivePayment.incoming_category)
        .order_by(SalesDrivePayment.incoming_category)
    )
    outgoing_rows = await session.execute(
        select(
            SalesDrivePayment.outgoing_category,
            SalesDrivePayment.mapping_status,
            func.count(SalesDrivePayment.id),
            func.coalesce(func.sum(SalesDrivePayment.amount), 0),
        )
        .where(
            SalesDrivePayment.payment_type == "outcoming",
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
        .group_by(SalesDrivePayment.outgoing_category, SalesDrivePayment.mapping_status)
        .order_by(SalesDrivePayment.outgoing_category, SalesDrivePayment.mapping_status)
    )

    return {
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
        "incoming_total": {"count": int(incoming_count or 0), "amount": _amount(incoming_amount)},
        "outcoming_total": {"count": int(outcoming_count or 0), "amount": _amount(outcoming_amount)},
        "incoming_by_category": [
            {"category": category, "count": int(count or 0), "amount": _amount(amount)}
            for category, count, amount in incoming_rows.all()
        ],
        "outgoing_by_category": [
            {
                "category": category,
                "mapping_status": mapping_status,
                "count": int(count or 0),
                "amount": _amount(amount),
            }
            for category, mapping_status, count, amount in outgoing_rows.all()
        ],
    }


async def build_supplier_payments_report(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
) -> dict[str, Any]:
    rows = await session.execute(
        select(
            SalesDrivePayment.supplier_code,
            DropshipEnterprise.name,
            func.count(SalesDrivePayment.id),
            func.coalesce(func.sum(SalesDrivePayment.amount), 0),
        )
        .join(DropshipEnterprise, DropshipEnterprise.code == SalesDrivePayment.supplier_code, isouter=True)
        .where(
            SalesDrivePayment.payment_type == "outcoming",
            SalesDrivePayment.mapping_status == "mapped",
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
        .group_by(SalesDrivePayment.supplier_code, DropshipEnterprise.name)
        .order_by(SalesDrivePayment.supplier_code)
    )
    suppliers = [
        {
            "supplier_code": supplier_code,
            "supplier_name": supplier_name,
            "count": int(count or 0),
            "amount": _amount(amount),
        }
        for supplier_code, supplier_name, count, amount in rows.all()
    ]
    return {
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
        "suppliers": suppliers,
        "total": {
            "count": sum(item["count"] for item in suppliers),
            "amount": _amount(sum(Decimal(item["amount"]) for item in suppliers)),
        },
    }


async def build_unmapped_counterparties_report(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    limit: int = 100,
    examples: int = 2,
) -> dict[str, Any]:
    rows = await session.execute(
        select(
            SalesDrivePayment.counterparty_name,
            SalesDrivePayment.counterparty_tax_id,
            func.count(SalesDrivePayment.id),
            func.coalesce(func.sum(SalesDrivePayment.amount), 0),
        )
        .where(
            SalesDrivePayment.payment_type == "outcoming",
            SalesDrivePayment.mapping_status == "unmapped",
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
        .group_by(SalesDrivePayment.counterparty_name, SalesDrivePayment.counterparty_tax_id)
        .order_by(func.sum(SalesDrivePayment.amount).desc())
        .limit(max(1, int(limit)))
    )
    groups: list[dict[str, Any]] = []
    for counterparty_name, counterparty_tax_id, count, amount in rows.all():
        query = select(SalesDrivePayment.id, SalesDrivePayment.amount, SalesDrivePayment.purpose).where(
            SalesDrivePayment.payment_type == "outcoming",
            SalesDrivePayment.mapping_status == "unmapped",
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
        if counterparty_name is None:
            query = query.where(SalesDrivePayment.counterparty_name.is_(None))
        else:
            query = query.where(SalesDrivePayment.counterparty_name == counterparty_name)
        if counterparty_tax_id is None:
            query = query.where(SalesDrivePayment.counterparty_tax_id.is_(None))
        else:
            query = query.where(SalesDrivePayment.counterparty_tax_id == counterparty_tax_id)
        example_rows = await session.execute(query.order_by(SalesDrivePayment.amount.desc()).limit(max(0, int(examples))))
        groups.append(
            {
                "counterparty_name": counterparty_name,
                "counterparty_tax_id": counterparty_tax_id,
                "count": int(count or 0),
                "amount": _amount(amount),
                "examples": [
                    {"payment_id": int(payment_id), "amount": _amount(row_amount), "purpose": _short_text(purpose)}
                    for payment_id, row_amount, purpose in example_rows.all()
                ],
            }
        )
    return {"period_from": period_from.isoformat(), "period_to": period_to.isoformat(), "groups": groups}


async def build_customer_receipts_report(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
) -> dict[str, Any]:
    rows = await session.execute(
        select(func.date(SalesDrivePayment.payment_date), func.count(SalesDrivePayment.id), func.sum(SalesDrivePayment.amount))
        .where(
            SalesDrivePayment.payment_type == "incoming",
            SalesDrivePayment.incoming_category == "customer_receipt",
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
        .group_by(func.date(SalesDrivePayment.payment_date))
        .order_by(func.date(SalesDrivePayment.payment_date))
    )
    daily = [{"date": str(day), "count": int(count or 0), "amount": _amount(amount)} for day, count, amount in rows.all()]
    return {
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
        "daily": daily,
        "total": {
            "count": sum(item["count"] for item in daily),
            "amount": _amount(sum(Decimal(item["amount"]) for item in daily)),
        },
    }


async def build_internal_transfers_report(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
) -> dict[str, Any]:
    rows = await session.execute(
        select(InternalTransferPair)
        .where(
            InternalTransferPair.outcoming_date >= period_from,
            InternalTransferPair.outcoming_date <= period_to,
        )
        .order_by(InternalTransferPair.outcoming_date.asc())
    )
    pairs = [
        {
            "id": int(pair.id),
            "amount": _amount(pair.amount),
            "outcoming_payment_id": int(pair.outcoming_payment_id),
            "incoming_payment_id": int(pair.incoming_payment_id),
            "outcoming_account_id": int(pair.outcoming_account_id),
            "incoming_account_id": int(pair.incoming_account_id),
            "outcoming_date": pair.outcoming_date.isoformat(),
            "incoming_date": pair.incoming_date.isoformat(),
            "reason": pair.reason,
        }
        for pair in rows.scalars().all()
    ]
    pair_payment_ids = {item["outcoming_payment_id"] for item in pairs} | {item["incoming_payment_id"] for item in pairs}
    unpaired_rows = await session.execute(
        select(SalesDrivePayment)
        .where(
            SalesDrivePayment.is_internal_transfer.is_(True),
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
        .order_by(SalesDrivePayment.payment_date.asc(), SalesDrivePayment.id.asc())
    )
    unpaired_payments = [
        {
            "payment_id": int(payment.id),
            "payment_type": payment.payment_type,
            "payment_date": payment.payment_date.isoformat(),
            "amount": _amount(payment.amount),
            "business_account_id": int(payment.business_account_id) if payment.business_account_id is not None else None,
            "counterparty_name": payment.counterparty_name,
            "purpose": _short_text(payment.purpose),
            "reason": payment.internal_transfer_reason,
        }
        for payment in unpaired_rows.scalars().all()
        if int(payment.id) not in pair_payment_ids
    ]
    return {
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
        "pairs": pairs,
        "unpaired_payments": unpaired_payments,
        "total_pairs": {"count": len(pairs), "amount": _amount(sum(Decimal(item["amount"]) for item in pairs))},
        "total_unpaired_payments": {
            "count": len(unpaired_payments),
            "amount": _amount(sum(Decimal(item["amount"]) for item in unpaired_payments)),
        },
    }


async def build_payment_import_runs_report(session: AsyncSession, *, limit: int = 50) -> list[dict[str, Any]]:
    rows = await session.execute(select(PaymentImportRun).order_by(PaymentImportRun.id.desc()).limit(max(1, int(limit))))
    return [
        {
            "id": int(item.id),
            "source_system": item.source_system,
            "period_from": item.period_from.isoformat(),
            "period_to": item.period_to.isoformat(),
            "payment_type": item.payment_type,
            "status": item.status,
            "incoming_count": int(item.incoming_count or 0),
            "outcoming_count": int(item.outcoming_count or 0),
            "created_count": int(item.created_count or 0),
            "updated_count": int(item.updated_count or 0),
            "error_message": item.error_message,
            "started_at": item.started_at.isoformat() if item.started_at else None,
            "finished_at": item.finished_at.isoformat() if item.finished_at else None,
        }
        for item in rows.scalars().all()
    ]


async def build_account_movements_report(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    business_entity_id: int | None = None,
    business_account_id: int | None = None,
) -> dict[str, Any]:
    accounts_query = (
        select(PaymentBusinessAccount, PaymentBusinessEntity)
        .join(PaymentBusinessEntity, PaymentBusinessEntity.id == PaymentBusinessAccount.business_entity_id)
        .order_by(PaymentBusinessEntity.short_name.asc(), PaymentBusinessAccount.label.asc())
    )
    if business_entity_id is not None:
        accounts_query = accounts_query.where(PaymentBusinessAccount.business_entity_id == business_entity_id)
    if business_account_id is not None:
        accounts_query = accounts_query.where(PaymentBusinessAccount.id == business_account_id)

    account_rows = (await session.execute(accounts_query)).all()
    account_items: list[dict[str, Any]] = []
    period_month = period_from.date().replace(day=1)

    for account, entity in account_rows:
        incoming_count, incoming_amount, outcoming_count, outcoming_amount = await _sum_account_payments(
            session,
            account_id=account.id,
            period_from=period_from,
            period_to=period_to,
        )
        internal_incoming_row = await session.execute(
            select(func.count(SalesDrivePayment.id), func.coalesce(func.sum(SalesDrivePayment.amount), 0)).where(
                SalesDrivePayment.payment_type == "incoming",
                SalesDrivePayment.business_account_id == account.id,
                SalesDrivePayment.is_internal_transfer.is_(True),
                SalesDrivePayment.payment_date >= period_from,
                SalesDrivePayment.payment_date <= period_to,
            )
        )
        internal_outcoming_row = await session.execute(
            select(func.count(SalesDrivePayment.id), func.coalesce(func.sum(SalesDrivePayment.amount), 0)).where(
                SalesDrivePayment.payment_type == "outcoming",
                SalesDrivePayment.business_account_id == account.id,
                SalesDrivePayment.is_internal_transfer.is_(True),
                SalesDrivePayment.payment_date >= period_from,
                SalesDrivePayment.payment_date <= period_to,
            )
        )
        adjustment = await session.scalar(
            select(AccountBalanceAdjustment).where(
                AccountBalanceAdjustment.account_id == account.id,
                AccountBalanceAdjustment.period_month == period_month,
            )
        )
        balance_checkpoint = await _latest_balance_checkpoint(
            session,
            account_id=account.id,
            period_to=period_to,
        )

        internal_incoming_count, internal_incoming_amount = internal_incoming_row.one()
        internal_outcoming_count, internal_outcoming_amount = internal_outcoming_row.one()
        opening_balance, opening_balance_source, opening_balance_source_period = await _derive_opening_balance(
            session,
            account_id=account.id,
            period_from=period_from,
            period_month=period_month,
            current_adjustment=adjustment,
            balance_checkpoint=balance_checkpoint,
        )
        closing_adjustment = adjustment.closing_balance_adjustment if adjustment is not None else Decimal("0")
        opening_adjustment = adjustment.opening_balance_adjustment if adjustment is not None else Decimal("0")
        actual_closing_balance = adjustment.actual_closing_balance if adjustment is not None else None

        calculated_closing_balance = None
        difference = None
        checkpoint_calculated_balance, checkpoint_difference = await _calculate_checkpoint_difference(
            session,
            account_id=account.id,
            checkpoint=balance_checkpoint,
        )
        checkpoint_based_closing = await _calculate_balance_from_checkpoint(
            session,
            account_id=account.id,
            checkpoint=balance_checkpoint,
            period_to=period_to,
        )
        if opening_balance is not None:
            calculated_closing_balance = (
                Decimal(opening_balance)
                + Decimal(incoming_amount or 0)
                - Decimal(outcoming_amount or 0)
                + Decimal(opening_adjustment or 0)
                + Decimal(closing_adjustment or 0)
            )
            if actual_closing_balance is not None:
                difference = Decimal(actual_closing_balance) - Decimal(calculated_closing_balance)
        elif checkpoint_based_closing is not None:
            calculated_closing_balance = checkpoint_based_closing

        account_items.append(
            {
                "business_entity_id": int(entity.id),
                "business_entity_name": entity.short_name,
                "account_id": int(account.id),
                "account_label": account.label,
                "account_number": account.account_number,
                "currency": account.currency,
                "opening_balance": _amount(opening_balance) if opening_balance is not None else None,
                "opening_balance_source": opening_balance_source,
                "opening_balance_source_period": opening_balance_source_period,
                "balance_checkpoint_date": balance_checkpoint.balance_date.isoformat()
                if balance_checkpoint is not None and balance_checkpoint.balance_date is not None
                else None,
                "balance_checkpoint_amount": _amount(balance_checkpoint.actual_balance)
                if balance_checkpoint is not None and balance_checkpoint.actual_balance is not None
                else None,
                "balance_checkpoint_calculated_amount": _amount(checkpoint_calculated_balance)
                if checkpoint_calculated_balance is not None
                else None,
                "balance_checkpoint_difference": _amount(checkpoint_difference)
                if checkpoint_difference is not None
                else None,
                "balance_checkpoint_status": "warning"
                if checkpoint_difference is not None and abs(checkpoint_difference) >= Decimal("0.01")
                else "ok"
                if checkpoint_difference is not None
                else "no_previous_checkpoint",
                "incoming": {"count": int(incoming_count or 0), "amount": _amount(incoming_amount)},
                "outcoming": {"count": int(outcoming_count or 0), "amount": _amount(outcoming_amount)},
                "internal_incoming": {
                    "count": int(internal_incoming_count or 0),
                    "amount": _amount(internal_incoming_amount),
                },
                "internal_outcoming": {
                    "count": int(internal_outcoming_count or 0),
                    "amount": _amount(internal_outcoming_amount),
                },
                "external_incoming": {
                    "count": int((incoming_count or 0) - (internal_incoming_count or 0)),
                    "amount": _amount(Decimal(incoming_amount or 0) - Decimal(internal_incoming_amount or 0)),
                },
                "external_outcoming": {
                    "count": int((outcoming_count or 0) - (internal_outcoming_count or 0)),
                    "amount": _amount(Decimal(outcoming_amount or 0) - Decimal(internal_outcoming_amount or 0)),
                },
                "opening_balance_adjustment": _amount(opening_adjustment),
                "closing_balance_adjustment": _amount(closing_adjustment),
                "calculated_closing_balance": _amount(calculated_closing_balance)
                if calculated_closing_balance is not None
                else None,
                "actual_closing_balance": _amount(actual_closing_balance) if actual_closing_balance is not None else None,
                "difference": _amount(difference) if difference is not None else None,
            }
        )

    return {"period_from": period_from.isoformat(), "period_to": period_to.isoformat(), "accounts": account_items}


async def build_counterparty_supplier_mappings_report(
    session: AsyncSession,
    *,
    limit: int = 500,
) -> list[dict[str, Any]]:
    rows = await session.execute(
        select(PaymentCounterpartySupplierMapping, DropshipEnterprise.name)
        .join(DropshipEnterprise, DropshipEnterprise.code == PaymentCounterpartySupplierMapping.supplier_code, isouter=True)
        .order_by(
            PaymentCounterpartySupplierMapping.is_active.desc(),
            PaymentCounterpartySupplierMapping.priority.asc(),
            PaymentCounterpartySupplierMapping.id.asc(),
        )
        .limit(max(1, int(limit)))
    )
    return [
        {
            "id": int(mapping.id),
            "supplier_code": mapping.supplier_code,
            "supplier_name": supplier_name,
            "supplier_salesdrive_id": mapping.supplier_salesdrive_id,
            "match_type": mapping.match_type,
            "field_scope": mapping.field_scope,
            "counterparty_pattern": mapping.counterparty_pattern,
            "counterparty_tax_id": mapping.counterparty_tax_id,
            "priority": int(mapping.priority or 100),
            "is_active": bool(mapping.is_active),
            "notes": mapping.notes,
            "created_by": mapping.created_by,
            "updated_by": mapping.updated_by,
        }
        for mapping, supplier_name in rows.all()
    ]


async def build_management_summary_report(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    business_entity_id: int | None = None,
    business_account_id: int | None = None,
) -> dict[str, Any]:
    payment_filters = [
        SalesDrivePayment.payment_date >= period_from,
        SalesDrivePayment.payment_date <= period_to,
    ]
    if business_entity_id is not None:
        payment_filters.append(SalesDrivePayment.business_entity_id == business_entity_id)
    if business_account_id is not None:
        payment_filters.append(SalesDrivePayment.business_account_id == business_account_id)

    account_movements = await build_account_movements_report(
        session,
        period_from=period_from,
        period_to=period_to,
        business_entity_id=business_entity_id,
        business_account_id=business_account_id,
    )
    incoming_rows = await session.execute(
        select(
            SalesDrivePayment.incoming_category,
            PaymentBusinessEntity.short_name,
            PaymentBusinessAccount.label,
            func.count(SalesDrivePayment.id),
            func.coalesce(func.sum(SalesDrivePayment.amount), 0),
        )
        .join(PaymentBusinessEntity, PaymentBusinessEntity.id == SalesDrivePayment.business_entity_id, isouter=True)
        .join(PaymentBusinessAccount, PaymentBusinessAccount.id == SalesDrivePayment.business_account_id, isouter=True)
        .where(SalesDrivePayment.payment_type == "incoming", *payment_filters)
        .group_by(SalesDrivePayment.incoming_category, PaymentBusinessEntity.short_name, PaymentBusinessAccount.label)
        .order_by(PaymentBusinessEntity.short_name, PaymentBusinessAccount.label, SalesDrivePayment.incoming_category)
    )
    outgoing_rows = await session.execute(
        select(
            SalesDrivePayment.outgoing_category,
            PaymentBusinessEntity.short_name,
            PaymentBusinessAccount.label,
            func.count(SalesDrivePayment.id),
            func.coalesce(func.sum(SalesDrivePayment.amount), 0),
        )
        .join(PaymentBusinessEntity, PaymentBusinessEntity.id == SalesDrivePayment.business_entity_id, isouter=True)
        .join(PaymentBusinessAccount, PaymentBusinessAccount.id == SalesDrivePayment.business_account_id, isouter=True)
        .where(SalesDrivePayment.payment_type == "outcoming", *payment_filters)
        .group_by(SalesDrivePayment.outgoing_category, PaymentBusinessEntity.short_name, PaymentBusinessAccount.label)
        .order_by(PaymentBusinessEntity.short_name, PaymentBusinessAccount.label, SalesDrivePayment.outgoing_category)
    )
    supplier_rows = await session.execute(
        select(
            PaymentBusinessEntity.short_name,
            SalesDrivePayment.supplier_code,
            DropshipEnterprise.name,
            func.count(SalesDrivePayment.id),
            func.coalesce(func.sum(SalesDrivePayment.amount), 0),
        )
        .join(PaymentBusinessEntity, PaymentBusinessEntity.id == SalesDrivePayment.business_entity_id, isouter=True)
        .join(DropshipEnterprise, DropshipEnterprise.code == SalesDrivePayment.supplier_code, isouter=True)
        .where(SalesDrivePayment.payment_type == "outcoming", SalesDrivePayment.mapping_status == "mapped", *payment_filters)
        .group_by(PaymentBusinessEntity.short_name, SalesDrivePayment.supplier_code, DropshipEnterprise.name)
        .order_by(PaymentBusinessEntity.short_name, SalesDrivePayment.supplier_code)
    )

    quality_specs = {
        "unmapped_outgoing": [
            SalesDrivePayment.payment_type == "outcoming",
            SalesDrivePayment.mapping_status == "unmapped",
        ],
        "unknown_incoming": [
            SalesDrivePayment.payment_type == "incoming",
            SalesDrivePayment.incoming_category == "unknown_incoming",
        ],
        "payments_without_entity": [SalesDrivePayment.business_entity_id.is_(None)],
        "payments_without_account": [SalesDrivePayment.business_account_id.is_(None)],
        "payments_without_counterparty": [
            SalesDrivePayment.payment_type == "outcoming",
            SalesDrivePayment.counterparty_name.is_(None),
        ],
        "direct_internal_without_pair": [
            SalesDrivePayment.is_internal_transfer.is_(True),
            SalesDrivePayment.internal_transfer_pair_id.is_(None),
        ],
    }
    quality: dict[str, dict[str, Any]] = {}
    for key, extra_filters in quality_specs.items():
        row = await session.execute(
            select(func.count(SalesDrivePayment.id), func.coalesce(func.sum(SalesDrivePayment.amount), 0)).where(
                *payment_filters,
                *extra_filters,
            )
        )
        count, amount = row.one()
        quality[key] = {"count": int(count or 0), "amount": _amount(amount)}

    unverified_entities = await session.execute(
        select(func.count(PaymentBusinessEntity.id)).where(
            PaymentBusinessEntity.verification_status != "verified",
            PaymentBusinessEntity.is_active.is_(True),
        )
    )
    quality["unverified_entities"] = {"count": int(unverified_entities.scalar() or 0), "amount": "0.00"}

    return {
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
        "account_movements": account_movements["accounts"],
        "incoming_by_category_entity_account": [
            {
                "category": category,
                "business_entity_name": entity_name,
                "account_label": account_label,
                "count": int(count or 0),
                "amount": _amount(amount),
            }
            for category, entity_name, account_label, count, amount in incoming_rows.all()
        ],
        "outgoing_by_category_entity_account": [
            {
                "category": category,
                "business_entity_name": entity_name,
                "account_label": account_label,
                "count": int(count or 0),
                "amount": _amount(amount),
            }
            for category, entity_name, account_label, count, amount in outgoing_rows.all()
        ],
        "supplier_payments_by_entity": [
            {
                "business_entity_name": entity_name,
                "supplier_code": supplier_code,
                "supplier_name": supplier_name,
                "count": int(count or 0),
                "amount": _amount(amount),
            }
            for entity_name, supplier_code, supplier_name, count, amount in supplier_rows.all()
        ],
        "data_quality": quality,
    }

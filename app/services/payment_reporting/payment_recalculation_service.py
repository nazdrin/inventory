from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    DropshipEnterprise,
    InternalTransferPair,
    InternalTransferRule,
    PaymentCounterpartySupplierMapping,
    SalesDrivePayment,
)


INCOMING_INCLUDE_CUSTOMER_MARKERS = [
    "новапей",
    "нова пей",
    "платежам, прийнятим від населення",
    "фоп бойко варвара",
]

INCOMING_EXCLUDE_MARKERS = [
    "петренко",
    "refund",
    "refunds",
    "chargeback",
    "чарджбек",
    "внутрішн",
    "власний рахунок",
]

INCOMING_OTHER_RECEIPT_MARKERS = [
    "повернення",
    "повернення коштів",
    "монамі груп",
    "мон амі груп",
]

SELF_TRANSFER_PHRASES = [
    "власний рахунок",
    "перерахування коштів на інший власний рахунок",
]

OUTGOING_TAX_MARKERS = [
    "дія | податки",
    "єдиний податок",
    "військовий збір",
    "гук ",
]

OUTGOING_OWNER_WITHDRAWAL_MARKERS = [
    "перерахування власних коштів",
    "особистий рахунок",
    "wayforpay",
    "portmone",
]

OUTGOING_LOGISTICS_MARKERS = [
    "нова пошта",
    "договір № 1040303",
]

OUTGOING_PLATFORM_FEE_MARKERS = [
    "фармел",
    "tabletki",
    "онлайн платформи tabletki",
    "тм tabletki",
]


@dataclass(frozen=True)
class PaymentRecalculationResult:
    total_payments: int
    internal_pairs: int
    internal_payments: int
    customer_receipts: int
    other_receipts: int
    excluded_receipts: int
    unknown_incoming: int
    supplier_mapped: int
    supplier_unmapped: int
    unknown_outgoing: int


def _text(value: str | None) -> str:
    return str(value or "").casefold()


def _contains_any(text: str, markers: list[str]) -> bool:
    return any(marker.casefold() in text for marker in markers)


def _payment_text(payment: SalesDrivePayment) -> str:
    return " ".join(
        part
        for part in [
            _text(payment.counterparty_name),
            _text(payment.counterparty_tax_id),
            _text(payment.organization_name),
            _text(payment.organization_tax_id),
            _text(payment.comment),
            _text(payment.purpose),
            _text(payment.search_text),
        ]
        if part
    )


def _self_marker_text(payment: SalesDrivePayment) -> str:
    return " ".join(
        part
        for part in [
            _text(payment.counterparty_name),
            _text(payment.counterparty_tax_id),
            _text(payment.comment),
            _text(payment.purpose),
        ]
        if part
    )


def _flow_classification_text(payment: SalesDrivePayment) -> str:
    return " ".join(
        part
        for part in [
            _text(payment.counterparty_name),
            _text(payment.counterparty_tax_id),
            _text(payment.comment),
            _text(payment.purpose),
        ]
        if part
    )


def _pair_key(outgoing: SalesDrivePayment, incoming: SalesDrivePayment) -> str:
    return f"salesdrive:{outgoing.id}:{incoming.id}"


def _date_diff_minutes(left: datetime, right: datetime) -> float:
    return abs((left - right).total_seconds()) / 60


def _matches_mapping(payment: SalesDrivePayment, mapping: PaymentCounterpartySupplierMapping) -> bool:
    match_type = str(mapping.match_type or "").strip()
    field_scope = str(mapping.field_scope or "").strip()

    if match_type == "tax_id":
        return bool(mapping.counterparty_tax_id and mapping.counterparty_tax_id == payment.counterparty_tax_id)

    if field_scope == "tax_id":
        value = _text(payment.counterparty_tax_id)
    elif field_scope == "counterparty_name":
        value = _text(payment.counterparty_name)
    elif field_scope == "purpose":
        value = _text(payment.purpose)
    elif field_scope == "comment":
        value = _text(payment.comment)
    else:
        value = _payment_text(payment)

    pattern = _text(mapping.normalized_pattern or mapping.counterparty_pattern)
    if not pattern:
        return False
    if match_type == "exact":
        return value == pattern
    if match_type in {"contains", "search_text_contains"}:
        return pattern in value
    return False


def _classify_known_outgoing_expense(payment: SalesDrivePayment) -> str | None:
    flow_text = _flow_classification_text(payment)
    if _contains_any(flow_text, OUTGOING_TAX_MARKERS):
        return "tax_payment"
    if _contains_any(flow_text, OUTGOING_OWNER_WITHDRAWAL_MARKERS):
        return "owner_withdrawal"
    if _contains_any(flow_text, OUTGOING_LOGISTICS_MARKERS):
        return "logistics_expense"
    if _contains_any(flow_text, OUTGOING_PLATFORM_FEE_MARKERS):
        return "platform_fee"
    return None


async def _load_payments(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
) -> list[SalesDrivePayment]:
    rows = await session.execute(
        select(SalesDrivePayment)
        .where(
            SalesDrivePayment.payment_date >= period_from,
            SalesDrivePayment.payment_date <= period_to,
        )
        .order_by(SalesDrivePayment.payment_date.asc(), SalesDrivePayment.id.asc())
    )
    return list(rows.scalars().all())


async def _clear_existing_internal_pairs(
    session: AsyncSession,
    *,
    payments: list[SalesDrivePayment],
) -> None:
    payment_ids = [payment.id for payment in payments]
    if not payment_ids:
        return
    for payment in payments:
        payment.is_internal_transfer = False
        payment.internal_transfer_pair_id = None
        payment.internal_transfer_reason = None
    await session.flush()
    await session.execute(
        delete(InternalTransferPair).where(
            (InternalTransferPair.outcoming_payment_id.in_(payment_ids))
            | (InternalTransferPair.incoming_payment_id.in_(payment_ids))
        )
    )
    await session.flush()


async def _detect_internal_transfers(
    session: AsyncSession,
    *,
    payments: list[SalesDrivePayment],
) -> int:
    await _clear_existing_internal_pairs(session, payments=payments)

    rules_rows = await session.execute(select(InternalTransferRule).where(InternalTransferRule.is_active.is_(True)))
    rules = list(rules_rows.scalars().all())
    if not rules:
        return 0

    rules_by_pair = {(rule.from_account_id, rule.to_account_id): rule for rule in rules}
    incoming = [
        payment
        for payment in payments
        if payment.payment_type == "incoming" and payment.business_account_id is not None
    ]
    outgoing = [
        payment
        for payment in payments
        if payment.payment_type == "outcoming" and payment.business_account_id is not None
    ]
    used_incoming_ids: set[int] = set()
    pair_count = 0

    for out_payment in outgoing:
        for in_payment in incoming:
            if int(in_payment.id) in used_incoming_ids:
                continue
            rule = rules_by_pair.get((out_payment.business_account_id, in_payment.business_account_id))
            if rule is None:
                continue
            if out_payment.business_entity_id != in_payment.business_entity_id:
                continue
            if rule.require_exact_amount and Decimal(out_payment.amount) != Decimal(in_payment.amount):
                continue
            if _date_diff_minutes(out_payment.payment_date, in_payment.payment_date) > int(rule.pairing_window_minutes or 5):
                continue

            pair = InternalTransferPair(
                pair_key=_pair_key(out_payment, in_payment),
                outcoming_payment_id=out_payment.id,
                incoming_payment_id=in_payment.id,
                amount=out_payment.amount,
                outcoming_account_id=out_payment.business_account_id,
                incoming_account_id=in_payment.business_account_id,
                outcoming_date=out_payment.payment_date,
                incoming_date=in_payment.payment_date,
                reason="matched account pair, amount, and time window",
                match_confidence=Decimal("1.0000"),
            )
            session.add(pair)
            await session.flush()
            out_payment.is_internal_transfer = True
            out_payment.internal_transfer_pair_id = pair.id
            out_payment.internal_transfer_reason = pair.reason
            in_payment.is_internal_transfer = True
            in_payment.internal_transfer_pair_id = pair.id
            in_payment.internal_transfer_reason = pair.reason
            used_incoming_ids.add(int(in_payment.id))
            pair_count += 1
            break

    for payment in payments:
        if payment.is_internal_transfer or payment.business_account_id is None:
            continue
        if _contains_any(_self_marker_text(payment), SELF_TRANSFER_PHRASES):
            payment.is_internal_transfer = True
            payment.internal_transfer_reason = "direct self-transfer marker"

    await session.flush()
    return pair_count


async def _load_supplier_mappings(session: AsyncSession) -> list[PaymentCounterpartySupplierMapping]:
    rows = await session.execute(
        select(PaymentCounterpartySupplierMapping)
        .where(PaymentCounterpartySupplierMapping.is_active.is_(True))
        .order_by(PaymentCounterpartySupplierMapping.priority.asc(), PaymentCounterpartySupplierMapping.id.asc())
    )
    return list(rows.scalars().all())


async def _load_suppliers(session: AsyncSession) -> dict[str, DropshipEnterprise]:
    rows = await session.execute(select(DropshipEnterprise))
    return {str(item.code): item for item in rows.scalars().all()}


async def recalculate_payment_period(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
) -> PaymentRecalculationResult:
    payments = await _load_payments(session, period_from=period_from, period_to=period_to)
    internal_pairs = await _detect_internal_transfers(session, payments=payments)
    supplier_mappings = await _load_supplier_mappings(session)
    suppliers_by_code = await _load_suppliers(session)

    customer_receipts = 0
    other_receipts = 0
    excluded_receipts = 0
    unknown_incoming = 0
    supplier_mapped = 0
    supplier_unmapped = 0
    unknown_outgoing = 0

    for payment in payments:
        payment_text = _payment_text(payment)
        flow_text = _flow_classification_text(payment)
        payment.supplier_code = None
        payment.supplier_salesdrive_id = None
        payment.counterparty_supplier_mapping_id = None
        payment.mapping_source = None

        if payment.payment_type == "incoming":
            payment.outgoing_category = None
            payment.mapping_status = "not_applicable"
            if payment.is_internal_transfer:
                payment.incoming_category = "internal_transfer"
                payment.payment_category = "internal_transfer"
            elif _contains_any(flow_text, INCOMING_EXCLUDE_MARKERS):
                payment.incoming_category = "excluded_receipt"
                payment.payment_category = "excluded_receipt"
                excluded_receipts += 1
            elif _contains_any(flow_text, INCOMING_OTHER_RECEIPT_MARKERS):
                payment.incoming_category = "other_receipt"
                payment.payment_category = "other_receipt"
                other_receipts += 1
            elif _contains_any(flow_text, INCOMING_INCLUDE_CUSTOMER_MARKERS):
                payment.incoming_category = "customer_receipt"
                payment.payment_category = "customer_receipt"
                customer_receipts += 1
            else:
                payment.incoming_category = "unknown_incoming"
                payment.payment_category = "unknown_incoming"
                unknown_incoming += 1
            continue

        payment.incoming_category = None
        if payment.is_internal_transfer:
            payment.outgoing_category = "internal_transfer"
            payment.payment_category = "internal_transfer"
            payment.mapping_status = "ignored"
            continue

        known_expense_category = _classify_known_outgoing_expense(payment)
        if known_expense_category is not None:
            payment.outgoing_category = known_expense_category
            payment.payment_category = known_expense_category
            payment.mapping_status = "ignored"
            continue

        matched_mapping = next((mapping for mapping in supplier_mappings if _matches_mapping(payment, mapping)), None)
        if matched_mapping is not None:
            supplier = suppliers_by_code.get(str(matched_mapping.supplier_code))
            payment.supplier_code = matched_mapping.supplier_code
            payment.supplier_salesdrive_id = matched_mapping.supplier_salesdrive_id or getattr(
                supplier,
                "salesdrive_supplier_id",
                None,
            )
            payment.counterparty_supplier_mapping_id = matched_mapping.id
            payment.mapping_source = matched_mapping.match_type
            payment.mapping_status = "mapped"
            payment.outgoing_category = "supplier_payment"
            payment.payment_category = "supplier_payment"
            supplier_mapped += 1
        else:
            payment.mapping_status = "unmapped"
            payment.outgoing_category = "unknown_outgoing"
            payment.payment_category = "unknown_outgoing"
            supplier_unmapped += 1
            unknown_outgoing += 1

    await session.flush()
    internal_payments = len([payment for payment in payments if payment.is_internal_transfer])
    return PaymentRecalculationResult(
        total_payments=len(payments),
        internal_pairs=internal_pairs,
        internal_payments=internal_payments,
        customer_receipts=customer_receipts,
        other_receipts=other_receipts,
        excluded_receipts=excluded_receipts,
        unknown_incoming=unknown_incoming,
        supplier_mapped=supplier_mapped,
        supplier_unmapped=supplier_unmapped,
        unknown_outgoing=unknown_outgoing,
    )

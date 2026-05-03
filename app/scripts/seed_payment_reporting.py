import asyncio
from dataclasses import dataclass

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import (
    InternalTransferRule,
    PaymentBusinessAccount,
    PaymentBusinessEntity,
    PaymentCategory,
)


PAYMENT_CATEGORIES = [
    ("customer_receipt", "Customer receipt", "incoming", 10),
    ("other_receipt", "Other receipt", "incoming", 20),
    ("excluded_receipt", "Excluded receipt", "incoming", 30),
    ("internal_transfer", "Internal transfer", "both", 40),
    ("unknown_incoming", "Unknown incoming", "incoming", 50),
    ("supplier_payment", "Supplier payment", "outgoing", 110),
    ("bank_fee", "Bank fee", "outgoing", 120),
    ("tax_payment", "Tax payment", "outgoing", 130),
    ("refund_to_customer", "Refund to customer", "outgoing", 140),
    ("salary_or_personal", "Salary or personal expense", "outgoing", 150),
    ("owner_withdrawal", "Owner withdrawal", "outgoing", 155),
    ("logistics_expense", "Logistics expense", "outgoing", 160),
    ("platform_fee", "Platform fee", "outgoing", 165),
    ("payment_service_fee", "Payment service fee", "outgoing", 170),
    ("other_expense", "Other expense", "outgoing", 180),
    ("unknown_outgoing", "Unknown outgoing", "outgoing", 190),
]


@dataclass(frozen=True)
class AccountSeed:
    account_number: str
    label: str
    card_mask: str | None = None
    account_title: str | None = None


PETRENKO_TAX_ID = "3254110820"
PETRENKO_SHORT_NAME = "ФОП Петренко Ірина Анатоліївна"

PETRENKO_ACCOUNTS = [
    AccountSeed("UA793220010000026000370005752", "Mono main"),
    AccountSeed("UA663220010000026202324004240", "Card 9227", "444111******9227"),
    AccountSeed("UA913220010000026208323546134", "Card 9661", "444111******9661"),
    AccountSeed("UA839358710000067321000080261", "FOP main"),
]

INTERNAL_TRANSFER_ACCOUNT_PAIRS = [
    ("UA839358710000067321000080261", "UA793220010000026000370005752"),
    ("UA793220010000026000370005752", "UA839358710000067321000080261"),
    ("UA839358710000067321000080261", "UA663220010000026202324004240"),
    ("UA663220010000026202324004240", "UA839358710000067321000080261"),
    ("UA839358710000067321000080261", "UA913220010000026208323546134"),
    ("UA913220010000026208323546134", "UA839358710000067321000080261"),
    ("UA793220010000026000370005752", "UA663220010000026202324004240"),
    ("UA663220010000026202324004240", "UA793220010000026000370005752"),
    ("UA793220010000026000370005752", "UA913220010000026208323546134"),
    ("UA913220010000026208323546134", "UA793220010000026000370005752"),
]


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    return " ".join(value.strip().casefold().split())


async def _ensure_categories(session) -> int:
    changed = 0
    for code, name, direction, sort_order in PAYMENT_CATEGORIES:
        existing = await session.scalar(select(PaymentCategory).where(PaymentCategory.code == code))
        if existing is None:
            session.add(
                PaymentCategory(
                    code=code,
                    name=name,
                    direction=direction,
                    sort_order=sort_order,
                    is_system=True,
                    is_active=True,
                )
            )
            changed += 1
            continue
        updates = {
            "name": name,
            "direction": direction,
            "sort_order": sort_order,
            "is_system": True,
            "is_active": True,
        }
        for field, value in updates.items():
            if getattr(existing, field) != value:
                setattr(existing, field, value)
                changed += 1
    return changed


async def _ensure_petrenko_entity(session) -> PaymentBusinessEntity:
    entity = await session.scalar(select(PaymentBusinessEntity).where(PaymentBusinessEntity.tax_id == PETRENKO_TAX_ID))
    if entity is None:
        entity = PaymentBusinessEntity(
            short_name=PETRENKO_SHORT_NAME,
            full_name=PETRENKO_SHORT_NAME,
            normalized_name=_normalize(PETRENKO_SHORT_NAME),
            tax_id=PETRENKO_TAX_ID,
            entity_type="fop",
            verification_status="needs_review",
            country="Україна",
            is_active=True,
            notes="Seeded from payment reporting transfer document.",
        )
        session.add(entity)
        await session.flush()
        return entity

    defaults = {
        "short_name": entity.short_name or PETRENKO_SHORT_NAME,
        "full_name": entity.full_name or PETRENKO_SHORT_NAME,
        "normalized_name": entity.normalized_name or _normalize(entity.short_name or PETRENKO_SHORT_NAME),
        "entity_type": entity.entity_type or "fop",
        "verification_status": entity.verification_status or "needs_review",
        "country": entity.country or "Україна",
        "is_active": True,
    }
    for field, value in defaults.items():
        setattr(entity, field, value)
    await session.flush()
    return entity


async def _ensure_accounts(session, entity: PaymentBusinessEntity) -> dict[str, PaymentBusinessAccount]:
    accounts: dict[str, PaymentBusinessAccount] = {}
    for seed in PETRENKO_ACCOUNTS:
        account = await session.scalar(
            select(PaymentBusinessAccount).where(PaymentBusinessAccount.account_number == seed.account_number)
        )
        if account is None:
            account = PaymentBusinessAccount(
                business_entity_id=entity.id,
                account_number=seed.account_number,
                account_title=seed.account_title,
                label=seed.label,
                card_mask=seed.card_mask,
                currency="UAH",
                is_active=True,
            )
            session.add(account)
            await session.flush()
        else:
            account.business_entity_id = entity.id
            account.label = account.label or seed.label
            account.account_title = account.account_title or seed.account_title
            account.card_mask = account.card_mask or seed.card_mask
            account.currency = account.currency or "UAH"
            account.is_active = True
        accounts[seed.account_number] = account
    return accounts


async def _ensure_internal_transfer_rules(
    session,
    entity: PaymentBusinessEntity,
    accounts: dict[str, PaymentBusinessAccount],
) -> int:
    changed = 0
    for from_account_number, to_account_number in INTERNAL_TRANSFER_ACCOUNT_PAIRS:
        from_account = accounts.get(from_account_number)
        to_account = accounts.get(to_account_number)
        if from_account is None or to_account is None:
            continue
        existing = await session.scalar(
            select(InternalTransferRule).where(
                InternalTransferRule.from_account_id == from_account.id,
                InternalTransferRule.to_account_id == to_account.id,
            )
        )
        if existing is None:
            session.add(
                InternalTransferRule(
                    business_entity_id=entity.id,
                    from_account_id=from_account.id,
                    to_account_id=to_account.id,
                    is_active=True,
                    pairing_window_minutes=5,
                    require_exact_amount=True,
                    allow_direct_self_marker=True,
                    notes="Seeded Petrenko own-account transfer rule.",
                )
            )
            changed += 1
            continue
        existing.business_entity_id = entity.id
        existing.is_active = True
        existing.pairing_window_minutes = 5
        existing.require_exact_amount = True
        existing.allow_direct_self_marker = True
    return changed


async def seed_payment_reporting() -> dict[str, int]:
    async with AsyncSessionLocal() as session:
        category_changes = await _ensure_categories(session)
        entity = await _ensure_petrenko_entity(session)
        accounts = await _ensure_accounts(session, entity)
        transfer_rule_changes = await _ensure_internal_transfer_rules(session, entity, accounts)
        await session.commit()
        return {
            "category_changes": category_changes,
            "business_entity_id": int(entity.id),
            "accounts": len(accounts),
            "transfer_rule_changes": transfer_rule_changes,
        }


async def _amain() -> None:
    result = await seed_payment_reporting()
    print(
        "payment reporting seed complete: "
        f"business_entity_id={result['business_entity_id']} "
        f"accounts={result['accounts']} "
        f"category_changes={result['category_changes']} "
        f"transfer_rule_changes={result['transfer_rule_changes']}"
    )


if __name__ == "__main__":
    asyncio.run(_amain())

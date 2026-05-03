from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from sqlalchemy import func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    PaymentBusinessAccount,
    PaymentBusinessEntity,
    PaymentImportRun,
    SalesDrivePayment,
)
from app.services.payment_reporting.salesdrive_payment_client import SalesDrivePaymentClient


ImportPaymentType = Literal["incoming", "outcoming", "all"]
SinglePaymentType = Literal["incoming", "outcoming"]


@dataclass(frozen=True)
class PaymentImportResult:
    import_run_id: int
    status: str
    incoming_count: int
    outcoming_count: int
    created_count: int
    updated_count: int
    error_message: str | None = None


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).strip().split())
    return text.casefold() if text else None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _nested(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key)
    return value if isinstance(value, dict) else {}


def _parse_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0")).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid SalesDrive payment amount: {value!r}") from exc


def _parse_payment_date(value: Any) -> datetime:
    text = _as_str(value)
    if not text:
        raise ValueError("SalesDrive payment date is empty")
    candidates = [text, text.replace("Z", "+00:00")]
    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError(f"Invalid SalesDrive payment date: {text!r}")


def _build_search_text(*values: Any) -> str | None:
    parts = [_normalize_text(value) for value in values]
    joined = " ".join(part for part in parts if part)
    return joined or None


def _normalize_payment_row(
    raw: dict[str, Any],
    *,
    payment_type: SinglePaymentType,
    import_run_id: int,
    business_entity_id: int | None,
    business_account_id: int | None,
) -> dict[str, Any]:
    counterparty = _nested(raw, "counterparty")
    organization = _nested(raw, "organization")
    organization_account = _nested(raw, "organizationAccount")

    source_payment_id = _as_str(raw.get("id"))
    if not source_payment_id:
        raise ValueError("SalesDrive payment id is empty")

    counterparty_name = _as_str(counterparty.get("title"))
    counterparty_tax_id = _as_str(counterparty.get("egrpou"))
    organization_name = _as_str(organization.get("title"))
    organization_tax_id = _as_str(organization.get("egrpou"))
    comment = _as_str(raw.get("comment"))
    purpose = _as_str(raw.get("purpose"))

    return {
        "source_system": "salesdrive",
        "source_payment_id": source_payment_id,
        "payment_type": payment_type,
        "payment_date": _parse_payment_date(raw.get("date")),
        "amount": _parse_decimal(raw.get("sum")),
        "currency": "UAH",
        "counterparty_source_id": _as_str(counterparty.get("id")),
        "counterparty_name": counterparty_name,
        "counterparty_normalized_name": _normalize_text(counterparty_name),
        "counterparty_tax_id": counterparty_tax_id,
        "organization_source_id": _as_str(organization.get("id")),
        "organization_name": organization_name,
        "organization_tax_id": organization_tax_id,
        "organization_account_source_id": _as_str(organization_account.get("id")),
        "account_reference": _as_str(organization_account.get("accountNumber")),
        "business_entity_id": business_entity_id,
        "business_account_id": business_account_id,
        "comment": comment,
        "purpose": purpose,
        "search_text": _build_search_text(
            counterparty_name,
            counterparty_tax_id,
            organization_name,
            organization_tax_id,
            comment,
            purpose,
        ),
        "mapping_status": "not_applicable" if payment_type == "incoming" else "unmapped",
        "raw_status": _as_str(raw.get("status")),
        "raw_payload": raw,
        "import_run_id": import_run_id,
    }


async def _ensure_business_entity_from_raw(session: AsyncSession, raw: dict[str, Any]) -> PaymentBusinessEntity | None:
    organization = _nested(raw, "organization")
    organization_id = _as_str(organization.get("id"))
    organization_name = _as_str(organization.get("title"))
    organization_tax_id = _as_str(organization.get("egrpou"))
    if not organization_id and not organization_name and not organization_tax_id:
        return None

    entity = None
    if organization_id:
        entity = await session.scalar(
            select(PaymentBusinessEntity).where(PaymentBusinessEntity.salesdrive_organization_id == organization_id)
        )
    if entity is None and organization_tax_id:
        entity = await session.scalar(select(PaymentBusinessEntity).where(PaymentBusinessEntity.tax_id == organization_tax_id))
    if entity is None:
        entity = PaymentBusinessEntity(
            salesdrive_organization_id=organization_id,
            short_name=organization_name or f"SalesDrive organization {organization_id or organization_tax_id}",
            full_name=organization_name,
            normalized_name=_normalize_text(organization_name),
            tax_id=organization_tax_id,
            verification_status="needs_review",
            is_active=True,
        )
        session.add(entity)
        await session.flush()
        return entity

    if organization_id and not entity.salesdrive_organization_id:
        entity.salesdrive_organization_id = organization_id
    if organization_name:
        entity.short_name = entity.short_name or organization_name
        entity.full_name = entity.full_name or organization_name
        entity.normalized_name = entity.normalized_name or _normalize_text(organization_name)
    if organization_tax_id and not entity.tax_id:
        entity.tax_id = organization_tax_id
    return entity


async def _ensure_business_account_from_raw(
    session: AsyncSession,
    raw: dict[str, Any],
    entity: PaymentBusinessEntity | None,
) -> PaymentBusinessAccount | None:
    if entity is None:
        return None
    organization_account = _nested(raw, "organizationAccount")
    account_number = _as_str(organization_account.get("accountNumber"))
    account_id = _as_str(organization_account.get("id"))
    account_title = _as_str(organization_account.get("title"))
    if not account_number and not account_id:
        return None

    account = None
    if account_number:
        account = await session.scalar(
            select(PaymentBusinessAccount).where(PaymentBusinessAccount.account_number == account_number)
        )
    if account is None and account_id:
        account = await session.scalar(
            select(PaymentBusinessAccount).where(PaymentBusinessAccount.salesdrive_account_id == account_id)
        )
    if account is None:
        account = PaymentBusinessAccount(
            business_entity_id=entity.id,
            salesdrive_account_id=account_id,
            account_number=account_number or f"salesdrive-account-{account_id}",
            account_title=account_title,
            label=account_title,
            currency="UAH",
            is_active=True,
        )
        session.add(account)
        await session.flush()
        return account

    account.business_entity_id = entity.id
    if account_id and not account.salesdrive_account_id:
        account.salesdrive_account_id = account_id
    if account_title:
        account.account_title = account.account_title or account_title
        account.label = account.label or account_title
    return account


async def _upsert_payments(session: AsyncSession, rows: list[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    keys = [(row["source_system"], row["source_payment_id"], row["payment_type"]) for row in rows]
    existing_rows = await session.execute(
        select(SalesDrivePayment.source_system, SalesDrivePayment.source_payment_id, SalesDrivePayment.payment_type).where(
            tuple_(
                SalesDrivePayment.source_system,
                SalesDrivePayment.source_payment_id,
                SalesDrivePayment.payment_type,
            ).in_(keys)
        )
    )
    existing = set(existing_rows.all())
    created = len([key for key in keys if key not in existing])
    updated = len(rows) - created

    stmt = insert(SalesDrivePayment).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["source_system", "source_payment_id", "payment_type"],
        set_={
            "payment_date": stmt.excluded.payment_date,
            "amount": stmt.excluded.amount,
            "currency": stmt.excluded.currency,
            "counterparty_source_id": stmt.excluded.counterparty_source_id,
            "counterparty_name": stmt.excluded.counterparty_name,
            "counterparty_normalized_name": stmt.excluded.counterparty_normalized_name,
            "counterparty_tax_id": stmt.excluded.counterparty_tax_id,
            "organization_source_id": stmt.excluded.organization_source_id,
            "organization_name": stmt.excluded.organization_name,
            "organization_tax_id": stmt.excluded.organization_tax_id,
            "organization_account_source_id": stmt.excluded.organization_account_source_id,
            "account_reference": stmt.excluded.account_reference,
            "business_entity_id": stmt.excluded.business_entity_id,
            "business_account_id": stmt.excluded.business_account_id,
            "comment": stmt.excluded.comment,
            "purpose": stmt.excluded.purpose,
            "search_text": stmt.excluded.search_text,
            "raw_status": stmt.excluded.raw_status,
            "raw_payload": stmt.excluded.raw_payload,
            "import_run_id": stmt.excluded.import_run_id,
            "updated_at": func.now(),
        },
    )
    await session.execute(stmt)
    return created, updated


async def _normalize_and_upsert_type(
    session: AsyncSession,
    *,
    raw_rows: list[dict[str, Any]],
    payment_type: SinglePaymentType,
    import_run_id: int,
) -> tuple[int, int]:
    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        entity = await _ensure_business_entity_from_raw(session, raw)
        account = await _ensure_business_account_from_raw(session, raw, entity)
        rows.append(
            _normalize_payment_row(
                raw,
                payment_type=payment_type,
                import_run_id=import_run_id,
                business_entity_id=int(entity.id) if entity is not None else None,
                business_account_id=int(account.id) if account is not None else None,
            )
        )
    return await _upsert_payments(session, rows)


def _types_for_import(payment_type: ImportPaymentType) -> list[SinglePaymentType]:
    if payment_type == "all":
        return ["incoming", "outcoming"]
    if payment_type in {"incoming", "outcoming"}:
        return [payment_type]
    raise ValueError("payment_type must be incoming, outcoming, or all")


async def import_salesdrive_payments(
    session: AsyncSession,
    *,
    period_from: datetime,
    period_to: datetime,
    payment_type: ImportPaymentType = "all",
    client: SalesDrivePaymentClient | None = None,
) -> PaymentImportResult:
    client = client or SalesDrivePaymentClient()
    import_run = PaymentImportRun(
        source_system="salesdrive",
        period_from=period_from,
        period_to=period_to,
        payment_type=payment_type,
        status="running",
        request_params={
            "payment_type": payment_type,
            "period_from": period_from.isoformat(),
            "period_to": period_to.isoformat(),
        },
    )
    session.add(import_run)
    await session.flush()

    incoming_count = 0
    outcoming_count = 0
    created_count = 0
    updated_count = 0

    try:
        for single_type in _types_for_import(payment_type):
            raw_rows = await client.fetch_payments(
                payment_type=single_type,
                period_from=period_from,
                period_to=period_to,
            )
            created, updated = await _normalize_and_upsert_type(
                session,
                raw_rows=raw_rows,
                payment_type=single_type,
                import_run_id=int(import_run.id),
            )
            if single_type == "incoming":
                incoming_count = len(raw_rows)
            else:
                outcoming_count = len(raw_rows)
            created_count += created
            updated_count += updated

        import_run.status = "success"
        import_run.finished_at = datetime.now(timezone.utc)
        import_run.incoming_count = incoming_count
        import_run.outcoming_count = outcoming_count
        import_run.created_count = created_count
        import_run.updated_count = updated_count
        await session.flush()
        return PaymentImportResult(
            import_run_id=int(import_run.id),
            status="success",
            incoming_count=incoming_count,
            outcoming_count=outcoming_count,
            created_count=created_count,
            updated_count=updated_count,
        )
    except Exception as exc:
        import_run.status = "failed"
        import_run.finished_at = datetime.now(timezone.utc)
        import_run.incoming_count = incoming_count
        import_run.outcoming_count = outcoming_count
        import_run.created_count = created_count
        import_run.updated_count = updated_count
        import_run.error_message = str(exc)
        await session.flush()
        raise

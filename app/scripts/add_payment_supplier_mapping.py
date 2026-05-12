from __future__ import annotations

import argparse
import asyncio

from sqlalchemy import select

from app.database import AsyncSessionLocal
from app.models import DropshipEnterprise, PaymentCounterpartySupplierMapping


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    text = " ".join(value.strip().casefold().split())
    return text or None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add or update a payment counterparty to supplier mapping rule.")
    parser.add_argument("--supplier-code", required=True, help="Existing dropship_enterprises.code, e.g. D1.")
    parser.add_argument(
        "--match-type",
        required=True,
        choices=["tax_id", "exact", "contains", "search_text_contains"],
    )
    parser.add_argument(
        "--field-scope",
        choices=["tax_id", "counterparty_name", "purpose", "comment", "search_text"],
        default=None,
        help="Defaults to tax_id for --match-type tax_id, otherwise counterparty_name/search_text.",
    )
    parser.add_argument("--pattern", default=None, help="Counterparty/name/text pattern for exact/contains rules.")
    parser.add_argument("--tax-id", default=None, help="Counterparty tax id for tax_id rules.")
    parser.add_argument("--priority", type=int, default=100)
    parser.add_argument("--notes", default=None)
    parser.add_argument("--created-by", default="cli")
    return parser.parse_args()


def _resolve_field_scope(match_type: str, field_scope: str | None) -> str:
    if field_scope:
        return field_scope
    if match_type == "tax_id":
        return "tax_id"
    if match_type == "search_text_contains":
        return "search_text"
    return "counterparty_name"


async def _amain() -> None:
    args = _parse_args()
    supplier_code = str(args.supplier_code or "").strip()
    match_type = str(args.match_type or "").strip()
    field_scope = _resolve_field_scope(match_type, args.field_scope)
    pattern = str(args.pattern or "").strip() or None
    tax_id = str(args.tax_id or "").strip() or None

    if match_type == "tax_id" and not tax_id:
        raise ValueError("--tax-id is required for tax_id mapping")
    if match_type != "tax_id" and not pattern:
        raise ValueError("--pattern is required for non-tax_id mapping")

    async with AsyncSessionLocal() as session:
        supplier = await session.scalar(select(DropshipEnterprise).where(DropshipEnterprise.code == supplier_code))
        if supplier is None:
            raise ValueError(f"supplier_code does not exist in dropship_enterprises: {supplier_code}")

        existing_query = select(PaymentCounterpartySupplierMapping).where(
            PaymentCounterpartySupplierMapping.supplier_code == supplier_code,
            PaymentCounterpartySupplierMapping.match_type == match_type,
            PaymentCounterpartySupplierMapping.field_scope == field_scope,
        )
        if match_type == "tax_id":
            existing_query = existing_query.where(PaymentCounterpartySupplierMapping.counterparty_tax_id == tax_id)
        else:
            existing_query = existing_query.where(PaymentCounterpartySupplierMapping.normalized_pattern == _normalize(pattern))

        mapping = await session.scalar(existing_query)
        action = "updated"
        if mapping is None:
            mapping = PaymentCounterpartySupplierMapping(
                supplier_code=supplier_code,
                match_type=match_type,
                field_scope=field_scope,
                counterparty_tax_id=tax_id,
                counterparty_pattern=pattern,
                normalized_pattern=_normalize(pattern),
                created_by=args.created_by,
            )
            session.add(mapping)
            action = "created"

        mapping.supplier_salesdrive_id = supplier.salesdrive_supplier_id
        mapping.priority = int(args.priority)
        mapping.is_active = True
        mapping.notes = args.notes
        mapping.updated_by = args.created_by
        await session.commit()
        await session.refresh(mapping)

    print(
        "payment supplier mapping "
        f"{action}: id={mapping.id} supplier_code={mapping.supplier_code} "
        f"match_type={mapping.match_type} field_scope={mapping.field_scope} "
        f"tax_id={mapping.counterparty_tax_id or ''} pattern={mapping.counterparty_pattern or ''}"
    )


if __name__ == "__main__":
    asyncio.run(_amain())

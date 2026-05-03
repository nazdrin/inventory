from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, time

from app.database import AsyncSessionLocal
from app.services.payment_reporting.payment_recalculation_service import recalculate_payment_period


def _parse_date_or_datetime(value: str, *, end_of_day: bool = False) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise argparse.ArgumentTypeError("date is required")
    try:
        date_value = datetime.strptime(text, "%Y-%m-%d").date()
        return datetime.combine(date_value, time.max if end_of_day else time.min).replace(microsecond=0)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date/datetime: {value}") from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recalculate payment reporting categories and mappings for a period.")
    parser.add_argument("--from", dest="period_from", required=True, help="Period start, YYYY-MM-DD or ISO datetime.")
    parser.add_argument("--to", dest="period_to", required=True, help="Period end, YYYY-MM-DD or ISO datetime.")
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    period_from = _parse_date_or_datetime(args.period_from)
    period_to = _parse_date_or_datetime(args.period_to, end_of_day=True)
    if period_to < period_from:
        raise ValueError("--to must be greater than or equal to --from")

    async with AsyncSessionLocal() as session:
        result = await recalculate_payment_period(
            session,
            period_from=period_from,
            period_to=period_to,
        )
        await session.commit()

    print(
        "payment period recalculation complete: "
        f"total={result.total_payments} "
        f"internal_pairs={result.internal_pairs} "
        f"internal_payments={result.internal_payments} "
        f"customer_receipts={result.customer_receipts} "
        f"excluded_receipts={result.excluded_receipts} "
        f"unknown_incoming={result.unknown_incoming} "
        f"supplier_mapped={result.supplier_mapped} "
        f"supplier_unmapped={result.supplier_unmapped} "
        f"unknown_outgoing={result.unknown_outgoing}"
    )


if __name__ == "__main__":
    asyncio.run(_amain())

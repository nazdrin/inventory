from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, time

from app.database import AsyncSessionLocal
from app.services.payment_reporting.payment_import_service import import_salesdrive_payments


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
    parser = argparse.ArgumentParser(description="Import SalesDrive payments into payment reporting tables.")
    parser.add_argument("--type", choices=["incoming", "outcoming", "all"], default="all")
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
        try:
            result = await import_salesdrive_payments(
                session,
                period_from=period_from,
                period_to=period_to,
                payment_type=args.type,
            )
            await session.commit()
        except Exception:
            try:
                await session.commit()
            except Exception:
                await session.rollback()
            raise

    print(
        "salesdrive payments import complete: "
        f"run_id={result.import_run_id} "
        f"status={result.status} "
        f"incoming={result.incoming_count} "
        f"outcoming={result.outcoming_count} "
        f"created={result.created_count} "
        f"updated={result.updated_count}"
    )


if __name__ == "__main__":
    asyncio.run(_amain())

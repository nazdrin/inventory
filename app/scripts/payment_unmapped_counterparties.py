from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, time

from sqlalchemy import func, select

from app.database import AsyncSessionLocal
from app.models import SalesDrivePayment


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
    parser = argparse.ArgumentParser(description="Show unmapped outgoing payment counterparties for a period.")
    parser.add_argument("--from", dest="period_from", required=True, help="Period start, YYYY-MM-DD or ISO datetime.")
    parser.add_argument("--to", dest="period_to", required=True, help="Period end, YYYY-MM-DD or ISO datetime.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--examples", type=int, default=2)
    return parser.parse_args()


def _short(value: str | None, max_len: int = 180) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


async def _amain() -> None:
    args = _parse_args()
    period_from = _parse_date_or_datetime(args.period_from)
    period_to = _parse_date_or_datetime(args.period_to, end_of_day=True)
    if period_to < period_from:
        raise ValueError("--to must be greater than or equal to --from")

    async with AsyncSessionLocal() as session:
        grouped_rows = await session.execute(
            select(
                SalesDrivePayment.counterparty_name,
                SalesDrivePayment.counterparty_tax_id,
                func.count(SalesDrivePayment.id).label("payment_count"),
                func.sum(SalesDrivePayment.amount).label("amount_sum"),
            )
            .where(
                SalesDrivePayment.payment_type == "outcoming",
                SalesDrivePayment.mapping_status == "unmapped",
                SalesDrivePayment.payment_date >= period_from,
                SalesDrivePayment.payment_date <= period_to,
            )
            .group_by(SalesDrivePayment.counterparty_name, SalesDrivePayment.counterparty_tax_id)
            .order_by(func.sum(SalesDrivePayment.amount).desc())
            .limit(max(1, int(args.limit)))
        )
        groups = grouped_rows.all()

        for idx, row in enumerate(groups, start=1):
            print(
                f"{idx}. counterparty={row.counterparty_name or '<empty>'} "
                f"tax_id={row.counterparty_tax_id or '<empty>'} "
                f"count={row.payment_count} amount={row.amount_sum}"
            )
            examples = await session.execute(
                select(SalesDrivePayment.id, SalesDrivePayment.amount, SalesDrivePayment.purpose)
                .where(
                    SalesDrivePayment.payment_type == "outcoming",
                    SalesDrivePayment.mapping_status == "unmapped",
                    SalesDrivePayment.payment_date >= period_from,
                    SalesDrivePayment.payment_date <= period_to,
                    SalesDrivePayment.counterparty_name.is_(None)
                    if row.counterparty_name is None
                    else SalesDrivePayment.counterparty_name == row.counterparty_name,
                    SalesDrivePayment.counterparty_tax_id.is_(None)
                    if row.counterparty_tax_id is None
                    else SalesDrivePayment.counterparty_tax_id == row.counterparty_tax_id,
                )
                .order_by(SalesDrivePayment.amount.desc())
                .limit(max(0, int(args.examples)))
            )
            for payment_id, amount, purpose in examples.all():
                print(f"   - payment_id={payment_id} amount={amount} purpose={_short(purpose)}")


if __name__ == "__main__":
    asyncio.run(_amain())

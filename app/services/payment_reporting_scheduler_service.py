from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.database import AsyncSessionLocal
from app.services.notification_service import send_notification
from app.services.payment_reporting.payment_import_service import import_salesdrive_payments
from app.services.payment_reporting.payment_recalculation_service import recalculate_payment_period


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KIEV_TZ = ZoneInfo("Europe/Kiev")


def _is_enabled() -> bool:
    return str(os.getenv("PAYMENT_REPORTING_SCHEDULER_ENABLED", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    return int(raw)


def _current_month_window(now: datetime) -> tuple[datetime, datetime]:
    period_from = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    period_to = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return period_from.replace(tzinfo=None), period_to.replace(tzinfo=None)


def _seconds_until_next_run(now: datetime) -> float:
    hour = _int_env("PAYMENT_REPORTING_DAILY_IMPORT_HOUR", 2)
    minute = _int_env("PAYMENT_REPORTING_DAILY_IMPORT_MINUTE", 0)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


async def run_payment_reporting_daily_job() -> dict[str, int | str]:
    now = datetime.now(KIEV_TZ)
    period_from, period_to = _current_month_window(now)
    logger.info("Payment reporting scheduler: import/recalculate period=%s..%s", period_from, period_to)

    async with AsyncSessionLocal() as session:
        import_result = await import_salesdrive_payments(
            session,
            period_from=period_from,
            period_to=period_to,
            payment_type="all",
        )
        recalc_result = await recalculate_payment_period(session, period_from=period_from, period_to=period_to)
        await session.commit()

    return {
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
        "import_run_id": import_result.import_run_id,
        "incoming_count": import_result.incoming_count,
        "outcoming_count": import_result.outcoming_count,
        "created_count": import_result.created_count,
        "updated_count": import_result.updated_count,
        "supplier_unmapped": recalc_result.supplier_unmapped,
        "unknown_incoming": recalc_result.unknown_incoming,
    }


async def schedule_payment_reporting_tasks() -> None:
    if not _is_enabled():
        logger.warning("Payment reporting scheduler disabled. Set PAYMENT_REPORTING_SCHEDULER_ENABLED=true to run.")
        return

    while True:
        now = datetime.now(KIEV_TZ)
        sleep_seconds = _seconds_until_next_run(now)
        logger.info("Payment reporting scheduler: next run in %.0f seconds", sleep_seconds)
        await asyncio.sleep(sleep_seconds)
        try:
            result = await run_payment_reporting_daily_job()
            logger.info("Payment reporting scheduler success: %s", result)
            if int(result.get("supplier_unmapped") or 0) > 0 or int(result.get("unknown_incoming") or 0) > 0:
                send_notification(
                    (
                        "Payment reporting daily import finished with data quality warnings: "
                        f"unmapped={result.get('supplier_unmapped')} unknown_incoming={result.get('unknown_incoming')}"
                    ),
                    "payment_reporting_scheduler",
                )
        except Exception as exc:
            logger.exception("Payment reporting scheduler failed")
            send_notification(f"Payment reporting scheduler failed: {exc}", "payment_reporting_scheduler")


if __name__ == "__main__":
    asyncio.run(schedule_payment_reporting_tasks())

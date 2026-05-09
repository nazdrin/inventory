from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from app.database import get_async_db
from app.integrations.checkbox.client import CheckboxClient
from app.integrations.checkbox.config import load_checkbox_settings
from app.integrations.checkbox.shift_service import close_current_shift


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("checkbox_shift_scheduler_service")

POLL_INTERVAL_SEC = max(10, int(os.getenv("CHECKBOX_SHIFT_SCHEDULER_POLL_INTERVAL_SEC", "60")))


def _is_close_time(now: datetime, close_time: str) -> bool:
    try:
        hour_raw, minute_raw = close_time.split(":", 1)
        hour = int(hour_raw)
        minute = int(minute_raw)
    except (ValueError, AttributeError):
        hour, minute = 23, 50
    return now.hour == hour and now.minute == minute


async def run_once(*, force_close: bool = False) -> dict:
    settings = load_checkbox_settings()
    stats = {"enabled_enterprises": len(settings.enabled_enterprises), "closed": 0, "skipped": 0, "failed": 0}
    if not settings.enabled_enterprises:
        return stats

    tz = ZoneInfo(os.getenv("CHECKBOX_SHIFT_TIMEZONE", "Europe/Kiev"))
    close_time = os.getenv("CHECKBOX_SHIFT_CLOSE_TIME", "23:50")
    now = datetime.now(tz)
    if not (force_close or _is_close_time(now, close_time)):
        stats["skipped"] = len(settings.enabled_enterprises)
        return stats

    client = CheckboxClient(settings)
    token = await client.signin()
    async with get_async_db() as session:
        for enterprise_code in sorted(settings.enabled_enterprises):
            try:
                shift = await close_current_shift(
                    session,
                    client=client,
                    settings=settings,
                    token=token,
                    enterprise_code=enterprise_code,
                    cash_register_code=settings.default_cash_register_code,
                )
                if shift:
                    stats["closed"] += 1
                else:
                    stats["skipped"] += 1
            except Exception:
                logger.exception("Checkbox shift close failed: enterprise_code=%s", enterprise_code)
                stats["failed"] += 1
    return stats


async def run_forever() -> None:
    logger.info("Checkbox shift scheduler started: poll=%ss", POLL_INTERVAL_SEC)
    last_close_key = None
    while True:
        try:
            tz = ZoneInfo(os.getenv("CHECKBOX_SHIFT_TIMEZONE", "Europe/Kiev"))
            now = datetime.now(tz)
            close_key = now.strftime("%Y-%m-%d %H:%M")
            should_force_skip = last_close_key == close_key
            result = await run_once(force_close=False if not should_force_skip else False)
            if result.get("closed"):
                last_close_key = close_key
                logger.info("Checkbox shift scheduler result: %s", result)
        except Exception:
            logger.exception("Checkbox shift scheduler iteration failed")
        await asyncio.sleep(POLL_INTERVAL_SEC)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Close Checkbox shifts and send shift summaries")
    parser.add_argument("--once", action="store_true", help="run one scheduler check and exit")
    parser.add_argument("--force-close", action="store_true", help="close open shifts immediately")
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    if args.once or args.force_close:
        result = await run_once(force_close=args.force_close)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    await run_forever()


if __name__ == "__main__":
    asyncio.run(_amain())

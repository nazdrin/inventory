import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.business.biotus_check_order import process_biotus_orders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning("Неверное значение %s=%r, используется default=%s", name, raw, default)
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw


def _is_night_hour(hour: int, start_hour: int, end_hour: int) -> bool:
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    if start_hour > end_hour:
        return hour >= start_hour or hour < end_hour
    return False


def _seconds_until_night_end(now_kyiv: datetime, end_hour: int) -> int:
    target = now_kyiv.replace(hour=end_hour, minute=0, second=0, microsecond=0)
    if now_kyiv >= target:
        target = target + timedelta(days=1)
    return max(1, int((target - now_kyiv).total_seconds()))


async def schedule_biotus_check_order() -> None:
    interval_seconds = _env_int("BIOTUS_SCHEDULER_INTERVAL_SECONDS", 60)
    enterprise_code = os.getenv("BIOTUS_ENTERPRISE_CODE", "223")
    verify_ssl = not _env_bool("BIOTUS_NO_SSL_VERIFY", False)
    dry_run = _env_bool("BIOTUS_DRY_RUN", False)
    tz_name = _env_str("BIOTUS_TZ", "Europe/Kyiv")
    night_start_hour = _env_int("BIOTUS_NIGHT_START_HOUR", 22)
    night_end_hour = _env_int("BIOTUS_NIGHT_END_HOUR", 8)
    night_mode = _env_str("BIOTUS_NIGHT_MODE", "skip").strip().lower()
    night_interval_seconds = _env_int("BIOTUS_NIGHT_INTERVAL_SECONDS", 3600)
    if night_mode not in {"skip", "hourly"}:
        logger.warning(
            "Неверное значение BIOTUS_NIGHT_MODE=%r, используется default=skip",
            night_mode,
        )
        night_mode = "skip"

    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        logger.warning("Не удалось загрузить TZ=%r, используется Europe/Kyiv", tz_name)
        tz = ZoneInfo("Europe/Kyiv")

    logger.info(
        "Biotus scheduler started: enterprise=%s interval=%ss dry_run=%s verify_ssl=%s tz=%s night=%s-%s mode=%s",
        enterprise_code,
        interval_seconds,
        dry_run,
        verify_ssl,
        tz.key,
        night_start_hour,
        night_end_hour,
        night_mode,
    )

    while True:
        now_kyiv = datetime.now(tz)
        is_night = _is_night_hour(now_kyiv.hour, night_start_hour, night_end_hour)
        sleep_seconds = interval_seconds
        run_orders = True
        mode = "day"
        if is_night:
            mode = f"night_{night_mode}"
            if night_mode == "skip":
                run_orders = False
                sleep_seconds = _seconds_until_night_end(now_kyiv, night_end_hour)
            else:
                sleep_seconds = night_interval_seconds

        logger.info(
            "Biotus scheduler tick: now_kyiv=%s night=%s mode=%s sleep=%ss",
            now_kyiv.isoformat(),
            is_night,
            mode,
            sleep_seconds,
        )
        try:
            if run_orders:
                result = await process_biotus_orders(
                    enterprise_code=enterprise_code,
                    min_age_minutes=None,
                    verify_ssl=verify_ssl,
                    dry_run=dry_run,
                )
                logger.info("Biotus run result: %s", result)
            else:
                logger.info(
                    "Biotus run skipped due to night mode; sleep until %s:00",
                    night_end_hour,
                )
        except Exception as exc:
            logger.exception("Ошибка в biotus_check_order: %s", exc)

        await asyncio.sleep(sleep_seconds)


if __name__ == "__main__":
    asyncio.run(schedule_biotus_check_order())

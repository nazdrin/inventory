import asyncio
import logging
import os

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


async def schedule_biotus_check_order() -> None:
    interval_seconds = _env_int("BIOTUS_SCHEDULER_INTERVAL_SECONDS", 60)
    enterprise_code = os.getenv("BIOTUS_ENTERPRISE_CODE", "223")
    verify_ssl = not _env_bool("BIOTUS_NO_SSL_VERIFY", False)
    dry_run = _env_bool("BIOTUS_DRY_RUN", False)

    logger.info(
        "Biotus scheduler started: enterprise=%s interval=%ss dry_run=%s verify_ssl=%s",
        enterprise_code,
        interval_seconds,
        dry_run,
        verify_ssl,
    )

    while True:
        try:
            result = await process_biotus_orders(
                enterprise_code=enterprise_code,
                min_age_minutes=None,
                verify_ssl=verify_ssl,
                dry_run=dry_run,
            )
            logger.info("Biotus run result: %s", result)
        except Exception as exc:
            logger.exception("Ошибка в biotus_check_order: %s", exc)

        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(schedule_biotus_check_order())

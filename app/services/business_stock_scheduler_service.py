import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytz
from sqlalchemy.future import select

from app.business.dropship_pipeline import run_pipeline
from app.database import EnterpriseSettings, get_async_db
from app.services.notification_service import send_notification

os_tz = "UTC"
KIEV_TZ = pytz.timezone("Europe/Kiev")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("business_stock_scheduler")


async def notify_error(message: str, enterprise_code: str = "Business"):
    logger.error(message)
    send_notification(message, enterprise_code)


async def _load_business_enterprises():
    async with get_async_db(commit_on_exit=False) as db:
        db.expire_all()
        result = await db.execute(select(EnterpriseSettings).order_by(EnterpriseSettings.enterprise_name))
        enterprises = list(result.scalars().all())
    return [
        enterprise
        for enterprise in enterprises
        if str(enterprise.data_format or "").strip().lower() == "business"
    ]


def _resolve_business_enterprise(candidates: list[EnterpriseSettings]) -> tuple[str, EnterpriseSettings | None]:
    if not candidates:
        return "none", None
    if len(candidates) > 1:
        return "ambiguous", None
    return "resolved", candidates[0]


def _is_stock_due(enterprise: EnterpriseSettings) -> bool:
    frequency = enterprise.stock_upload_frequency
    if frequency is None or frequency <= 0:
        return False

    now = datetime.now(tz=timezone.utc).astimezone(KIEV_TZ)
    if enterprise.last_stock_upload is None:
        return True

    return enterprise.last_stock_upload.astimezone(KIEV_TZ) + timedelta(minutes=frequency) <= now


async def run_business_stock_once() -> bool:
    logger.info("Business stock: resolving target enterprise")
    candidates = await _load_business_enterprises()
    resolution, enterprise = _resolve_business_enterprise(candidates)

    if resolution == "none":
        logger.warning("Business stock: no enterprise found with data_format=Business; skipping run")
        return False

    if resolution == "ambiguous":
        codes = ", ".join(str(item.enterprise_code) for item in candidates)
        logger.warning(
            "Business stock: multiple enterprises found with data_format=Business (%s); skipping run",
            codes,
        )
        return False

    assert enterprise is not None
    enterprise_code = str(enterprise.enterprise_code)
    logger.info(
        "Business stock: resolved enterprise_code=%s stock_enabled=%s stock_upload_frequency=%s",
        enterprise_code,
        bool(enterprise.stock_enabled),
        enterprise.stock_upload_frequency,
    )

    if enterprise.stock_enabled is False:
        logger.info(
            "Business stock: skip enterprise_code=%s because stock_enabled=false",
            enterprise_code,
        )
        return False

    if not _is_stock_due(enterprise):
        logger.info(
            "Business stock: skip enterprise_code=%s because stock run is not due yet",
            enterprise_code,
        )
        return False

    try:
        logger.info("Business stock: start enterprise_code=%s", enterprise_code)
        await run_pipeline(enterprise_code, "stock")
        logger.info("Business stock: success enterprise_code=%s", enterprise_code)
        return True
    except Exception as exc:
        logger.exception("Business stock: failure enterprise_code=%s", enterprise_code)
        await notify_error(
            f"Ошибка Business stock pipeline для предприятия {enterprise_code}: {exc}",
            enterprise_code,
        )
        return False


async def schedule_business_stock_tasks():
    interval_minutes = 1
    try:
        while True:
            await run_business_stock_once()
            await asyncio.sleep(interval_minutes * 60)
    except Exception as main_error:
        logger.exception("Business stock scheduler: unexpected failure")
        await notify_error(
            f"Критическая ошибка в Business stock scheduler: {main_error}",
            "business_stock_scheduler",
        )
    finally:
        await notify_error(
            "Сервис business_stock_scheduler неожиданно остановлен.",
            "business_stock_scheduler",
        )


if __name__ == "__main__":
    asyncio.run(schedule_business_stock_tasks())

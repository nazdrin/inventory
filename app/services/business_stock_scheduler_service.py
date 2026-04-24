import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytz
from sqlalchemy.future import select

from app.database import EnterpriseSettings, get_async_db
from app.models import BusinessSettings
from app.services.notification_service import send_notification
from app.services.business_stock_publish_service import publish_business_stock_for_enterprise
from app.services.business_runtime_mode_service import build_business_runtime_mode_report

os_tz = "UTC"
KIEV_TZ = pytz.timezone("Europe/Kiev")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("business_stock_scheduler")
FALLBACK_INTERVAL_SECONDS = 60


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


async def _load_business_settings_row():
    async with get_async_db(commit_on_exit=False) as db:
        db.expire_all()
        result = await db.execute(
            select(BusinessSettings)
            .order_by(BusinessSettings.id)
            .limit(1)
        )
        return result.scalar_one_or_none()


def _resolve_business_enterprise(candidates: list[EnterpriseSettings]) -> tuple[str, EnterpriseSettings | None]:
    if not candidates:
        return "none", None
    if len(candidates) > 1:
        return "ambiguous", None
    return "resolved", candidates[0]


async def _run_business_stock_for_enterprise(
    enterprise: EnterpriseSettings,
    *,
    business_stock_enabled: bool,
    control_source: str,
) -> bool:
    enterprise_code = str(enterprise.enterprise_code)
    runtime_mode_report = build_business_runtime_mode_report(
        enterprise_code,
        getattr(enterprise, "business_runtime_mode", None),
        source="enterprise_settings",
    )
    logger.info(
        "Business stock: resolved enterprise_code=%s enabled=%s runtime_mode=%s stock_mode=%s source=%s",
        enterprise_code,
        business_stock_enabled,
        runtime_mode_report.get("business_runtime_mode"),
        runtime_mode_report.get("stock_runtime_path"),
        runtime_mode_report.get("runtime_mode_source"),
    )

    if not business_stock_enabled:
        logger.info(
            "Business stock: skip enterprise_code=%s because business_stock_enabled=false",
            enterprise_code,
        )
        return False

    if control_source == "fallback" and not _is_stock_due(enterprise):
        logger.info(
            "Business stock: skip enterprise_code=%s because stock run is not due yet",
            enterprise_code,
        )
        return False

    try:
        logger.info(
            "Business stock: start enterprise_code=%s stock_mode=%s",
            enterprise_code,
            runtime_mode_report.get("stock_runtime_path"),
        )
        async with get_async_db(commit_on_exit=False) as db:
            result = await publish_business_stock_for_enterprise(
                db,
                enterprise_code=enterprise_code,
                dry_run=False,
                require_confirm=False,
                confirm=True,
            )
        logger.info(
            "Business stock: success enterprise_code=%s stock_mode=%s status=%s",
            enterprise_code,
            result.get("stock_mode"),
            result.get("status"),
        )
        return True
    except Exception as exc:
        logger.exception("Business stock: failure enterprise_code=%s", enterprise_code)
        await notify_error(
            f"Ошибка Business stock pipeline для предприятия {enterprise_code}: {exc}",
            enterprise_code,
        )
        return False


def _is_stock_due(enterprise: EnterpriseSettings) -> bool:
    frequency = enterprise.stock_upload_frequency
    if frequency is None or frequency <= 0:
        return False

    now = datetime.now(tz=timezone.utc).astimezone(KIEV_TZ)
    if enterprise.last_stock_upload is None:
        return True

    return enterprise.last_stock_upload.astimezone(KIEV_TZ) + timedelta(minutes=frequency) <= now


def _fallback_interval_seconds(enterprise: EnterpriseSettings | None) -> int:
    if enterprise is None:
        return FALLBACK_INTERVAL_SECONDS

    frequency = enterprise.stock_upload_frequency
    if frequency is None:
        return FALLBACK_INTERVAL_SECONDS
    try:
        normalized = int(frequency)
    except (TypeError, ValueError):
        return FALLBACK_INTERVAL_SECONDS
    if normalized < 1:
        return FALLBACK_INTERVAL_SECONDS
    return normalized * 60


def _resolve_scheduler_control(
    business_settings_row: BusinessSettings | None,
    enterprise: EnterpriseSettings | None,
) -> tuple[str, bool, int]:
    if business_settings_row is not None:
        interval_seconds = int(business_settings_row.business_stock_interval_seconds)
        return "db", bool(business_settings_row.business_stock_enabled), max(1, interval_seconds)

    fallback_enabled = bool(enterprise.stock_enabled) if enterprise is not None else True
    return "fallback", fallback_enabled, _fallback_interval_seconds(enterprise)


async def run_business_stock_once() -> tuple[bool, int]:
    logger.info("Business stock: resolving target enterprise")
    business_settings_row = await _load_business_settings_row()
    candidates = await _load_business_enterprises()
    resolution, enterprise = _resolve_business_enterprise(candidates)

    control_source, business_stock_enabled, interval_seconds = _resolve_scheduler_control(
        business_settings_row,
        enterprise,
    )
    if control_source == "db":
        logger.info(
            "Business stock: settings row found enabled=%s interval_seconds=%s",
            business_stock_enabled,
            interval_seconds,
        )
    else:
        logger.info(
            "Business stock: fallback used enabled=%s interval_seconds=%s",
            business_stock_enabled,
            interval_seconds,
        )

    if resolution == "none":
        logger.warning("Business stock: no enterprise found with data_format=Business; skipping run")
        return False, interval_seconds

    if resolution == "ambiguous":
        codes = ", ".join(str(item.enterprise_code) for item in candidates)
        logger.info(
            "Business stock: multiple enterprises found with data_format=Business (%s); processing each enterprise with stock mode selector",
            codes,
        )

    target_enterprises = candidates if resolution == "ambiguous" else [enterprise]
    ran_any = False
    for target in target_enterprises:
        if target is None:
            continue
        ran = await _run_business_stock_for_enterprise(
            target,
            business_stock_enabled=business_stock_enabled,
            control_source=control_source,
        )
        ran_any = ran_any or ran

    return ran_any, interval_seconds


async def schedule_business_stock_tasks():
    try:
        while True:
            _ran, interval_seconds = await run_business_stock_once()
            logger.info("Business stock: scheduler sleep interval_seconds=%s", interval_seconds)
            await asyncio.sleep(max(1, interval_seconds))
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

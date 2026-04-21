import asyncio
import logging
import os

from app.database import get_async_db
from app.services.business_offers_refresh_service import run_business_offers_refresh_once
from app.services.business_store_stock_publish_service import (
    OFFERS_FRESHNESS_WARNING,
    publish_enabled_business_store_stocks,
)
from app.services.notification_service import send_notification


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("business_store_stock_scheduler")

DEFAULT_INTERVAL_SECONDS = 300
MIN_INTERVAL_SECONDS = 30


def _env_bool(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    return int((os.getenv(name) or str(default)).strip())


def _scheduler_enabled() -> bool:
    return _env_bool("BUSINESS_STORE_STOCK_SCHEDULER_ENABLED", "0")


def _scheduler_dry_run() -> bool:
    return _env_bool("BUSINESS_STORE_STOCK_SCHEDULER_DRY_RUN", "1")


def _scheduler_interval_seconds() -> int:
    return max(MIN_INTERVAL_SECONDS, _env_int("BUSINESS_STORE_STOCK_SCHEDULER_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS))


def _refresh_before_publish_enabled() -> bool:
    return _env_bool("BUSINESS_STORE_STOCK_REFRESH_OFFERS_BEFORE_PUBLISH", "0")


def _refresh_allow_partial() -> bool:
    return _env_bool("BUSINESS_STORE_STOCK_REFRESH_ALLOW_PARTIAL", "0")


def _refresh_enterprise_code() -> str | None:
    value = (os.getenv("BUSINESS_STORE_STOCK_REFRESH_ENTERPRISE_CODE") or "").strip()
    return value or None


def _log_refresh_summary(report: dict) -> None:
    logger.info(
        (
            "Business offers refresh summary: status=%s enterprise_code=%s total=%s "
            "processed=%s blocked=%s failed=%s offers_rows_after=%s duration_sec=%s"
        ),
        report.get("status"),
        report.get("enterprise_code"),
        report.get("suppliers_total"),
        report.get("suppliers_processed"),
        report.get("suppliers_blocked"),
        report.get("suppliers_failed"),
        report.get("offers_rows_after"),
        report.get("duration_sec"),
    )
    for warning in report.get("warnings", []):
        logger.warning("Business offers refresh warning: %s", warning)
    for error in report.get("errors", []):
        if isinstance(error, dict):
            logger.error(
                "Business offers refresh error: supplier_code=%s message=%s",
                error.get("supplier_code"),
                error.get("message"),
            )
        else:
            logger.error("Business offers refresh error: %s", error)


def _log_publish_summary(report: dict) -> None:
    logger.info(
        (
            "Store-aware stock publish summary: dry_run=%s total=%s eligible=%s "
            "skipped=%s published=%s failed=%s status=%s"
        ),
        report.get("dry_run"),
        report.get("total_stores_found"),
        report.get("eligible_stores"),
        report.get("skipped_stores"),
        report.get("published_stores"),
        report.get("failed_stores"),
        report.get("status"),
    )
    for store in report.get("stores", []):
        logger.info(
            (
                "Store-aware stock store result: store_code=%s branch=%s status=%s "
                "exportable=%s sent=%s skip_reason=%s"
            ),
            store.get("store_code"),
            store.get("tabletki_branch"),
            store.get("status"),
            store.get("exportable_products"),
            store.get("sent_products"),
            store.get("skip_reason"),
        )

    for warning in report.get("warnings", []):
        logger.warning("Store-aware stock publish warning: %s", warning)
    for error in report.get("errors", []):
        logger.error("Store-aware stock publish error: %s", error)


async def run_business_store_stock_publish_once() -> dict:
    interval_seconds = _scheduler_interval_seconds()
    dry_run = _scheduler_dry_run()
    enabled = _scheduler_enabled()
    refresh_before_publish = _refresh_before_publish_enabled()
    refresh_allow_partial = _refresh_allow_partial()
    refresh_enterprise_code = _refresh_enterprise_code()

    if not enabled:
        logger.info(
            "Store-aware stock scheduler disabled: BUSINESS_STORE_STOCK_SCHEDULER_ENABLED=false interval_seconds=%s",
            interval_seconds,
        )
        return {
            "status": "disabled",
            "dry_run": dry_run,
            "interval_seconds": interval_seconds,
            "refresh_before_publish": refresh_before_publish,
            "warnings": [],
            "errors": [],
        }

    logger.info(
        "Store-aware stock scheduler %s started interval_seconds=%s refresh_before_publish=%s refresh_allow_partial=%s refresh_enterprise_code=%s",
        "dry-run" if dry_run else "LIVE publish",
        interval_seconds,
        refresh_before_publish,
        refresh_allow_partial,
        refresh_enterprise_code,
    )
    logger.warning(OFFERS_FRESHNESS_WARNING)
    if not dry_run:
        logger.warning("Store-aware stock scheduler LIVE publish started")

    refresh_report = None
    if refresh_before_publish:
        logger.info("Business store stock scheduler: refreshing offers before publish")
        try:
            refresh_report = await run_business_offers_refresh_once(
                enterprise_code=refresh_enterprise_code,
            )
        except Exception as exc:
            logger.exception("Business store stock scheduler: offers refresh failed before publish")
            return {
                "status": "refresh_error",
                "dry_run": dry_run,
                "interval_seconds": interval_seconds,
                "refresh_before_publish": True,
                "refresh_report": None,
                "warnings": [],
                "errors": [f"Offers refresh failed before publish: {exc}"],
            }

        _log_refresh_summary(refresh_report)
        refresh_status = str(refresh_report.get("status") or "").strip().lower()
        if refresh_status == "error":
            logger.error("Business store stock scheduler: skipping publish because refresh status=error")
            return {
                "status": "refresh_error",
                "dry_run": dry_run,
                "interval_seconds": interval_seconds,
                "refresh_before_publish": True,
                "refresh_report": refresh_report,
                "warnings": list(refresh_report.get("warnings") or []),
                "errors": [f"Offers refresh status=error for enterprise_code={refresh_report.get('enterprise_code')}"],
            }
        if refresh_status == "partial" and not refresh_allow_partial:
            logger.warning(
                "Business store stock scheduler: skipping publish because refresh status=partial and BUSINESS_STORE_STOCK_REFRESH_ALLOW_PARTIAL=false"
            )
            return {
                "status": "refresh_partial_skipped",
                "dry_run": dry_run,
                "interval_seconds": interval_seconds,
                "refresh_before_publish": True,
                "refresh_report": refresh_report,
                "warnings": list(refresh_report.get("warnings") or [])
                + [
                    "Offers refresh returned partial status and publish was skipped because BUSINESS_STORE_STOCK_REFRESH_ALLOW_PARTIAL=false."
                ],
                "errors": [],
            }
        if refresh_status == "partial" and refresh_allow_partial:
            logger.warning(
                "Business store stock scheduler: refresh status=partial but publish is allowed by BUSINESS_STORE_STOCK_REFRESH_ALLOW_PARTIAL=true"
            )

    async with get_async_db(commit_on_exit=False) as session:
        report = await publish_enabled_business_store_stocks(
            session,
            dry_run=dry_run,
            require_confirm=bool(dry_run),
            confirm=not dry_run,
        )

    if refresh_report is not None:
        report["refresh_before_publish"] = True
        report["refresh_report"] = refresh_report
    else:
        report["refresh_before_publish"] = False
    _log_publish_summary(report)
    report["interval_seconds"] = interval_seconds
    return report


async def schedule_business_store_stock_tasks() -> None:
    interval_seconds = _scheduler_interval_seconds()
    if not _scheduler_enabled():
        logger.info(
            "BUSINESS_STORE_STOCK_SCHEDULER_ENABLED is disabled; exiting scheduler interval_seconds=%s",
            interval_seconds,
        )
        return

    logger.info(
        "Store-aware stock scheduler started: interval_seconds=%s dry_run=%s refresh_before_publish=%s",
        interval_seconds,
        _scheduler_dry_run(),
        _refresh_before_publish_enabled(),
    )

    stopped_gracefully = False
    try:
        while True:
            try:
                await run_business_store_stock_publish_once()
            except Exception as exc:
                logger.exception("Store-aware stock scheduler cycle failed")
                send_notification(
                    f"Ошибка store-aware stock scheduler cycle: {exc}",
                    "business_store_stock_scheduler",
                )

            interval_seconds = _scheduler_interval_seconds()
            logger.info("Store-aware stock scheduler sleep interval_seconds=%s", interval_seconds)
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        stopped_gracefully = True
        logger.info("Store-aware stock scheduler stopped gracefully")
        raise
    except Exception as main_error:
        logger.exception("Store-aware stock scheduler crashed")
        send_notification(
            f"Критическая ошибка в store-aware stock scheduler: {main_error}",
            "business_store_stock_scheduler",
        )
    finally:
        if not stopped_gracefully:
            send_notification(
                "Сервис business_store_stock_scheduler неожиданно остановлен.",
                "business_store_stock_scheduler",
            )


if __name__ == "__main__":
    try:
        asyncio.run(schedule_business_store_stock_tasks())
    except KeyboardInterrupt:
        logger.info("Store-aware stock scheduler interrupted by operator")

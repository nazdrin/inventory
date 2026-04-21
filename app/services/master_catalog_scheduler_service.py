import asyncio
import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Optional

import fcntl
from zoneinfo import ZoneInfo

from app.business.master_catalog_orchestrator import run_master_catalog_orchestrator
from app.core.paths import STATE_CACHE_DIR
from app.database import get_async_db
from app.services.business_store_catalog_publish_service import (
    publish_enabled_business_store_catalogs,
)
from app.services.master_business_settings_resolver import (
    MasterBusinessSettingsSnapshot,
    load_master_business_settings_snapshot,
)
from app.services.notification_service import send_notification


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_catalog_scheduler")

KYIV_TZ = ZoneInfo(os.getenv("MASTER_SCHEDULER_TIMEZONE", "Europe/Kiev"))
STATE_DIR = Path(os.getenv("MASTER_SCHEDULER_STATE_DIR") or STATE_CACHE_DIR)
STATE_PATH = STATE_DIR / "master_catalog_scheduler_state.json"
LOCK_PATH = STATE_DIR / "master_catalog_scheduler.lock"
FIRE_WINDOW_SEC = int(os.getenv("MASTER_SCHEDULER_FIRE_WINDOW_SEC", "90"))
POLL_INTERVAL_SEC = int(os.getenv("MASTER_SCHEDULER_POLL_INTERVAL_SEC", "30"))

WEEKDAY_MAP = {
    "MON": 0,
    "TUE": 1,
    "WED": 2,
    "THU": 3,
    "FRI": 4,
    "SAT": 5,
    "SUN": 6,
}


@dataclass
class JobResult:
    name: str
    status: str
    duration_sec: float
    details: Optional[Dict[str, Any]] = None


def _env_bool(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    return int((os.getenv(name) or str(default)).strip())


def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to read scheduler state file: %s", STATE_PATH)
        return {}


def _save_state(state: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = STATE_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(STATE_PATH)


@contextmanager
def _global_lock():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    handle = LOCK_PATH.open("w")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError("master_catalog_scheduler is already running")

    try:
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _slot_for_daily(now_local: datetime) -> str:
    return now_local.strftime("%Y-%m-%d")


def _slot_for_weekly(now_local: datetime) -> str:
    iso_year, iso_week, _ = now_local.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _slot_for_interval(now_local: datetime, every_minutes: int) -> str:
    bucket = (now_local.hour * 60 + now_local.minute) // every_minutes
    return f"{now_local.strftime('%Y-%m-%d')}-{bucket}"


def _is_within_window(now_local: datetime, target_hour: int, target_minute: int) -> bool:
    target = now_local.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    delta_sec = abs((now_local - target).total_seconds())
    return delta_sec <= FIRE_WINDOW_SEC


def _has_errors(result: Dict[str, Any]) -> bool:
    return any(step.get("status") == "error" for step in result.get("steps", []))


def _extract_job_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    steps = result.get("steps", [])
    if steps:
        summary["steps_count"] = len(steps)
        summary["step_messages"] = [step.get("message") for step in steps if step.get("message")]
    return summary


def _store_catalog_scheduler_enabled() -> bool:
    return _env_bool("BUSINESS_STORE_CATALOG_SCHEDULER_ENABLED", "0")


def _store_catalog_scheduler_dry_run() -> bool:
    return _env_bool("BUSINESS_STORE_CATALOG_SCHEDULER_DRY_RUN", "1")


def _summarize_store_publish_report(report: Dict[str, Any]) -> None:
    logger.info(
        (
            "Store-aware catalog publish summary: dry_run=%s total=%s eligible=%s "
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
                "Store-aware catalog store result: store_code=%s branch=%s status=%s "
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
        logger.warning("Store-aware catalog publish warning: %s", warning)
    for error in report.get("errors", []):
        logger.error("Store-aware catalog publish error: %s", error)


async def _run_store_catalog_publish_hook() -> None:
    if not _store_catalog_scheduler_enabled():
        logger.info("Store-aware catalog scheduler hook is disabled")
        return

    dry_run = _store_catalog_scheduler_dry_run()
    logger.info(
        "Store-aware catalog scheduler %s started",
        "dry-run" if dry_run else "live publish",
    )

    try:
        async with get_async_db(commit_on_exit=False) as session:
            report = await publish_enabled_business_store_catalogs(
                session,
                dry_run=dry_run,
                require_confirm=False,
                confirm=not dry_run,
            )
        _summarize_store_publish_report(report)
    except Exception:
        logger.exception("Store-aware catalog scheduler hook failed")


async def _run_orchestrator_job(name: str, **kwargs: Any) -> JobResult:
    logger.info("Master scheduler starting job=%s kwargs=%s", name, kwargs)
    started = perf_counter()
    try:
        result = await run_master_catalog_orchestrator(**kwargs)
        status = "error" if _has_errors(result) else "ok"
        duration_sec = perf_counter() - started
        summary = _extract_job_summary(result)
        logger.info(
            "Master scheduler finished job=%s status=%s duration=%.3fs summary=%s",
            name,
            status,
            duration_sec,
            summary,
        )
        return JobResult(name=name, status=status, duration_sec=duration_sec, details=result)
    except Exception as exc:
        duration_sec = perf_counter() - started
        logger.exception("Master scheduler failed job=%s", name)
        return JobResult(
            name=name,
            status="error",
            duration_sec=duration_sec,
            details={"error": str(exc)},
        )


def _settings_signature(settings: MasterBusinessSettingsSnapshot) -> tuple[Any, ...]:
    return (
        settings.source,
        settings.business_enterprise_code,
        settings.daily_publish_enterprise_code_override,
        settings.weekly_salesdrive_enterprise_code_override,
        settings.master_weekly_enabled,
        settings.master_weekly_day,
        settings.master_weekly_hour,
        settings.master_weekly_minute,
        settings.master_daily_publish_enabled,
        settings.master_daily_publish_hour,
        settings.master_daily_publish_minute,
        settings.master_daily_publish_limit,
        settings.master_archive_enabled,
        settings.master_archive_every_minutes,
        settings.primary_enterprise_exists,
        settings.inconsistency,
    )


_LAST_SETTINGS_SIGNATURE: tuple[Any, ...] | None = None


def _log_master_settings_if_changed(settings: MasterBusinessSettingsSnapshot) -> None:
    global _LAST_SETTINGS_SIGNATURE
    signature = _settings_signature(settings)
    if signature == _LAST_SETTINGS_SIGNATURE:
        return

    _LAST_SETTINGS_SIGNATURE = signature
    if settings.source == "db":
        logger.info(
            "Master DB-first settings row found: primary=%s daily_effective=%s weekly_effective=%s",
            settings.business_enterprise_code,
            settings.effective_daily_publish_enterprise_code,
            settings.effective_weekly_salesdrive_enterprise_code,
        )
    else:
        logger.info(
            "Master settings fallback to env because business_settings row is missing: primary=%s daily_effective=%s weekly_effective=%s",
            settings.business_enterprise_code,
            settings.effective_daily_publish_enterprise_code,
            settings.effective_weekly_salesdrive_enterprise_code,
        )

    if settings.inconsistency:
        logger.warning("Master settings inconsistency: %s", settings.inconsistency)


def _notify_weekly_success(duration_sec: float, settings: MasterBusinessSettingsSnapshot) -> None:
    enterprise_code = settings.resolve_weekly_salesdrive_enterprise()
    send_notification(
        (
            "🟡 Weekly master catalog enrichment успешно завершен\n"
            f"duration_sec={round(duration_sec, 3)}"
        ),
        enterprise_code,
    )


async def _run_weekly_enrichment(settings: MasterBusinessSettingsSnapshot) -> JobResult:
    logger.info(
        "Master weekly job targets: source=%s daily_effective=%s weekly_effective=%s",
        settings.source,
        settings.effective_daily_publish_enterprise_code,
        settings.effective_weekly_salesdrive_enterprise_code,
    )
    enrichment = await _run_orchestrator_job(
        "weekly_enrichment",
        mode="weekly_enrichment",
        fail_fast=True,
        skip_report=False,
    )
    if enrichment.status != "ok":
        return enrichment

    salesdrive = await _run_orchestrator_job(
        "weekly_salesdrive",
        mode="salesdrive",
        fail_fast=True,
        enterprise=settings.resolve_weekly_salesdrive_enterprise(),
        batch_size=_env_int("MASTER_WEEKLY_SALESDRIVE_BATCH_SIZE", 100),
    )
    if salesdrive.status != "ok":
        return JobResult(
            name="weekly_enrichment_with_salesdrive",
            status="error",
            duration_sec=enrichment.duration_sec + salesdrive.duration_sec,
            details={
                "enrichment": enrichment.details,
                "salesdrive": salesdrive.details,
            },
        )

    result = JobResult(
        name="weekly_enrichment_with_salesdrive",
        status="ok",
        duration_sec=enrichment.duration_sec + salesdrive.duration_sec,
        details={
            "enrichment": enrichment.details,
            "salesdrive": salesdrive.details,
        },
    )
    _notify_weekly_success(result.duration_sec, settings)
    return result


async def _run_daily_publish(settings: MasterBusinessSettingsSnapshot) -> JobResult:
    logger.info(
        "Master daily publish target: source=%s effective_daily=%s limit=%s",
        settings.source,
        settings.effective_daily_publish_enterprise_code,
        settings.master_daily_publish_limit,
    )
    return await _run_orchestrator_job(
        "daily_publish",
        mode="publish",
        fail_fast=True,
        enterprise=settings.resolve_publish_enterprise(),
        limit=settings.master_daily_publish_limit,
        send=True,
    )


async def _run_daily_publish_with_store_hook(settings: MasterBusinessSettingsSnapshot) -> JobResult:
    result = await _run_daily_publish(settings)
    if result.status != "ok":
        logger.info(
            "Store-aware catalog scheduler hook skipped because legacy daily publish failed: status=%s",
            result.status,
        )
        return result

    await _run_store_catalog_publish_hook()
    return result


async def _run_hourly_archive() -> JobResult:
    return await _run_orchestrator_job(
        "hourly_archive",
        mode="archive",
        fail_fast=True,
    )


async def _maybe_run_weekly(
    now_local: datetime,
    state: Dict[str, Any],
    settings: MasterBusinessSettingsSnapshot,
) -> Optional[JobResult]:
    if not settings.master_weekly_enabled:
        return None

    target_day = WEEKDAY_MAP[settings.master_weekly_day]
    if now_local.weekday() != target_day:
        return None

    target_hour = settings.master_weekly_hour
    target_minute = settings.master_weekly_minute
    if not _is_within_window(now_local, target_hour, target_minute):
        return None

    slot = _slot_for_weekly(now_local)
    if state.get("weekly_enrichment") == slot:
        return None

    result = await _run_weekly_enrichment(settings)
    if result.status == "ok":
        state["weekly_enrichment"] = slot
        _save_state(state)
    return result


async def _maybe_run_daily_publish(
    now_local: datetime,
    state: Dict[str, Any],
    settings: MasterBusinessSettingsSnapshot,
) -> Optional[JobResult]:
    if not settings.master_daily_publish_enabled:
        return None

    target_hour = settings.master_daily_publish_hour
    target_minute = settings.master_daily_publish_minute
    if not _is_within_window(now_local, target_hour, target_minute):
        return None

    slot = _slot_for_daily(now_local)
    if state.get("daily_publish") == slot:
        return None

    result = await _run_daily_publish_with_store_hook(settings)
    if result.status == "ok":
        state["daily_publish"] = slot
        _save_state(state)
    return result


async def _maybe_run_archive(
    now_local: datetime,
    state: Dict[str, Any],
    settings: MasterBusinessSettingsSnapshot,
) -> Optional[JobResult]:
    if not settings.master_archive_enabled:
        return None

    every_minutes = settings.master_archive_every_minutes
    slot = _slot_for_interval(now_local, every_minutes)
    if state.get("hourly_archive") == slot:
        return None

    minute_of_day = now_local.hour * 60 + now_local.minute
    minute_in_bucket = minute_of_day % every_minutes
    if minute_in_bucket != 0 and minute_in_bucket * 60 > FIRE_WINDOW_SEC:
        return None

    result = await _run_hourly_archive()
    if result.status == "ok":
        state["hourly_archive"] = slot
        _save_state(state)
    return result


async def run_master_catalog_scheduler_once() -> Dict[str, Any]:
    now_local = datetime.now(timezone.utc).astimezone(KYIV_TZ)
    state = _load_state()
    settings = await load_master_business_settings_snapshot()
    _log_master_settings_if_changed(settings)
    executed: list[Dict[str, Any]] = []

    weekly = await _maybe_run_weekly(now_local, state, settings)
    if weekly:
        executed.append({"job": weekly.name, "status": weekly.status, "duration_sec": round(weekly.duration_sec, 3)})
        return {"now": now_local.isoformat(), "jobs": executed}

    daily = await _maybe_run_daily_publish(now_local, state, settings)
    if daily:
        executed.append({"job": daily.name, "status": daily.status, "duration_sec": round(daily.duration_sec, 3)})
        return {"now": now_local.isoformat(), "jobs": executed}

    archive = await _maybe_run_archive(now_local, state, settings)
    if archive:
        executed.append({"job": archive.name, "status": archive.status, "duration_sec": round(archive.duration_sec, 3)})

    return {"now": now_local.isoformat(), "jobs": executed}


async def schedule_master_catalog_tasks() -> None:
    if not _env_bool("MASTER_SCHEDULER_ENABLED", "1"):
        logger.info("MASTER_SCHEDULER_ENABLED is disabled; exiting scheduler")
        return

    logger.info(
        "Master catalog scheduler started: tz=%s poll=%ss window=%ss",
        KYIV_TZ,
        POLL_INTERVAL_SEC,
        FIRE_WINDOW_SEC,
    )

    try:
        with _global_lock():
            while True:
                try:
                    result = await run_master_catalog_scheduler_once()
                    if result["jobs"]:
                        logger.info("Master scheduler executed jobs: %s", result["jobs"])
                except Exception as exc:
                    logger.exception("Master scheduler loop failed")
                    send_notification(f"Ошибка master_catalog_scheduler: {exc}", "master_catalog_scheduler")

                await asyncio.sleep(POLL_INTERVAL_SEC)
    except RuntimeError as exc:
        logger.warning(str(exc))
    except Exception as main_error:
        logger.exception("Master scheduler crashed")
        send_notification(f"🔴 Сервис master_catalog_scheduler неожиданно остановлен: {main_error}", "master_catalog_scheduler")


if __name__ == "__main__":
    asyncio.run(schedule_master_catalog_tasks())

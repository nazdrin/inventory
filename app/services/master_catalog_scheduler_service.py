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
from app.services.notification_service import send_notification


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("master_catalog_scheduler")

KYIV_TZ = ZoneInfo(os.getenv("MASTER_SCHEDULER_TIMEZONE", "Europe/Kiev"))
STATE_DIR = Path(os.getenv("MASTER_SCHEDULER_STATE_DIR", "state_cache"))
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


def _resolve_publish_enterprise() -> str:
    value = (
        (os.getenv("MASTER_DAILY_PUBLISH_ENTERPRISE") or "").strip()
        or (os.getenv("MASTER_CATALOG_ENTERPRISE_CODE") or "").strip()
    )
    if not value:
        raise RuntimeError("MASTER_DAILY_PUBLISH_ENTERPRISE or MASTER_CATALOG_ENTERPRISE_CODE is required for daily publish")
    return value


def _resolve_salesdrive_enterprise() -> str:
    value = (
        (os.getenv("MASTER_WEEKLY_SALESDRIVE_ENTERPRISE") or "").strip()
        or (os.getenv("MASTER_CATALOG_ENTERPRISE_CODE") or "").strip()
    )
    if not value:
        raise RuntimeError("MASTER_WEEKLY_SALESDRIVE_ENTERPRISE or MASTER_CATALOG_ENTERPRISE_CODE is required for weekly salesdrive export")
    return value


async def _run_weekly_enrichment() -> JobResult:
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
        enterprise=_resolve_salesdrive_enterprise(),
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

    return JobResult(
        name="weekly_enrichment_with_salesdrive",
        status="ok",
        duration_sec=enrichment.duration_sec + salesdrive.duration_sec,
        details={
            "enrichment": enrichment.details,
            "salesdrive": salesdrive.details,
        },
    )


async def _run_daily_publish() -> JobResult:
    return await _run_orchestrator_job(
        "daily_publish",
        mode="publish",
        fail_fast=True,
        enterprise=_resolve_publish_enterprise(),
        limit=_env_int("MASTER_DAILY_PUBLISH_LIMIT", 0),
        send=True,
    )


async def _run_hourly_archive() -> JobResult:
    return await _run_orchestrator_job(
        "hourly_archive",
        mode="archive",
        fail_fast=True,
    )


async def _maybe_run_weekly(now_local: datetime, state: Dict[str, Any]) -> Optional[JobResult]:
    if not _env_bool("MASTER_WEEKLY_ENABLED", "1"):
        return None

    target_day = WEEKDAY_MAP[(os.getenv("MASTER_WEEKLY_DAY", "SUN") or "SUN").strip().upper()]
    if now_local.weekday() != target_day:
        return None

    target_hour = _env_int("MASTER_WEEKLY_HOUR", 3)
    target_minute = _env_int("MASTER_WEEKLY_MINUTE", 0)
    if not _is_within_window(now_local, target_hour, target_minute):
        return None

    slot = _slot_for_weekly(now_local)
    if state.get("weekly_enrichment") == slot:
        return None

    result = await _run_weekly_enrichment()
    if result.status == "ok":
        state["weekly_enrichment"] = slot
        _save_state(state)
    return result


async def _maybe_run_daily_publish(now_local: datetime, state: Dict[str, Any]) -> Optional[JobResult]:
    if not _env_bool("MASTER_DAILY_PUBLISH_ENABLED", "1"):
        return None

    target_hour = _env_int("MASTER_DAILY_PUBLISH_HOUR", 9)
    target_minute = _env_int("MASTER_DAILY_PUBLISH_MINUTE", 0)
    if not _is_within_window(now_local, target_hour, target_minute):
        return None

    slot = _slot_for_daily(now_local)
    if state.get("daily_publish") == slot:
        return None

    result = await _run_daily_publish()
    if result.status == "ok":
        state["daily_publish"] = slot
        _save_state(state)
    return result


async def _maybe_run_archive(now_local: datetime, state: Dict[str, Any]) -> Optional[JobResult]:
    if not _env_bool("MASTER_ARCHIVE_ENABLED", "1"):
        return None

    every_minutes = max(1, _env_int("MASTER_ARCHIVE_EVERY_MINUTES", 60))
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
    executed: list[Dict[str, Any]] = []

    weekly = await _maybe_run_weekly(now_local, state)
    if weekly:
        executed.append({"job": weekly.name, "status": weekly.status, "duration_sec": round(weekly.duration_sec, 3)})
        return {"now": now_local.isoformat(), "jobs": executed}

    daily = await _maybe_run_daily_publish(now_local, state)
    if daily:
        executed.append({"job": daily.name, "status": daily.status, "duration_sec": round(daily.duration_sec, 3)})
        return {"now": now_local.isoformat(), "jobs": executed}

    archive = await _maybe_run_archive(now_local, state)
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

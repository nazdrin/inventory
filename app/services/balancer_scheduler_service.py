import os
import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from app.business.balancer.jobs import run_balancer_pipeline_async

logger = logging.getLogger("balancer_scheduler")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Файл состояния — чтобы не запускать одну и ту же границу несколько раз
STATE_FILE = Path(os.getenv("BALANCER_SCHEDULER_STATE_FILE", ".balancer_last_boundary.json"))

# Таймзона сегментов (как в конфиге)
TZ = ZoneInfo(os.getenv("BALANCER_TZ", "Europe/Kyiv"))

# Границы сегментов (локальное время)
# Под твою текущую схему: NIGHT (21:00-09:00), WD_09_15, WD_15_21
BOUNDARIES_LOCAL = [(9, 0), (15, 0), (21, 0)]


def _load_last_boundary_utc_iso() -> str | None:
    if not STATE_FILE.exists():
        return None
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return data.get("last_boundary_utc")
    except Exception:
        return None


def _save_last_boundary_utc_iso(boundary_utc_iso: str) -> None:
    try:
        STATE_FILE.write_text(
            json.dumps({"last_boundary_utc": boundary_utc_iso}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        # не падаем из-за файла состояния
        pass


def _make_local_dt(d: datetime, hh: int, mm: int) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=TZ)


def _prev_boundary_end_utc(now_utc: datetime) -> datetime:
    """Предыдущая граница (окончание сегмента) в UTC относительно now_utc."""
    now_local = now_utc.astimezone(TZ)
    today = now_local.date()

    candidates_today = [_make_local_dt(datetime(today.year, today.month, today.day, tzinfo=TZ), h, m) for h, m in BOUNDARIES_LOCAL]
    candidates_today = sorted(candidates_today)

    # Если сейчас до первой границы (09:00), значит предыдущая граница = вчера 21:00
    if now_local < candidates_today[0]:
        yday = (now_local - timedelta(days=1)).date()
        prev_local = datetime(yday.year, yday.month, yday.day, 21, 0, tzinfo=TZ)
        return prev_local.astimezone(timezone.utc)

    # Иначе берем максимум из границ <= now_local
    prev_local = max([c for c in candidates_today if c <= now_local])
    return prev_local.astimezone(timezone.utc)


def _next_boundary_end_utc(now_utc: datetime) -> datetime:
    """Следующая граница (окончание сегмента) в UTC относительно now_utc."""
    now_local = now_utc.astimezone(TZ)
    today = now_local.date()

    candidates = []
    for h, m in BOUNDARIES_LOCAL:
        candidates.append(datetime(today.year, today.month, today.day, h, m, tzinfo=TZ))

    tomorrow = (now_local + timedelta(days=1)).date()
    for h, m in BOUNDARIES_LOCAL:
        candidates.append(datetime(tomorrow.year, tomorrow.month, tomorrow.day, h, m, tzinfo=TZ))

    future = [c for c in candidates if c > now_local]
    next_local = min(future)
    return next_local.astimezone(timezone.utc)


async def loop() -> None:
    # Окно, в котором мы разрешаем запуск после границы (чтобы не промахнуться по времени)
    fire_window_sec = int(os.getenv("BALANCER_FIRE_WINDOW_SEC", "180"))  # 3 минуты по умолчанию

    # Запускать оба режима подряд (TEST -> LIVE)
    run_both = os.getenv("BALANCER_RUN_BOTH", "0").strip() == "1"

    logger.info("🚀 Balancer scheduler started. tz=%s boundaries=%s fire_window_sec=%s run_both=%s state_file=%s",
                str(TZ), BOUNDARIES_LOCAL, fire_window_sec, run_both, str(STATE_FILE))

    while True:
        now = datetime.now(timezone.utc)
        prev_boundary = _prev_boundary_end_utc(now)
        next_boundary = _next_boundary_end_utc(now)

        prev_iso = prev_boundary.isoformat()
        last_done = _load_last_boundary_utc_iso()

        # Запускаемся только в первые N секунд после границы
        in_fire_window = now >= prev_boundary and (now - prev_boundary).total_seconds() <= fire_window_sec

        logger.info("🕒 Tick. now_utc=%s prev_boundary_utc=%s next_boundary_utc=%s in_fire_window=%s last_done=%s",
                    now.isoformat(), prev_iso, next_boundary.isoformat(), in_fire_window, last_done)

        if in_fire_window and last_done != prev_iso:
            logger.info("✅ Boundary fired: %s", prev_iso)

            # Говорим jobs, какой сегмент закрыли (по его segment_end)
            os.environ["BALANCER_COLLECT_SEGMENT_END_UTC"] = prev_iso

            try:
                if run_both:
                    os.environ["BALANCER_RUN_MODE"] = "TEST"
                    await run_balancer_pipeline_async()

                    os.environ["BALANCER_RUN_MODE"] = "LIVE"
                    await run_balancer_pipeline_async()
                else:
                    await run_balancer_pipeline_async()

                _save_last_boundary_utc_iso(prev_iso)
                logger.info("✅ Boundary processed and saved: %s", prev_iso)

            except Exception:
                logger.exception("❌ Balancer scheduler boundary iteration failed")

        # Спим до следующей границы (с запасом)
        sleep_sec = max(10, int((next_boundary - now).total_seconds()) - 5)
        logger.info("⏳ Sleep %s sec (to next boundary)", sleep_sec)
        await asyncio.sleep(sleep_sec)


if __name__ == "__main__":
    asyncio.run(loop())
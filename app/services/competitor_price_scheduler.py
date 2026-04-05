import os
import asyncio
import logging
import pytz
from datetime import datetime, timezone, timedelta

# Установка временной зоны (как в других шедулерах)
os.environ["TZ"] = "UTC"
KIEV_TZ = pytz.timezone("Europe/Kiev")

# Импорты из проекта
from app.business.competitor_price_loader import run as run_competitor_loader
from app.services.notification_service import send_notification

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ========= Утилиты уведомлений =========

def _success_notifications_enabled() -> bool:
    return os.getenv("COMPETITOR_SCHEDULER_NOTIFY_SUCCESS", "0").strip().lower() in {
        "1", "true", "yes", "on"
    }

async def notify_error(message: str, enterprise_code: str = "unknown"):
    logging.error(message)
    # если send_notification синхронная — вызываем без await
    try:
        send_notification(message, enterprise_code)
    except Exception as e:
        logging.error("Ошибка при отправке уведомления: %s", str(e))


async def notify_info(message: str, enterprise_code: str = "unknown"):
    logging.info(message)
    try:
        send_notification(message, enterprise_code)
    except Exception as e:
        logging.error("Ошибка при отправке уведомления: %s", str(e))


def build_schedule_times(start_hhmm: str, end_hhmm: str, interval_minutes: int) -> set[str]:
    start_dt = datetime.strptime(start_hhmm, "%H:%M")
    end_dt = datetime.strptime(end_hhmm, "%H:%M")
    if interval_minutes <= 0:
        raise ValueError("interval_minutes must be greater than 0")
    if end_dt < start_dt:
        raise ValueError("end_hhmm must be greater than or equal to start_hhmm")

    times: set[str] = set()
    cur = start_dt
    while cur <= end_dt:
        times.add(cur.strftime("%H:%M"))
        cur += timedelta(minutes=interval_minutes)
    return times


# ========= Основной шедулер =========

async def schedule_competitor_price_loader():
    """
    Планировщик для competitor_price_loader:

    - Раз в минуту проверяет текущее время по Киевскому времени.
    - Сравнивает его с набором времён 'HH:MM' из env-окна и интервала.
    - Если текущее HH:MM входит в набор и мы ещё не запускали сегодня в это время —
      запускаем run_competitor_loader().
    """
    window_start = os.getenv("COMPETITOR_SCHEDULER_WINDOW_START", "09:00")
    window_end = os.getenv("COMPETITOR_SCHEDULER_WINDOW_END", "21:00")
    interval_raw = os.getenv("COMPETITOR_SCHEDULER_INTERVAL_MINUTES", "15")
    try:
        interval_minutes = int(interval_raw)
    except ValueError as exc:
        raise ValueError(
            "COMPETITOR_SCHEDULER_INTERVAL_MINUTES must be an integer."
        ) from exc

    schedule_times = build_schedule_times(window_start, window_end, interval_minutes)
    logging.info(
        "[Competitor Scheduler] Запуск по env: start=%s end=%s interval=%s min; "
        "слотов=%s: %s",
        window_start,
        window_end,
        interval_minutes,
        len(schedule_times),
        ", ".join(sorted(schedule_times)),
    )

    # В память забиваем, когда последний раз запускались по конкретному времени
    # Ключ: строка 'HH:MM', значение: дата (datetime.date)
    last_run_by_time = {}

    # Чтобы не пропускать запуск из-за дрейфа цикла и/или долгой работы loader-а,
    # мы обрабатываем «пропущенные минуты» между итерациями.
    last_seen_kiev: datetime | None = None

    # Не допускаем параллельных запусков loader-а
    loader_task: asyncio.Task | None = None
    loader_started_at: datetime | None = None

    async def _start_loader(trigger_time_str: str, trigger_dt_kiev: datetime):
        nonlocal loader_task, loader_started_at

        if loader_task is not None and not loader_task.done():
            logging.warning(
                "[Competitor Scheduler] Пропуск запуска для %s — loader ещё выполняется (стартовал %s).",
                trigger_time_str,
                loader_started_at,
            )
            return

        async def _run_and_report():
            nonlocal loader_started_at
            loader_started_at = datetime.now(timezone.utc).astimezone(KIEV_TZ)
            t0 = datetime.now(timezone.utc)
            try:
                await run_competitor_loader()
                t1 = datetime.now(timezone.utc)
                duration_s = (t1 - t0).total_seconds()

                msg = (
                    "Загружено\n"
                    f"• Триггер: {trigger_time_str} (Киев)\n"
                    f"• Факт старта: {loader_started_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"• Длительность: {duration_s:.1f} сек\n"
                    f"• Проверка шедулера: {trigger_dt_kiev.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                if _success_notifications_enabled():
                    await notify_info(msg, "competitor_price_scheduler")

                logging.info(
                    "[Competitor Scheduler] Загрузка цен конкурентов успешно завершена за %.1f сек.",
                    duration_s,
                )
            except Exception as e:
                await notify_error(
                    f"🔥 Ошибка при запуске competitor_price_loader: {str(e)}",
                    "competitor_price_scheduler",
                )

        loader_task = asyncio.create_task(_run_and_report())

    try:
        while True:
            loop_t0 = datetime.now(timezone.utc)

            now_utc = datetime.now(timezone.utc)
            now_kiev = now_utc.astimezone(KIEV_TZ)

            logging.info(f"[Competitor Scheduler] Текущее время: {now_kiev} (Киев)")

            # Если это первая итерация — считаем, что прошлое время равно текущему
            if last_seen_kiev is None:
                last_seen_kiev = now_kiev

            # Обрабатываем все минуты между last_seen_kiev и now_kiev (включительно),
            # чтобы не пропускать запуск из-за дрейфа цикла.
            start_minute = last_seen_kiev.replace(second=0, microsecond=0)
            end_minute = now_kiev.replace(second=0, microsecond=0)

            # Ограничение, чтобы в случае долгого простоя не запускать лавину задач
            max_catchup_minutes = 10
            delta_minutes = int((end_minute - start_minute).total_seconds() // 60)
            if delta_minutes > max_catchup_minutes:
                logging.warning(
                    "[Competitor Scheduler] Большой разрыв между итерациями (%s мин). "
                    "Ограничиваем догон до последних %s минут.",
                    delta_minutes,
                    max_catchup_minutes,
                )
                start_minute = end_minute - timedelta(minutes=max_catchup_minutes)

            cur = start_minute
            while cur <= end_minute:
                current_time_str = cur.strftime("%H:%M")
                current_date = cur.date()

                if current_time_str in schedule_times:
                    last_run_date = last_run_by_time.get(current_time_str)

                    # Проверяем, запускали ли уже сегодня в это время
                    if last_run_date != current_date:
                        logging.info(
                            "[Competitor Scheduler] Триггер времени %s (минутный слот %s) — запускаем loader.",
                            current_time_str,
                            cur,
                        )
                        await _start_loader(current_time_str, cur)
                        last_run_by_time[current_time_str] = current_date
                    else:
                        logging.info(
                            "[Competitor Scheduler] Для времени %s уже был запуск сегодня, пропускаем.",
                            current_time_str,
                        )

                cur = cur + timedelta(minutes=1)

            last_seen_kiev = now_kiev

            # Спим до начала следующей минуты (минимизируем дрейф)
            loop_t1 = datetime.now(timezone.utc)
            loop_s = (loop_t1 - loop_t0).total_seconds()

            # вычисляем, сколько секунд до следующей минуты по Киевскому времени
            now_kiev_after = datetime.now(timezone.utc).astimezone(KIEV_TZ)
            next_minute = (now_kiev_after.replace(second=0, microsecond=0) + timedelta(minutes=1))
            sleep_s = (next_minute - now_kiev_after).total_seconds()

            logging.info(
                "[Competitor Scheduler] Итерация заняла %.3f сек, sleep %.3f сек до следующей минуты.",
                loop_s,
                sleep_s,
            )

            # safety: если вдруг вышло отрицательно — не падаем
            if sleep_s < 0:
                sleep_s = 1

            await asyncio.sleep(sleep_s)

    except Exception as main_error:
        await notify_error(
            f"🔥 Критическая ошибка в планировщике конкурентов: {str(main_error)}",
            "competitor_price_scheduler",
        )
    finally:
        await notify_error(
            "🔴 Сервис competitor_price_scheduler неожиданно остановлен.",
            "competitor_price_scheduler",
        )


if __name__ == "__main__":
    asyncio.run(schedule_competitor_price_loader())

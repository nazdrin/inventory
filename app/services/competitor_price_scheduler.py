import os
import asyncio
import logging
import pytz
from datetime import datetime, timezone
from typing import Set

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text

# Установка временной зоны (как в других шедулерах)
os.environ["TZ"] = "UTC"
KIEV_TZ = pytz.timezone("Europe/Kiev")

# Импорты из проекта
from app.business.competitor_price_loader import run as run_competitor_loader
from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ========= Утилиты уведомлений =========

async def notify_error(message: str, enterprise_code: str = "unknown"):
    logging.error(message)
    # если send_notification синхронная — вызываем без await
    try:
        send_notification(message, enterprise_code)
    except Exception as e:
        logging.error("Ошибка при отправке уведомления: %s", str(e))


# ========= Работа с БД: получаем расписание =========

async def get_business_schedule_times(db: AsyncSession) -> Set[str]:
    """
    Берём из EnterpriseSettings все предприятия с data_format == 'Business'
    и вытаскиваем из их поля google_drive_folder_id_ref время запуска в формате 'HH:MM'.

    Возвращаем set всех уникальных времён.
    """
    try:
        # Проверка соединения
        try:
            await db.execute(text("SELECT 1"))
        except OperationalError:
            await notify_error("Соединение с базой данных закрыто, попытка восстановления...")
            await db.rollback()
            return set()

        db.expire_all()

        result = await db.execute(
            select(EnterpriseSettings).where(
                EnterpriseSettings.data_format == "Business",
                EnterpriseSettings.google_drive_folder_id_ref.isnot(None),
            )
        )
        enterprises = result.scalars().all()

        times: Set[str] = set()

        for enterprise in enterprises:
            raw = (enterprise.google_drive_folder_id_ref or "").strip()
            if not raw:
                continue

            # Нормализуем: ожидаем 'HH:MM'
            # Если формат другой — просто логируем и пропускаем
            try:
                # Если не упадёт — значит формат валидный
                dt = datetime.strptime(raw, "%H:%M")
                norm = dt.strftime("%H:%M")
                times.add(norm)
            except ValueError:
                logging.warning(
                    "Неверный формат времени '%s' в google_drive_folder_id_ref "
                    "для Enterprise Code=%s. Ожидается 'HH:MM' (например, '09:00').",
                    raw,
                    enterprise.enterprise_code,
                )

        if not times:
            logging.warning(
                "Нет валидных времён запуска конкурентов для предприятий с форматом Business."
            )

        return times

    except Exception as e:
        await notify_error(f"Ошибка при чтении расписания для конкурентов: {str(e)}")
        await db.rollback()
        return set()


# ========= Основной шедулер =========

async def schedule_competitor_price_loader():
    """
    Планировщик для competitor_price_loader:

    - Раз в минуту проверяет текущее время по Киевскому времени.
    - Сравнивает его с набором времён 'HH:MM' из google_drive_folder_id_ref
      для предприятий с data_format == 'Business'.
    - Если текущее HH:MM входит в набор и мы ещё не запускали сегодня в это время —
      запускаем run_competitor_loader().
    """
    # В память забиваем, когда последний раз запускались по конкретному времени
    # Ключ: строка 'HH:MM', значение: дата (datetime.date)
    last_run_by_time = {}

    try:
        async with get_async_db() as db:
            while True:
                now_utc = datetime.now(timezone.utc)
                now_kiev = now_utc.astimezone(KIEV_TZ)
                current_time_str = now_kiev.strftime("%H:%M")
                current_date = now_kiev.date()

                logging.info(f"[Competitor Scheduler] Текущее время: {now_kiev} (Киев)")

                # Получаем все уникальные времена запуска для 'Business'
                schedule_times = await get_business_schedule_times(db)

                if schedule_times:
                    logging.info(
                        "[Competitor Scheduler] Настроенные времена запуска: %s",
                        ", ".join(sorted(schedule_times)),
                    )

                # Если текущее время совпадает с одним из расписаний
                if current_time_str in schedule_times:
                    last_run_date = last_run_by_time.get(current_time_str)

                    # Проверяем, запускали ли уже сегодня в это время
                    if last_run_date != current_date:
                        logging.info(
                            "[Competitor Scheduler] Запуск competitor_price_loader "
                            f"для времени {current_time_str} (сегодня ещё не запускали)."
                        )
                        try:
                            await run_competitor_loader()
                            last_run_by_time[current_time_str] = current_date
                            logging.info(
                                "[Competitor Scheduler] Загрузка цен конкурентов успешно завершена."
                            )
                        except Exception as e:
                            await notify_error(
                                f"🔥 Ошибка при запуске competitor_price_loader: {str(e)}",
                                "competitor_price_scheduler",
                            )
                    else:
                        logging.info(
                            "[Competitor Scheduler] Для времени %s уже был запуск сегодня, пропускаем.",
                            current_time_str,
                        )

                # Спим 60 секунд
                await asyncio.sleep(60)

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
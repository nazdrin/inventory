# app/services/order_scheduler_service.py
import os
import asyncio
import logging
import pytz
from datetime import datetime, timezone
from sqlalchemy.future import select

os.environ['TZ'] = 'UTC'
KIEV_TZ = pytz.timezone("Europe/Kiev")

from app.database import get_async_db, EnterpriseSettings
from app.services.notification_service import send_notification
from app.services.order_fetcher import fetch_orders_for_enterprise
from app.business.order_sender import process_cancelled_orders_service

# Настройка логирования (аналогично стоку)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def notify_error(message: str, enterprise_code: str = "unknown"):
    logging.error(message)
    send_notification(message, enterprise_code)

async def get_enterprises_for_order_fetcher(db):
    """
    Возвращает список enterprise_code, для которых включён флаг order_fetcher=True.
    """
    try:
        db.expire_all()
        now = datetime.now(tz=timezone.utc).astimezone(KIEV_TZ)
        logging.info(f"Текущее время: {now} [Timezone: {now.tzinfo}]")

        result = await db.execute(
            select(EnterpriseSettings.enterprise_code).where(EnterpriseSettings.order_fetcher == True)
        )
        return [row[0] for row in result.fetchall()]
    except Exception as e:
        try:
            await db.rollback()
        except Exception:
            pass
        await notify_error(f"Ошибка получения списка предприятий для fetcher: {e}")
        return []

async def schedule_order_fetcher_tasks():
    """
    Главный цикл забора заказов:
    - Каждую минуту ищет предприятия с order_fetcher=True и для каждого вызывает fetch_orders_for_enterprise.
    """
    interval_minutes = 1
    try:
        while True:
            async with get_async_db() as db:
                logging.info("📥 Поиск предприятий с флагом order_fetcher=True...")
                fetcher_enterprises = await get_enterprises_for_order_fetcher(db)

                if fetcher_enterprises:
                    logging.info(f"🔄 Найдено {len(fetcher_enterprises)} предприятий для загрузки заказов")
                    for enterprise_code in fetcher_enterprises:
                        try:
                            await fetch_orders_for_enterprise(db, enterprise_code)
                            # После загрузки заказов — обработать отказы только для предприятий с data_format='business'
                            try:
                                result_df = await db.execute(
                                    select(EnterpriseSettings.data_format).where(
                                        EnterpriseSettings.enterprise_code == enterprise_code
                                    )
                                )
                                df_val = result_df.scalar_one_or_none()
                                if isinstance(df_val, str) and df_val.strip().lower() == 'business':
                                    await process_cancelled_orders_service(enterprise_code=enterprise_code)
                                    logging.info(f"✅ Обработаны отказы (business) для {enterprise_code}")
                                else:
                                    logging.info(f"⏭ Пропуск обработки отказов для {enterprise_code} (data_format='{df_val}')")
                            except Exception as ce:
                                logging.error(f"❌ Ошибка при обработке отказов для {enterprise_code}: {ce}")
                                await notify_error(f"Ошибка обработки отказов для {enterprise_code}: {ce}", enterprise_code)
                            logging.info(f"✅ Заказы получены для {enterprise_code}")
                        except Exception as fe:
                            logging.error(f"❌ Ошибка при получении заказов для {enterprise_code}: {fe}")
                            await notify_error(f"Ошибка получения заказов для {enterprise_code}: {fe}", enterprise_code)
                else:
                    logging.info("📭 Предприятия с order_fetcher=True не найдены – заказов не будет загружено")

            logging.info("⏳ Ожидание 1 минуты перед следующим циклом заказов...")
            await asyncio.sleep(interval_minutes * 60)
    except Exception as main_error:
        await notify_error(f"🔥 Критическая ошибка в планировщике заказов: {str(main_error)}", "order_scheduler")
    finally:
        await notify_error("❌ Сервис order_scheduler неожиданно остановлен.", "order_scheduler")

if __name__ == "__main__":
    asyncio.run(schedule_order_fetcher_tasks())

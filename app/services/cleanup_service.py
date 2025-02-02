import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from datetime import datetime, timedelta
from sqlalchemy import delete
from app.models import DeveloperSettings, InventoryData, InventoryStock

async def cleanup_old_data(session: AsyncSession):
    """
    Удаляет записи из таблиц InventoryData и InventoryStock,
    которые старше времени, определенного в DeveloperSettings.
    :param session: Сессия базы данных
    """
    try:
        # Получаем текущее UTC время
        now = datetime.utcnow()

        # Получаем параметры retention из DeveloperSettings
        stmt = select(DeveloperSettings).limit(1)
        result = await session.execute(stmt)
        settings = result.scalars().first()

        if not settings:
            logging.warning("Настройки DeveloperSettings не найдены. Очистка данных пропущена.")
            return

        # Вычисляем пороговые даты/время для каждой таблицы
        if settings.catalog_data_retention is not None:
            threshold_time_catalog = now - timedelta(hours=settings.catalog_data_retention)
            # Удаление устаревших записей из InventoryData
            delete_catalog_stmt = delete(InventoryData).where(InventoryData.updated_at < threshold_time_catalog)
            result_catalog = await session.execute(delete_catalog_stmt)
            logging.info(f"Удалено {result_catalog.rowcount} записей из InventoryData старше {threshold_time_catalog}.")

        if settings.stock_data_retention is not None:
            threshold_time_stock = now - timedelta(hours=settings.stock_data_retention)
            # Удаление устаревших записей из InventoryStock
            delete_stock_stmt = delete(InventoryStock).where(InventoryStock.updated_at < threshold_time_stock)
            result_stock = await session.execute(delete_stock_stmt)
            logging.info(f"Удалено {result_stock.rowcount} записей из InventoryStock старше {threshold_time_stock}.")

        # Фиксация изменений
        await session.commit()

    except Exception as e:
        logging.error(f"Ошибка при очистке данных: {str(e)}")
        await session.rollback()
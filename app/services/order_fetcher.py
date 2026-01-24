import base64
import aiohttp
import json
import logging
import os
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models import DeveloperSettings, EnterpriseSettings, MappingBranch
from app.services.auto_confirm import process_orders
from app.services.order_sender import send_orders_to_tabletki
from app.services.order_sender import send_single_order_status_2
from app.key_crm_data_service.key_crm_send_order import send_order_to_key_crm
from app.key_crm_data_service.key_crm_status_check import check_statuses_key_crm
from app.business.order_sender import process_and_send_order

logger = logging.getLogger(__name__)

# --- Logging controls (env) ---
# ORDER_FETCHER_LOG_LEVEL: DEBUG/INFO/WARNING/ERROR (default INFO)
# ORDER_FETCHER_VERBOSE_ORDER_LOGS: 1 to log full order JSON + per-order lines (default 0)
_LOG_LEVEL = os.getenv("ORDER_FETCHER_LOG_LEVEL", "INFO").upper()
logger.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))

VERBOSE_ORDER_LOGS = os.getenv("ORDER_FETCHER_VERBOSE_ORDER_LOGS", "0") == "1"
ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS = os.getenv("ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS", "1") == "1"

ORDER_SEND_PROCESSORS = {
    "KeyCRM": send_order_to_key_crm,
    "ComboKeyCRM": send_order_to_key_crm,
    "Business": process_and_send_order,
    # Добавишь сюда новые форматы позже
}
ORDER_STATUS_CHECKERS = {
    "KeyCRM": check_statuses_key_crm,
    "ComboKeyCRM": check_statuses_key_crm,
    # Примеры для будущих форматов:
    # "Dntrade": check_statuses_dntrade,
    # "Prom": check_statuses_prom,
}

async def fetch_orders_for_enterprise(session: AsyncSession, enterprise_code: str):
    """
    Получает заказы для заданного предприятия (enterprise_code),
    если активирован флаг `order_fetcher = True` и найдены филиалы.
    """
    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    enterprise_q = await session.execute(
        select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    )
    enterprise = enterprise_q.scalar()

    if not enterprise:
        logger.warning("EnterpriseSettings not found for enterprise_code=%s", enterprise_code)
        return []

    if not enterprise.order_fetcher:
        logger.info("order_fetcher disabled, skip enterprise_code=%s", enterprise_code)
        return []

    auth_header = base64.b64encode(
        f"{enterprise.tabletki_login}:{enterprise.tabletki_password}".encode()
    ).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}"
    }

    branches_q = await session.execute(
        select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
    )
    branches = [row[0] for row in branches_q.fetchall()]
    auto_confirm_flag = enterprise.auto_confirm

    all_orders = []
    async with aiohttp.ClientSession() as http_session:
        for branch in branches:
            if auto_confirm_flag:
                # ===== Вариант с авто-подтверждением =====
                for status in [0, 2, 4, 4.1]:
                    url = f"{endpoint_orders}/api/Orders/{branch}/{status}"
                    try:
                        async with http_session.get(url, headers=headers) as response:
                            logger.info("Orders request: %s", url)
                            if response.status == 200:
                                data = await response.json()
                                if isinstance(data, list):
                                    for order in data:
                                        if VERBOSE_ORDER_LOGS:
                                            logger.info("Order payload: %s", json.dumps(order, ensure_ascii=False))
                                        else:
                                            logger.debug("Order payload (truncated): %.2000s", json.dumps(order, ensure_ascii=False))

                                        if status in [0, 2]:
                                            processor = ORDER_SEND_PROCESSORS.get(enterprise.data_format)
                                            if processor:
                                                await processor(order, enterprise_code, branch)
                                            else:
                                                logger.warning("No order send processor for data_format=%s", enterprise.data_format)

                                        # Для формата Business не выполняем проверку статусов у продавца
                                        if enterprise.data_format != "Business" and status in [2, 4, 4.1]:
                                            status_checker = ORDER_STATUS_CHECKERS.get(enterprise.data_format)
                                            if status_checker:
                                                await status_checker(order, enterprise_code, branch)
                                            else:
                                                logger.warning("No status checker for data_format=%s", enterprise.data_format)

                                    if status in [0, 2]:
                                        processed_orders = await process_orders(session, data)
                                        logger.info("Auto-confirm processed %d orders", len(processed_orders))
                                        await send_orders_to_tabletki(
                                            session,
                                            processed_orders,
                                            tabletki_login=enterprise.tabletki_login,
                                            tabletki_password=enterprise.tabletki_password,
                                            cancel_reason=2,
                                        )

                                    if status == 0 and ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS:
                                        order_codes = list({order["code"] for order in data if "code" in order})
                                        if order_codes:
                                            from app.services.telegram_bot import notify_user
                                            await notify_user(branch, order_codes)
                            else:
                                logger.warning("Orders request failed: status=%s branch=%s", response.status, branch)
                    except Exception as e:
                        logger.exception("Orders request exception: branch=%s status=%s", branch, status)
            else:
                # ===== Вариант без авто-подтверждения =====
                for status in [0, 2, 4, 4.1]:
                    url = f"{endpoint_orders}/api/Orders/{branch}/{status}"
                    try:
                        async with http_session.get(url, headers=headers) as response:
                            logger.info("Orders request: %s", url)
                            if response.status == 200:
                                data = await response.json()
                                if isinstance(data, list):
                                    for order in data:
                                        if VERBOSE_ORDER_LOGS:
                                            logger.info("Order payload: %s", json.dumps(order, ensure_ascii=False))
                                        else:
                                            logger.debug("Order payload (truncated): %.2000s", json.dumps(order, ensure_ascii=False))
                                        if status == 0:
                                            # TODO: передача заказов продавцу
                                            processor = ORDER_SEND_PROCESSORS.get(enterprise.data_format)
                                            if processor:
                                                await processor(order, enterprise_code, branch)
                                            else:
                                                logger.warning("No order send processor for data_format=%s", enterprise.data_format)
                                            # Отправка на Tabletki.ua со статусом 2.0
                                            order["statusID"] = 2.0
                                            await send_single_order_status_2(
                                                session=session,
                                                order=order,
                                                tabletki_login=enterprise.tabletki_login,
                                                tabletki_password=enterprise.tabletki_password
                                            )
                                        elif status in [2, 4, 4.1]:
                                            # TODO: передача статуса продавцу
                                            # Отправка актуального статуса продавцу через соответствующий обработчик
                                            status_checker = ORDER_STATUS_CHECKERS.get(enterprise.data_format)
                                            if status_checker:
                                                await status_checker(order, enterprise_code, branch)
                                            else:
                                                logger.warning("No status checker for data_format=%s", enterprise.data_format)
                                        all_orders.append(order)
                                    if status == 0 and ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS:
                                        order_codes = list({order["code"] for order in data if "code" in order})
                                        if order_codes:
                                            from app.services.telegram_bot import notify_user
                                            await notify_user(branch, order_codes)
                            else:
                                logger.warning("Orders request failed: status=%s branch=%s", response.status, branch)
                    except Exception as e:
                        logger.exception("Orders request exception: branch=%s status=%s", branch, status)

    logger.info("Total orders fetched: %d enterprise_code=%s", len(all_orders), enterprise_code)
    return all_orders
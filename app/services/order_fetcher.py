import base64
import aiohttp
import json
import logging
import os
from typing import Any, Dict, List, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models import DeveloperSettings, EnterpriseSettings, MappingBranch
from app.business.business_store_order_mapper import (
    ORIGINAL_EXTERNAL_GOODS_CODE_FIELD,
    normalize_store_order_payload,
    restore_tabletki_goods_codes_for_status,
)
from app.services.auto_confirm import process_orders
from app.services.order_sender import process_due_tabletki_cancel_retries, send_orders_to_tabletki
from app.services.order_sender import send_single_order_status_2
from app.key_crm_data_service.key_crm_send_order import send_order_to_key_crm
from app.key_crm_data_service.key_crm_status_check import check_statuses_key_crm
from app.business.order_sender import process_and_send_order
from app.salesdrive_simple.salesdrive_simple_sender import send_order_to_salesdrive_simple

logger = logging.getLogger(__name__)

# --- Logging controls (env) ---
# ORDER_FETCHER_LOG_LEVEL: DEBUG/INFO/WARNING/ERROR (default INFO)
# ORDER_FETCHER_VERBOSE_ORDER_LOGS: 1 to log full order JSON + per-order lines (default 0)
_LOG_LEVEL = os.getenv("ORDER_FETCHER_LOG_LEVEL", "INFO").upper()
logger.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))

VERBOSE_ORDER_LOGS = os.getenv("ORDER_FETCHER_VERBOSE_ORDER_LOGS", "0") == "1"
ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS = os.getenv("ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS", "1") == "1"
BUSINESS_STORE_ORDER_MAPPING_ENABLED = os.getenv("BUSINESS_STORE_ORDER_MAPPING_ENABLED", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED = os.getenv(
    "BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED",
    "0",
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

ORDER_SEND_PROCESSORS = {
    "KeyCRM": send_order_to_key_crm,
    "ComboKeyCRM": send_order_to_key_crm,
    "Business": process_and_send_order,
    "SalesDriveSimple": send_order_to_salesdrive_simple,
}
ORDER_STATUS_CHECKERS = {
    "KeyCRM": check_statuses_key_crm,
    "ComboKeyCRM": check_statuses_key_crm,
    # Примеры для будущих форматов:
    # "Dntrade": check_statuses_dntrade,
    # "Prom": check_statuses_prom,
}


async def _send_store_aware_status_2_if_enabled(
    *,
    session: AsyncSession,
    enterprise: EnterpriseSettings,
    order: Dict[str, Any],
    branch: str,
) -> None:
    order_id = order.get("id")

    if not BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED:
        logger.info(
            "Store-aware status 2 send disabled by BUSINESS_STORE_ORDER_SEND_STATUS_2_ENABLED: enterprise_code=%s branch=%s order_id=%s",
            enterprise.enterprise_code,
            branch,
            order_id,
        )
        return

    tabletki_order = restore_tabletki_goods_codes_for_status(order)
    tabletki_order["statusID"] = 2.0
    restored_rows = [
        row for row in (tabletki_order.get("rows") or [])
        if isinstance(row, dict) and str(row.get("goodsCode") or "").strip()
    ]
    tabletki_order["rows"] = restored_rows

    restored_from_external = sum(
        1
        for row in (order.get("rows") or [])
        if isinstance(row, dict) and str(row.get(ORIGINAL_EXTERNAL_GOODS_CODE_FIELD) or "").strip()
    )

    logger.info(
        "Store-aware status 2 send started: enterprise_code=%s branch=%s order_id=%s restored_external_rows=%s",
        enterprise.enterprise_code,
        branch,
        order_id,
        restored_from_external,
    )
    await send_single_order_status_2(
        session=session,
        order=tabletki_order,
        tabletki_login=enterprise.tabletki_login,
        tabletki_password=enterprise.tabletki_password,
    )
    logger.info(
        "Store-aware status 2 sent: enterprise_code=%s branch=%s order_id=%s",
        enterprise.enterprise_code,
        branch,
        order_id,
    )


async def _normalize_business_orders_for_runtime(
    session: AsyncSession,
    *,
    enterprise: EnterpriseSettings,
    branch: str,
    orders: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    legacy_orders: List[Dict[str, Any]] = []
    store_aware_orders: List[Dict[str, Any]] = []
    mapping_error_orders: List[Dict[str, Any]] = []

    if enterprise.data_format != "Business":
        return list(orders), store_aware_orders, mapping_error_orders

    if not BUSINESS_STORE_ORDER_MAPPING_ENABLED:
        logger.debug(
            "Business store order mapping is disabled; legacy order flow unchanged for enterprise_code=%s branch=%s",
            enterprise.enterprise_code,
            branch,
        )
        return list(orders), store_aware_orders, mapping_error_orders

    for order in orders:
        order_branch = str(order.get("branchID") or branch or "").strip() or branch
        try:
            normalization = await normalize_store_order_payload(
                session,
                order_payload=order,
                tabletki_branch=order_branch,
            )
        except Exception:
            logger.exception(
                "Store-aware order normalization failed unexpectedly: enterprise_code=%s branch=%s order_id=%s",
                enterprise.enterprise_code,
                order_branch,
                order.get("id"),
            )
            mapping_error_orders.append(order)
            continue

        status = str(normalization.get("status") or "")
        if status == "legacy_passthrough":
            logger.debug(
                "Legacy order passthrough unchanged: enterprise_code=%s branch=%s order_id=%s",
                enterprise.enterprise_code,
                order_branch,
                order.get("id"),
            )
            legacy_orders.append(order)
            continue

        if status == "ok":
            normalized_order = normalization.get("order") or order
            logger.info(
                "Store-aware order normalized: enterprise_code=%s branch=%s order_id=%s store_id=%s store_code=%s mapped_rows=%s",
                enterprise.enterprise_code,
                order_branch,
                order.get("id"),
                normalization.get("store_id"),
                normalization.get("store_code"),
                normalization.get("mapped_rows"),
            )
            store_aware_orders.append(normalized_order)
            continue

        logger.warning(
            "Store-aware mapping_error skipped: enterprise_code=%s branch=%s order_id=%s store_id=%s store_code=%s errors=%s missing=%s",
            enterprise.enterprise_code,
            order_branch,
            order.get("id"),
            normalization.get("store_id"),
            normalization.get("store_code"),
            normalization.get("errors"),
            normalization.get("missing_mappings"),
        )
        mapping_error_orders.append(order)

    return legacy_orders, store_aware_orders, mapping_error_orders

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

    retry_stats = await process_due_tabletki_cancel_retries(session, enterprise_code=enterprise_code)
    if retry_stats["due_found"]:
        logger.info("Processed due Tabletki cancel retries: %s", retry_stats)

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
                                    legacy_orders, store_aware_orders, _mapping_error_orders = await _normalize_business_orders_for_runtime(
                                        session,
                                        enterprise=enterprise,
                                        branch=branch,
                                        orders=data,
                                    )

                                    for order in legacy_orders:
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

                                    for order in store_aware_orders:
                                        if VERBOSE_ORDER_LOGS:
                                            logger.info("Store-aware normalized order payload: %s", json.dumps(order, ensure_ascii=False))
                                        else:
                                            logger.debug("Store-aware normalized order payload (truncated): %.2000s", json.dumps(order, ensure_ascii=False))

                                        if status in [0, 2]:
                                            processor = ORDER_SEND_PROCESSORS.get(enterprise.data_format)
                                            if processor:
                                                await processor(order, enterprise_code, branch)
                                                logger.info(
                                                    "Store-aware auto-confirm bypassed after Business processing: enterprise_code=%s branch=%s order_id=%s",
                                                    enterprise_code,
                                                    branch,
                                                    order.get("id"),
                                                )
                                                if status == 0:
                                                    try:
                                                        await _send_store_aware_status_2_if_enabled(
                                                            session=session,
                                                            enterprise=enterprise,
                                                            order=order,
                                                            branch=branch,
                                                        )
                                                    except Exception as exc:
                                                        logger.warning(
                                                            "Store-aware status 2 send failed after successful outbound processing: enterprise_code=%s branch=%s order_id=%s error=%s",
                                                            enterprise_code,
                                                            branch,
                                                            order.get("id"),
                                                            exc,
                                                        )
                                            else:
                                                logger.warning("No order send processor for data_format=%s", enterprise.data_format)

                                    if status in [0, 2] and legacy_orders:
                                        processed_orders = await process_orders(session, legacy_orders)
                                        logger.info("Auto-confirm processed %d orders", len(processed_orders))
                                        await send_orders_to_tabletki(
                                            session,
                                            processed_orders,
                                            tabletki_login=enterprise.tabletki_login,
                                            tabletki_password=enterprise.tabletki_password,
                                            cancel_reason=2,
                                            enterprise_code=enterprise_code,
                                        )

                                    if status == 0 and ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS:
                                        order_codes = list({order["code"] for order in (legacy_orders + store_aware_orders) if "code" in order})
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
                                    legacy_orders, store_aware_orders, _mapping_error_orders = await _normalize_business_orders_for_runtime(
                                        session,
                                        enterprise=enterprise,
                                        branch=branch,
                                        orders=data,
                                    )

                                    for order in legacy_orders:
                                        if VERBOSE_ORDER_LOGS:
                                            logger.info("Order payload: %s", json.dumps(order, ensure_ascii=False))
                                        else:
                                            logger.debug("Order payload (truncated): %.2000s", json.dumps(order, ensure_ascii=False))
                                        if status == 0:
                                            # TODO: передача заказов продавцу
                                            processor = ORDER_SEND_PROCESSORS.get(enterprise.data_format)
                                            processor_ok = True
                                            if processor:
                                                result = await processor(order, enterprise_code, branch)
                                                if enterprise.data_format == "SalesDriveSimple":
                                                    processor_ok = bool(result)
                                            else:
                                                logger.warning("No order send processor for data_format=%s", enterprise.data_format)
                                            if processor_ok:
                                                # Отправка на Tabletki.ua со статусом 2.0
                                                order["statusID"] = 2.0
                                                await send_single_order_status_2(
                                                    session=session,
                                                    order=order,
                                                    tabletki_login=enterprise.tabletki_login,
                                                    tabletki_password=enterprise.tabletki_password
                                                )
                                            else:
                                                logger.warning(
                                                        "Skip Tabletki status 2.0 because outbound send failed: data_format=%s order_id=%s",
                                                        enterprise.data_format,
                                                        order.get("id"),
                                                    )
                                        elif status in [2, 4, 4.1]:
                                            # TODO: передача статуса продавцу
                                            # Отправка актуального статуса продавцу через соответствующий обработчик
                                            if enterprise.data_format != "Business":
                                                status_checker = ORDER_STATUS_CHECKERS.get(enterprise.data_format)
                                                if status_checker:
                                                    await status_checker(order, enterprise_code, branch)
                                                else:
                                                    logger.warning("No status checker for data_format=%s", enterprise.data_format)
                                        all_orders.append(order)

                                    for order in store_aware_orders:
                                        if VERBOSE_ORDER_LOGS:
                                            logger.info("Store-aware normalized order payload: %s", json.dumps(order, ensure_ascii=False))
                                        else:
                                            logger.debug("Store-aware normalized order payload (truncated): %.2000s", json.dumps(order, ensure_ascii=False))
                                        if status == 0:
                                            processor = ORDER_SEND_PROCESSORS.get(enterprise.data_format)
                                            processor_ok = True
                                            if processor:
                                                result = await processor(order, enterprise_code, branch)
                                                if enterprise.data_format == "SalesDriveSimple":
                                                    processor_ok = bool(result)
                                            else:
                                                logger.warning("No order send processor for data_format=%s", enterprise.data_format)
                                            if processor_ok:
                                                logger.info(
                                                    "Store-aware order processed with legacy auto-confirm bypass: enterprise_code=%s branch=%s order_id=%s",
                                                    enterprise_code,
                                                    branch,
                                                    order.get("id"),
                                                )
                                                try:
                                                    await _send_store_aware_status_2_if_enabled(
                                                        session=session,
                                                        enterprise=enterprise,
                                                        order=order,
                                                        branch=branch,
                                                    )
                                                except Exception as exc:
                                                    logger.warning(
                                                        "Store-aware status 2 send failed after successful outbound processing: enterprise_code=%s branch=%s order_id=%s error=%s",
                                                        enterprise_code,
                                                        branch,
                                                        order.get("id"),
                                                        exc,
                                                    )
                                            else:
                                                logger.warning(
                                                    "Store-aware order outbound processing failed: data_format=%s order_id=%s",
                                                    enterprise.data_format,
                                                    order.get("id"),
                                                )
                                        elif status in [2, 4, 4.1]:
                                            if enterprise.data_format != "Business":
                                                status_checker = ORDER_STATUS_CHECKERS.get(enterprise.data_format)
                                                if status_checker:
                                                    await status_checker(order, enterprise_code, branch)
                                                else:
                                                    logger.warning("No status checker for data_format=%s", enterprise.data_format)
                                        all_orders.append(order)

                                    if status == 0 and ORDER_FETCHER_NOTIFY_ON_NEW_ORDERS:
                                        order_codes = list({order["code"] for order in (legacy_orders + store_aware_orders) if "code" in order})
                                        if order_codes:
                                            from app.services.telegram_bot import notify_user
                                            await notify_user(branch, order_codes)
                            else:
                                logger.warning("Orders request failed: status=%s branch=%s", response.status, branch)
                    except Exception as e:
                        logger.exception("Orders request exception: branch=%s status=%s", branch, status)

    logger.info("Total orders fetched: %d enterprise_code=%s", len(all_orders), enterprise_code)
    return all_orders

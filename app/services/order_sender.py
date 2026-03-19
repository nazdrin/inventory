# app/services/order_sender.py

import aiohttp
import asyncio
import base64
import json
import logging
import os

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models import DeveloperSettings


logger = logging.getLogger(__name__)

TABLETKI_RETRY_ATTEMPTS = max(1, int(os.getenv("TABLETKI_ORDER_RETRY_ATTEMPTS", "3")))
TABLETKI_RETRY_DELAY_SEC = max(0.0, float(os.getenv("TABLETKI_ORDER_RETRY_DELAY_SEC", "2")))


async def _post_with_retry(
    http_session: aiohttp.ClientSession,
    *,
    url: str,
    payload,
    headers: dict,
    operation_label: str,
) -> None:
    last_error: Exception | None = None

    for attempt in range(1, TABLETKI_RETRY_ATTEMPTS + 1):
        try:
            async with http_session.post(url, json=payload, headers=headers) as response:
                response_text = await response.text()
                logger.info(
                    "Tabletki %s attempt=%s/%s status=%s",
                    operation_label,
                    attempt,
                    TABLETKI_RETRY_ATTEMPTS,
                    response.status,
                )
                if 200 <= response.status < 300:
                    logger.debug("Tabletki %s response: %s", operation_label, response_text)
                    return
                last_error = RuntimeError(
                    f"Tabletki {operation_label} failed with status={response.status}: {response_text}"
                )
        except aiohttp.ClientError as exc:
            last_error = exc
            logger.warning(
                "Tabletki %s request error attempt=%s/%s: %s",
                operation_label,
                attempt,
                TABLETKI_RETRY_ATTEMPTS,
                exc,
            )

        if attempt < TABLETKI_RETRY_ATTEMPTS:
            await asyncio.sleep(TABLETKI_RETRY_DELAY_SEC)

    raise RuntimeError(
        f"Tabletki {operation_label} failed after {TABLETKI_RETRY_ATTEMPTS} attempts: {last_error}"
    )


async def send_orders_to_tabletki(
    session: AsyncSession,
    orders: list,
    tabletki_login: str,
    tabletki_password: str,
    cancel_reason: int,
):
    """
    Отправляет заказы в Tabletki.ua по API:
    - статус 4 или 6: подтверждение → /api/orders
    - статус 7 или все qtyShip == 0: отказ → /api/Orders/cancelledOrders
    Поле id_CancelReason берётся из аргумента cancel_reason.
    На non-2xx и сетевых ошибках делает retry и затем пробрасывает ошибку.
    """
    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    auth_header = base64.b64encode(f"{tabletki_login}:{tabletki_password}".encode()).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}",
    }

    async with aiohttp.ClientSession() as http_session:
        for order in orders:
            is_cancel = (order.get("statusID") == 7) or all(
                (row.get("qtyShip", 0) == 0) for row in order.get("rows", [])
            )

            if is_cancel:
                url = f"{endpoint_orders}/api/Orders/cancelledOrders"
                cancel_data = [{
                    "id": order["id"],
                    "id_CancelReason": cancel_reason,
                    "rows": [
                        {
                            "goodsCode": item["goodsCode"],
                            "qty": item.get("qty", item.get("qtyShip", 0)),
                        }
                        for item in order["rows"]
                    ],
                }]
                logger.info(
                    "🚫 Отправка отказа заказа %s (id_CancelReason=%s)",
                    order["id"],
                    cancel_reason,
                )
                logger.debug("Tabletki cancel payload: %s", json.dumps(cancel_data, ensure_ascii=False))
                await _post_with_retry(
                    http_session,
                    url=url,
                    payload=cancel_data,
                    headers=headers,
                    operation_label=f"cancel order_id={order['id']}",
                )
                continue

            if order.get("statusID") not in [4, 6]:
                continue

            valid_rows = [item for item in order["rows"] if item.get("qtyShip", 0) > 0]
            if not valid_rows:
                logger.warning("⚠️ Пропущен заказ %s — нет строк с qtyShip > 0", order["id"])
                continue

            order_to_send = dict(order)
            order_to_send["rows"] = valid_rows

            await _post_with_retry(
                http_session,
                url=f"{endpoint_orders}/api/orders",
                payload=[order_to_send],
                headers=headers,
                operation_label=f"send order_id={order['id']}",
            )


async def send_single_order_status_2(
    session: AsyncSession,
    order: dict,
    tabletki_login: str,
    tabletki_password: str,
):
    """
    Отправляет заказ на Tabletki.ua со статусом 2.0.
    На non-2xx и сетевых ошибках делает retry и затем пробрасывает ошибку.
    """
    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    auth_header = base64.b64encode(f"{tabletki_login}:{tabletki_password}".encode()).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}",
    }

    valid_rows = [item for item in order["rows"] if item.get("qty", 0) > 0 or item.get("qtyShip", 0) > 0]
    if not valid_rows:
        logger.warning("⚠️ Пропущен заказ %s — нет строк с qty или qtyShip > 0", order["id"])
        return

    order_to_send = dict(order)
    order_to_send["rows"] = valid_rows

    async with aiohttp.ClientSession() as http_session:
        await _post_with_retry(
            http_session,
            url=f"{endpoint_orders}/api/orders",
            payload=[order_to_send],
            headers=headers,
            operation_label=f"status_2 order_id={order['id']}",
        )

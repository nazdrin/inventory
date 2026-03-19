# app/services/order_sender.py

import aiohttp
import asyncio
import base64
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import fcntl
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models import DeveloperSettings, EnterpriseSettings
from app.services.notification_service import send_notification


logger = logging.getLogger(__name__)

TABLETKI_RETRY_ATTEMPTS = max(1, int(os.getenv("TABLETKI_ORDER_RETRY_ATTEMPTS", "3")))
TABLETKI_RETRY_DELAY_SEC = max(0.0, float(os.getenv("TABLETKI_ORDER_RETRY_DELAY_SEC", "2")))
TABLETKI_CANCEL_WARNING_RETRY_MAX = max(0, int(os.getenv("TABLETKI_CANCEL_WARNING_RETRY_MAX", "2")))
TABLETKI_CANCEL_WARNING_RETRY_DELAY_MINUTES = max(
    1,
    int(os.getenv("TABLETKI_CANCEL_WARNING_RETRY_DELAY_MINUTES", "30")),
)
TABLETKI_CANCEL_RETRY_QUEUE_PATH = (
    Path(__file__).resolve().parents[2] / "state_cache" / "tabletki_cancel_retry_queue.json"
)
TABLETKI_CANCEL_WARNING_TEXT = "cancel fact will be setted only by delivery service data"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(timezone.utc)


@contextmanager
def _locked_queue_file():
    TABLETKI_CANCEL_RETRY_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TABLETKI_CANCEL_RETRY_QUEUE_PATH.open("a+", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        fh.seek(0)
        try:
            yield fh
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _load_entries_from_handle(handle) -> List[Dict[str, Any]]:
    raw = handle.read().strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Tabletki cancel retry queue is corrupted, resetting it")
        return []
    return data if isinstance(data, list) else []


def _write_entries_to_handle(handle, entries: List[Dict[str, Any]]) -> None:
    handle.seek(0)
    json.dump(entries, handle, ensure_ascii=False, indent=2)
    handle.truncate()
    handle.flush()


def _build_cancel_payload(order: Dict[str, Any], cancel_reason: int) -> List[Dict[str, Any]]:
    return [{
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


def _has_cancel_warning(response_text: str) -> bool:
    normalized = str(response_text or "").lower()
    if TABLETKI_CANCEL_WARNING_TEXT in normalized:
        return True

    try:
        payload = json.loads(response_text)
    except Exception:
        return False

    processed_docs = payload.get("processedDocs")
    if not isinstance(processed_docs, list):
        return False

    for item in processed_docs:
        if not isinstance(item, dict):
            continue
        result = str(item.get("result") or "").lower()
        if TABLETKI_CANCEL_WARNING_TEXT in result:
            return True
    return False


def _enqueue_cancel_retry(
    *,
    order: Dict[str, Any],
    enterprise_code: str,
    cancel_reason: int,
    next_attempt_no: int,
    last_response: str,
) -> None:
    next_retry_at = _utcnow() + timedelta(minutes=TABLETKI_CANCEL_WARNING_RETRY_DELAY_MINUTES)
    entry = {
        "order_id": str(order.get("id") or "").strip(),
        "enterprise_code": str(enterprise_code).strip(),
        "cancel_reason": cancel_reason,
        "attempt_no": next_attempt_no,
        "next_retry_at": _to_iso(next_retry_at),
        "last_response": last_response,
        "order": order,
        "created_at": _to_iso(_utcnow()),
    }

    if not entry["order_id"] or not entry["enterprise_code"]:
        return

    with _locked_queue_file() as fh:
        entries = _load_entries_from_handle(fh)
        filtered = [
            item for item in entries
            if not (
                str(item.get("order_id") or "").strip() == entry["order_id"]
                and str(item.get("enterprise_code") or "").strip() == entry["enterprise_code"]
            )
        ]
        filtered.append(entry)
        _write_entries_to_handle(fh, filtered)

    logger.warning(
        "Tabletki cancel warning queued: order_id=%s enterprise=%s retry_no=%s next_retry_at=%s",
        entry["order_id"],
        entry["enterprise_code"],
        next_attempt_no,
        entry["next_retry_at"],
    )


def _pop_due_cancel_retries(*, enterprise_code: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    due: List[Dict[str, Any]] = []
    now = _utcnow()
    enterprise_filter = str(enterprise_code or "").strip()

    with _locked_queue_file() as fh:
        entries = _load_entries_from_handle(fh)
        keep: List[Dict[str, Any]] = []

        for item in entries:
            item_enterprise = str(item.get("enterprise_code") or "").strip()
            if enterprise_filter and item_enterprise != enterprise_filter:
                keep.append(item)
                continue

            next_retry_raw = str(item.get("next_retry_at") or "").strip()
            if not next_retry_raw:
                keep.append(item)
                continue

            try:
                next_retry_at = _from_iso(next_retry_raw)
            except Exception:
                keep.append(item)
                continue

            if len(due) < limit and next_retry_at <= now:
                due.append(item)
            else:
                keep.append(item)

        _write_entries_to_handle(fh, keep)

    return due


async def _post_with_retry(
    http_session: aiohttp.ClientSession,
    *,
    url: str,
    payload,
    headers: dict,
    operation_label: str,
) -> str:
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
                    return response_text
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


async def _send_cancel_order(
    *,
    http_session: aiohttp.ClientSession,
    endpoint_orders: str,
    headers: Dict[str, str],
    order: Dict[str, Any],
    cancel_reason: int,
    enterprise_code: Optional[str],
    queue_next_attempt_no: Optional[int],
) -> str:
    cancel_data = _build_cancel_payload(order, cancel_reason)
    logger.info(
        "🚫 Отправка отказа заказа %s (id_CancelReason=%s)",
        order["id"],
        cancel_reason,
    )
    logger.debug("Tabletki cancel payload: %s", json.dumps(cancel_data, ensure_ascii=False))

    response_text = await _post_with_retry(
        http_session,
        url=f"{endpoint_orders}/api/Orders/cancelledOrders",
        payload=cancel_data,
        headers=headers,
        operation_label=f"cancel order_id={order['id']}",
    )

    if _has_cancel_warning(response_text):
        logger.warning(
            "Tabletki cancel warning received for order_id=%s: %s",
            order["id"],
            response_text,
        )
        if enterprise_code and queue_next_attempt_no is not None and queue_next_attempt_no <= TABLETKI_CANCEL_WARNING_RETRY_MAX:
            _enqueue_cancel_retry(
                order=order,
                enterprise_code=enterprise_code,
                cancel_reason=cancel_reason,
                next_attempt_no=queue_next_attempt_no,
                last_response=response_text,
            )
        elif enterprise_code:
            send_notification(
                (
                    f"⚠️ Tabletki cancel warning persisted after delayed retries | "
                    f"order_id={order.get('id')} | response={response_text}"
                ),
                enterprise_code,
            )
        return "warning"

    return "success"


async def process_due_tabletki_cancel_retries(
    session: AsyncSession,
    *,
    enterprise_code: Optional[str] = None,
    limit: int = 20,
) -> Dict[str, int]:
    due_items = _pop_due_cancel_retries(enterprise_code=enterprise_code, limit=limit)
    stats = {
        "due_found": len(due_items),
        "processed": 0,
        "requeued": 0,
        "completed": 0,
        "notified": 0,
    }
    if not due_items:
        return stats

    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    async with aiohttp.ClientSession() as http_session:
        for item in due_items:
            stats["processed"] += 1
            item_enterprise = str(item.get("enterprise_code") or "").strip()
            order = item.get("order") or {}
            cancel_reason = int(item.get("cancel_reason") or 1)
            attempt_no = int(item.get("attempt_no") or 1)

            creds_row = (
                await session.execute(
                    select(
                        EnterpriseSettings.tabletki_login,
                        EnterpriseSettings.tabletki_password,
                    ).where(EnterpriseSettings.enterprise_code == item_enterprise)
                )
            ).first()
            if not creds_row or not creds_row[0] or not creds_row[1]:
                send_notification(
                    f"❌ Tabletki cancel retry skipped: no credentials | order_id={order.get('id')}",
                    item_enterprise,
                )
                stats["notified"] += 1
                continue

            auth_header = base64.b64encode(f"{creds_row[0]}:{creds_row[1]}".encode()).decode()
            headers = {
                "accept": "application/json",
                "Authorization": f"Basic {auth_header}",
            }

            try:
                result = await _send_cancel_order(
                    http_session=http_session,
                    endpoint_orders=endpoint_orders,
                    headers=headers,
                    order=order,
                    cancel_reason=cancel_reason,
                    enterprise_code=item_enterprise,
                    queue_next_attempt_no=attempt_no + 1,
                )
                if result == "warning":
                    if attempt_no < TABLETKI_CANCEL_WARNING_RETRY_MAX:
                        stats["requeued"] += 1
                    else:
                        stats["notified"] += 1
                    continue
                stats["completed"] += 1
            except Exception as exc:
                logger.exception(
                    "Tabletki delayed cancel retry failed: order_id=%s attempt_no=%s",
                    order.get("id"),
                    attempt_no,
                )
                if attempt_no < TABLETKI_CANCEL_WARNING_RETRY_MAX:
                    _enqueue_cancel_retry(
                        order=order,
                        enterprise_code=item_enterprise,
                        cancel_reason=cancel_reason,
                        next_attempt_no=attempt_no + 1,
                        last_response=str(exc),
                    )
                    stats["requeued"] += 1
                else:
                    send_notification(
                        (
                            f"❌ Tabletki cancel retry exhausted | order_id={order.get('id')} | "
                            f"attempt_no={attempt_no} | err={exc}"
                        ),
                        item_enterprise,
                    )
                    stats["notified"] += 1

    return stats


async def send_orders_to_tabletki(
    session: AsyncSession,
    orders: list,
    tabletki_login: str,
    tabletki_password: str,
    cancel_reason: int,
    enterprise_code: Optional[str] = None,
):
    """
    Отправляет заказы в Tabletki.ua по API:
    - статус 4 или 6: подтверждение → /api/orders
    - статус 7 или все qtyShip == 0: отказ → /api/Orders/cancelledOrders
    Поле id_CancelReason берётся из аргумента cancel_reason.
    На non-2xx и сетевых ошибках делает retry и затем пробрасывает ошибку.
    На специальном cancel-warning ставит delayed retry в файловую очередь без миграции.
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
                await _send_cancel_order(
                    http_session=http_session,
                    endpoint_orders=endpoint_orders,
                    headers=headers,
                    order=order,
                    cancel_reason=cancel_reason,
                    enterprise_code=enterprise_code,
                    queue_next_attempt_no=1,
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

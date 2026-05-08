from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
from urllib import request, error

from app.business.reporting.orders.report_service import build_summary
from app.database import AsyncSessionLocal


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

KIEV_TZ = ZoneInfo("Europe/Kiev")


def _is_enabled() -> bool:
    return str(os.getenv("ORDER_REPORT_TELEGRAM_ENABLED", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _bot_token() -> str | None:
    return (
        os.getenv("ORDER_REPORT_TELEGRAM_BOT_TOKEN")
        or os.getenv("TELEGRAM_DEVELOP")
        or os.getenv("TELEGRAM_BOT_TOKEN")
    )


def _chat_ids() -> list[str]:
    raw = (
        os.getenv("ORDER_REPORT_TELEGRAM_CHAT_IDS")
        or os.getenv("ORDER_REPORT_TELEGRAM_CHAT_ID")
        or os.getenv("TELEGRAM_CHAT_IDS")
        or os.getenv("TELEGRAM_CHAT_ID")
        or ""
    )
    chat_ids = [part.strip() for part in raw.split(",") if part.strip()]
    return chat_ids or ["807661373", "1041598119"]


def _money(value: object) -> str:
    try:
        number = Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        number = Decimal("0")
    rounded = number.quantize(Decimal("1"))
    sign, digits, exponent = rounded.as_tuple()
    text = "".join(str(digit) for digit in digits) or "0"
    groups: list[str] = []
    while text:
        groups.append(text[-3:])
        text = text[:-3]
    formatted = " ".join(reversed(groups))
    return f"-{formatted}" if sign else formatted


def _today_window(now: datetime) -> tuple[datetime, datetime]:
    period_from = now.replace(hour=0, minute=0, second=0, microsecond=0)
    period_to = now.replace(second=59, microsecond=0)
    return period_from.replace(tzinfo=None), period_to.replace(tzinfo=None)


async def build_today_order_report_message() -> tuple[str, dict[str, object]]:
    now = datetime.now(KIEV_TZ)
    period_from, period_to = _today_window(now)
    async with AsyncSessionLocal() as session:
        # Source of truth: local PostgreSQL reporting tables, not Google Sheets/Excel.
        summary = await build_summary(
            session,
            period_from=period_from,
            period_to=period_to,
            enterprise_code=None,
        )

    total_orders = int(summary.get("total_orders") or 0)
    profit_basis = str(os.getenv("ORDER_REPORT_TELEGRAM_PROFIT_BASIS", "orders")).strip().lower()
    net_profit_key = "net_profit_amount" if profit_basis == "sales" else "order_net_profit_amount"
    net_profit = summary.get(net_profit_key) or "0"
    now_str = now.strftime("%d.%m.%Y %H:%M")
    message = (
        "📊 <b>Orders — daily cumulative</b>\n"
        f"🕒 {now_str}\n\n"
        f"Total orders: <b>{total_orders}</b>\n"
        f"Net profit: <b>{_money(net_profit)}</b>"
    )
    meta = {
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
        "total_orders": total_orders,
        "net_profit": str(net_profit),
        "profit_basis": "sales" if profit_basis == "sales" else "orders",
    }
    return message, meta


def _send_telegram_message(text: str) -> None:
    token = _bot_token()
    chat_ids = _chat_ids()
    if not token:
        raise RuntimeError("ORDER_REPORT_TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN is not set")
    if not chat_ids:
        raise RuntimeError("ORDER_REPORT_TELEGRAM_CHAT_IDS is not set")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for chat_id in chat_ids:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        last_error: str | None = None
        for attempt in range(1, 4):
            try:
                body = json.dumps(payload).encode("utf-8")
                req = request.Request(
                    url,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(req, timeout=15) as response:
                    status_code = int(getattr(response, "status", 0) or 0)
                    response_body = response.read().decode("utf-8", errors="replace")
                if status_code == 200:
                    last_error = None
                    break
                last_error = f"HTTP {status_code}: {response_body}"
            except error.HTTPError as exc:
                response_body = exc.read().decode("utf-8", errors="replace")
                last_error = f"HTTP {exc.code}: {response_body}"
            except Exception as exc:
                last_error = str(exc)
            if attempt < 3:
                import time

                time.sleep(2 * attempt)
        if last_error:
            raise RuntimeError(f"Telegram send failed for chat_id={chat_id}: {last_error}")


async def send_today_order_report_to_telegram() -> dict[str, object]:
    message, meta = await build_today_order_report_message()
    await asyncio.to_thread(_send_telegram_message, message)
    logger.info("Order reporting Telegram sent: %s", meta)
    return meta


def _seconds_until_next_hour(now: datetime) -> float:
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    return max(1.0, (next_hour - now).total_seconds())


async def schedule_order_reporting_telegram_tasks() -> None:
    if not _is_enabled():
        logger.warning(
            "Order reporting Telegram scheduler disabled. Set ORDER_REPORT_TELEGRAM_ENABLED=true to run."
        )
        return

    send_on_start = str(os.getenv("ORDER_REPORT_TELEGRAM_SEND_ON_START", "false")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if send_on_start:
        try:
            await send_today_order_report_to_telegram()
        except Exception:
            logger.exception("Order reporting Telegram initial send failed")

    while True:
        sleep_seconds = _seconds_until_next_hour(datetime.now(KIEV_TZ))
        logger.info("Order reporting Telegram: next run in %.0f seconds", sleep_seconds)
        await asyncio.sleep(sleep_seconds)
        try:
            await send_today_order_report_to_telegram()
        except Exception:
            logger.exception("Order reporting Telegram send failed")


if __name__ == "__main__":
    asyncio.run(schedule_order_reporting_telegram_tasks())

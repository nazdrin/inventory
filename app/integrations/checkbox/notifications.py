from __future__ import annotations

import logging

from app.integrations.checkbox.config import CheckboxSettings
from app.models import CheckboxReceipt, CheckboxShift
from app.services.notification_service import send_notification


logger = logging.getLogger("checkbox.notifications")


def notify_receipt_fiscalized(settings: CheckboxSettings, row: CheckboxReceipt) -> None:
    if not (settings.telegram_notifications_enabled and settings.telegram_receipt_notifications_enabled):
        return
    message = (
        "Checkbox fiscalized receipt\n"
        f"Mode: {'TEST' if settings.test_mode else 'PROD'}\n"
        f"SalesDrive order: {row.salesdrive_order_id}\n"
        f"External ID: {row.salesdrive_external_id or '-'}\n"
        f"Cash register: {row.cash_register_code or '-'}\n"
        f"Items: {row.items_count or 0}\n"
        f"Total: {row.total_amount or 0}\n"
        f"URL: {row.receipt_url or '-'}"
    )
    try:
        send_notification(message, row.enterprise_code)
    except Exception:
        logger.exception("Failed to send Checkbox receipt notification")


def notify_shift_opened(settings: CheckboxSettings, shift: CheckboxShift) -> None:
    if not (settings.telegram_notifications_enabled and settings.telegram_shift_notifications_enabled):
        return
    message = (
        "Checkbox shift opened\n"
        f"Mode: {'TEST' if settings.test_mode else 'PROD'}\n"
        f"Cash register: {shift.cash_register_code or '-'}\n"
        f"Shift ID: {shift.checkbox_shift_id or '-'}"
    )
    try:
        send_notification(message, shift.enterprise_code)
    except Exception:
        logger.exception("Failed to send Checkbox shift opened notification")


def notify_shift_closed(settings: CheckboxSettings, shift: CheckboxShift) -> None:
    if not (settings.telegram_notifications_enabled and settings.telegram_shift_notifications_enabled):
        return
    message = (
        "Checkbox shift closed\n"
        f"Mode: {'TEST' if settings.test_mode else 'PROD'}\n"
        f"Cash register: {shift.cash_register_code or '-'}\n"
        f"Shift ID: {shift.checkbox_shift_id or '-'}\n"
        f"Receipts: {shift.receipts_count or 0}\n"
        f"Total: {shift.receipts_total_amount or 0}"
    )
    try:
        send_notification(message, shift.enterprise_code)
    except Exception:
        logger.exception("Failed to send Checkbox shift closed notification")

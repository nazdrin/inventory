from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderStatusInfo:
    status_id: int | None
    status_name: str | None
    status_group: str
    is_order: bool
    is_sale: bool
    is_return: bool
    is_cancelled: bool
    is_deleted: bool


STATUS_NAMES: dict[int, str] = {
    1: "Новый",
    4: "Обработано",
    5: "Продажа",
    6: "Отказ",
    7: "Возврат",
    8: "Удален",
    11: "В дороге НП",
    12: "Прибыл НП",
    13: "Получено без наложки",
    14: "Отказ НП",
    15: "Изменен адрес НП",
    16: "Недозвон 2",
    17: "Свой отказ",
    18: "Ожидание предоплаты",
    19: "Недозвон 1",
    20: "Потом",
    21: "На отправку",
}

ACTIVE_STATUSES = {1, 4, 11, 12, 15, 16, 18, 19, 20, 21}
SALE_STATUSES = {5, 13}
RETURN_STATUSES = {7, 14}
CANCELLED_STATUSES = {6, 17}
DELETED_STATUSES = {8}

STATUS_FUNNEL_ORDER = [
    1,
    19,
    16,
    18,
    20,
    21,
    4,
    11,
    12,
    15,
    5,
    13,
    6,
    17,
    14,
    7,
    8,
]


def classify_status(status_id: int | None, status_name: str | None = None) -> OrderStatusInfo:
    if status_id in SALE_STATUSES:
        group = "sale"
    elif status_id in RETURN_STATUSES:
        group = "return"
    elif status_id in CANCELLED_STATUSES:
        group = "cancelled"
    elif status_id in DELETED_STATUSES:
        group = "deleted"
    else:
        group = "active"

    return OrderStatusInfo(
        status_id=status_id,
        status_name=status_name or STATUS_NAMES.get(status_id or -1),
        status_group=group,
        is_order=group == "active",
        is_sale=group == "sale",
        is_return=group == "return",
        is_cancelled=group == "cancelled",
        is_deleted=group == "deleted",
    )

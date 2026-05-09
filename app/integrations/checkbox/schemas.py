from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class CheckboxMappedReceipt:
    checkbox_order_id: str
    payload: dict[str, Any]
    total_amount: Decimal
    items_count: int
    payment_label: str
    payment_type: str

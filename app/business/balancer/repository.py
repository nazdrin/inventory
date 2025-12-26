from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime

# ВАЖНО: тут мы пока не пишем в БД, потому что нужно понять,
# как именно у тебя устроены сессии (sync/async) и где фабрика сессий.
# На следующем шаге подключим SQLAlchemy session и реальные insert'ы.

def create_policy_log_record(
    *,
    mode: str,
    config_version: int,
    city: str,
    supplier: str,
    segment_id: str,
    segment_start: datetime,
    segment_end: datetime,
    rules: list[dict[str, Any]],
    min_porog_by_band: dict[str, Any],
    reason: str,
    reason_details: Optional[dict[str, Any]] = None,
    config_snapshot: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Готовит dict для вставки в balancer_policy_log.
    Реальная запись в БД будет на шаге 2.5.
    """
    return {
        "mode": mode,
        "config_version": config_version,
        "config_snapshot": config_snapshot,
        "city": city,
        "supplier": supplier,
        "segment_id": segment_id,
        "segment_start": segment_start,
        "segment_end": segment_end,
        "rules": rules,
        "min_porog_by_band": min_porog_by_band,
        "reason": reason,
        "reason_details": reason_details,
        "hash": None,        # на шаге 2.5 сделаем hash/idempotency
        "is_applied": True,
    }
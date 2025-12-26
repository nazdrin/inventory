from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class PolicyPayload:
    rules: List[Dict[str, Any]]              # [{"band_id":"B1","porog":0.17}, ...]
    min_porog_by_band: Dict[str, float]      # {"B1":0.15, ...}
    reason: str                               # schedule / best_30d / ...
    reason_details: Dict[str, Any] | None = None


def build_test_policy(profile: Dict[str, Any]) -> PolicyPayload:
    """
    Этап 2.1: заглушка.
    В шаге 2.4 добавим реальный test_schedule + таблицу состояния.
    Сейчас просто берём min_porog_by_band как porog_used.
    """
    min_porog = profile.get("min_porog_by_band", {})
    if not min_porog:
        raise ValueError("min_porog_by_band is required in profile")

    rules = [{"band_id": band_id, "porog": float(p)} for band_id, p in min_porog.items()]
    return PolicyPayload(
        rules=rules,
        min_porog_by_band={k: float(v) for k, v in min_porog.items()},
        reason="schedule_stub",
        reason_details={"note": "step 2.1 stub: porog = min_porog"},
    )
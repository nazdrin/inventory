from __future__ import annotations

from typing import Any, Dict

from .config import load_config
from .segments import resolve_current_segment
from .policy import build_test_policy
from .repository import create_policy_log_record


def start_segment_dry_run() -> list[dict[str, Any]]:
    """
    Этап 2.1: без записи в БД.
    Возвращает список подготовленных записей policy_log по всем профилям.
    """
    cfg = load_config()
    out: list[dict[str, Any]] = []

    for profile in cfg.profiles:
        scope = profile.get("scope", {})
        cities = scope.get("cities", [])
        suppliers = scope.get("suppliers", [])

        if not cities or not suppliers:
            continue

        seg = resolve_current_segment(profile)
        policy = build_test_policy(profile)

        for city in cities:
            for supplier in suppliers:
                rec = create_policy_log_record(
                    mode=profile.get("mode", "TEST"),
                    config_version=int(cfg.balancer.get("version", 1)),
                    city=city,
                    supplier=supplier,
                    segment_id=seg.segment_id,
                    segment_start=seg.start,
                    segment_end=seg.end,
                    rules=policy.rules,
                    min_porog_by_band=policy.min_porog_by_band,
                    reason=policy.reason,
                    reason_details=policy.reason_details,
                    config_snapshot=cfg.balancer,  # пока целиком, позже сузим
                )
                out.append(rec)

    return out
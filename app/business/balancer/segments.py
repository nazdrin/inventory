from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any


TZ = ZoneInfo("Europe/Kyiv")


@dataclass(frozen=True)
class SegmentWindow:
    segment_id: str
    start: datetime
    end: datetime


def _parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(hour=int(hh), minute=int(mm))


def resolve_current_segment(profile: Dict[str, Any], now: datetime | None = None) -> SegmentWindow:
    """
    Находит активный сегмент по now (Europe/Kyiv).
    Поддерживает сегменты, которые пересекают полночь (например 21:00–09:00).
    """
    now = now or datetime.now(TZ)
    segments = profile.get("time_segments", [])
    if not segments:
        raise ValueError("No time_segments in profile")

    is_weekend = now.weekday() >= 5  # 5=Sat,6=Sun

    for seg in segments:
        seg_type = seg.get("type", "ALL")
        if seg_type == "WEEKDAY" and is_weekend:
            continue
        if seg_type == "WEEKEND" and not is_weekend:
            continue

        start_t = _parse_hhmm(seg["start"])
        end_t = _parse_hhmm(seg["end"])

        # базовые даты
        start_dt = now.replace(hour=start_t.hour, minute=start_t.minute, second=0, microsecond=0)
        end_dt = now.replace(hour=end_t.hour, minute=end_t.minute, second=0, microsecond=0)

        # если пересекает полночь
        if end_dt <= start_dt:
            # сегмент: start сегодня, end завтра
            if now >= start_dt:
                end_dt = end_dt + timedelta(days=1)
            else:
                # сегмент начался вчера, заканчивается сегодня
                start_dt = start_dt - timedelta(days=1)

        if start_dt <= now < end_dt:
            return SegmentWindow(segment_id=seg["segment_id"], start=start_dt, end=end_dt)

    raise ValueError(f"No active segment found for now={now.isoformat()}")
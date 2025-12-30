from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List
from decimal import Decimal
from sqlalchemy import select

from app.business.balancer.repository import get_best_porog_30d
from sqlalchemy.exc import IntegrityError

from app.database import get_async_db
from app.models import BalancerTestState


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


def _clamp_decimal(value: Decimal, lo: Decimal, hi: Decimal) -> Decimal:
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


def _next_porog(current: Decimal, step: Decimal, lo: Decimal, hi: Decimal, direction: int) -> tuple[Decimal, int]:
    """Пилообразный график: min→max→min.

    Возвращает (next_current_porog, next_direction).
    """
    nxt = current + (Decimal(direction) * step)

    if nxt > hi:
        # разворот вниз
        nxt = hi - step
        nxt = _clamp_decimal(nxt, lo, hi)
        return nxt, -1

    if nxt < lo:
        # разворот вверх
        nxt = lo + step
        nxt = _clamp_decimal(nxt, lo, hi)
        return nxt, 1

    return nxt, direction


async def build_test_policy_async(
    profile: Dict[str, Any],
    *,
    profile_name: str,
    city: str,
    supplier: str,
    segment_id: str,
    day_date: date,
    suppliers_in_profile: list[str] | None = None,
    advance_state: bool = True,
) -> PolicyPayload:
    """Реальная TEST-политика на базе таблицы balancer_test_state.

    Для каждого band берём текущий current_porog как porog_used,
    затем пересчитываем следующий current_porog по step/direction и сохраняем обратно.

    ВНИМАНИЕ: функция изменяет состояние (продвигает график), поэтому вызывает commit.
    """

    min_porog_by_band = profile.get("min_porog_by_band", {})
    if not min_porog_by_band:
        raise ValueError("min_porog_by_band is required in profile")

    sched = profile.get("test_schedule", {})
    step_f = sched.get("step")
    max_porog_by_band = sched.get("max_porog_by_band", {})

    if step_f is None:
        raise ValueError("test_schedule.step is required in profile for TEST mode")

    step = Decimal(str(step_f))

    # Если профиль охватывает несколько поставщиков, состояние TEST-графика должно быть общим,
    # иначе будет "переток" и некорректная статистика. Используем общий ключ supplier_state_key.
    if suppliers_in_profile and len(suppliers_in_profile) > 1:
        supplier_state_key = "|".join(sorted([str(s) for s in suppliers_in_profile]))
    else:
        supplier_state_key = supplier

    # Если в профиле несколько поставщиков, состояние должен продвигать только один "лидер",
    # иначе график будет сдвигаться несколько раз за один сегмент.
    if suppliers_in_profile and len(suppliers_in_profile) > 1:
        leader_supplier = sorted([str(s) for s in suppliers_in_profile])[0]
        advance_state_effective = bool(advance_state) and (str(supplier) == leader_supplier)
    else:
        leader_supplier = None
        advance_state_effective = bool(advance_state)

    # Детерминированный порядок band'ов
    band_ids = sorted(min_porog_by_band.keys())

    rules: list[dict[str, Any]] = []

    # Получаем 1 async-сессию
    async with get_async_db() as db:
        now = datetime.utcnow()

        for band_id in band_ids:
            lo = Decimal(str(min_porog_by_band[band_id]))
            hi_val = max_porog_by_band.get(band_id)
            if hi_val is None:
                raise ValueError(f"test_schedule.max_porog_by_band must contain band {band_id}")
            hi = Decimal(str(hi_val))

            if hi < lo:
                raise ValueError(f"max_porog ({hi}) < min_porog ({lo}) for band {band_id}")

            # 1) Пробуем прочитать состояние
            q = select(BalancerTestState).where(
                BalancerTestState.profile_name == profile_name,
                BalancerTestState.city == city,
                BalancerTestState.supplier == supplier_state_key,
                BalancerTestState.segment_id == segment_id,
                BalancerTestState.band_id == band_id,
                BalancerTestState.day_date == day_date,
            )
            res = await db.execute(q)
            row = res.scalar_one_or_none()

            # 2) Если нет — создаём (idempotent через unique)
            if row is None:
                row = BalancerTestState(
                    profile_name=profile_name,
                    mode="TEST",
                    city=city,
                    supplier=supplier_state_key,
                    segment_id=segment_id,
                    band_id=band_id,
                    day_date=day_date,
                    current_porog=float(lo),
                    step=float(step),
                    min_porog=float(lo),
                    max_porog=float(hi),
                    direction=1,
                    updated_at=now,
                )
                db.add(row)
                try:
                    await db.flush()
                except IntegrityError:
                    # кто-то успел вставить — перечитаем
                    await db.rollback()
                    res = await db.execute(q)
                    row = res.scalar_one()

            # 3) Используем текущий порог как применяемый
            porog_used = Decimal(str(row.current_porog))
            # safety clamp
            porog_used = _clamp_decimal(porog_used, lo, hi)

            rules.append({"band_id": band_id, "porog": float(porog_used)})

            # 4) Считаем следующий и сохраняем (только если это "ведущий" вызов для группы)
            if advance_state_effective:
                direction = int(row.direction)
                next_current, next_dir = _next_porog(porog_used, step, lo, hi, direction)

                row.current_porog = float(next_current)
                row.step = float(step)
                row.min_porog = float(lo)
                row.max_porog = float(hi)
                row.direction = int(next_dir)
                row.updated_at = now

        if advance_state_effective:
            await db.commit()
        else:
            await db.rollback()

    return PolicyPayload(
        rules=rules,
        min_porog_by_band={k: float(v) for k, v in min_porog_by_band.items()},
        reason="schedule",
        reason_details={
            "schedule": "test_schedule",
            "step": float(step),
            "day_date": day_date.isoformat(),
            "supplier_state_key": supplier_state_key,
            "advance_state_requested": bool(advance_state),
            "advance_state_effective": advance_state_effective,
            "advance_state": advance_state_effective,
            "leader_supplier": leader_supplier,
        },
    )


# LIVE‑политика: best_30d
async def build_live_policy_async(
    profile: Dict[str, Any],
    *,
    city: str,
    supplier: str,
    segment_id: str,
    day_date: date,
    suppliers_in_profile: list[str] | None = None,
) -> PolicyPayload:
    """
    LIVE‑политика:
    - first try LIVE best_30d
    - else use TEST seed best_30d
    - else fallback min_porog_by_band
    """

    min_porog_by_band = profile.get("min_porog_by_band", {})
    if not min_porog_by_band:
        raise ValueError("min_porog_by_band is required in profile")

    # Если профиль охватывает несколько поставщиков, статистика best_30d должна быть общей
    # (как и состояние графика в TEST). Поэтому используем общий ключ supplier_state_key.
    if suppliers_in_profile and len(suppliers_in_profile) > 1:
        supplier_state_key = "|".join(sorted([str(s) for s in suppliers_in_profile]))
    else:
        supplier_state_key = supplier

    # 1) Prefer LIVE best_30d; if absent per-band, seed from TEST best_30d
    best_live_map = await get_best_porog_30d(
        mode="LIVE",
        city=city,
        supplier=supplier_state_key,
        segment_id=segment_id,
        day_date=day_date,
    )

    best_test_seed_map = await get_best_porog_30d(
        mode="TEST",
        city=city,
        supplier=supplier_state_key,
        segment_id=segment_id,
        day_date=day_date,
    )

    # В LIVE тоже ограничиваем порог сверху, если задан max_porog_by_band (обычно из test_schedule)
    sched = profile.get("test_schedule", {})
    max_porog_by_band = sched.get("max_porog_by_band", {})

    rules: list[dict[str, Any]] = []
    band_sources: dict[str, str] = {}

    for band_id in sorted(min_porog_by_band.keys()):
        min_porog = min_porog_by_band[band_id]
        if band_id in best_live_map:
            porog = Decimal(str(best_live_map[band_id]))
            band_sources[band_id] = "best_30d_live"
        elif band_id in best_test_seed_map:
            porog = Decimal(str(best_test_seed_map[band_id]))
            band_sources[band_id] = "seed_best_30d_test"
        else:
            porog = Decimal(str(min_porog))
            band_sources[band_id] = "fallback_min_porog"

        hi_val = max_porog_by_band.get(band_id)
        hi = Decimal(str(hi_val)) if hi_val is not None else Decimal("1.0")

        porog = _clamp_decimal(
            porog,
            Decimal(str(min_porog)),
            hi,
        )

        rules.append(
            {
                "band_id": band_id,
                "porog": float(porog),
            }
        )

    return PolicyPayload(
        rules=rules,
        min_porog_by_band={k: float(v) for k, v in min_porog_by_band.items()},
        reason="best_30d",
        reason_details={
            "source": "balancer_segment_stats_live_then_test_seed",
            "day_date": day_date.isoformat(),
            "band_sources": band_sources,
            "supplier_state_key": supplier_state_key,
        },
    )
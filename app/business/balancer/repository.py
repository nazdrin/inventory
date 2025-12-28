from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import BalancerPolicyLog, BalancerOrderFacts, BalancerSegmentStats

__all__ = [
    "create_policy_log_record",
    "compute_policy_hash",
    "upsert_policy_log",
    "get_last_applied_policies",
    "upsert_order_fact",
    "get_order_facts_for_policy",
    "upsert_segment_stats",
    "get_segment_stats_for_day_scope",
    "update_day_metrics_for_segment",
    "get_best_porog_30d",
]


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
    """Готовит dict для вставки в balancer_policy_log.

    Здесь НЕ пишем в БД — только формируем payload.
    Реальная запись в БД выполняется функцией upsert_policy_log().
    """

    return {
        "mode": mode,
        "config_version": int(config_version),
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
        "hash": None,  # вычислим на шаге записи
        "is_applied": True,
    }


def compute_policy_hash(payload: dict[str, Any]) -> str:
    """Стабильный hash для идемпотентности StartSegment.

    Хэшируем только то, что однозначно определяет применяемую политику на сегмент.
    """

    base = {
        "mode": payload.get("mode"),
        "config_version": payload.get("config_version"),
        "city": payload.get("city"),
        "supplier": payload.get("supplier"),
        "segment_id": payload.get("segment_id"),
        "segment_start": str(payload.get("segment_start")),
        "segment_end": str(payload.get("segment_end")),
        "rules": payload.get("rules"),
        "min_porog_by_band": payload.get("min_porog_by_band"),
    }

    raw = json.dumps(base, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


async def upsert_policy_log(payload: dict[str, Any]) -> BalancerPolicyLog:
    """Создаёт запись в balancer_policy_log, но не дублирует при повторном запуске.

    Ищем по hash. Если нашли — возвращаем существующую запись.
    Если нет — создаём новую.
    """

    payload = dict(payload)
    payload_hash = payload.get("hash") or compute_policy_hash(payload)
    payload["hash"] = payload_hash

    async with get_async_db() as db:
        q = select(BalancerPolicyLog).where(BalancerPolicyLog.hash == payload_hash)
        res = await db.execute(q)
        existing = res.scalar_one_or_none()
        if existing is not None:
            return existing

        obj = BalancerPolicyLog(**payload)
        db.add(obj)
        await db.commit()
        await db.refresh(obj)
        return obj


async def get_last_applied_policies() -> list[BalancerPolicyLog]:
    """Возвращает последние применённые политики по каждой паре (mode, city, supplier).

    Используется job'ом сбора заказов (этап 2.6), чтобы понять, за какой сегмент
    собирать заказы прямо сейчас.
    """

    async with get_async_db() as db:
        q = (
            select(BalancerPolicyLog)
            .where(BalancerPolicyLog.is_applied.is_(True))
            .order_by(
                BalancerPolicyLog.mode.asc(),
                BalancerPolicyLog.city.asc(),
                BalancerPolicyLog.supplier.asc(),
                BalancerPolicyLog.segment_start.desc(),
            )
            .distinct(
                BalancerPolicyLog.mode,
                BalancerPolicyLog.city,
                BalancerPolicyLog.supplier,
            )
        )
        res = await db.execute(q)
        return list(res.scalars().all())


async def upsert_order_fact(payload: dict[str, Any]) -> BalancerOrderFacts:
    """Идемпотентно пишет строку в balancer_order_facts.

    Уникальность обеспечивается парой (policy_log_id, order_id).
    Если такая запись уже есть — возвращаем её.
    """

    payload = dict(payload)
    policy_log_id = payload["policy_log_id"]
    order_id = str(payload["order_id"])

    async with get_async_db() as db:
        q = select(BalancerOrderFacts).where(
            BalancerOrderFacts.policy_log_id == policy_log_id,
            BalancerOrderFacts.order_id == order_id,
        )
        res = await db.execute(q)
        existing = res.scalar_one_or_none()
        if existing is not None:
            return existing

        obj = BalancerOrderFacts(**payload)
        db.add(obj)
        await db.commit()
        await db.refresh(obj)
        return obj


# --- Aggregation step segment functions ---

async def get_order_facts_for_policy(policy_log_id: int) -> list[BalancerOrderFacts]:
    """Возвращает все факты заказов для конкретной политики.

    Используется на этапе агрегации (2.7).
    Статусы НЕ фильтруем — в зачёт идут все записи.
    """
    async with get_async_db() as db:
        q = select(BalancerOrderFacts).where(
            BalancerOrderFacts.policy_log_id == policy_log_id
        )
        res = await db.execute(q)
        return list(res.scalars().all())


async def upsert_segment_stats(payload: dict[str, Any]) -> BalancerSegmentStats:
    """Идемпотентно пишет агрегированную статистику сегмента.

    Уникальность логическая:
    (policy_log_id, band_id)
    """

    payload = dict(payload)
    policy_log_id = payload["policy_log_id"]
    band_id = payload["band_id"]

    async with get_async_db() as db:
        q = select(BalancerSegmentStats).where(
            BalancerSegmentStats.policy_log_id == policy_log_id,
            BalancerSegmentStats.band_id == band_id,
        )
        res = await db.execute(q)
        existing = res.scalar_one_or_none()

        if existing is not None:
            for k, v in payload.items():
                setattr(existing, k, v)
            await db.commit()
            await db.refresh(existing)
            return existing

        obj = BalancerSegmentStats(**payload)
        db.add(obj)
        await db.commit()
        await db.refresh(obj)
        return obj


# --- Day metrics (step 2.8) ---

async def get_segment_stats_for_day_scope(
    *,
    mode: str,
    city: str,
    supplier: str,
    day_date,
) -> list[BalancerSegmentStats]:
    """
    Возвращает все строки balancer_segment_stats за конкретный день
    для связки (mode, city, supplier), по всем segment_id и band_id.
    """
    async with get_async_db() as db:
        q = (
            select(BalancerSegmentStats)
            .where(
                BalancerSegmentStats.mode == mode,
                BalancerSegmentStats.city == city,
                BalancerSegmentStats.supplier == supplier,
                BalancerSegmentStats.day_date == day_date,
            )
        )
        res = await db.execute(q)
        return list(res.scalars().all())


async def update_day_metrics_for_segment(
    *,
    policy_log_id: int,
    segment_id: str,
    day_total_orders: int,
    segment_share: float | None,
) -> int:
    """
    Обновляет day_total_orders и segment_share
    для ВСЕХ band_id одного сегмента (segment_id) в рамках policy_log_id.
    Возвращает количество обновлённых строк.
    """
    async with get_async_db() as db:
        q = select(BalancerSegmentStats).where(
            BalancerSegmentStats.policy_log_id == policy_log_id,
            BalancerSegmentStats.segment_id == segment_id,
        )
        res = await db.execute(q)
        rows = list(res.scalars().all())

        for r in rows:
            r.day_total_orders = int(day_total_orders)
            r.segment_share = segment_share

        if rows:
            await db.commit()

        return len(rows)


async def get_best_porog_30d(
    *,
    mode: str,
    city: str,
    supplier: str,
    segment_id: str,
    day_date,
    lookback_days: int = 30,
) -> dict[str, float]:
    """
    Возвращает лучший porog по каждому band_id за последние N дней.

    Критерий выбора:
    - максимальный excess_profit_sum
    - только строки где orders_sample_ok = true
    - porog_used >= min_porog (гарантировано CHECK-констрейнтом)

    Возвращает:
    { band_id: porog_used }
    """
    from datetime import timedelta

    start_date = day_date - timedelta(days=int(lookback_days))

    async with get_async_db() as db:
        q = (
            select(BalancerSegmentStats)
            .where(
                BalancerSegmentStats.mode == mode,
                BalancerSegmentStats.city == city,
                BalancerSegmentStats.supplier == supplier,
                BalancerSegmentStats.segment_id == segment_id,
                BalancerSegmentStats.day_date >= start_date,
                BalancerSegmentStats.day_date < day_date,
                BalancerSegmentStats.orders_sample_ok.is_(True),
            )
            .order_by(
                BalancerSegmentStats.band_id.asc(),
                BalancerSegmentStats.excess_profit_sum.desc(),
            )
        )

        res = await db.execute(q)
        rows = list(res.scalars().all())

    best: dict[str, float] = {}

    for r in rows:
        band_id = str(r.band_id)
        if band_id in best:
            continue
        best[band_id] = float(r.porog_used)

    return best
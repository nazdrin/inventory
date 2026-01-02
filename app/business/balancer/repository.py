from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from sqlalchemy import select, delete

import os

from app.database import get_async_db
from app.models import BalancerPolicyLog, BalancerOrderFacts, BalancerSegmentStats

__all__ = [
    "create_policy_log_record",
    "compute_policy_hash",
    "upsert_policy_log",
    "get_last_applied_policies",
    "get_applied_policies_by_segment_end",
    "upsert_order_fact",
    "get_order_facts_for_policy",
    "upsert_segment_stats",
    "get_segment_stats_for_day_scope",
    "update_day_metrics_for_segment",
    "get_best_porog_30d",
    "get_best_porog_30d_global_test",
    "get_best_porog_30d_global_live",
    "get_best_porog_30d_global_best",
    "get_active_policy_for_pricing",
    "cleanup_old_balancer_data",
    "get_policy_log_ids_older_than",
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


async def upsert_policy_log(payload: dict[str, Any]) -> tuple[BalancerPolicyLog, bool]:
    """Создаёт запись в balancer_policy_log, но не дублирует при повторном запуске.

    Ищем по hash. Если нашли — возвращаем существующую запись и created=False.
    Если нет — создаём новую и возвращаем created=True.

    Важно: jobs.py ожидает распаковку `obj, created = await upsert_policy_log(payload)`.
    """

    payload = dict(payload)
    payload_hash = payload.get("hash") or compute_policy_hash(payload)
    payload["hash"] = payload_hash

    async with get_async_db() as db:
        q = select(BalancerPolicyLog).where(BalancerPolicyLog.hash == payload_hash)
        res = await db.execute(q)
        existing = res.scalar_one_or_none()
        if existing is not None:
            # If we re-run start_segment with the same hash, we still want to
            # refresh mutable fields (e.g. reason/reason_details) because
            # upstream normalization logic may have changed.
            # NOTE: We do NOT change `hash`.
            update_keys = (
                "config_version",
                "config_snapshot",
                "city",
                "supplier",
                "segment_id",
                "segment_start",
                "segment_end",
                "rules",
                "min_porog_by_band",
                "reason",
                "reason_details",
                "is_applied",
            )
            for k in update_keys:
                if k in payload:
                    setattr(existing, k, payload.get(k))
            await db.commit()
            await db.refresh(existing)
            return existing, False

        obj = BalancerPolicyLog(**payload)
        db.add(obj)
        await db.commit()
        await db.refresh(obj)
        return obj, True



async def get_last_applied_policies() -> list[BalancerPolicyLog]:
    """Возвращает последние применённые политики по каждой паре (mode, city, supplier).

    Используется job'ом сбора заказов (этап 2.6), чтобы понять, за какой сегмент
    собирать заказы прямо сейчас.
    """

    async with get_async_db() as db:
        run_mode = os.getenv("BALANCER_RUN_MODE")
        q = select(BalancerPolicyLog).where(BalancerPolicyLog.is_applied.is_(True))
        if run_mode in ("LIVE", "TEST"):
            q = q.where(BalancerPolicyLog.mode == run_mode)
        q = q.order_by(
            BalancerPolicyLog.mode.asc(),
            BalancerPolicyLog.city.asc(),
            BalancerPolicyLog.supplier.asc(),
            BalancerPolicyLog.segment_start.desc(),
        ).distinct(
            BalancerPolicyLog.mode,
            BalancerPolicyLog.city,
            BalancerPolicyLog.supplier,
        )
        res = await db.execute(q)
        return list(res.scalars().all())


# --- New helper and function: get_applied_policies_by_segment_end ---

def _parse_dt_utc(value: str) -> datetime:
    """Parse ISO datetime string into timezone-aware UTC datetime."""
    s = (value or "").strip()
    if not s:
        raise ValueError("empty datetime")
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)



async def get_applied_policies_by_segment_end(
    *,
    segment_end: datetime | str,
    mode: str | None = None,
) -> list[BalancerPolicyLog]:
    """Возвращает применённые политики ровно для закрытого сегмента (по segment_end).

    Используется шедуллером/джобой, которая запускается ПОСЛЕ окончания сегмента,
    чтобы собрать заказы и записать факты строго для завершившегося окна.

    segment_end:
      - datetime (tz-aware желательно) или ISO-строка (UTC)
    mode:
      - None -> берём из env BALANCER_RUN_MODE (если LIVE/TEST)
      - иначе конкретно "LIVE" или "TEST"

    Возвращает список политик (по всем city/supplier), которые:
      - is_applied = true
      - segment_end == указанному
      - mode == выбранному (если задан)
    """

    if isinstance(segment_end, str):
        seg_end = _parse_dt_utc(segment_end)
    else:
        seg_end = segment_end
        if seg_end.tzinfo is None:
            seg_end = seg_end.replace(tzinfo=timezone.utc)
        seg_end = seg_end.astimezone(timezone.utc)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = None

    async with get_async_db() as db:
        q = (
            select(BalancerPolicyLog)
            .where(
                BalancerPolicyLog.is_applied.is_(True),
                BalancerPolicyLog.segment_end == seg_end,
            )
            .order_by(
                BalancerPolicyLog.mode.asc(),
                BalancerPolicyLog.city.asc(),
                BalancerPolicyLog.supplier.asc(),
            )
        )

        if run_mode:
            q = q.where(BalancerPolicyLog.mode == run_mode)

        res = await db.execute(q)
        return list(res.scalars().all())


async def get_policy_log_ids_older_than(*, cutoff_utc: datetime) -> list[int]:
    """Возвращает id записей BalancerPolicyLog, у которых segment_end < cutoff_utc."""
    if cutoff_utc.tzinfo is None:
        cutoff_utc = cutoff_utc.replace(tzinfo=timezone.utc)
    cutoff_utc = cutoff_utc.astimezone(timezone.utc)

    async with get_async_db() as db:
        q = select(BalancerPolicyLog.id).where(BalancerPolicyLog.segment_end < cutoff_utc)
        res = await db.execute(q)
        return [int(x) for x in res.scalars().all()]


async def cleanup_old_balancer_data(*, keep_days: int = 90) -> dict[str, int]:
    """Удаляет старые записи балансировщика, чтобы БД не росла бесконечно."""
    keep_days = int(keep_days)
    if keep_days <= 0:
        raise ValueError("keep_days must be positive")

    cutoff_utc = datetime.now(timezone.utc) - timedelta(days=keep_days)

    policy_ids = await get_policy_log_ids_older_than(cutoff_utc=cutoff_utc)
    if not policy_ids:
        return {"deleted_order_facts": 0, "deleted_segment_stats": 0, "deleted_policy_logs": 0}

    async with get_async_db() as db:
        res1 = await db.execute(
            delete(BalancerOrderFacts).where(BalancerOrderFacts.policy_log_id.in_(policy_ids))
        )
        deleted_order_facts = int(getattr(res1, "rowcount", 0) or 0)

        res2 = await db.execute(
            delete(BalancerSegmentStats).where(BalancerSegmentStats.policy_log_id.in_(policy_ids))
        )
        deleted_segment_stats = int(getattr(res2, "rowcount", 0) or 0)

        res3 = await db.execute(delete(BalancerPolicyLog).where(BalancerPolicyLog.id.in_(policy_ids)))
        deleted_policy_logs = int(getattr(res3, "rowcount", 0) or 0)

        await db.commit()

    return {
        "deleted_order_facts": deleted_order_facts,
        "deleted_segment_stats": deleted_segment_stats,
        "deleted_policy_logs": deleted_policy_logs,
    }


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


async def _get_best_porog_30d_global(
    *,
    mode: str,  # "TEST" или "LIVE"
    segment_id: str,
    day_date,
    lookback_days: int = 30,
) -> dict[str, dict[str, float | str]]:
    """Глобальный best porog по (segment_id, band_id) без разреза по city/supplier.

    Фильтры:
    - orders_sample_ok = true
    - day_date in [day_date-lookback_days, day_date)

    Критерий выбора по каждому band_id:
    - максимальный excess_profit_sum

    Возвращает:
    {
      "B1": {"porog": 0.15, "excess_profit_sum": 123.45, "source": "best_30d_test_global"},
      ...
    }
    """
    from datetime import timedelta

    mode = str(mode or "").upper().strip()
    start_date = day_date - timedelta(days=int(lookback_days))

    async with get_async_db() as db:
        q = (
            select(BalancerSegmentStats)
            .where(
                BalancerSegmentStats.mode == mode,
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

    src = "best_30d_test_global" if mode == "TEST" else "best_30d_live_global"

    best: dict[str, dict[str, float | str]] = {}
    for r in rows:
        band_id = str(r.band_id)
        if band_id in best:
            continue
        best[band_id] = {
            "porog": float(r.porog_used),
            "excess_profit_sum": float(r.excess_profit_sum or 0),
            "source": src,
        }

    return best


async def get_best_porog_30d_global_test(
    *,
    segment_id: str,
    day_date,
    lookback_days: int = 30,
) -> dict[str, dict[str, float | str]]:
    """Глобальный best porog из TEST по (segment_id, band_id)."""
    return await _get_best_porog_30d_global(
        mode="TEST",
        segment_id=segment_id,
        day_date=day_date,
        lookback_days=lookback_days,
    )


async def get_best_porog_30d_global_live(
    *,
    segment_id: str,
    day_date,
    lookback_days: int = 30,
) -> dict[str, dict[str, float | str]]:
    """Глобальный best porog из LIVE по (segment_id, band_id)."""
    return await _get_best_porog_30d_global(
        mode="LIVE",
        segment_id=segment_id,
        day_date=day_date,
        lookback_days=lookback_days,
    )


async def get_best_porog_30d_global_best(
    *,
    segment_id: str,
    day_date,
    lookback_days: int = 30,
) -> dict[str, dict[str, float | str]]:
    """Лучший global porog по band_id из (TEST global vs LIVE global).

    Выбор между TEST и LIVE делаем по максимальному excess_profit_sum.
    """
    test_best = await get_best_porog_30d_global_test(
        segment_id=segment_id,
        day_date=day_date,
        lookback_days=lookback_days,
    )
    live_best = await get_best_porog_30d_global_live(
        segment_id=segment_id,
        day_date=day_date,
        lookback_days=lookback_days,
    )

    out: dict[str, dict[str, float | str]] = {}

    for band_id in set(test_best.keys()) | set(live_best.keys()):
        t = test_best.get(band_id)
        l = live_best.get(band_id)

        if t and not l:
            out[band_id] = t
            continue
        if l and not t:
            out[band_id] = l
            continue

        # оба есть — выбираем по excess_profit_sum
        if float(l.get("excess_profit_sum", 0)) >= float(t.get("excess_profit_sum", 0)):
            out[band_id] = l
        else:
            out[band_id] = t

    return out
# --- Pricing read-only API ---
async def get_active_policy_for_pricing(
    *,
    mode: str,
    supplier: str,
    city: str,
    as_of: datetime,
) -> dict[str, Any] | None:
    """
    Возвращает активную (последнюю применённую) политику для ценообразования.

    Используется модулем ценообразования (dropship_pipeline).
    READ-ONLY, никаких записей в БД.

    Вход:
      - mode: "LIVE" | "TEST"
      - supplier: код поставщика (D1, D2, ...)
      - city: город
      - as_of: datetime, для которого ищем активный сегмент

    Выход (или None):
      {
        "policy_id": int,
        "hash": str,
        "segment_id": str,
        "segment_start": datetime,
        "segment_end": datetime,
        "rules": list[{band_id, porog}],
        "reason": str,
        "reason_details": dict | None,
      }
    """

    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)

    async with get_async_db() as db:
        q = (
            select(BalancerPolicyLog)
            .where(
                BalancerPolicyLog.is_applied.is_(True),
                BalancerPolicyLog.mode == mode,
                BalancerPolicyLog.supplier == supplier,
                BalancerPolicyLog.city == city,
                BalancerPolicyLog.segment_start <= as_of,
                BalancerPolicyLog.segment_end > as_of,
            )
            .order_by(BalancerPolicyLog.segment_start.desc())
            .limit(1)
        )

        res = await db.execute(q)
        policy = res.scalar_one_or_none()

        if policy is None:
            return None

        return {
            "policy_id": policy.id,
            "hash": policy.hash,
            "mode": policy.mode,
            "segment_id": policy.segment_id,
            "segment_start": policy.segment_start,
            "segment_end": policy.segment_end,
            "rules": policy.rules or [],
            "min_porog_by_band": policy.min_porog_by_band or {},
            "reason": policy.reason,
            "reason_details": policy.reason_details,
        }
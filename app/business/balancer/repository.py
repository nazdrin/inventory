from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone, timedelta, date

# --- Helper: normalize day_date to datetime.date ---
def _normalize_day_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"Invalid day_date type: {type(value)}")
from typing import Any, Optional

from sqlalchemy import select, delete, text

import os

from app.database import get_async_db
from app.models import BalancerPolicyLog, BalancerOrderFacts, BalancerSegmentStats, BalancerLiveState

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
    "get_live_state",
    "upsert_live_state",
    # --- LIVE state metric helpers ---
    "ensure_live_state",
    "try_set_live_baseline",
    "update_live_last_metric",
    "update_live_best_if_better",
    "set_live_stop",
    "inc_live_orders",
    "set_live_limit_reached",
    "bump_live_iter",
    "increment_live_day_orders",
    "update_live_state_last_policy",
    "try_mark_live_run_key",
    "update_live_best_rules",
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


# --- LIVE state (daily counters / controls) ---

async def get_live_state(
    *,
    day_date,
    supplier: str,
    mode: str | None = None,
) -> BalancerLiveState | None:
    """Возвращает live-state для (day_date, supplier[, mode]).

    Таблица используется для LIVE-контролей: дневные лимиты, итерации и флаги.

    Примечание: ключи зависят от модели. Мы фильтруем по тем полям,
    которые реально присутствуют на модели `BalancerLiveState`.
    """

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()

    async with get_async_db() as db:
        q = select(BalancerLiveState)
        # обязательные ключи
        if hasattr(BalancerLiveState, "day_date"):
            q = q.where(BalancerLiveState.day_date == day_date)
        if hasattr(BalancerLiveState, "supplier"):
            q = q.where(BalancerLiveState.supplier == supplier)
        # опциональный ключ mode
        if run_mode in ("LIVE", "TEST") and hasattr(BalancerLiveState, "mode"):
            q = q.where(BalancerLiveState.mode == run_mode)

        res = await db.execute(q.limit(1))
        return res.scalar_one_or_none()


async def upsert_live_state(
    *,
    day_date,
    supplier: str,
    mode: str | None = None,
    defaults: dict[str, Any] | None = None,
    updates: dict[str, Any] | None = None,
) -> BalancerLiveState:
    """Создаёт или обновляет live-state (UPSERT).

    - defaults: применяются только при создании
    - updates: применяются и при создании, и при обновлении

    Требование: в БД должен быть UNIQUE по (mode, supplier, day_date).
    """

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    defaults = dict(defaults or {})
    updates = dict(updates or {})

    # формируем значения по умолчанию на insert
    insert_live_iter = int(defaults.get("live_iter", 0)) if hasattr(BalancerLiveState, "live_iter") else 0
    insert_day_orders = int(defaults.get("day_orders_count", 0)) if hasattr(BalancerLiveState, "day_orders_count") else 0
    insert_is_limit = bool(defaults.get("is_limit_reached", False)) if hasattr(BalancerLiveState, "is_limit_reached") else False
    insert_last_policy = defaults.get("last_policy_log_id") if hasattr(BalancerLiveState, "last_policy_log_id") else None

    # updates применяем как для insert, так и для update
    if "live_iter" in updates and hasattr(BalancerLiveState, "live_iter"):
        insert_live_iter = int(updates["live_iter"])
    if "day_orders_count" in updates and hasattr(BalancerLiveState, "day_orders_count"):
        insert_day_orders = int(updates["day_orders_count"])
    if "is_limit_reached" in updates and hasattr(BalancerLiveState, "is_limit_reached"):
        insert_is_limit = bool(updates["is_limit_reached"])
    if "last_policy_log_id" in updates and hasattr(BalancerLiveState, "last_policy_log_id"):
        insert_last_policy = updates["last_policy_log_id"]

    # дополнительные поля updates (кроме базовых) — обновим отдельным UPDATE после upsert
    extra_update_keys = {
        k: v
        for k, v in updates.items()
        if k not in {"live_iter", "day_orders_count", "is_limit_reached", "last_policy_log_id"}
        and hasattr(BalancerLiveState, k)
    }

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (mode, supplier, day_date, live_iter, day_orders_count, is_limit_reached, last_policy_log_id)
                VALUES (:mode, :supplier, :day_date, :live_iter, :day_orders_count, :is_limit_reached, :last_policy_log_id)
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    live_iter = EXCLUDED.live_iter,
                    day_orders_count = EXCLUDED.day_orders_count,
                    is_limit_reached = EXCLUDED.is_limit_reached,
                    last_policy_log_id = EXCLUDED.last_policy_log_id,
                    updated_at = now()
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "live_iter": insert_live_iter,
                "day_orders_count": insert_day_orders,
                "is_limit_reached": insert_is_limit,
                "last_policy_log_id": int(insert_last_policy) if insert_last_policy is not None else None,
            },
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))

        if extra_update_keys and obj is not None:
            for k, v in extra_update_keys.items():
                setattr(obj, k, v)
            await db.commit()
            await db.refresh(obj)
        elif obj is not None:
            await db.refresh(obj)

        return obj  # type: ignore[return-value]


async def inc_live_orders(
    *,
    day_date,
    supplier: str,
    delta: int,
    mode: str | None = None,
) -> BalancerLiveState:
    """Увеличивает дневной счётчик заказов по поставщику (day_orders_count += delta)."""

    # backward-compatible wrapper around atomic counter update
    return await increment_live_day_orders(
        day_date=day_date,
        supplier=supplier,
        delta_orders=delta,
        mode=mode,
        policy_log_id=None,
    )


async def set_live_limit_reached(
    *,
    day_date,
    supplier: str,
    is_reached: bool = True,
    mode: str | None = None,
) -> BalancerLiveState:
    """Ставит флаг достижения дневного лимита (is_limit_reached)."""

    updates: dict[str, Any] = {}
    if hasattr(BalancerLiveState, "is_limit_reached"):
        updates["is_limit_reached"] = bool(is_reached)

    # гарантируем, что запись существует
    obj = await upsert_live_state(
        day_date=day_date,
        supplier=supplier,
        mode=mode,
        defaults={"day_orders_count": 0} if hasattr(BalancerLiveState, "day_orders_count") else {},
        updates=updates,
    )
    return obj


# --- atomic helpers for live state ---

async def bump_live_iter(
    *,
    day_date: date,
    supplier: str,
    delta: int = 1,
    mode: str | None = None,
) -> BalancerLiveState:
    """Атомарно увеличивает live_iter на delta (по умолчанию +1)."""

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    delta = int(delta)
    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (mode, supplier, day_date, live_iter, day_orders_count, is_limit_reached)
                VALUES (:mode, :supplier, :day_date, :delta, 0, false)
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    live_iter = COALESCE(balancer_live_state.live_iter, 0) + :delta,
                    updated_at = now()
                RETURNING id
                """
            ),
            {"mode": run_mode, "supplier": supplier, "day_date": day_date, "delta": delta},
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))
        return obj  # type: ignore[return-value]


async def increment_live_day_orders(
    *,
    day_date: date,
    supplier: str,
    delta_orders: int,
    mode: str | None = None,
    policy_log_id: int | None = None,
) -> BalancerLiveState:
    """Атомарно увеличивает day_orders_count на delta_orders.

    Если передан policy_log_id — также обновляет last_policy_log_id.
    """

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    delta_orders = int(delta_orders)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (mode, supplier, day_date, live_iter, day_orders_count, is_limit_reached, last_policy_log_id)
                VALUES (:mode, :supplier, :day_date, 0, :delta, false, :policy_log_id)
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    day_orders_count = COALESCE(balancer_live_state.day_orders_count, 0) + :delta,
                    last_policy_log_id = COALESCE(:policy_log_id, balancer_live_state.last_policy_log_id),
                    updated_at = now()
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "delta": delta_orders,
                "policy_log_id": int(policy_log_id) if policy_log_id is not None else None,
            },
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))
        return obj  # type: ignore[return-value]



async def update_live_state_last_policy(
    *,
    day_date: date,
    supplier: str,
    policy_log_id: int | None,
    mode: str | None = None,
) -> BalancerLiveState:
    """Обновляет last_policy_log_id (и updated_at) без изменения счётчиков."""

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (mode, supplier, day_date, live_iter, day_orders_count, is_limit_reached, last_policy_log_id)
                VALUES (:mode, :supplier, :day_date, 0, 0, false, :policy_log_id)
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    last_policy_log_id = :policy_log_id,
                    updated_at = now()
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "policy_log_id": int(policy_log_id) if policy_log_id is not None else None,
            },
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))
        return obj  # type: ignore[return-value]


# --- Helper: try_mark_live_run_key ---

async def try_mark_live_run_key(
    *,
    day_date: date,
    supplier: str,
    run_key: str,
    mode: str | None = None,
) -> bool:
    """Idempotency gate for LIVE runs.

    Returns True only if `run_key` was set/changed for (mode, supplier, day_date).
    If the same run_key was already stored, returns False.

    This is intended to prevent double counting when the same segment pipeline
    is executed multiple times.

    Requires column `last_run_key` (and UNIQUE on (mode, supplier, day_date)).
    If the column does not exist (older schema), this function returns True.
    """

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_key = str(run_key or "").strip()
    if not run_key:
        raise ValueError("run_key is required")

    # Backward-compat: if schema isn't expanded yet, don't block the run.
    if not hasattr(BalancerLiveState, "last_run_key"):
        return True

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (mode, supplier, day_date, live_iter, day_orders_count, is_limit_reached, last_run_key)
                VALUES (:mode, :supplier, :day_date, 0, 0, false, :run_key)
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    last_run_key = EXCLUDED.last_run_key,
                    updated_at = now()
                WHERE balancer_live_state.last_run_key IS DISTINCT FROM EXCLUDED.last_run_key
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "run_key": run_key,
            },
        )
        row_id = res.scalar_one_or_none()
        await db.commit()
        return row_id is not None


async def update_live_best_rules(
    *,
    day_date: date,
    supplier: str,
    best_rules: dict[str, Any],
    mode: str | None = None,
) -> BalancerLiveState:
    """Stores the current best rules snapshot into live_state.best_rules.

    Requires column `best_rules` (json/jsonb). If the column does not exist,
    this function will simply ensure the row exists and return it.
    """

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    # Backward-compat: if schema isn't expanded yet, just ensure row exists.
    if not hasattr(BalancerLiveState, "best_rules"):
        return await ensure_live_state(day_date=day_date, supplier=supplier, mode=run_mode)

    payload_json = json.dumps(best_rules or {}, ensure_ascii=False, default=str)

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (mode, supplier, day_date, live_iter, day_orders_count, is_limit_reached, best_rules)
                VALUES (:mode, :supplier, :day_date, 0, 0, false, (:best_rules)::jsonb)
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    best_rules = (:best_rules)::jsonb,
                    updated_at = now()
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "best_rules": payload_json,
            },
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))
        return obj  # type: ignore[return-value]


# --- LIVE state: metrics/baseline/best/freeze helpers ---

async def ensure_live_state(
    *,
    day_date: date,
    supplier: str,
    mode: str | None = None,
) -> BalancerLiveState:
    """Гарантирует наличие строки live_state для (mode, supplier, day_date)."""
    day_date = _normalize_day_date(day_date)
    return await upsert_live_state(
        day_date=day_date,
        supplier=supplier,
        mode=mode,
        defaults={
            "live_iter": 0,
            "day_orders_count": 0,
            "is_limit_reached": False,
        },
        updates={},
    )


async def try_set_live_baseline(
    *,
    day_date: date,
    supplier: str,
    baseline_metric: float,
    mode: str | None = None,
) -> BalancerLiveState:
    """Атомарно выставляет baseline_metric, но только если он ещё не задан."""

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (mode, supplier, day_date, live_iter, day_orders_count, is_limit_reached, baseline_metric)
                VALUES (:mode, :supplier, :day_date, 0, 0, false, :baseline_metric)
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    baseline_metric = COALESCE(balancer_live_state.baseline_metric, EXCLUDED.baseline_metric),
                    updated_at = now()
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "baseline_metric": float(baseline_metric),
            },
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))
        return obj  # type: ignore[return-value]


async def update_live_last_metric(
    *,
    day_date: date,
    supplier: str,
    last_metric: float,
    last_segment_end: datetime | None = None,
    last_policy_log_id: int | None = None,
    mode: str | None = None,
) -> BalancerLiveState:
    """Атомарно обновляет last_metric (+ опционально last_segment_end/last_policy_log_id)."""

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (
                    mode, supplier, day_date,
                    live_iter, day_orders_count, is_limit_reached,
                    last_metric, last_segment_end, last_policy_log_id
                )
                VALUES (
                    :mode, :supplier, :day_date,
                    0, 0, false,
                    :last_metric, :last_segment_end, :last_policy_log_id
                )
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    last_metric = EXCLUDED.last_metric,
                    last_segment_end = COALESCE(EXCLUDED.last_segment_end, balancer_live_state.last_segment_end),
                    last_policy_log_id = COALESCE(EXCLUDED.last_policy_log_id, balancer_live_state.last_policy_log_id),
                    updated_at = now()
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "last_metric": float(last_metric),
                "last_segment_end": last_segment_end,
                "last_policy_log_id": int(last_policy_log_id) if last_policy_log_id is not None else None,
            },
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))
        return obj  # type: ignore[return-value]


async def update_live_best_if_better(
    *,
    day_date: date,
    supplier: str,
    candidate_metric: float,
    candidate_iter: int,
    mode: str | None = None,
) -> BalancerLiveState:
    """Атомарно обновляет best_metric/best_iter, если candidate лучше текущего best."""

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    cand = float(candidate_metric)
    it = int(candidate_iter)

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (
                    mode, supplier, day_date,
                    live_iter, day_orders_count, is_limit_reached,
                    best_metric, best_iter
                )
                VALUES (
                    :mode, :supplier, :day_date,
                    0, 0, false,
                    :cand, :it
                )
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    best_metric = CASE
                        WHEN balancer_live_state.best_metric IS NULL THEN :cand
                        WHEN :cand > balancer_live_state.best_metric THEN :cand
                        ELSE balancer_live_state.best_metric
                    END,
                    best_iter = CASE
                        WHEN balancer_live_state.best_metric IS NULL THEN :it
                        WHEN :cand > balancer_live_state.best_metric THEN :it
                        ELSE balancer_live_state.best_iter
                    END,
                    updated_at = now()
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "cand": cand,
                "it": it,
            },
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))
        return obj  # type: ignore[return-value]


async def set_live_stop(
    *,
    day_date: date,
    supplier: str,
    stop_reason: str,
    freeze: bool = True,
    mode: str | None = None,
) -> BalancerLiveState:
    """Ставит stop_reason и (опционально) замораживает LIVE-управление на день."""

    supplier = str(supplier or "").strip()
    if not supplier:
        raise ValueError("supplier is required")

    day_date = _normalize_day_date(day_date)

    run_mode = (mode or os.getenv("BALANCER_RUN_MODE") or "").strip().upper()
    if run_mode not in ("LIVE", "TEST"):
        run_mode = "LIVE"

    async with get_async_db() as db:
        res = await db.execute(
            text(
                """
                INSERT INTO balancer_live_state (
                    mode, supplier, day_date,
                    live_iter, day_orders_count, is_limit_reached,
                    stop_reason, is_frozen
                )
                VALUES (
                    :mode, :supplier, :day_date,
                    0, 0, false,
                    :stop_reason, :is_frozen
                )
                ON CONFLICT (mode, supplier, day_date)
                DO UPDATE SET
                    stop_reason = EXCLUDED.stop_reason,
                    is_frozen = EXCLUDED.is_frozen,
                    updated_at = now()
                RETURNING id
                """
            ),
            {
                "mode": run_mode,
                "supplier": supplier,
                "day_date": day_date,
                "stop_reason": str(stop_reason)[:255],
                "is_frozen": bool(freeze),
            },
        )
        row_id = res.scalar_one()
        await db.commit()
        obj = await db.get(BalancerLiveState, int(row_id))
        return obj  # type: ignore[return-value]


async def upsert_order_fact(payload: dict[str, Any]) -> tuple[BalancerOrderFacts, bool]:
    """Идемпотентно пишет строку в balancer_order_facts.

    Уникальность обеспечивается парой (policy_log_id, order_id).
    Если такая запись уже есть — возвращаем её и created=False.
    Если нет — создаём новую и возвращаем created=True.

    Это важно для LIVE-дневных лимитов: мы должны уметь посчитать delta
    по новым вставкам (created=True), чтобы второй прогон не увеличивал счётчик.
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
            return existing, False

        obj = BalancerOrderFacts(**payload)
        db.add(obj)
        await db.commit()
        await db.refresh(obj)
        return obj, True


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
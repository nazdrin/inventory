from __future__ import annotations

import os
from app.services.notification_service import send_notification
from collections import Counter

from typing import Any

from .config import load_config
from .segments import resolve_current_segment
from .policy import build_test_policy_async, build_live_policy_async
from .repository import (
    create_policy_log_record,
    upsert_policy_log,
    get_best_porog_30d_global_best,
    cleanup_old_balancer_data,
)
from .salesdrive_client import fetch_orders_for_segment
from .order_processor import build_order_facts
from datetime import timedelta, datetime, timezone




__all__ = [
    "start_segment_dry_run_async",
    "start_segment_apply_async",
    "collect_orders_for_last_policy_async",
    "aggregate_stats_for_last_policy_async",
    "compute_day_metrics_for_last_policies_async",
    "run_balancer_pipeline_async",
]


# --- Normalization helpers ---

def _norm_key(v: Any) -> str:
    return str(v or "").strip().lower()


def _norm_mode(v: Any) -> str:
    return _norm_key(v).upper()


def _norm_city_key(v: Any) -> str:
    """Normalize city keys for config/profile matching (case-insensitive).

    We keep policy.city as stored, but when comparing with config.scope.cities we compare normalized keys.
    Also normalizes dash variants.
    """
    s = _norm_key(v)
    if not s:
        return ""
    return s.replace("–", "-").replace("—", "-")


def _norm_supplier_key(v: Any) -> str:
    return _norm_key(v)

def _parse_collect_segment_end_utc() -> datetime | None:
    """
    Scheduler can pass BALANCER_COLLECT_SEGMENT_END_UTC.
    If set -> this run is a 'segment close' run, and we must collect/aggregate
    ONLY for policies whose segment_end equals this boundary.
    """
    v = (os.getenv("BALANCER_COLLECT_SEGMENT_END_UTC") or "").strip()
    if not v:
        return None
    try:
        dt = datetime.fromisoformat(v)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _filter_policies_by_collect_end(policies: list[Any]) -> list[Any]:
    collect_end = _parse_collect_segment_end_utc()
    if not collect_end:
        return policies

    out: list[Any] = []
    for p in policies:
        try:
            pe = getattr(p, "segment_end", None)
            if pe is None:
                continue
            if pe.tzinfo is None:
                pe = pe.replace(tzinfo=timezone.utc)
            if pe.astimezone(timezone.utc) == collect_end:
                out.append(p)
        except Exception:
            continue
    return out

async def start_segment_dry_run_async() -> list[dict[str, Any]]:
    """Этап 2.4: без записи в БД policy_log, но TEST использует состояние в БД.

    Возвращает список подготовленных записей policy_log по всем профилям.
    В режиме TEST пороги берутся из таблицы balancer_test_state и продвигаются по графику.
    """

    cfg = load_config()
    # Optional selector: run only profiles matching this mode (LIVE or TEST)
    run_mode = os.getenv("BALANCER_RUN_MODE")
    run_mode = str(run_mode).upper().strip() if run_mode else None
    out: list[dict[str, Any]] = []

    def _normalized_reason_and_details(policy_obj: Any) -> tuple[str, dict[str, Any]]:
        """Normalize LIVE reason so it matches real band sources.

        We keep `reason_details["band_sources"]` as the source of truth.
        If ALL bands are fallback_min_porog, set reason=fallback_min_porog.
        Otherwise keep best_30d.
        """
        reason = str(getattr(policy_obj, "reason", "") or "")
        details = dict(getattr(policy_obj, "reason_details", None) or {})
        band_sources = dict(details.get("band_sources") or {})

        # If there is no band_sources, just return as-is.
        if not band_sources:
            return reason, details

        # Detect if any band actually used best_30d.
        any_best = any(str(v).startswith("best_30d") for v in band_sources.values())
        all_fallback = all(str(v) == "fallback_min_porog" for v in band_sources.values())

        if all_fallback and not any_best:
            # Make top-level reason consistent with per-band sources.
            reason = "fallback_min_porog"
            # Also reflect it in the details source when present.
            if details.get("source") == "balancer_segment_stats_live_then_test_seed":
                details["source"] = "fallback_min_porog"
        else:
            reason = "best_30d"
        details["band_sources"] = band_sources
        return reason, details

    for profile in cfg.profiles:
        scope = profile.get("scope", {})
        cities = scope.get("cities", [])
        suppliers = scope.get("suppliers", [])

        if not cities or not suppliers:
            continue

        profile_name = profile.get("name") or profile.get("profile_name") or "profile"

        seg = resolve_current_segment(profile)
        day_date = seg.start.date()

        profile_mode = str(profile.get("mode", "TEST")).upper()

        # If BALANCER_RUN_MODE is set, we RUN ONLY profiles of that mode.
        # We do NOT override a TEST profile into LIVE (or vice versa).
        if run_mode and profile_mode != run_mode:
            continue

        effective_mode = profile_mode

        for city in cities:
            # IMPORTANT:
            # Если TEST-профиль включает несколько поставщиков (D1+D2), мы обязаны:
            # 1) применить ОДИНАКОВЫЕ пороги на весь сегмент для всех поставщиков
            # 2) НЕ продвигать состояние TEST-графика в dry-run (симуляция без побочных эффектов)
            if effective_mode == "TEST" and len(suppliers) > 1:
                leader = sorted([str(s) for s in suppliers])[0]

                # 1) Получаем общую политику БЕЗ продвижения состояния
                shared_policy = await build_test_policy_async(
                    profile,
                    profile_name=profile_name,
                    city=city,
                    supplier=leader,
                    segment_id=seg.segment_id,
                    day_date=day_date,
                    suppliers_in_profile=[str(s) for s in suppliers],
                    advance_state=False,
                )

                # 2) Создаём записи policy_log для каждого supplier с ОДИНАКОВЫМИ rules
                for supplier in suppliers:
                    rec = create_policy_log_record(
                        mode=effective_mode,
                        config_version=int(cfg.balancer.get("version", 1)),
                        city=city,
                        supplier=supplier,
                        segment_id=seg.segment_id,
                        segment_start=seg.start,
                        segment_end=seg.end,
                        rules=shared_policy.rules,
                        min_porog_by_band=shared_policy.min_porog_by_band,
                        reason=shared_policy.reason,
                        reason_details=shared_policy.reason_details,
                        config_snapshot=cfg.balancer,  # пока целиком, позже сузим
                    )
                    out.append(rec)
                # 3) В DRY-RUN не продвигаем состояние вообще!
            else:
                # Обычный режим: по одному supplier (или LIVE заглушка)
                for supplier in suppliers:
                    if effective_mode == "TEST":
                        policy = await build_test_policy_async(
                            profile,
                            profile_name=profile_name,
                            city=city,
                            supplier=supplier,
                            segment_id=seg.segment_id,
                            day_date=day_date,
                            suppliers_in_profile=[str(s) for s in suppliers] if len(suppliers) > 1 else None,
                            advance_state=False,
                        )
                        rec = create_policy_log_record(
                            mode=effective_mode,
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
                    else:
                        # LIVE: строим политику из GLOBAL best_30d (TEST_GLOBAL + LIVE_GLOBAL -> BEST_GLOBAL)
                        # (не city/supplier-специфично, чтобы не упираться в маленькую выборку)
                        # IMPORTANT: repository uses window day_date < day_date_param,
                        # so to include already-computed stats for `day_date` (today),
                        # we query best "as of" next day.
                        best_asof = day_date + timedelta(days=1)

                        best_global = await get_best_porog_30d_global_best(
                            segment_id=seg.segment_id,
                            day_date=best_asof,
                            lookback_days=30,
                        )

                        # Собираем rules: если для band есть global best — берем его, иначе fallback на min_porog_by_band
                        rules: list[dict[str, Any]] = []
                        band_sources: dict[str, str] = {}
                        for band in profile.get("price_bands") or []:
                            band_id = str(band.get("band_id"))
                            if not band_id:
                                continue
                            if band_id in (best_global or {}):
                                info = (best_global or {}).get(band_id) or {}
                                porog_val = float(info.get("porog"))
                                src = str(info.get("source") or "best_30d")
                                rules.append({"band_id": band_id, "porog": porog_val})
                                band_sources[band_id] = src
                            else:
                                porog_val = float((profile.get("min_porog_by_band") or {}).get(band_id) or 0)
                                rules.append({"band_id": band_id, "porog": porog_val})
                                band_sources[band_id] = "fallback_min_porog"

                        # reason_details: храним band_sources как источник истины
                        reason_details = {
                            "source": "best_30d_global",
                            "day_date": str(best_asof),
                            "band_sources": band_sources,
                            "supplier_state_key": str(supplier),
                        }

                        # Нормализуем верхнеуровневую reason по фактическим band_sources
                        class _TmpPolicy:
                            def __init__(self):
                                self.reason = "best_30d"
                                self.reason_details = reason_details

                        norm_reason, norm_details = _normalized_reason_and_details(_TmpPolicy())

                        rec = create_policy_log_record(
                            mode=effective_mode,
                            config_version=int(cfg.balancer.get("version", 1)),
                            city=city,
                            supplier=supplier,
                            segment_id=seg.segment_id,
                            segment_start=seg.start,
                            segment_end=seg.end,
                            rules=rules,
                            min_porog_by_band=profile.get("min_porog_by_band") or {},
                            reason=norm_reason,
                            reason_details=norm_details,
                            config_snapshot=cfg.balancer,  # пока целиком, позже сузим
                        )
                        out.append(rec)

    return out


async def start_segment_apply_async() -> list[dict[str, Any]]:
    """Этап 2.5: применяет сегмент — пишет записи в balancer_policy_log.

    Идемпотентно: при повторном запуске для того же набора параметров/правил
    возвращает уже существующую запись (по hash).

    Возвращает список кратких результатов (id, hash, city, supplier, segment_id).
    """

    prepared = await start_segment_dry_run_async()
    results: list[dict[str, Any]] = []

    for payload in prepared:
        res = await upsert_policy_log(payload)
        # repository versions may return either (obj, created) or just obj
        if isinstance(res, tuple) and len(res) == 2:
            obj, created = res
        else:
            obj, created = res, False
        # TEST: продвигаем состояние тестового графика ТОЛЬКО если policy_log реально создан.
        if str(payload.get("mode", "")).upper() == "TEST" and created:
            config_snapshot = payload.get("config_snapshot") or {}
            profiles = config_snapshot.get("profiles", []) or []

            matched_profile = None
            for prof in profiles:
                if str(prof.get("mode", "")).upper() != "TEST":
                    continue
                scope = prof.get("scope", {}) or {}
                scope_cities = scope.get("cities", []) or []
                scope_suppliers = scope.get("suppliers", []) or []

                if _norm_city_key(obj.city) not in {_norm_city_key(x) for x in scope_cities}:
                    continue
                if _norm_supplier_key(obj.supplier) not in {_norm_supplier_key(x) for x in scope_suppliers}:
                    continue
                matched_profile = prof
                break

            if matched_profile:
                scope = matched_profile.get("scope", {}) or {}
                suppliers = [str(s) for s in (scope.get("suppliers", []) or [])]
                leader = sorted(suppliers)[0] if suppliers else None

                if leader and str(obj.supplier) == leader:
                    profile_name = matched_profile.get("name") or matched_profile.get("profile_name") or "profile"
                    day_date = obj.segment_start.date()
                    await build_test_policy_async(
                        matched_profile,
                        profile_name=profile_name,
                        city=obj.city,
                        supplier=leader,
                        segment_id=obj.segment_id,
                        day_date=day_date,
                        suppliers_in_profile=suppliers,
                        advance_state=True,
                    )
        results.append(
            {
                "id": obj.id,
                "hash": obj.hash,
                "mode": obj.mode,
                "city": obj.city,
                "supplier": obj.supplier,
                "segment_id": obj.segment_id,
                "segment_start": str(obj.segment_start),
                "segment_end": str(obj.segment_end),
                "reason": obj.reason,
            }
        )

    return results


async def compute_day_metrics_for_last_policies_async() -> list[dict[str, Any]]:
    """Этап 2.8: заполняет day_total_orders и segment_share в balancer_segment_stats.

    Логика:
    - берём последние применённые политики (mode, city, supplier)
    - для каждой политики считаем:
        day_total_orders = сумма orders_count по всем band_id внутри этой policy_log_id
        segment_orders = сумма orders_count по всем band_id внутри этой policy_log_id и segment_id
        segment_share = segment_orders / day_total_orders
    - обновляем ВСЕ строки (все band_id) данного сегмента через repository.update_day_metrics_for_segment

    В зачёт идут все статусы (мы работаем уже по агрегированным segment_stats).
    """
    from decimal import Decimal

    from .repository import (
        get_last_applied_policies,
        get_segment_stats_for_day_scope,
        update_day_metrics_for_segment,
    )

    results: list[dict[str, Any]] = []

    policies = await get_last_applied_policies()
    # If scheduler provided boundary end, compute metrics ONLY for the segment that just ended.
    policies = _filter_policies_by_collect_end(policies)

    # --- Filter policies by current config and BALANCER_RUN_MODE (same idea as collect_orders) ---
    cfg = load_config()
    run_mode = os.getenv("BALANCER_RUN_MODE")
    run_mode = str(run_mode).upper().strip() if run_mode else None

    allowed_set = set()
    for profile in cfg.profiles:
        profile_mode = str(profile.get("mode", "TEST")).upper()
        if run_mode and profile_mode != run_mode:
            continue
        scope = profile.get("scope", {}) or {}
        cities = scope.get("cities", []) or []
        suppliers = scope.get("suppliers", []) or []
        for city in cities:
            for supplier in suppliers:
                allowed_set.add((_norm_mode(profile_mode), _norm_city_key(city), _norm_supplier_key(supplier)))

    if allowed_set:
        policies = [
            p for p in policies
            if (_norm_mode(p.mode), _norm_city_key(p.city), _norm_supplier_key(p.supplier)) in allowed_set
        ]

    for policy in policies:
        day_date = policy.segment_start.date()

        # Берём все segment_stats за этот день для связки (mode, city, supplier)
        rows = await get_segment_stats_for_day_scope(
            mode=policy.mode,
            city=policy.city,
            supplier=policy.supplier,
            day_date=day_date,
        )

        # Ограничимся текущей policy_log_id (чтобы не смешивать разные сегменты/пере-запуски)
        rows = [r for r in rows if int(r.policy_log_id) == int(policy.id)]

        # day_total_orders
        day_total_orders = sum((int(r.orders_count) for r in rows), 0)

        # группируем по segment_id
        seg_totals: dict[str, int] = {}
        for r in rows:
            seg_totals.setdefault(str(r.segment_id), 0)
            seg_totals[str(r.segment_id)] += int(r.orders_count)

        updated_total = 0

        for segment_id, seg_orders in seg_totals.items():
            if day_total_orders > 0:
                share = (Decimal(str(seg_orders)) / Decimal(str(day_total_orders))).quantize(Decimal("0.000001"))
                segment_share = float(share)
            else:
                segment_share = None

            updated = await update_day_metrics_for_segment(
                policy_log_id=int(policy.id),
                segment_id=segment_id,
                day_total_orders=int(day_total_orders),
                segment_share=segment_share,
            )
            updated_total += int(updated)

        results.append(
            {
                "policy_log_id": int(policy.id),
                "day_date": str(day_date),
                "day_total_orders": int(day_total_orders),
                "segments": {k: int(v) for k, v in seg_totals.items()},
                "rows_updated": int(updated_total),
            }
        )

    return results


async def aggregate_stats_for_last_policy_async() -> list[dict[str, Any]]:
    """
    Этап 2.7: агрегирует balancer_order_facts в balancer_segment_stats
    по последним применённым политикам (mode, city, supplier).

    В зачёт идут все статусы (мы не фильтруем status_id).
    """
    from decimal import Decimal

    from .repository import (
        get_last_applied_policies,
        get_order_facts_for_policy,
        upsert_segment_stats,
    )

    cfg = load_config()

    def _resolve_profile_name(policy) -> str:
        snap = getattr(policy, "config_snapshot", None) or {}
        profiles = snap.get("profiles", []) or []
        for prof in profiles:
            if str(prof.get("mode", "")).upper() != str(policy.mode).upper():
                continue
            scope = prof.get("scope", {}) or {}
            scope_cities = scope.get("cities", []) or []
            scope_suppliers = scope.get("suppliers", []) or []

            if _norm_city_key(policy.city) not in {_norm_city_key(x) for x in scope_cities}:
                continue
            if _norm_supplier_key(policy.supplier) not in {_norm_supplier_key(x) for x in scope_suppliers}:
                continue
            return prof.get("name") or prof.get("profile_name") or "profile"
        return "profile"

    def _resolve_min_orders(policy) -> int:
        # Берём thresholds.min_orders_per_segment из профиля (если есть), иначе 0
        snap = getattr(policy, "config_snapshot", None) or {}
        profiles = snap.get("profiles", []) or []
        for prof in profiles:
            if str(prof.get("mode", "")).upper() != str(policy.mode).upper():
                continue
            scope = prof.get("scope", {}) or {}
            scope_cities = scope.get("cities", []) or []
            scope_suppliers = scope.get("suppliers", []) or []

            if _norm_city_key(policy.city) not in {_norm_city_key(x) for x in scope_cities}:
                continue
            if _norm_supplier_key(policy.supplier) not in {_norm_supplier_key(x) for x in scope_suppliers}:
                continue
            thresholds = prof.get("thresholds", {}) or {}
            return int(thresholds.get("min_orders_per_segment", 0) or 0)
        return 0

    results: list[dict[str, Any]] = []

    policies = await get_last_applied_policies()
    # If scheduler provided boundary end, aggregate ONLY for the segment that just ended.
    policies = _filter_policies_by_collect_end(policies)

    # --- Filter policies by current config and BALANCER_RUN_MODE (same idea as collect_orders) ---
    run_mode = os.getenv("BALANCER_RUN_MODE")
    run_mode = str(run_mode).upper().strip() if run_mode else None

    allowed_set = set()
    for profile in cfg.profiles:
        profile_mode = str(profile.get("mode", "TEST")).upper()
        if run_mode and profile_mode != run_mode:
            continue
        scope = profile.get("scope", {}) or {}
        cities = scope.get("cities", []) or []
        suppliers = scope.get("suppliers", []) or []
        for city in cities:
            for supplier in suppliers:
                allowed_set.add((_norm_mode(profile_mode), _norm_city_key(city), _norm_supplier_key(supplier)))

    if allowed_set:
        policies = [
            p for p in policies
            if (_norm_mode(p.mode), _norm_city_key(p.city), _norm_supplier_key(p.supplier)) in allowed_set
        ]

    for policy in policies:
        facts = await get_order_facts_for_policy(int(policy.id))

        # группируем по band_id
        by_band: dict[str, list[Any]] = {}
        for f in facts:
            by_band.setdefault(str(f.band_id), []).append(f)

        profile_name = _resolve_profile_name(policy)
        min_orders_required = _resolve_min_orders(policy)

        for band_id, items in by_band.items():
            orders_count = len(items)

            sale_sum = sum((Decimal(str(x.sale_price)) for x in items), Decimal("0"))
            cost_sum = sum((Decimal(str(x.cost)) for x in items), Decimal("0"))
            profit_sum = sum((Decimal(str(x.profit)) for x in items), Decimal("0"))
            min_profit_sum = sum((Decimal(str(x.min_profit)) for x in items), Decimal("0"))
            excess_profit_sum = sum((Decimal(str(x.excess_profit)) for x in items), Decimal("0"))

            excess_profit_per_order = None
            if orders_count > 0:
                excess_profit_per_order = (excess_profit_sum / Decimal(str(orders_count))).quantize(Decimal("0.0001"))

            # берем "параметры бэнда" из первой записи (они одинаковые для этого бэнда)
            first = items[0]
            band_min_price = first.band_min_price
            band_max_price = first.band_max_price
            porog_used = first.porog_used
            min_porog = first.min_porog

            day_date = policy.segment_start.date()

            payload = {
                "profile_name": profile_name,
                "mode": policy.mode,
                "policy_log_id": policy.id,
                "city": policy.city,
                "supplier": policy.supplier,
                "segment_id": policy.segment_id,
                "segment_start": policy.segment_start,
                "segment_end": policy.segment_end,
                "band_id": band_id,
                "band_min_price": band_min_price,
                "band_max_price": band_max_price,
                "porog_used": porog_used,
                "min_porog": min_porog,
                "orders_count": orders_count,
                "sale_sum": sale_sum,
                "cost_sum": cost_sum,
                "profit_sum": profit_sum,
                "min_profit_sum": min_profit_sum,
                "excess_profit_sum": excess_profit_sum,
                "excess_profit_per_order": excess_profit_per_order,
                "day_date": day_date,
                # ограничения по заказам будем добавлять позже:
                "day_total_orders": None,
                "segment_share": None,
                "orders_sample_ok": bool(orders_count >= min_orders_required),
                "note": None,
            }

            obj = await upsert_segment_stats(payload)
            results.append(
                {
                    "policy_log_id": int(policy.id),
                    "band_id": band_id,
                    "orders_count": int(obj.orders_count),
                    "excess_profit_sum": float(obj.excess_profit_sum),
                }
            )

    return results


async def collect_orders_for_last_policy_async() -> list[dict[str, Any]]:
    """
    Этап 2.6: собирает заказы SalesDrive за последний применённый сегмент
    и пишет строки в balancer_order_facts.
    """
    from .repository import (
        get_last_applied_policies,
        upsert_order_fact,
    )
    from .salesdrive_client import _extract_supplier_value
    # _extract_city_value may not exist in some versions; import defensively
    try:
        from .salesdrive_client import _extract_city_value  # type: ignore
    except Exception:  # pragma: no cover
        _extract_city_value = None  # type: ignore

    results: list[dict[str, Any]] = []

    # --- Filtering policies by current config and BALANCER_RUN_MODE ---
    policies = await get_last_applied_policies()
    # If scheduler provided boundary end, collect ONLY for the segment that just ended.
    policies = _filter_policies_by_collect_end(policies)

    cfg = load_config()
    import os
    run_mode = os.getenv("BALANCER_RUN_MODE")
    run_mode = str(run_mode).upper().strip() if run_mode else None
    allowed_set = set()
    for profile in cfg.profiles:
        profile_mode = str(profile.get("mode", "TEST")).upper()
        if run_mode and profile_mode != run_mode:
            continue
        scope = profile.get("scope", {}) or {}
        cities = scope.get("cities", []) or []
        suppliers = scope.get("suppliers", []) or []
        for city in cities:
            for supplier in suppliers:
                allowed_set.add((_norm_mode(profile_mode), _norm_city_key(city), _norm_supplier_key(supplier)))
    # Only filter if allowed_set is non-empty
    before_count = len(policies)
    if allowed_set:
        filtered_policies = [
            p for p in policies
            if (_norm_mode(p.mode), _norm_city_key(p.city), _norm_supplier_key(p.supplier)) in allowed_set
        ]
    else:
        filtered_policies = policies
    after_count = len(filtered_policies)
    # If filtering removed policies, note ONCE
    if after_count < before_count:
        results.append(
            {
                "policy_log_id": None,
                "order_id": None,
                "excess_profit": None,
                "note": f"policies filtered by current config: run_mode={run_mode or 'None'} before={before_count} after={after_count}"
            }
        )
    policies = filtered_policies

    # --- SalesDrive fetch: cache by time window to avoid burst requests (especially in TEST) ---
    # In TEST we may have десятки policy на один и тот же сегмент; SalesDrive может отвечать 400 на частые повторы.
    # Поэтому делаем 1 запрос на окно времени и дальше фильтруем в Python.
    try:
        import httpx  # local import to avoid global dependency in module import time
    except Exception:  # pragma: no cover
        httpx = None  # type: ignore

    # Cache lives for the duration of this job run
    if not hasattr(collect_orders_for_last_policy_async, "_orders_cache"):
        setattr(collect_orders_for_last_policy_async, "_orders_cache", {})
    _orders_cache: dict[tuple[str, str], list[Any]] = getattr(collect_orders_for_last_policy_async, "_orders_cache")

    for policy in policies:
        # SalesDrive хранит supplier как ЧЕЛОВЕЧЕСКОЕ ИМЯ (например "DSN"),
        # а в policy_log у нас код (например "D2").
        # ВАЖНО: берем справочник supplier_names ИЗ config_snapshot policy_log,
        # чтобы не зависеть от текущего YAML-конфига (он мог измениться после apply).
        snap = getattr(policy, "config_snapshot", None) or {}
        profiles = snap.get("profiles", []) or []

        supplier_name = None
        matched_profile_mode = None
        for prof in profiles:
            scope = prof.get("scope", {}) or {}
            scope_cities = scope.get("cities", []) or []
            scope_suppliers = scope.get("suppliers", []) or []

            if _norm_city_key(policy.city) not in {_norm_city_key(x) for x in scope_cities}:
                continue
            if _norm_supplier_key(policy.supplier) not in {_norm_supplier_key(x) for x in scope_suppliers}:
                continue
            supplier_name = (prof.get("supplier_names") or {}).get(policy.supplier)
            matched_profile_mode = str(prof.get("mode", "")).upper() if prof.get("mode") is not None else None
            break

        # Fallback 1: if we matched a profile by scope but supplier_names is missing,
        # try to resolve using the first profile that has supplier_names for this supplier.
        if not supplier_name:
            for prof in profiles:
                names = prof.get("supplier_names") or {}
                if policy.supplier in names:
                    supplier_name = names.get(policy.supplier)
                    matched_profile_mode = str(prof.get("mode", "")).upper() if prof.get("mode") is not None else None
                    break

        # Если по какой-то причине имя не найдено — лучше НЕ собирать факты вообще,
        # чем случайно собрать заказы всех поставщиков.
        if not supplier_name:
            results.append(
                {
                    "policy_log_id": policy.id,
                    "order_id": None,
                    "excess_profit": None,
                    "note": f"skip: supplier_name not resolved for supplier={policy.supplier} city={policy.city} policy_mode={policy.mode} matched_profile_mode={matched_profile_mode}",
                }
            )
            continue

        # В SalesDrive supplier приходит как человеко-читабельное имя (Biotus/DSN/DOBAVKI.UA/...)
        # Поэтому фильтруем по имени из supplier_names.
        # Код (D1/D2/...) добавляем только как запасной вариант, если вдруг где-то хранится код.
        supplier_aliases = [supplier_name]
        if str(policy.supplier).strip() and str(policy.supplier) not in supplier_aliases:
            supplier_aliases.append(str(policy.supplier))

        def _norm_text(v: Any) -> str:
            return str(v or "").strip().lower()

        def _normalize_city(v: Any) -> str:
            """Normalize city names coming from SalesDrive to a canonical key.

            We expect config cities like: Kyiv, Lviv, Kremenchuk, Ivano-Frankivsk.
            Orders may contain UA/RU variants or different transliteration.
            """
            s = _norm_text(v)
            if not s:
                return ""

            # Common punctuation/hyphen variants
            s = s.replace("–", "-").replace("—", "-")

            # Canonical mapping (lowercased)
            mapping = {
                # Kyiv
                "kyiv": "kyiv",
                "kiev": "kyiv",
                "київ": "kyiv",
                "киев": "kyiv",
                "м. київ": "kyiv",
                "г. киев": "kyiv",
                "город киев": "kyiv",

                # Lviv
                "lviv": "lviv",
                "львів": "lviv",
                "львов": "lviv",
                "м. львів": "lviv",
                "г. львов": "lviv",

                # Kremenchuk
                "kremenchuk": "kremenchuk",
                "кременчук": "kremenchuk",
                "кременчуг": "kremenchuk",

                # Ivano-Frankivsk
                "ivano-frankivsk": "ivano-frankivsk",
                "ivano frankivsk": "ivano-frankivsk",
                "ивано-франковск": "ivano-frankivsk",
                "ивано франковск": "ivano-frankivsk",
                "івано-франківськ": "ivano-frankivsk",
                "івано франківськ": "ivano-frankivsk",
            }

            # Exact mapping first
            if s in mapping:
                return mapping[s]

            # Heuristics (substring) for common cases
            if "київ" in s or "киев" in s or s == "kiev" or s == "kyiv":
                return "kyiv"
            if "льв" in s or s == "lviv":
                return "lviv"
            if "кременч" in s:
                return "kremenchuk"
            if "івано" in s or "ивано" in s or "frank" in s:
                return "ivano-frankivsk"

            return s

        def _fallback_extract_city(order_obj: Any) -> Any:
            """Best-effort city extraction if salesdrive_client._extract_city_value is absent."""
            try:
                if isinstance(order_obj, dict):
                    # Try common keys
                    for k in (
                        "city",
                        "clientCity",
                        "deliveryCity",
                        "shippingCity",
                        "receiverCity",
                        "warehouseCity",
                    ):
                        if k in order_obj and order_obj.get(k):
                            return order_obj.get(k)
                    # Sometimes stored in nested structures
                    for parent_key in ("customer", "delivery", "shipping", "receiver"):
                        if parent_key in order_obj and isinstance(order_obj.get(parent_key), dict):
                            nested = order_obj.get(parent_key) or {}
                            for k in ("city", "clientCity", "deliveryCity", "shippingCity", "receiverCity"):
                                if nested.get(k):
                                    return nested.get(k)
            except Exception:
                pass
            return None

        def _city_matches(order_obj: Any, expected_city: str) -> bool:
            if not expected_city:
                return True

            # Extract city value
            if _extract_city_value is not None:
                try:
                    val = _extract_city_value(order_obj)
                except Exception:
                    val = None
            else:
                val = _fallback_extract_city(order_obj)

            # If we can't extract city at all, do NOT filter it out (avoid false negatives)
            if val is None:
                return True

            return _normalize_city(val) == _normalize_city(expected_city)

        # IMPORTANT:
        # Забор из SalesDrive делаем ТОЛЬКО по времени. Любые фильтры (city/supplier)
        # выполняем здесь, чтобы 1) не терять заказы из-за несовпадений форматов, 2) иметь диагностику.
        window_key = (str(policy.segment_start), str(policy.segment_end))

        async def _fetch_with_retries() -> list[Any]:
            # Simple retry loop; SalesDrive sometimes returns 400 on bursts.
            last_err = None
            for attempt in range(1, 4):
                try:
                    return await fetch_orders_for_segment(
                        city=None,
                        supplier_aliases=[],  # IMPORTANT: no supplier filter at SalesDrive side
                        start_dt=policy.segment_start,
                        end_dt=policy.segment_end,
                    )
                except Exception as e:
                    last_err = e
                    # backoff: 0.3s, 0.9s, 2.7s
                    try:
                        import asyncio
                        await asyncio.sleep(0.3 * (3 ** (attempt - 1)))
                    except Exception:
                        pass
            raise last_err  # type: ignore[misc]

        if window_key in _orders_cache:
            raw_orders = _orders_cache[window_key]
        else:
            try:
                raw_orders = await _fetch_with_retries()
                _orders_cache[window_key] = raw_orders
            except Exception as e:
                # Do not crash the whole job; record diagnostics and continue.
                note = f"salesdrive_fetch_error: {type(e).__name__}: {e}"
                # If it's an HTTPStatusError, include status, url and response text head.
                if httpx is not None and isinstance(e, httpx.HTTPStatusError):
                    try:
                        status = e.response.status_code
                        url = str(e.request.url)
                        txt = (e.response.text or "")
                        note = (
                            f"salesdrive_http_error status={status} url={url} "
                            f"resp_head={txt[:500]}"
                        )
                    except Exception:
                        pass
                results.append(
                    {
                        "policy_log_id": policy.id,
                        "order_id": None,
                        "excess_profit": None,
                        "note": note,
                    }
                )
                continue

        fetched_total = len(raw_orders)

        # Диагностика: какие supplier реально пришли в выборке
        try:
            from collections import Counter
            suppliers_top = Counter([str(_extract_supplier_value(r)) for r in raw_orders]).most_common(10)
        except Exception:
            suppliers_top = []

        # Диагностика: какие города реально пришли в выборке
        try:
            from collections import Counter
            if _extract_city_value is not None:
                _cities_raw = [str(_extract_city_value(r)) for r in raw_orders]
            else:
                _cities_raw = [str(_fallback_extract_city(r)) for r in raw_orders]
            cities_top = Counter([_normalize_city(x) for x in _cities_raw if x and x != "None"]).most_common(10)
        except Exception:
            cities_top = []

        _aliases_norm = {_norm_text(a) for a in supplier_aliases if a}

        def _supplier_matches(order_obj: Any) -> bool:
            if not _aliases_norm:
                return True
            val = _extract_supplier_value(order_obj)
            return _norm_text(val) in _aliases_norm

        # 1) фильтр по поставщику (по alias-именам из supplier_names)
        orders = [o for o in raw_orders if _supplier_matches(o)]
        supplier_filtered_total = len(orders)

        # Диагностика: какие города у ЭТОГО supplier в выборке (до фильтра по policy.city)
        try:
            from collections import Counter
            if _extract_city_value is not None:
                _sup_cities_raw = [str(_extract_city_value(r)) for r in orders]
            else:
                _sup_cities_raw = [str(_fallback_extract_city(r)) for r in orders]
            supplier_cities_top = Counter(
                [_normalize_city(x) for x in _sup_cities_raw if x and x != "None"]
            ).most_common(10)
        except Exception:
            supplier_cities_top = []

        # 2) фильтр по городу
        orders = [o for o in orders if _city_matches(o, policy.city)]
        city_filtered_total = len(orders)

        filtered_total = city_filtered_total

        if filtered_total == 0:
            # Не ошибка: просто для понимания почему пусто
            results.append(
                {
                    "policy_log_id": policy.id,
                    "order_id": None,
                    "excess_profit": None,
                    "note": (
                        f"no orders for policy after filtering: supplier={policy.supplier} city={policy.city} "
                        f"aliases={supplier_aliases} fetched_total={fetched_total} "
                        f"supplier_filtered_total={supplier_filtered_total} filtered_total={filtered_total} "
                        f"suppliers_top={suppliers_top} cities_top={cities_top} supplier_cities_top={supplier_cities_top}"
                    ),
                }
            )
            continue

        for order in orders:
            fact = build_order_facts(policy, order)
            obj = await upsert_order_fact(fact)
            results.append(
                {
                    "policy_log_id": policy.id,
                    "order_id": obj.order_id,
                    "excess_profit": float(obj.excess_profit),
                    "note": None,
                }
            )

    return results


# --- New function: run_balancer_pipeline_async ---

async def run_balancer_pipeline_async() -> dict[str, Any]:
    """Run full balancer pipeline for the current BALANCER_RUN_MODE.

    This is a convenience orchestrator so we can run the 4 core steps in order:
      1) start_segment_apply_async
      2) collect_orders_for_last_policy_async
      3) aggregate_stats_for_last_policy_async
      4) compute_day_metrics_for_last_policies_async

    Returns a short summary that is easy to print as JSON.
    """

    # --- TTL cleanup (best-effort) ---
    # Keep balancer tables bounded in size. Default keep_days=90, override via env.
    ttl_cleanup: dict[str, int] | None = None
    try:
        keep_days = int(os.getenv("BALANCER_TTL_KEEP_DAYS", "90") or 90)
        ttl_cleanup = await cleanup_old_balancer_data(keep_days=keep_days)
    except Exception:
        ttl_cleanup = None

    # --- Snapshot BEST(global) before the run (to detect changes) ---
    best_before: dict[str, Any] | None = None
    best_after: dict[str, Any] | None = None
    seg_id_for_best: str | None = None
    day_date_for_best = None

    try:
        cfg0 = load_config()
        first_profile = (cfg0.profiles or [])[0] if getattr(cfg0, "profiles", None) else None
        if first_profile:
            seg0 = resolve_current_segment(first_profile)
            seg_id_for_best = str(seg0.segment_id)
            day_date_for_best = seg0.start.date()
            best_asof0 = day_date_for_best + timedelta(days=1)
            best_before = await get_best_porog_30d_global_best(
                segment_id=seg_id_for_best,
                day_date=best_asof0,
                lookback_days=30,
            )
    except Exception:
        best_before = None
    collect_end = _parse_collect_segment_end_utc()

    if collect_end is not None:
        # End-of-segment run: close the finished segment first, then apply policies for the NEW current segment.
        collected = await collect_orders_for_last_policy_async()
        aggregated = await aggregate_stats_for_last_policy_async()
        metrics = await compute_day_metrics_for_last_policies_async()
        applied = await start_segment_apply_async()
    else:
        # Regular run (no boundary): keep old behaviour.
        applied = await start_segment_apply_async()
        collected = await collect_orders_for_last_policy_async()
        aggregated = await aggregate_stats_for_last_policy_async()
        metrics = await compute_day_metrics_for_last_policies_async()

    # --- Snapshot BEST(global) after the run ---
    try:
        if seg_id_for_best and day_date_for_best:
            best_asof1 = day_date_for_best + timedelta(days=1)
            best_after = await get_best_porog_30d_global_best(
                segment_id=seg_id_for_best,
                day_date=best_asof1,
                lookback_days=30,
            )
    except Exception:
        best_after = None

    collected_facts = [x for x in collected if x.get("policy_log_id") is not None and x.get("order_id") is not None]

    # --- Telegram notification block (RU, end-of-run summary) ---
    def _parse_top_from_note(note: str, key: str) -> list[tuple[str, int]]:
        """Extract list of (name, count) tuples from a diagnostic note string for a given key."""
        import re
        try:
            idx = note.find(f"{key}=[")
            if idx == -1:
                return []
            start = idx + len(f"{key}=[")
            end = note.find("]", start)
            if end == -1:
                return []
            substr = note[start:end]
            pattern = r"\('([^']+)',\s*(\d+)\)"
            return [(name, int(cnt)) for name, cnt in re.findall(pattern, substr)]
        except Exception:
            return []

    def _fmt_best(best_map: dict[str, Any] | None) -> tuple[str, dict[str, tuple[float | None, str]]]:
        """Return pretty lines and normalized mapping band_id -> (porog, source)."""
        if not best_map:
            return "— нет данных —", {}
        norm: dict[str, tuple[float | None, str]] = {}
        parts: list[str] = []
        for band_id, info in (best_map or {}).items():
            try:
                porog = float((info or {}).get("porog"))
            except Exception:
                porog = None
            src = str((info or {}).get("source") or "-")
            norm[str(band_id)] = (porog, src)
            if porog is None:
                parts.append(f"{band_id}: ?% ({src})")
            else:
                parts.append(f"{band_id}: {porog*100:.2f}% ({src})")
        return ", ".join(parts) if parts else "— нет данных —", norm

    def _diff_best(before: dict[str, tuple[float | None, str]], after: dict[str, tuple[float | None, str]]) -> str:
        keys = sorted(set(before.keys()) | set(after.keys()))
        changes: list[str] = []
        for k in keys:
            b = before.get(k)
            a = after.get(k)
            if b == a:
                continue
            b_por = None if not b else b[0]
            a_por = None if not a else a[0]
            if b_por is None and a_por is None:
                changes.append(f"{k}: ? → ?")
            elif b_por is None:
                changes.append(f"{k}: ? → {a_por*100:.2f}%")
            elif a_por is None:
                changes.append(f"{k}: {b_por*100:.2f}% → ?")
            else:
                changes.append(f"{k}: {b_por*100:.2f}% → {a_por*100:.2f}%")
        return "; ".join(changes) if changes else "без изменений"

    try:
        run_mode = (os.getenv("BALANCER_RUN_MODE") or "").upper() or None
        collect_end = _parse_collect_segment_end_utc()

        applied_policies = len(applied)

        # Determine segment window for message
        seg_id = None
        seg_start = None
        seg_end = None
        if applied:
            try:
                seg_id = applied[0].get("segment_id")
                seg_start = applied[0].get("segment_start")
                seg_end = applied[0].get("segment_end")
            except Exception:
                seg_id = None
        if not seg_id:
            try:
                cfg0 = load_config()
                first_profile = (cfg0.profiles or [])[0] if getattr(cfg0, "profiles", None) else None
                if first_profile:
                    seg0 = resolve_current_segment(first_profile)
                    seg_id = str(seg0.segment_id)
                    seg_start = str(seg0.start)
                    seg_end = str(seg0.end)
            except Exception:
                pass

        # Count only real collected facts
        collected_facts = [x for x in collected if x.get("policy_log_id") is not None and x.get("order_id") is not None]
        collected_facts_count = len(collected_facts)

        # Profit stats
        eps_values = []
        for x in collected_facts:
            try:
                eps_values.append(float(x.get("excess_profit") or 0.0))
            except Exception:
                pass
        eps_sum = sum(eps_values) if eps_values else 0.0
        eps_avg = (eps_sum / collected_facts_count) if collected_facts_count else 0.0

        # Empty policies diagnostics
        empty_notes = [
            x for x in collected
            if x.get("note", "") and "no orders for policy after filtering" in str(x.get("note", ""))
        ]
        empty_count = len(empty_notes)

        suppliers_counter = Counter()
        cities_counter = Counter()
        for en in empty_notes:
            note = str(en.get("note", ""))
            for s, c in _parse_top_from_note(note, "suppliers_top"):
                suppliers_counter[s] += c
            for city, c in _parse_top_from_note(note, "cities_top"):
                cities_counter[city] += c
        suppliers_top = suppliers_counter.most_common(5)
        cities_top = cities_counter.most_common(5)

        # Applied reason breakdown (LIVE)
        applied_reasons = Counter()
        if run_mode == "LIVE":
            for a in applied:
                r = a.get("reason", "")
                applied_reasons[str(r or "-")] += 1

        # Compact band_sources aggregation from applied policies (if present)
        band_sources_counter = Counter()
        try:
            for a in applied:
                # `a` is dict from start_segment_apply_async; it doesn't include reason_details
                # so we only show counts by reason here.
                pass
        except Exception:
            pass

        best_before_s, best_before_norm = _fmt_best(best_before)
        best_after_s, best_after_norm = _fmt_best(best_after)
        best_diff = _diff_best(best_before_norm, best_after_norm)

        msg_lines: list[str] = []
        msg_lines.append("📊 Балансировщик: запуск завершён")
        msg_lines.append(f"Режим: {run_mode or '-' }")
        if collect_end is not None:
            msg_lines.append(f"Тип запуска: закрытие сегмента (boundary_end_utc={collect_end.isoformat()})")
        else:
            msg_lines.append("Тип запуска: периодический/ручной (без boundary)")
        if seg_id:
            msg_lines.append(f"Сегмент: {seg_id}")
        if seg_start and seg_end:
            msg_lines.append(f"Окно: {seg_start} → {seg_end}")

        if run_mode == "LIVE" and applied_policies > 0:
            by_reason = ", ".join(f"{k}={v}" for k, v in applied_reasons.items())
            msg_lines.append(f"Применено политик: {applied_policies} ({by_reason})")
        else:
            msg_lines.append(f"Применено политик: {applied_policies}")

        msg_lines.append(f"Фактов записано: {collected_facts_count}")
        msg_lines.append(f"Σ excess_profit: {eps_sum:.2f} грн; среднее: {eps_avg:.2f} грн/заказ")
        msg_lines.append(f"Пустых политик (0 заказов после фильтров): {empty_count}")

        if ttl_cleanup:
            msg_lines.append(
                "TTL cleanup: " + ", ".join(f"{k}={v}" for k, v in ttl_cleanup.items())
            )

        if suppliers_top:
            msg_lines.append(
                "Топ поставщиков в выборке: " + ", ".join(f"{name}({cnt})" for name, cnt in suppliers_top)
            )
        if cities_top:
            msg_lines.append(
                "Топ городов в выборке: " + ", ".join(f"{name}({cnt})" for name, cnt in cities_top)
            )

        msg_lines.append("")
        msg_lines.append("🏁 Best пороги (global, 30d)")
        msg_lines.append(f"До: {best_before_s}")
        msg_lines.append(f"После: {best_after_s}")
        msg_lines.append(f"Изменения: {best_diff}")

        msg = "\n".join(msg_lines)
        try:
            send_notification(msg, "Разработчик")
        except Exception:
            pass
    except Exception:
        pass

    return {
        "run_mode": (os.getenv("BALANCER_RUN_MODE") or "").upper() or None,
        "ttl_cleanup": ttl_cleanup,
        "applied_policies": len(applied),
        "applied": applied,
        "collected_total": len(collected),
        "collected_facts": len(collected_facts),
        "collected": collected,
        "aggregated_rows": len(aggregated),
        "aggregated": aggregated,
        "metrics_rows": len(metrics),
        "metrics": metrics,
    }

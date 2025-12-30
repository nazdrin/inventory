from __future__ import annotations

import os

from typing import Any

from .config import load_config
from .segments import resolve_current_segment
from .policy import build_test_policy_async, build_live_policy_async
from .repository import create_policy_log_record, upsert_policy_log
from .salesdrive_client import fetch_orders_for_segment
from .order_processor import build_order_facts

__all__ = [
    "start_segment_dry_run_async",
    "start_segment_apply_async",
    "collect_orders_for_last_policy_async",
    "aggregate_stats_for_last_policy_async",
    "compute_day_metrics_for_last_policies_async",
]


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
        any_best = any(str(v) == "best_30d" for v in band_sources.values())
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
                        # LIVE: строим политику из best_30d (с фоллбеком на min_porog_by_band внутри policy.py)
                        policy = await build_live_policy_async(
                            profile,
                            city=city,
                            supplier=supplier,
                            segment_id=seg.segment_id,
                            day_date=day_date,
                            suppliers_in_profile=[str(s) for s in suppliers] if len(suppliers) > 1 else None,
                        )
                        # Normalize reason to avoid misleading "best_30d" when we actually used fallback for all bands.
                        norm_reason, norm_details = _normalized_reason_and_details(policy)
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
                if obj.city not in (scope.get("cities", []) or []):
                    continue
                if obj.supplier not in (scope.get("suppliers", []) or []):
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
            if policy.city not in (scope.get("cities", []) or []):
                continue
            if policy.supplier not in (scope.get("suppliers", []) or []):
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
            if policy.city not in (scope.get("cities", []) or []):
                continue
            if policy.supplier not in (scope.get("suppliers", []) or []):
                continue
            thresholds = prof.get("thresholds", {}) or {}
            return int(thresholds.get("min_orders_per_segment", 0) or 0)
        return 0

    results: list[dict[str, Any]] = []

    policies = await get_last_applied_policies()

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
                allowed_set.add((profile_mode, city, supplier))
    # Only filter if allowed_set is non-empty
    before_count = len(policies)
    if allowed_set:
        filtered_policies = [
            p for p in policies
            if (str(p.mode).upper(), p.city, p.supplier) in allowed_set
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
            if policy.city not in (scope.get("cities", []) or []):
                continue
            if policy.supplier not in (scope.get("suppliers", []) or []):
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

        def _city_matches(order_obj: Any, expected_city: str) -> bool:
            if not expected_city:
                return True
            if _extract_city_value is None:
                # If we can't extract city reliably, don't filter it here.
                return True
            val = _extract_city_value(order_obj)
            return _norm_text(val) == _norm_text(expected_city)

        # IMPORTANT:
        # Забор из SalesDrive делаем ТОЛЬКО по времени. Любые фильтры (city/supplier)
        # выполняем здесь, чтобы 1) не терять заказы из-за несовпадений форматов, 2) иметь диагностику.
        raw_orders = await fetch_orders_for_segment(
            city=None,
            supplier_aliases=[],
            start_dt=policy.segment_start,
            end_dt=policy.segment_end,
        )

        fetched_total = len(raw_orders)

        # Диагностика: какие supplier реально пришли в выборке
        try:
            from collections import Counter

            suppliers_top = Counter([str(_extract_supplier_value(r)) for r in raw_orders]).most_common(10)
        except Exception:
            suppliers_top = []

        def _supplier_matches(order_obj: Any, aliases: list[str]) -> bool:
            if not aliases:
                return True
            val = _extract_supplier_value(order_obj)
            return _norm_text(val) in {_norm_text(a) for a in aliases}

        # 1) фильтр по поставщику (по alias-именам из supplier_names)
        orders = [o for o in raw_orders if _supplier_matches(o, supplier_aliases)]
        supplier_filtered_total = len(orders)

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
                        f"suppliers_top={suppliers_top}"
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

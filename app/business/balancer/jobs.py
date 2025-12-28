from __future__ import annotations

from typing import Any

from .config import load_config
from .segments import resolve_current_segment
from .policy import build_test_policy, build_test_policy_async
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
    out: list[dict[str, Any]] = []

    for profile in cfg.profiles:
        scope = profile.get("scope", {})
        cities = scope.get("cities", [])
        suppliers = scope.get("suppliers", [])

        if not cities or not suppliers:
            continue

        profile_name = profile.get("name") or profile.get("profile_name") or "profile"

        seg = resolve_current_segment(profile)
        day_date = seg.start.date()

        mode = str(profile.get("mode", "TEST")).upper()

        for city in cities:
            for supplier in suppliers:
                if mode == "TEST":
                    policy = await build_test_policy_async(
                        profile,
                        profile_name=profile_name,
                        city=city,
                        supplier=supplier,
                        segment_id=seg.segment_id,
                        day_date=day_date,
                    )
                else:
                    # LIVE пока заглушка (на следующем этапе подключим best_30d)
                    policy = build_test_policy(profile)

                rec = create_policy_log_record(
                    mode=mode,
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


async def start_segment_apply_async() -> list[dict[str, Any]]:
    """Этап 2.5: применяет сегмент — пишет записи в balancer_policy_log.

    Идемпотентно: при повторном запуске для того же набора параметров/правил
    возвращает уже существующую запись (по hash).

    Возвращает список кратких результатов (id, hash, city, supplier, segment_id).
    """

    prepared = await start_segment_dry_run_async()
    results: list[dict[str, Any]] = []

    for payload in prepared:
        obj = await upsert_policy_log(payload)
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

    results: list[dict[str, Any]] = []
    cfg = load_config()

    policies = await get_last_applied_policies()

    for policy in policies:
        # SalesDrive возвращает supplier как имя (например "DSN"), а в policy_log у нас код (например "D2").
        # Поэтому строим список алиасов: [код, имя]
        supplier_name = None
        for profile in cfg.profiles:
            if str(profile.get("mode", "")).upper() != str(policy.mode).upper():
                continue
            scope = profile.get("scope", {})
            if policy.city not in scope.get("cities", []):
                continue
            if policy.supplier not in scope.get("suppliers", []):
                continue
            supplier_name = (profile.get("supplier_names") or {}).get(policy.supplier)
            break

        supplier_aliases = [policy.supplier]
        if supplier_name:
            supplier_aliases.append(supplier_name)

        orders = await fetch_orders_for_segment(
            city=policy.city,
            supplier_aliases=supplier_aliases,
            start_dt=policy.segment_start,
            end_dt=policy.segment_end,
        )

        for order in orders:
            fact = build_order_facts(policy, order)
            obj = await upsert_order_fact(fact)
            results.append(
                {
                    "policy_log_id": policy.id,
                    "order_id": obj.order_id,
                    "excess_profit": float(obj.excess_profit),
                }
            )

    return results

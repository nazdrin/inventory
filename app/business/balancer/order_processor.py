from __future__ import annotations

from typing import Any
from decimal import Decimal
from datetime import datetime
from zoneinfo import ZoneInfo


KYIV_TZ = ZoneInfo("Europe/Kyiv")


def _get_profile_from_snapshot(policy: Any) -> dict[str, Any] | None:
    """
    В policy.config_snapshot мы сохраняем cfg.balancer, где есть profiles.

    ВАЖНО: config_snapshot — это снимок всего cfg.balancer, где могут быть одновременно
    TEST и LIVE профили. Поэтому профиль нужно выбирать по scope (city + supplier),
    а НЕ по mode.

    Приоритет выбора:
      1) точное совпадение: city ∈ scope.cities И supplier ∈ scope.suppliers
      2) fallback: supplier ∈ scope.suppliers (если city в заказах смешан/не совпал)
      3) fallback: любой профиль, где есть price_bands (чтобы не падать в проде)
    """
    snap = getattr(policy, "config_snapshot", None) or {}
    profiles = snap.get("profiles", []) or []

    policy_city = getattr(policy, "city", None)
    policy_supplier = getattr(policy, "supplier", None)

    # 1) основной матч: по scope (city + supplier)
    for prof in profiles:
        scope = prof.get("scope", {}) or {}
        if policy_city not in (scope.get("cities", []) or []):
            continue
        if policy_supplier not in (scope.get("suppliers", []) or []):
            continue
        return prof

    # 2) fallback: по supplier
    for prof in profiles:
        scope = prof.get("scope", {}) or {}
        if policy_supplier in (scope.get("suppliers", []) or []):
            return prof

    # 3) fallback: любой профиль с price_bands
    for prof in profiles:
        if prof.get("price_bands"):
            return prof

    return None


def _resolve_band(price: Decimal, price_bands: list[dict[str, Any]]) -> tuple[str, Decimal, Decimal | None]:
    """
    Возвращает (band_id, band_min, band_max)
    """
    for b in price_bands:
        b_min = Decimal(str(b.get("min", 0) or 0))
        b_max_raw = b.get("max", None)
        b_max = None if b_max_raw is None else Decimal(str(b_max_raw))
        if price >= b_min and (b_max is None or price < b_max):
            return str(b.get("band_id")), b_min, b_max

    # fallback: если ничего не подошло — берем последнюю
    last = price_bands[-1]
    b_min = Decimal(str(last.get("min", 0) or 0))
    b_max_raw = last.get("max", None)
    b_max = None if b_max_raw is None else Decimal(str(b_max_raw))
    return str(last.get("band_id")), b_min, b_max


def _get_porog_for_band(rules: list[dict[str, Any]], band_id: str) -> Decimal:
    for r in rules:
        if str(r.get("band_id")) == str(band_id):
            return Decimal(str(r.get("porog")))
    # если вдруг правила не содержат band — тогда порог = min_porog (безопасно)
    return Decimal("0")


def _parse_salesdrive_dt(value: Any) -> datetime | None:
    """
    SalesDrive отдаёт время строкой вида 'YYYY-MM-DD HH:MM:SS'
    """
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=KYIV_TZ)
        except Exception:
            return None
    return None


def build_order_facts(policy: Any, order: dict[str, Any]) -> dict[str, Any]:
    """
    1) sale_sum = Σ(price * amount)
    2) cost_sum = Σ(costPrice * amount) если costPrice есть у всех,
       иначе cost_sum = opt (общая себестоимость заказа)
    3) profit = sale_sum - cost_sum
    4) variable_cost = cost_sum * min_porog (по band)
    5) net_profit = profit - variable_cost
    """

    profile = _get_profile_from_snapshot(policy) or {}
    profile_name = profile.get("name") or profile.get("profile_name") or "unknown"

    price_bands = profile.get("price_bands") or []
    if not price_bands:
        raise RuntimeError(
            "price_bands is empty in config_snapshot (cannot resolve band): "
            f"policy_id={getattr(policy, 'id', None)} "
            f"mode={getattr(policy, 'mode', None)} "
            f"city={getattr(policy, 'city', None)} "
            f"supplier={getattr(policy, 'supplier', None)} "
            f"matched_profile_name={profile_name} "
            f"matched_profile_mode={profile.get('mode')}"
        )

    products = order.get("products", []) or []

    sale_sum = Decimal("0")
    cost_sum = Decimal("0")
    missing_cost = False

    for p in products:
        qty = Decimal(str(p.get("amount", 0) or 0))
        price = Decimal(str(p.get("price", 0) or 0))
        sale_sum += price * qty

        if p.get("costPrice") is None:
            missing_cost = True
        else:
            cost = Decimal(str(p.get("costPrice", 0) or 0))
            cost_sum += cost * qty

    if missing_cost:
        cost_sum = Decimal(str(order.get("opt", 0) or 0))

    profit = sale_sum - cost_sum

    band_id, band_min, band_max = _resolve_band(sale_sum, price_bands)

    min_porog_map = getattr(policy, "min_porog_by_band", None) or {}
    min_porog = Decimal(str(min_porog_map.get(band_id)))

    rules = getattr(policy, "rules", None) or []
    porog_used = _get_porog_for_band(rules, band_id)
    if porog_used == Decimal("0"):
        porog_used = min_porog

    # Переменные затраты по ТЗ: variable_cost = order_cost * min_threshold%
    # Здесь order_cost уже посчитан как cost_sum
    min_profit = cost_sum * min_porog
    excess_profit = profit - min_profit

    created_at_source = _parse_salesdrive_dt(order.get("orderTime"))

    return {
        "policy_log_id": policy.id,
        "profile_name": profile_name,
        "mode": policy.mode,
        "city": policy.city,
        "supplier": policy.supplier,
        "segment_id": policy.segment_id,
        "segment_start": policy.segment_start,
        "segment_end": policy.segment_end,
        "order_id": str(order.get("id")),
        "order_number": order.get("tabletkiOrder"),
        "status_id": int(order.get("statusId")),
        "created_at_source": created_at_source,
        "band_id": band_id,
        "band_min_price": band_min,
        "band_max_price": band_max,
        "sale_price": sale_sum,
        "cost": cost_sum,
        "profit": profit,
        "gross_profit": profit,
        "variable_cost": min_profit,
        "net_profit": excess_profit,
        "porog_used": porog_used,
        "min_porog": min_porog,
        "min_profit": min_profit,
        "excess_profit": excess_profit,
        "is_in_scope": True,
        "note": None,
        "raw": order,
    }
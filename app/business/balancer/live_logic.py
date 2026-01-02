# app/business/balancer/live_logic.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

# NOTE:
# Этот модуль специально "самодостаточный" и максимально защитный:
# - Все доступы к БД делаем через repository, но импортируем локально/внутри функций,
#   чтобы не ломать импорт при частичных конфигурациях.
# - Если каких-то функций repository нет — логика деградирует мягко (ничего не меняет).


@dataclass(frozen=True)
class LiveControls:
    """Параметры LIVE-логики (берём из profile['live'] в config.yaml)."""
    daily_order_limit: Optional[int] = None          # дневной лимит заказов (например 200)
    step: float = 0.01                              # шаг изменения порога (например 0.01 == +1%)
    max_iterations: int = 10                        # максимум итераций изменения за день
    min_porog_floor: float = 0.0                    # нижний пол (обычно 0.0)
    max_porog_cap: float = 0.50                     # верхний кап (например 0.50 == 50%)
    degrade_threshold: float = 0.15                 # порог деградации (например 0.15 == 15%)
    enabled: bool = True                            # рубильник


def load_live_controls(profile: Dict[str, Any]) -> LiveControls:
    """Достаём live-настройки из профиля YAML."""
    live = (profile or {}).get("live") or {}
    try:
        step = float(live.get("step", 0.01) or 0.01)
    except Exception:
        step = 0.01
    try:
        max_iterations = int(live.get("max_iterations", 10) or 10)
    except Exception:
        max_iterations = 10

    def _opt_int(v) -> Optional[int]:
        if v is None or v == "":
            return None
        try:
            return int(v)
        except Exception:
            return None

    def _opt_float(v, default: float) -> float:
        try:
            return float(v)
        except Exception:
            return default

    return LiveControls(
        daily_order_limit=_opt_int(live.get("daily_order_limit")),
        step=step,
        max_iterations=max_iterations,
        min_porog_floor=_opt_float(live.get("min_porog_floor", 0.0), 0.0),
        max_porog_cap=_opt_float(live.get("max_porog_cap", 0.50), 0.50),
        degrade_threshold=_opt_float(live.get("degrade_threshold", 0.15), 0.15),
        enabled=bool(live.get("enabled", True)),
    )


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _as_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _safe_dict(d: Any) -> Dict[str, Any]:
    return dict(d or {}) if isinstance(d, dict) else {}


def extract_live_iter(reason_details: Dict[str, Any]) -> int:
    """Достаём итерацию из reason_details (вариант A)."""
    rd = _safe_dict(reason_details)
    return _as_int(rd.get("live_iter"), 0)


async def get_today_live_iteration(
    *,
    city: str,
    supplier: str,
    day_date: date,
    segment_id: Optional[str] = None,
) -> int:
    """
    Вариант A: iteration = max(live_iter) среди LIVE политик за сегодня (city/supplier).
    Если segment_id передан — можно сузить.
    """
    try:
        from .repository import get_last_applied_policies
    except Exception:
        return 0

    try:
        policies = await get_last_applied_policies()
    except Exception:
        return 0

    it = 0
    for p in policies or []:
        try:
            if str(getattr(p, "mode", "")).upper() != "LIVE":
                continue
            if str(getattr(p, "city", "")) != str(city):
                continue
            if str(getattr(p, "supplier", "")) != str(supplier):
                continue
            ss = getattr(p, "segment_start", None)
            if not ss:
                continue
            if ss.date() != day_date:
                continue
            if segment_id is not None and str(getattr(p, "segment_id", "")) != str(segment_id):
                continue

            rd = _safe_dict(getattr(p, "reason_details", None))
            it = max(it, extract_live_iter(rd))
        except Exception:
            continue

    return it


async def get_day_total_orders_so_far(
    *,
    mode: str,
    city: str,
    supplier: str,
    day_date: date,
) -> int:
    """
    Достаём "сколько заказов за день уже набрали" по (mode, city, supplier).
    Стараемся взять day_total_orders из segment_stats (если уже вычислялось),
    иначе fallback: суммируем orders_count по всем строкам за день.
    """
    try:
        from .repository import get_segment_stats_for_day_scope
    except Exception:
        return 0

    try:
        rows = await get_segment_stats_for_day_scope(
            mode=mode,
            city=city,
            supplier=supplier,
            day_date=day_date,
        )
    except Exception:
        return 0

    if not rows:
        return 0

    # 1) если day_total_orders заполнен — возьмём max среди строк (они одинаковые, но на всякий случай max)
    dvals = []
    for r in rows:
        try:
            v = getattr(r, "day_total_orders", None)
            if v is not None:
                dvals.append(_as_int(v, 0))
        except Exception:
            pass
    if dvals:
        return max(dvals)

    # 2) fallback: суммируем orders_count
    total = 0
    for r in rows:
        try:
            total += _as_int(getattr(r, "orders_count", 0), 0)
        except Exception:
            pass
    return total


def _increase_rules_by_step(
    rules: List[Dict[str, Any]],
    *,
    step: float,
    floor: float,
    cap: float,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rules or []:
        band_id = str((r or {}).get("band_id") or "")
        porog = _as_float((r or {}).get("porog"), 0.0)
        porog2 = _clamp(porog + step, floor, cap)
        out.append({"band_id": band_id, "porog": porog2})
    return out


async def apply_live_controls(
    *,
    profile: Dict[str, Any],
    city: str,
    supplier: str,
    segment_id: str,
    day_date: date,
    base_rules: List[Dict[str, Any]],
    base_reason: str,
    base_reason_details: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
    """
    Главная функция для jobs.py:
    - принимает уже построенные base_rules (например best_30d_global)
    - применяет LIVE-правила (пока: дневной лимит заказов)
    - возвращает (rules, reason, reason_details)

    Важно: эта функция НЕ пишет в БД, только вычисляет.
    """
    controls = load_live_controls(profile)
    if not controls.enabled:
        rd = _safe_dict(base_reason_details)
        rd.setdefault("live", {})
        rd["live"]["enabled"] = False
        return base_rules, base_reason, rd

    # Итерация "A": хранится в reason_details у последней LIVE-политики за день
    current_iter = await get_today_live_iteration(
        city=city,
        supplier=supplier,
        day_date=day_date,
        segment_id=None,  # за день по связке city+supplier
    )

    rd = _safe_dict(base_reason_details)
    rd.setdefault("live", {})
    rd["live"]["enabled"] = True
    rd["live"]["prev_iter"] = int(current_iter)

    # Базовые стоп-условия
    if current_iter >= controls.max_iterations:
        rd["live"]["action"] = "stop_max_iterations"
        rd["live"]["max_iterations"] = int(controls.max_iterations)
        rd["live"]["note"] = "iteration cap reached; keep base rules"
        # фиксируем итерацию (не увеличиваем)
        rd["live_iter"] = int(current_iter)
        return base_rules, base_reason, rd

    # Считаем текущие заказы за день (по сегментной статистике)
    day_total = await get_day_total_orders_so_far(
        mode="LIVE",
        city=city,
        supplier=supplier,
        day_date=day_date,
    )
    rd["live"]["day_total_orders_so_far"] = int(day_total)

    # --- RULE 1: дневной лимит заказов ---
    if controls.daily_order_limit is not None and day_total >= controls.daily_order_limit:
        # если лимит достигнут/превышен — ужесточаем (повышаем пороги)
        new_rules = _increase_rules_by_step(
            base_rules,
            step=controls.step,
            floor=controls.min_porog_floor,
            cap=controls.max_porog_cap,
        )
        rd["live"]["action"] = "increase_due_to_daily_limit"
        rd["live"]["daily_order_limit"] = int(controls.daily_order_limit)
        rd["live"]["step"] = float(controls.step)
        rd["live"]["cap"] = float(controls.max_porog_cap)
        rd["live"]["floor"] = float(controls.min_porog_floor)

        # увеличиваем итерацию
        rd["live_iter"] = int(current_iter + 1)

        # reason можно оставить "best_30d", но лучше явно показать, что правила модифицированы LIVE-логикой
        # (чтобы позже легче отлаживать).
        reason = "live_daily_limit"
        return new_rules, reason, rd

    # Если лимит не достигнут — оставляем базовые правила
    rd["live"]["action"] = "keep_base_rules"
    rd["live_iter"] = int(current_iter)  # не меняем
    return base_rules, base_reason, rd


# --- Заготовки под следующую LIVE-часть (сравнение лучше/хуже, деградация, лучший прогон и т.д.) ---

@dataclass(frozen=True)
class LiveComparisonResult:
    """Результат сравнения текущего прогона с эталоном (заготовка под ТЗ 3.2)."""
    is_better: Optional[bool] = None
    degradation_pct: Optional[float] = None
    baseline_profit: Optional[float] = None
    current_profit: Optional[float] = None
    note: Optional[str] = None


async def compare_with_baseline_stub(*args, **kwargs) -> LiveComparisonResult:
    """
    Заглушка: позже сюда добавим:
      - эталон (baseline) по прибыли/марже
      - сравнение текущего сегмента
      - правило деградации > X%
      - выбор "лучший результат прогона", остановка
    """
    return LiveComparisonResult(note="not implemented")
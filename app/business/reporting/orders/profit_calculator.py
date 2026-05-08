from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


ZERO = Decimal("0")


def as_decimal(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return ZERO


def money(value: Decimal) -> Decimal:
    return as_decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def percent(numerator: Decimal, denominator: Decimal) -> Decimal:
    denominator_dec = as_decimal(denominator)
    if denominator_dec == ZERO:
        return ZERO
    return (as_decimal(numerator) * Decimal("100") / denominator_dec).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


def calculate_order_financials(
    *,
    order_amount: Decimal,
    sale_amount: Decimal,
    supplier_cost_total: Decimal,
    expense_percent: Decimal,
    is_sale: bool,
) -> dict[str, Decimal]:
    effective_sale_amount = money(sale_amount if is_sale else ZERO)
    effective_cost = money(supplier_cost_total if is_sale else ZERO)
    gross_profit = money(effective_sale_amount - effective_cost)
    expense_amount = money(effective_sale_amount * as_decimal(expense_percent) / Decimal("100"))
    return {
        "order_amount": money(order_amount),
        "sale_amount": effective_sale_amount,
        "supplier_cost_total": effective_cost,
        "gross_profit_amount": gross_profit,
        "expense_percent": as_decimal(expense_percent),
        "expense_amount": expense_amount,
        "net_profit_amount": money(gross_profit - expense_amount),
    }

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from uuid import uuid5, NAMESPACE_URL

from app.integrations.checkbox.config import CheckboxSettings
from app.integrations.checkbox.schemas import CheckboxMappedReceipt


PAYMENT_METHODS = {
    13: ("CARD", "Наложенный платеж"),
    14: ("CARD", "Безнал"),
    15: ("CASH", "Наличными"),
    16: ("CARD", "Оплата на Р/С"),
    20: ("CARD", "Післяплата"),
}


def _as_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _money_to_kopiykas(value: Any) -> int:
    amount = _as_decimal(value)
    return int((amount * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _qty_to_thousandths(value: Any) -> int:
    qty = _as_decimal(value)
    return int((qty * Decimal("1000")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _first_clean(value: Any) -> str:
    if isinstance(value, list):
        for item in value:
            cleaned = _first_clean(item)
            if cleaned:
                return cleaned
        return ""
    if isinstance(value, dict):
        for key in ("email", "phone", "value"):
            cleaned = _first_clean(value.get(key))
            if cleaned:
                return cleaned
        return ""
    return _clean(value)


def _salesdrive_payment_method_id(data: dict[str, Any], settings: CheckboxSettings) -> int:
    raw = data.get("payment_method")
    if raw is None or raw == "":
        raw = data.get("paymentMethod")
    if isinstance(raw, dict):
        raw = raw.get("id") or raw.get("value") or raw.get("parameter")
    try:
        method_id = int(str(raw).strip())
    except (TypeError, ValueError):
        method_id = settings.default_payment_method_id
    return method_id if method_id in PAYMENT_METHODS else settings.default_payment_method_id


def _extract_contact_delivery(data: dict[str, Any]) -> dict[str, str]:
    contacts = data.get("contacts") or []
    contact = contacts[0] if isinstance(contacts, list) and contacts and isinstance(contacts[0], dict) else {}
    delivery: dict[str, str] = {}
    email = _first_clean(contact.get("email") or data.get("email"))
    phone = _first_clean(contact.get("phone") or data.get("phone"))
    if email:
        delivery["email"] = email
    if phone:
        normalized = "".join(ch for ch in phone if ch.isdigit())
        if normalized:
            delivery["phone"] = normalized
    return delivery


def map_salesdrive_order_to_receipt(
    data: dict[str, Any],
    *,
    enterprise_code: str,
    settings: CheckboxSettings,
) -> CheckboxMappedReceipt:
    salesdrive_order_id = _clean(data.get("id"))
    if not salesdrive_order_id:
        raise ValueError("SalesDrive order payload has no id")

    goods: list[dict[str, Any]] = []
    total_kopiykas = 0
    products = data.get("products") or []
    if not isinstance(products, list):
        products = []

    for index, product in enumerate(products, start=1):
        if not isinstance(product, dict):
            continue
        code = _clean(product.get("sku") or product.get("parameter") or product.get("id") or f"line-{index}")
        name = _clean(product.get("name") or product.get("documentName") or product.get("title") or code)
        price_kopiykas = _money_to_kopiykas(product.get("price", product.get("costPerItem", 0)))
        quantity = _qty_to_thousandths(product.get("amount", product.get("quantity", 1)))
        if not code or not name or price_kopiykas <= 0 or quantity <= 0:
            continue

        good = {
            "good": {
                "code": code,
                "name": name[:255],
                "price": price_kopiykas,
            },
            "quantity": quantity,
            "is_return": False,
        }
        if settings.default_tax_code is not None:
            good["good"]["tax"] = [settings.default_tax_code]
        goods.append(good)
        total_kopiykas += int((Decimal(price_kopiykas) * Decimal(quantity) / Decimal("1000")).quantize(Decimal("1")))

    if not goods:
        raise ValueError(f"SalesDrive order has no fiscalizable products: id={salesdrive_order_id}")

    method_id = _salesdrive_payment_method_id(data, settings)
    payment_type, payment_label = PAYMENT_METHODS.get(method_id, PAYMENT_METHODS[settings.default_payment_method_id])
    payment: dict[str, Any] = {
        "type": payment_type,
        "value": total_kopiykas,
        "label": payment_label,
    }

    checkbox_order_id = str(uuid5(NAMESPACE_URL, f"salesdrive:{enterprise_code}:{salesdrive_order_id}"))
    payload: dict[str, Any] = {
        "id": checkbox_order_id,
        "goods": goods,
        "payments": [payment],
        "rounding": False,
        "context": {
            "source": "salesdrive",
            "enterprise_code": enterprise_code,
            "salesdrive_order_id": salesdrive_order_id,
            "salesdrive_external_id": _clean(data.get("externalId")),
        },
    }
    delivery = _extract_contact_delivery(data)
    if delivery:
        payload["delivery"] = delivery

    return CheckboxMappedReceipt(
        checkbox_order_id=checkbox_order_id,
        payload=payload,
        total_amount=(Decimal(total_kopiykas) / Decimal("100")).quantize(Decimal("0.01")),
        items_count=len(goods),
        payment_label=payment_label,
        payment_type=payment_type,
    )

from __future__ import annotations

from typing import Optional


SUPPLIER_CODE_TO_ID = {
    "D1": 38,
    "D2": 39,
    "D3": 40,
    "D4": 41,
    "D5": 42,
    "D6": 43,
    "D7": 44,
    "D8": 45,
    "D9": 46,
    "D10": 47,
    "D11": 48,
    "D12": 49,
    "D13": 51,
}

SUPPLIERLIST_MAP = {
    code: f"id_{supplier_id}"
    for code, supplier_id in SUPPLIER_CODE_TO_ID.items()
}


def get_supplier_id_by_code(supplier_code: str) -> Optional[int]:
    code = str(supplier_code or "").strip().upper()
    if not code:
        return None
    return SUPPLIER_CODE_TO_ID.get(code)


def get_supplier_token_by_code(supplier_code: str) -> Optional[str]:
    code = str(supplier_code or "").strip().upper()
    if not code:
        return None
    return SUPPLIERLIST_MAP.get(code)

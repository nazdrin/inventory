from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DropshipEnterprise


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
    "D14": 52,
}

SUPPLIER_CODE_TO_NAME = {
    "D14": "Fulfillment warehouse",
}

SUPPLIERLIST_MAP = {
    code: f"id_{supplier_id}"
    for code, supplier_id in SUPPLIER_CODE_TO_ID.items()
}


def _normalize_supplier_code(supplier_code: str | None) -> str:
    return str(supplier_code or "").strip().upper()


def build_supplierlist_token(supplier_id: int | None) -> Optional[str]:
    if supplier_id is None:
        return None
    return f"id_{int(supplier_id)}"


def get_supplier_display_name_by_code(supplier_code: str) -> Optional[str]:
    code = _normalize_supplier_code(supplier_code)
    if not code:
        return None
    return SUPPLIER_CODE_TO_NAME.get(code)


def get_supplier_id_by_code(supplier_code: str) -> Optional[int]:
    code = _normalize_supplier_code(supplier_code)
    if not code:
        return None
    return SUPPLIER_CODE_TO_ID.get(code)


def get_supplier_token_by_code(supplier_code: str) -> Optional[str]:
    code = _normalize_supplier_code(supplier_code)
    if not code:
        return None
    return SUPPLIERLIST_MAP.get(code)


async def resolve_supplier_id_by_code(session: AsyncSession, supplier_code: str) -> Optional[int]:
    code = _normalize_supplier_code(supplier_code)
    if not code:
        return None

    supplier_id = (
        await session.execute(
            select(DropshipEnterprise.salesdrive_supplier_id)
            .where(DropshipEnterprise.code == code)
            .limit(1)
        )
    ).scalar_one_or_none()
    if supplier_id is not None:
        return int(supplier_id)

    return get_supplier_id_by_code(code)


async def resolve_supplier_token_by_code(session: AsyncSession, supplier_code: str) -> Optional[str]:
    supplier_id = await resolve_supplier_id_by_code(session, supplier_code)
    if supplier_id is None:
        return None
    return build_supplierlist_token(supplier_id)

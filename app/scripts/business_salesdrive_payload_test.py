from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.business.business_store_order_integration_simulator import (
    simulate_store_order_after_reverse_mapping,
)
from app.business.business_store_order_mapper import resolve_business_store_for_order
from app.business.order_sender import (
    _normalize_order_rows,
    build_salesdrive_payload,
    resolve_salesdrive_organization_context,
)
from app.database import get_async_db


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Business SalesDrive payload preview test without external API calls.")
    parser.add_argument("--store-code", default="")
    parser.add_argument("--branch", default="")
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--external-code", default="")
    parser.add_argument("--price", default="100")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _build_manual_order(*, branch: str | None, product_code: str, price: Decimal) -> dict[str, Any]:
    return {
        "id": "SIM_ORDER",
        "code": "SIM_ORDER_CODE",
        "branchID": branch or "",
        "customerPhone": "",
        "deliveryData": [
            {"key": "DeliveryServiceName", "value": "Nova Poshta"},
            {"key": "ReceiverWhs", "value": "Test receiver branch"},
        ],
        "rows": [
            {
                "goodsCode": str(product_code or ""),
                "goodsName": "Mock product",
                "goodsProducer": "",
                "qty": 1,
                "price": format(price, "f"),
            }
        ],
    }


async def _amain() -> None:
    args = _parse_args()
    external_code = str(args.external_code or "").strip()
    if not external_code:
        raise ValueError("--external-code is required")

    store_code = str(args.store_code or "").strip() or None
    branch = str(args.branch or "").strip() or None
    enterprise_code_arg = str(args.enterprise_code or "").strip() or None
    price = Decimal(str(args.price))

    async with get_async_db(commit_on_exit=False) as session:
        resolved_store = await resolve_business_store_for_order(
            session,
            store_code=store_code,
            tabletki_branch=branch,
            tabletki_enterprise_code=enterprise_code_arg,
        )

        simulator_result = None
        order_for_payload: dict[str, Any]
        effective_enterprise_code = enterprise_code_arg or (str(resolved_store.enterprise_code) if resolved_store is not None else "")
        effective_branch = branch or (str(resolved_store.tabletki_branch) if resolved_store is not None else "")

        if resolved_store is not None:
            simulator_result = await simulate_store_order_after_reverse_mapping(
                session,
                store_id=int(resolved_store.id),
                store_code=resolved_store.store_code,
                tabletki_branch=effective_branch,
                tabletki_enterprise_code=effective_enterprise_code or None,
                external_product_code=external_code,
                qty=1,
                price=price,
            )

        if simulator_result and str(simulator_result.get("status") or "") == "ok":
            order_for_payload = dict(simulator_result.get("normalized_order") or {})
            order_for_payload.setdefault("deliveryData", [
                {"key": "DeliveryServiceName", "value": "Nova Poshta"},
                {"key": "ReceiverWhs", "value": "Test receiver branch"},
            ])
            order_for_payload.setdefault("customerPhone", "")
            order_for_payload.setdefault("id", "SIM_ORDER")
            order_for_payload.setdefault("code", "SIM_ORDER_CODE")
            order_for_payload.setdefault("branchID", effective_branch)
        else:
            order_for_payload = _build_manual_order(
                branch=effective_branch,
                product_code=external_code,
                price=price,
            )

        rows = _normalize_order_rows(order_for_payload)
        organization_context = await resolve_salesdrive_organization_context(
            session,
            order=order_for_payload,
            enterprise_code=effective_enterprise_code,
            branch=effective_branch,
        )
        payload = await build_salesdrive_payload(
            session,
            order_for_payload,
            effective_enterprise_code,
            rows,
            supplier_code=None,
            supplier_name="",
            branch=effective_branch,
            comment_override=None,
        )

    result = {
        "status": "ok",
        "store_code": getattr(resolved_store, "store_code", None) if resolved_store is not None else None,
        "enterprise_code": organization_context.get("enterprise_code"),
        "branch": organization_context.get("branch"),
        "organizationId": payload.get("organizationId"),
        "organizationId_source": organization_context.get("organizationId_source"),
        "payment_method": payload.get("payment_method"),
        "shipping_method": payload.get("shipping_method"),
        "warnings": organization_context.get("warnings") or [],
        "simulator_status": simulator_result.get("status") if simulator_result else None,
        "payload_preview": payload,
    }

    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    asyncio.run(_amain())

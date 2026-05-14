import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.business.business_store_order_mapper import (
    normalize_store_order_payload,
    resolve_business_store_for_order,
)
from app.database import get_async_db


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual Business Store order reverse mapping test.")
    parser.add_argument("--store-id", type=int, default=0)
    parser.add_argument("--store-code", default="")
    parser.add_argument("--tabletki-branch", "--branch", dest="tabletki_branch", default="")
    parser.add_argument("--tabletki-enterprise-code", default="")
    parser.add_argument("--enterprise-code", dest="tabletki_enterprise_code", default="")
    parser.add_argument("--external-code", default="")
    parser.add_argument("--qty", type=int, default=1)
    parser.add_argument("--price", default="100")
    parser.add_argument("--mock-order-id", default="TEST_ORDER")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


async def _build_mock_order(args: argparse.Namespace) -> tuple[dict[str, Any], str | None]:
    branch = str(args.tabletki_branch or "").strip() or None
    if branch:
        return (
            {
                "id": str(args.mock_order_id),
                "code": str(args.mock_order_id),
                "branchID": branch,
                "rows": [
                    {
                        "goodsCode": str(args.external_code or "").strip(),
                        "goodsName": "Mock product",
                        "qty": int(args.qty or 1),
                        "price": str(args.price),
                    }
                ],
            },
            branch,
        )

    return (
        {
            "id": str(args.mock_order_id),
            "code": str(args.mock_order_id),
            "branchID": None,
            "rows": [
                {
                    "goodsCode": str(args.external_code or "").strip(),
                    "goodsName": "Mock product",
                    "qty": int(args.qty or 1),
                    "price": str(args.price),
                }
            ],
        },
        None,
    )


async def _amain() -> None:
    args = _parse_args()
    external_code = str(args.external_code or "").strip()
    if not external_code:
        raise ValueError("--external-code is required")

    store_id = int(args.store_id or 0) or None
    store_code = str(args.store_code or "").strip() or None
    tabletki_branch = str(args.tabletki_branch or "").strip() or None
    tabletki_enterprise_code = str(args.tabletki_enterprise_code or "").strip() or None

    async with get_async_db(commit_on_exit=False) as session:
        resolved_store = await resolve_business_store_for_order(
            session,
            store_id=store_id,
            store_code=store_code,
            tabletki_branch=tabletki_branch,
            tabletki_enterprise_code=tabletki_enterprise_code,
        )

        mock_order, explicit_branch = await _build_mock_order(args)
        if mock_order.get("branchID") is None and resolved_store is not None:
            mock_order["branchID"] = str(resolved_store.tabletki_branch or "")

        result = await normalize_store_order_payload(
            session,
            order_payload=mock_order,
            store_id=store_id,
            store_code=store_code,
            tabletki_branch=explicit_branch or mock_order.get("branchID"),
            tabletki_enterprise_code=tabletki_enterprise_code,
        )

    normalized_order = result.get("order") or {}
    normalized_rows = normalized_order.get("rows") or []
    first_row = normalized_rows[0] if normalized_rows and isinstance(normalized_rows[0], dict) else {}

    output = {
        "status": result.get("status"),
        "code_mapping_mode": result.get("code_mapping_mode"),
        "store_id": result.get("store_id"),
        "store_code": result.get("store_code"),
        "enterprise_code": result.get("enterprise_code"),
        "branch": mock_order.get("branchID"),
        "input_external_code": external_code,
        "mapped_internal_code": first_row.get("goodsCode"),
        "originalGoodsCodeExternal": first_row.get("originalGoodsCodeExternal"),
        "mapped_rows": result.get("mapped_rows"),
        "missing_mappings": result.get("missing_mappings") or [],
        "warnings": result.get("warnings") or [],
        "errors": result.get("errors") or [],
        "normalized_order": normalized_order,
    }

    print(json.dumps(output, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    asyncio.run(_amain())

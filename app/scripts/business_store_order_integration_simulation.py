import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.business.business_store_order_integration_simulator import (
    simulate_store_order_after_reverse_mapping,
)
from app.database import get_async_db


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Business Store order reverse-mapping integration simulation.")
    parser.add_argument("--store-id", type=int, default=0)
    parser.add_argument("--store-code", default="")
    parser.add_argument("--tabletki-branch", default="")
    parser.add_argument("--tabletki-enterprise-code", default="")
    parser.add_argument("--external-code", default="")
    parser.add_argument("--qty", type=int, default=1)
    parser.add_argument("--price", default="100")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


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
        result = await simulate_store_order_after_reverse_mapping(
            session,
            store_id=store_id,
            store_code=store_code,
            tabletki_branch=tabletki_branch,
            tabletki_enterprise_code=tabletki_enterprise_code,
            external_product_code=external_code,
            qty=int(args.qty or 1),
            price=Decimal(str(args.price)),
        )

    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    asyncio.run(_amain())

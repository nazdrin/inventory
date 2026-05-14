from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from app.database import get_async_db
from app.services.business_stock_mode_service import resolve_business_stock_mode_from_db


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve Business enterprise stock mode without runtime side effects.")
    parser.add_argument("--enterprise-code", required=True)
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _print_summary(result: dict[str, Any]) -> None:
    print(
        "enterprise={enterprise_code} runtime_mode={business_runtime_mode} stock_mode={stock_mode} "
        "source={runtime_mode_source} baseline={is_baseline_mode}".format(
            enterprise_code=result.get("enterprise_code"),
            business_runtime_mode=result.get("business_runtime_mode"),
            stock_mode=result.get("stock_mode"),
            runtime_mode_source=result.get("runtime_mode_source"),
            is_baseline_mode=result.get("is_baseline_mode"),
        )
    )


async def _amain() -> None:
    args = _parse_args()
    async with get_async_db(commit_on_exit=False) as session:
        result = await resolve_business_stock_mode_from_db(session, str(args.enterprise_code))
    if args.output_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        _print_summary(result)


if __name__ == "__main__":
    asyncio.run(_amain())

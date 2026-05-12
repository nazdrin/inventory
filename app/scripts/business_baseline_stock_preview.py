from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.database import get_async_db
from app.services.business_baseline_stock_preview_service import (
    build_business_baseline_stock_preview,
)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only baseline legacy stock preview for a Business enterprise.",
    )
    parser.add_argument("--enterprise-code", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _print_summary(result: dict[str, Any]) -> None:
    print(
        "status={status} stock_mode={stock_mode} enterprise={enterprise_code} "
        "rows_total={rows_total} output_branches={output_branches_count}".format(
            status=result.get("status"),
            stock_mode=result.get("stock_mode"),
            enterprise_code=result.get("enterprise_code"),
            rows_total=result.get("rows_total"),
            output_branches_count=result.get("output_branches_count"),
        )
    )
    if result.get("warnings"):
        print("warnings:")
        for item in result["warnings"]:
            print(f"- {item}")
    if result.get("errors"):
        print("errors:")
        for item in result["errors"]:
            print(f"- {item}")


async def _amain() -> None:
    args = _parse_args()
    limit = None if int(args.limit or 0) <= 0 else int(args.limit)
    async with get_async_db(commit_on_exit=False) as session:
        result = await build_business_baseline_stock_preview(
            session,
            enterprise_code=str(args.enterprise_code),
            limit=limit,
        )

    if args.output_json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
    else:
        _print_summary(result)


if __name__ == "__main__":
    asyncio.run(_amain())

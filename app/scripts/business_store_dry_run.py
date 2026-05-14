import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.business.business_store_export_dry_run import (
    build_business_stores_dry_run,
    build_single_store_dry_run,
)
from app.database import get_async_db


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Business Stores dry-run for stock/catalog export.")
    parser.add_argument("--store-code", default="")
    parser.add_argument("--store-id", type=int, default=0)
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--auto-generate-missing-codes", action="store_true")
    parser.add_argument("--auto-generate-missing-names", action="store_true")
    parser.add_argument("--auto-generate-missing-price-adjustments", action="store_true")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


async def _amain() -> None:
    args = _parse_args()
    store_code = str(args.store_code or "").strip()
    store_id = int(args.store_id or 0)
    enterprise_code = str(args.enterprise_code or "").strip()
    auto_generate_missing_codes = bool(args.auto_generate_missing_codes)
    auto_generate_missing_names = bool(args.auto_generate_missing_names)
    auto_generate_missing_price_adjustments = bool(args.auto_generate_missing_price_adjustments)

    commit_on_exit = (
        auto_generate_missing_codes
        or auto_generate_missing_names
        or auto_generate_missing_price_adjustments
    )
    async with get_async_db(commit_on_exit=commit_on_exit) as session:
        if store_id > 0 or store_code:
            result = await build_single_store_dry_run(
                session,
                store_id=store_id if store_id > 0 else None,
                store_code=store_code or None,
                auto_generate_missing_codes=auto_generate_missing_codes,
                auto_generate_missing_names=auto_generate_missing_names,
                auto_generate_missing_price_adjustments=auto_generate_missing_price_adjustments,
            )
        else:
            result = await build_business_stores_dry_run(
                session,
                enterprise_code=enterprise_code or None,
                auto_generate_missing_codes=auto_generate_missing_codes,
                auto_generate_missing_names=auto_generate_missing_names,
                auto_generate_missing_price_adjustments=auto_generate_missing_price_adjustments,
            )

    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    asyncio.run(_amain())

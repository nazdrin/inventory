from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.database import get_async_db
from app.services.business_store_offers_builder import build_business_store_offers


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build store-native Business offers from active store-supplier links.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--store-id", type=int, default=0)
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--supplier-code", default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--compare-legacy", action="store_true")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _resolve_dry_run(args: argparse.Namespace) -> bool:
    if bool(args.dry_run) and bool(args.apply):
        raise ValueError("Use either --dry-run or --apply, not both.")
    if not bool(args.dry_run) and not bool(args.apply):
        return True
    return bool(args.dry_run)


def _print_summary(result: dict[str, Any]) -> None:
    print(
        "status={status} dry_run={dry_run} stores_total={stores_total} stores_processed={stores_processed} "
        "supplier_links_total={supplier_links_total} upsert_rows={upsert_rows} price_source={price_source}".format(
            status=result.get("status"),
            dry_run=result.get("dry_run"),
            stores_total=result.get("stores_total"),
            stores_processed=result.get("stores_processed"),
            supplier_links_total=result.get("supplier_links_total"),
            upsert_rows=result.get("upsert_rows"),
            price_source=",".join(result.get("price_source") or []),
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
    dry_run = _resolve_dry_run(args)
    limit = None if int(args.limit or 0) <= 0 else int(args.limit)
    store_id = None if int(args.store_id or 0) <= 0 else int(args.store_id)
    enterprise_code = str(args.enterprise_code or "").strip() or None
    supplier_code = str(args.supplier_code or "").strip() or None

    async with get_async_db(commit_on_exit=not dry_run) as session:
        result = await build_business_store_offers(
            session,
            dry_run=bool(dry_run),
            store_id=store_id,
            enterprise_code=enterprise_code,
            supplier_code=supplier_code,
            limit=limit,
            compare_legacy=bool(args.compare_legacy),
        )

    if args.output_json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
        return

    _print_summary(result)


if __name__ == "__main__":
    asyncio.run(_amain())

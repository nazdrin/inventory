from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.database import get_async_db
from app.services.business_store_branch_sync_service import apply_business_store_branch_sync


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BusinessStore ↔ mapping_branch sync report/apply.")
    parser.add_argument("--enterprise-code", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _resolve_mode(args: argparse.Namespace) -> bool:
    if bool(args.dry_run) and bool(args.apply):
        raise ValueError("Use either --dry-run or --apply, not both.")
    if not bool(args.dry_run) and not bool(args.apply):
        return True
    return bool(args.dry_run)


async def _amain() -> None:
    args = _parse_args()
    dry_run = _resolve_mode(args)
    enterprise_code = str(args.enterprise_code or "").strip() or None

    async with get_async_db(commit_on_exit=False) as session:
        result = await apply_business_store_branch_sync(
            session,
            enterprise_code=enterprise_code,
            dry_run=bool(dry_run),
        )

    if args.output_json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
        return

    print(
        "status={status} dry_run={dry_run} enterprises_scanned={enterprises} missing={missing} orphan={orphan} duplicates={duplicates}".format(
            status=result.get("status"),
            dry_run=result.get("dry_run"),
            enterprises=result.get("enterprises_scanned"),
            missing=result.get("missing_stores_to_create"),
            orphan=result.get("orphan_stores_to_deactivate"),
            duplicates=result.get("duplicates"),
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


if __name__ == "__main__":
    asyncio.run(_amain())

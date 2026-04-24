from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.database import get_async_db
from app.services.business_stock_publish_service import publish_business_stock_for_enterprise


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mode-aware Business stock publish/dry-run.")
    parser.add_argument("--enterprise-code", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--include-legacy-default", action="store_true")
    parser.add_argument("--output-json", action="store_true")
    return parser.parse_args()


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _resolve_modes(args: argparse.Namespace) -> tuple[bool, bool]:
    if bool(args.send) and bool(args.dry_run):
        raise ValueError("Use either --dry-run or --send, not both.")
    if not bool(args.send) and not bool(args.dry_run):
        return True, False
    return bool(args.dry_run), bool(args.send)


async def _amain() -> None:
    args = _parse_args()
    dry_run, send = _resolve_modes(args)
    if send and not bool(args.confirm):
        raise ValueError("--send requires --confirm")

    limit = None if int(args.limit or 0) <= 0 else int(args.limit)
    async with get_async_db(commit_on_exit=False) as session:
        result = await publish_business_stock_for_enterprise(
            session,
            enterprise_code=str(args.enterprise_code),
            dry_run=dry_run,
            limit=limit,
            include_legacy_default=bool(args.include_legacy_default),
            require_confirm=not bool(args.confirm),
            confirm=bool(args.confirm),
        )

    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    asyncio.run(_amain())

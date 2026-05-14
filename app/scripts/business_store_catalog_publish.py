import argparse
import asyncio
import json
from decimal import Decimal
from typing import Any

from app.database import get_async_db
from app.services.business_store_catalog_publish_service import (
    publish_enabled_business_store_catalogs,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual Business Store catalog publish for all eligible stores.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--store-id", type=int, default=0)
    parser.add_argument("--store-code", default="")
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
    store_id = int(args.store_id or 0)
    store_code = str(args.store_code or "").strip()

    async with get_async_db(commit_on_exit=False) as session:
        result = await publish_enabled_business_store_catalogs(
            session,
            dry_run=dry_run,
            limit=limit,
            include_legacy_default=bool(args.include_legacy_default),
            store_code=store_code or None,
            store_id=store_id if store_id > 0 else None,
            require_confirm=not bool(args.confirm),
            confirm=bool(args.confirm),
        )

    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    asyncio.run(_amain())

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.database import get_async_db
from app.services.order_sender import (
    TABLETKI_CANCEL_RETRY_QUEUE_PATH,
    process_due_tabletki_cancel_retries,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("tabletki_cancel_retry_service")

POLL_INTERVAL_SEC = max(10, int(os.getenv("TABLETKI_CANCEL_RETRY_POLL_INTERVAL_SEC", "60")))


async def run_once(limit: int = 20) -> dict:
    async with get_async_db() as session:
        result = await process_due_tabletki_cancel_retries(session, limit=limit)
        if result["due_found"]:
            logger.info("Processed Tabletki cancel retry queue: %s", result)
        return result


async def run_forever(limit: int = 20) -> None:
    logger.info(
        "Tabletki cancel retry service started: poll=%ss limit=%s queue=%s",
        POLL_INTERVAL_SEC,
        limit,
        TABLETKI_CANCEL_RETRY_QUEUE_PATH,
    )
    while True:
        try:
            await run_once(limit=limit)
        except Exception:
            logger.exception("Tabletki cancel retry service iteration failed")
        await asyncio.sleep(POLL_INTERVAL_SEC)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delayed retry service for Tabletki cancel warnings")
    parser.add_argument("--once", action="store_true", help="process queue once and exit")
    parser.add_argument("--limit", type=int, default=20, help="max due queue items per iteration")
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    if args.once:
        result = await run_once(limit=args.limit)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    await run_forever(limit=args.limit)


if __name__ == "__main__":
    asyncio.run(_amain())

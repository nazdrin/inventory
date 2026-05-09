from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os

from app.database import get_async_db
from app.integrations.checkbox.client import CheckboxClient
from app.integrations.checkbox.config import load_checkbox_settings
from app.integrations.checkbox.notifications import notify_receipt_fiscalized
from app.integrations.checkbox.repository import (
    due_receipts,
    mark_receipt_failed,
    mark_receipt_fiscalized,
    mark_receipt_pending,
)
from app.integrations.checkbox.service import (
    _extract_fiscal_code,
    _extract_receipt_id,
    _extract_receipt_url,
    _extract_shift_id,
    _update_salesdrive_check,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("checkbox_receipt_retry_service")

POLL_INTERVAL_SEC = max(10, int(os.getenv("CHECKBOX_RECEIPT_RETRY_INTERVAL_SEC", "60")))


async def run_once(limit: int = 20) -> dict:
    settings = load_checkbox_settings()
    if not settings.enabled_enterprises:
        return {"enabled": False, "processed": 0, "fiscalized": 0, "failed": 0}

    stats = {"enabled": True, "processed": 0, "fiscalized": 0, "failed": 0}
    client = CheckboxClient(settings)

    async with get_async_db() as session:
        rows = await due_receipts(session, limit=limit, max_attempts=settings.receipt_retry_max_attempts)
        if not rows:
            return stats
        token = await client.signin()

        for row in rows:
            stats["processed"] += 1
            try:
                if row.checkbox_receipt_id:
                    final_response = await client.wait_receipt_done(token, row.checkbox_receipt_id)
                else:
                    create_response = await client.create_sell_receipt(token, row.payload_json or {})
                    await mark_receipt_pending(
                        row,
                        response_json=create_response,
                        checkbox_receipt_id=_extract_receipt_id(create_response),
                        checkbox_shift_id=_extract_shift_id(create_response),
                    )
                    if not row.checkbox_receipt_id:
                        raise RuntimeError("Checkbox retry create response has no id")
                    final_response = await client.wait_receipt_done(token, row.checkbox_receipt_id)

                await mark_receipt_fiscalized(
                    row,
                    response_json=final_response,
                    receipt_url=_extract_receipt_url(final_response),
                    fiscal_code=_extract_fiscal_code(final_response),
                )
                salesdrive_updated = await _update_salesdrive_check(session, settings=settings, row=row)
                if salesdrive_updated:
                    notify_receipt_fiscalized(settings, row)
                    stats["fiscalized"] += 1
                else:
                    stats["failed"] += 1
            except Exception as exc:
                logger.exception("Checkbox retry failed: receipt_row_id=%s", row.id)
                await mark_receipt_failed(row, error_message=str(exc))
                stats["failed"] += 1
    return stats


async def run_forever(limit: int = 20) -> None:
    logger.info("Checkbox receipt retry service started: poll=%ss limit=%s", POLL_INTERVAL_SEC, limit)
    while True:
        try:
            result = await run_once(limit=limit)
            if result.get("processed"):
                logger.info("Checkbox receipt retry result: %s", result)
        except Exception:
            logger.exception("Checkbox receipt retry iteration failed")
        await asyncio.sleep(POLL_INTERVAL_SEC)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry Checkbox receipt fiscalization and SalesDrive check updates")
    parser.add_argument("--once", action="store_true", help="process due receipts once and exit")
    parser.add_argument("--limit", type=int, default=20, help="max due receipts per iteration")
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

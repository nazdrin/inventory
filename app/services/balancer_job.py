import os
import asyncio
import json
import logging
from datetime import datetime, timezone

from app.business.balancer.jobs import run_balancer_pipeline_async

logger = logging.getLogger("balancer_job")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def main() -> None:
    mode = os.getenv("BALANCER_RUN_MODE", "TEST").strip().upper()
    logger.info("🚀 Balancer job start. mode=%s now_utc=%s", mode, datetime.now(timezone.utc))

    res = await run_balancer_pipeline_async()
    logger.info("✅ Balancer job done: %s", json.dumps(res, ensure_ascii=False, default=str)[:2000])

if __name__ == "__main__":
    asyncio.run(main())
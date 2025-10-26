# app/services/salesdrive_webhook.py
import logging
from typing import Dict, Any

logger = logging.getLogger("salesdrive")

async def process_salesdrive_webhook(payload: Dict[str, Any]) -> None:
    info = (payload.get("info") or {})
    data = (payload.get("data") or {})
    logger.info(
        "🔧 [stub] event=%s order_id=%s status_id=%s products=%s",
        info.get("webhookEvent"), data.get("id"), data.get("statusId"),
        len((data.get("products") or []))
    )
    # тут позже добавите вашу бизнес-логику
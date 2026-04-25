from __future__ import annotations

import json
import logging
from asyncio import sleep
from typing import Any, Dict, Optional, Tuple

import httpx
from sqlalchemy import select

from app.database import get_async_db
from app.models import EnterpriseSettings
from app.services.notification_service import send_notification

logger = logging.getLogger(__name__)

SALESDRIVE_SIMPLE_RETRY_ATTEMPTS = 3
SALESDRIVE_SIMPLE_RETRY_DELAY_SEC = 2.0
SALESDRIVE_SIMPLE_TIMEOUT_SEC = 20.0


def _delivery_dict(order: Dict[str, Any]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in order.get("deliveryData", []) or []:
        key = item.get("key")
        value = item.get("value")
        if key:
            result[str(key)] = "" if value is None else str(value)
    return result


def _extract_name_parts(order: Dict[str, Any], delivery: Dict[str, str]) -> Tuple[str, str, str]:
    first_name = delivery.get("Name") or str(order.get("customer") or "")
    last_name = delivery.get("LastName") or ""
    middle_name = delivery.get("MiddleName") or ""
    return first_name, last_name, middle_name


def _build_products(order: Dict[str, Any]) -> list[Dict[str, Any]]:
    products: list[Dict[str, Any]] = []
    for row in order.get("rows", []) or []:
        goods_code = str(row.get("goodsCode") or "").strip()
        qty = row.get("qty", row.get("qtyShip", 0))
        if not goods_code:
            continue

        products.append(
            {
                "id": goods_code,
                "name": str(row.get("goodsName") or ""),
                "costPerItem": str(row.get("price") or row.get("priceShip") or 0),
                "amount": str(qty or 0),
                "discount": "",
                "sku": goods_code,
            }
        )
    return products


def _build_novaposhta_block(delivery: Dict[str, str]) -> Dict[str, Any]:
    if delivery.get("DeliveryServiceAlias") != "NP":
        return {}
    return {
        "ServiceType": delivery.get("ServiceType", "Warehouse"),
        "payer": delivery.get("payer", "recipient"),
        "area": delivery.get("area", ""),
        "region": delivery.get("region", ""),
        "city": delivery.get("CityReceiver") or delivery.get("CitySender", ""),
        "cityNameFormat": delivery.get("cityNameFormat", ""),
        "WarehouseNumber": delivery.get("ID_Whs", ""),
        "Street": delivery.get("Street", ""),
        "BuildingNumber": delivery.get("BuildingNumber", ""),
        "Flat": delivery.get("Flat", ""),
    }


def _build_ukrposhta_block(delivery: Dict[str, str]) -> Dict[str, Any]:
    if delivery.get("DeliveryServiceAlias") != "UP":
        return {}
    return {
        "ServiceType": delivery.get("ServiceType", ""),
        "payer": delivery.get("payer", ""),
        "type": delivery.get("type", ""),
        "city": delivery.get("CityReceiver") or delivery.get("CitySender", ""),
        "WarehouseNumber": delivery.get("ID_Whs", ""),
        "Street": delivery.get("Street", ""),
        "BuildingNumber": delivery.get("BuildingNumber", ""),
        "Flat": delivery.get("Flat", ""),
    }


def _build_payload(order: Dict[str, Any]) -> Dict[str, Any]:
    delivery = _delivery_dict(order)
    first_name, last_name, middle_name = _extract_name_parts(order, delivery)
    products = _build_products(order)

    payload: Dict[str, Any] = {
        "getResultData": "1",
        "fName": first_name,
        "lName": last_name,
        "mName": middle_name,
        "phone": str(order.get("customerPhone") or ""),
        "email": "",
        "company": "",
        "products": products,
        "shipping_method": delivery.get("DeliveryServiceName", ""),
        "shipping_address": delivery.get("ReceiverWhs") or delivery.get("Address", ""),
        "externalId": str(order.get("id") or ""),
        "organizationId": "1",
    }
    if order.get("customerEmail"):
        payload["email"] = order["customerEmail"]

    payload["sajt"] = "Tabletki.ua"

    novaposhta = _build_novaposhta_block(delivery)
    if novaposhta:
        payload["novaposhta"] = novaposhta

    ukrposhta = _build_ukrposhta_block(delivery)
    if ukrposhta:
        payload["ukrposhta"] = ukrposhta

    return payload


async def _get_salesdrive_settings(enterprise_code: str) -> Tuple[Optional[str], Optional[str]]:
    async with get_async_db(commit_on_exit=False) as session:
        result = await session.execute(
            select(EnterpriseSettings.token).where(EnterpriseSettings.enterprise_code == str(enterprise_code)).limit(1)
        )
        raw_value = result.scalar_one_or_none()

    if not raw_value:
        return None, None

    settings_parts = [part.strip() for part in str(raw_value).split(",", 1)]
    if len(settings_parts) != 2 or not settings_parts[0] or not settings_parts[1]:
        logger.warning(
            "SalesDriveSimple invalid token format: enterprise_code=%s raw_present=%s",
            enterprise_code,
            bool(raw_value),
        )
        return None, None

    base_url, api_key = settings_parts
    return f"{base_url.rstrip('/')}/handler/", api_key


async def _post_to_salesdrive(handler_url: str, api_key: str, payload: Dict[str, Any], enterprise_code: str) -> bool:
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }
    external_id = str(payload.get("externalId") or "")
    last_error: Exception | None = None

    async with httpx.AsyncClient(timeout=SALESDRIVE_SIMPLE_TIMEOUT_SEC) as client:
        for attempt in range(1, SALESDRIVE_SIMPLE_RETRY_ATTEMPTS + 1):
            try:
                response = await client.post(handler_url, json=payload, headers=headers)
                logger.info(
                    "SalesDriveSimple POST attempt=%s/%s externalId=%s status=%s",
                    attempt,
                    SALESDRIVE_SIMPLE_RETRY_ATTEMPTS,
                    external_id,
                    response.status_code,
                )
                response.raise_for_status()
                return True
            except httpx.HTTPStatusError as exc:
                last_error = exc
                logger.warning(
                    "SalesDriveSimple HTTP error attempt=%s/%s externalId=%s status=%s body=%s",
                    attempt,
                    SALESDRIVE_SIMPLE_RETRY_ATTEMPTS,
                    external_id,
                    exc.response.status_code if exc.response is not None else "n/a",
                    (exc.response.text[:1000] if exc.response is not None else ""),
                )
            except httpx.RequestError as exc:
                last_error = exc
                logger.warning(
                    "SalesDriveSimple request error attempt=%s/%s externalId=%s err=%s",
                    attempt,
                    SALESDRIVE_SIMPLE_RETRY_ATTEMPTS,
                    external_id,
                    exc,
                )

            if attempt < SALESDRIVE_SIMPLE_RETRY_ATTEMPTS:
                await sleep(SALESDRIVE_SIMPLE_RETRY_DELAY_SEC)

    message = (
        f"❌ SalesDriveSimple send failed after {SALESDRIVE_SIMPLE_RETRY_ATTEMPTS} attempts | "
        f"externalId={external_id} | err={last_error}"
    )
    logger.error(message)
    try:
        send_notification(message, enterprise_code)
    except Exception:
        logger.exception("SalesDriveSimple notification failed: externalId=%s", external_id)
    return False


async def send_order_to_salesdrive_simple(
    order: Dict[str, Any],
    enterprise_code: str,
    branch: Optional[str] = None,
) -> bool:
    del branch

    payload = _build_payload(order)
    external_id = str(payload.get("externalId") or "")

    if not external_id:
        logger.warning("SalesDriveSimple skip order without externalId")
        return False

    if not payload["products"]:
        logger.warning("SalesDriveSimple skip order without products: externalId=%s", external_id)
        return False

    handler_url, api_key = await _get_salesdrive_settings(enterprise_code)
    if not handler_url or not api_key:
        logger.warning(
            "SalesDriveSimple settings missing: enterprise_code=%s externalId=%s",
            enterprise_code,
            external_id,
        )
        return False

    logger.info(
        "SalesDriveSimple prepared payload: externalId=%s shipping_method=%s products=%s",
        external_id,
        payload.get("shipping_method"),
        len(payload["products"]),
    )
    logger.debug("SalesDriveSimple payload: %s", json.dumps(payload, ensure_ascii=False))
    return await _post_to_salesdrive(handler_url, api_key, payload, enterprise_code)

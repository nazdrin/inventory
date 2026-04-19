from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

from sqlalchemy import select

from app.database import get_async_db
from app.models import EnterpriseSettings, MappingBranch
from app.services.order_sender import send_orders_to_tabletki
from app.services.send_TTN import send_ttn

logger = logging.getLogger("salesdrive_simple.webhook")

CONFIRM_STATUS_IDS = {2, 3, 4}
CANCEL_STATUS_ID = 6
TABLETKI_CONFIRM_STATUS_ID = 4
TABLETKI_CANCEL_STATUS_ID = 7
TABLETKI_CANCEL_REASON_DEFAULT = 5
DELIVERY_MAP = {"novaposhta": "NP", "ukrposhta": "UP"}


def _build_order_rows(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for product in products or []:
        if not isinstance(product, dict):
            continue
        rows.append(
            {
                "goodsCode": str(product.get("parameter") or ""),
                "goodsName": product.get("name") or "",
                "goodsProducer": "",
                "qtyShip": product.get("amount") or 0,
                "priceShip": product.get("price") or 0,
            }
        )
    return rows


def _extract_ttn_block(data: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    for item in (data.get("ord_delivery_data") or []):
        if not isinstance(item, dict):
            continue
        ttn = item.get("trackingNumber")
        if ttn:
            return str(ttn), str(item.get("provider") or "")
    return None, None


def _extract_phone(data: Dict[str, Any]) -> Optional[str]:
    contacts = data.get("contacts") or []
    if contacts and isinstance(contacts, list):
        first_contact = contacts[0]
        if isinstance(first_contact, dict):
            return first_contact.get("phone")
    return None


async def process_salesdrive_simple_webhook(payload: Dict[str, Any], branch: str) -> None:
    try:
        data = payload.get("data")
        if not isinstance(data, dict):
            logger.warning("Ignored webhook without data object: branch=%s", branch)
            return

        status_id_raw = data.get("statusId")
        try:
            status_id = int(status_id_raw)
        except (TypeError, ValueError):
            logger.warning(
                "Ignored webhook with invalid statusId: branch=%s externalId=%s id=%s statusId=%r",
                branch,
                data.get("externalId"),
                data.get("id"),
                status_id_raw,
            )
            return

        external_id = str(data.get("externalId") or data.get("id") or "").strip()
        payload_branch = data.get("branch")
        payload_utm_source = data.get("utmSource")
        products = data.get("products") or []
        rows = _build_order_rows(products if isinstance(products, list) else [])

        async with get_async_db(commit_on_exit=False) as session:
            mapping = (
                await session.execute(
                    select(MappingBranch).where(MappingBranch.branch == str(branch)).limit(1)
                )
            ).scalar_one_or_none()
            if not mapping:
                logger.warning(
                    "MappingBranch not found: branch=%s externalId=%s statusId=%s",
                    branch,
                    external_id,
                    status_id,
                )
                return

            enterprise = (
                await session.execute(
                    select(EnterpriseSettings).where(
                        EnterpriseSettings.enterprise_code == mapping.enterprise_code
                    ).limit(1)
                )
            ).scalar_one_or_none()
            if not enterprise:
                logger.warning(
                    "EnterpriseSettings not found: branch=%s enterprise_code=%s externalId=%s statusId=%s",
                    branch,
                    mapping.enterprise_code,
                    external_id,
                    status_id,
                )
                return

            if not enterprise.tabletki_login or not enterprise.tabletki_password:
                logger.warning(
                    "Tabletki credentials missing: branch=%s enterprise_code=%s externalId=%s statusId=%s",
                    branch,
                    enterprise.enterprise_code,
                    external_id,
                    status_id,
                )
                return

            if payload_branch is not None and str(payload_branch) != str(branch):
                logger.warning(
                    "Payload branch differs from URL branch: branch=%s payload_branch=%s enterprise_code=%s externalId=%s",
                    branch,
                    payload_branch,
                    enterprise.enterprise_code,
                    external_id,
                )
            elif payload_utm_source is not None and str(payload_utm_source) != str(branch):
                logger.debug(
                    "Payload utmSource differs from URL branch: branch=%s utmSource=%s enterprise_code=%s externalId=%s",
                    branch,
                    payload_utm_source,
                    enterprise.enterprise_code,
                    external_id,
                )

            if status_id in CONFIRM_STATUS_IDS:
                action = "confirm"
                tabletki_status_id = TABLETKI_CONFIRM_STATUS_ID
                cancel_reason = 1
            elif status_id == CANCEL_STATUS_ID:
                action = "cancel"
                tabletki_status_id = TABLETKI_CANCEL_STATUS_ID
                cancel_reason = TABLETKI_CANCEL_REASON_DEFAULT
            else:
                logger.info(
                    "Ignored SalesDriveSimple status: branch=%s enterprise_code=%s externalId=%s statusId=%s action=ignored",
                    branch,
                    enterprise.enterprise_code,
                    external_id,
                    status_id,
                )
                await _send_ttn_if_present(
                    session=session,
                    data=data,
                    branch=branch,
                    enterprise_code=enterprise.enterprise_code,
                    external_id=external_id,
                )
                return

            if not external_id:
                logger.warning(
                    "Skipped SalesDriveSimple webhook without order id: branch=%s enterprise_code=%s statusId=%s action=%s",
                    branch,
                    enterprise.enterprise_code,
                    status_id,
                    action,
                )
                await _send_ttn_if_present(
                    session=session,
                    data=data,
                    branch=branch,
                    enterprise_code=enterprise.enterprise_code,
                    external_id=external_id,
                )
                return

            if not rows:
                logger.warning(
                    "Skipped SalesDriveSimple status send because rows are empty: branch=%s enterprise_code=%s externalId=%s statusId=%s action=%s",
                    branch,
                    enterprise.enterprise_code,
                    external_id,
                    status_id,
                    action,
                )
                await _send_ttn_if_present(
                    session=session,
                    data=data,
                    branch=branch,
                    enterprise_code=enterprise.enterprise_code,
                    external_id=external_id,
                )
                return

            order_obj = {
                "id": external_id,
                "tabletkiOrder": str(data.get("externalId") or data.get("id") or ""),
                "statusID": tabletki_status_id,
                "branchID": str(branch),
                "rows": rows,
            }

            try:
                await send_orders_to_tabletki(
                    session=session,
                    orders=[order_obj],
                    tabletki_login=enterprise.tabletki_login,
                    tabletki_password=enterprise.tabletki_password,
                    cancel_reason=cancel_reason,
                    enterprise_code=enterprise.enterprise_code,
                )
                logger.info(
                    "Processed SalesDriveSimple webhook: branch=%s enterprise_code=%s externalId=%s statusId=%s action=%s result=success",
                    branch,
                    enterprise.enterprise_code,
                    external_id,
                    status_id,
                    action,
                )
            except Exception:
                logger.exception(
                    "SalesDriveSimple status send failed: branch=%s enterprise_code=%s externalId=%s statusId=%s action=%s",
                    branch,
                    enterprise.enterprise_code,
                    external_id,
                    status_id,
                    action,
                )

            await _send_ttn_if_present(
                session=session,
                data=data,
                branch=branch,
                enterprise_code=enterprise.enterprise_code,
                external_id=external_id,
            )
    except Exception:
        logger.exception("Unhandled SalesDriveSimple webhook error: branch=%s", branch)


async def _send_ttn_if_present(
    *,
    session,
    data: Dict[str, Any],
    branch: str,
    enterprise_code: str,
    external_id: str,
) -> None:
    ttn, provider = _extract_ttn_block(data)
    if not ttn:
        return

    alias = DELIVERY_MAP.get((provider or "").lower())
    if not alias:
        logger.warning(
            "TTN provider is unsupported: branch=%s enterprise_code=%s externalId=%s provider=%r",
            branch,
            enterprise_code,
            external_id,
            provider,
        )
        return

    if not external_id:
        logger.warning(
            "TTN skipped because order id is empty: branch=%s enterprise_code=%s",
            branch,
            enterprise_code,
        )
        return

    phone_raw = _extract_phone(data)
    phone_number = re.sub(r"\D+", "", str(phone_raw or ""))

    try:
        sent = await send_ttn(
            session=session,
            id=external_id,
            enterprise_code=enterprise_code,
            ttn=ttn,
            deliveryServiceAlias=alias,
            phoneNumber=phone_number,
        )
        if sent:
            logger.info(
                "TTN processed: branch=%s enterprise_code=%s externalId=%s result=success",
                branch,
                enterprise_code,
                external_id,
            )
        else:
            logger.info(
                "TTN skipped or unchanged: branch=%s enterprise_code=%s externalId=%s",
                branch,
                enterprise_code,
                external_id,
            )
    except Exception:
        logger.exception(
            "TTN send failed: branch=%s enterprise_code=%s externalId=%s",
            branch,
            enterprise_code,
            external_id,
        )

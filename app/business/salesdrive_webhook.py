# app/services/salesdrive_webhook.py
from __future__ import annotations

import re
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

# Импорты из проекта
from app.business.business_store_tabletki_outbound_mapper import (
    restore_salesdrive_products_for_tabletki_outbound,
)
from app.database import get_async_db, EnterpriseSettings
from app.models import MappingBranch
from app.services.order_sender import process_due_tabletki_cancel_retries, send_orders_to_tabletki
from app.services.order_reporting_sync_service import safe_upsert_salesdrive_order
from app.services.send_TTN import send_ttn  # async def send_ttn(...) -> bool
from app.services.telegram_bot import notify_call_request

logger = logging.getLogger("salesdrive")
logger.setLevel(logging.INFO)

# === Справочники ===
STATUS_MAP = {2: 4, 3: 4, 4: 4, 5: 6, 6: 7, 10: 4, 16: 4}
CANCEL_REASON = {
    # поддержка как строк, так и кодов (пример: 24 -> "Відмова споживача")
    "Відмова споживача": 1,
    "Недостатня кількість": 5,
    24: 6,
    27: 1,
}
DELIVERY_MAP = {"novaposhta": "NP", "ukrposhta": "UP"}  # ключи в нижнем регистре без пробелов
async def _get_enterprise_code_by_branch(session: AsyncSession, branch_value: Any) -> Optional[str]:
    """Возвращает enterprise_code по значению branch (берём из data.branch вебхука, ранее из data.utmSource)."""
    if branch_value is None:
        return None
    branch_str = str(branch_value)
    q = select(MappingBranch.enterprise_code).where(MappingBranch.branch == branch_str)
    res = await session.execute(q)
    return res.scalar_one_or_none()

async def _get_tabletki_credentials(session: AsyncSession, enterprise_code: str) -> Optional[tuple[str, str]]:
    """Возвращает (login, password) из EnterpriseSettings по enterprise_code."""
    q = select(
        EnterpriseSettings.tabletki_login,
        EnterpriseSettings.tabletki_password
    ).where(EnterpriseSettings.enterprise_code == enterprise_code)
    row = (await session.execute(q)).first()
    if not row:
        return None
    return row[0], row[1]

def _build_order_rows(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Трансформирует products из вебхука в rows для отправки."""
    rows: List[Dict[str, Any]] = []
    for p in products or []:
        rows.append({
            # ⬇️ goodsCode теперь берём из поля 'parameter' (НЕ из productId)
            "goodsCode": str(p.get("parameter") or ""),
            "goodsName": p.get("name") or "",
            "goodsProducer": "",
            "qtyShip": p.get("amount") or 0,
            "priceShip": p.get("price") or 0,
        })
    return rows

def _extract_ttn_block(data: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Возвращает (trackingNumber, provider) из data. Берём первую запись с TTN из ord_delivery_data[]."""
    for item in (data.get("ord_delivery_data") or []):
        ttn = item.get("trackingNumber")
        if ttn:
            return str(ttn), str(item.get("provider") or "")
    return None, None

def _extract_phone(data: Dict[str, Any]) -> Optional[str]:
    """Пытаемся взять phone из contacts[0].phone."""
    contacts = data.get("contacts") or []
    if contacts and isinstance(contacts, list):
        return contacts[0].get("phone")
    return None


def _extract_data_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _first_product_code_snapshot(data: Dict[str, Any]) -> tuple[str | None, str | None]:
    products = data.get("products")
    if not isinstance(products, list) or not products:
        return None, None
    first_product = products[0]
    if not isinstance(first_product, dict):
        return None, None
    parameter = first_product.get("parameter")
    sku = first_product.get("sku")
    return (
        str(parameter).strip() if parameter is not None and str(parameter).strip() else None,
        str(sku).strip() if sku is not None and str(sku).strip() else None,
    )


async def _apply_store_aware_outbound_mapping_if_enabled(
    *,
    session: AsyncSession,
    data: Dict[str, Any],
    branch_value: Any,
    enterprise_code: str,
) -> tuple[Optional[Dict[str, Any]], Optional[dict[str, Any]]]:
    branch = str(branch_value or "").strip()
    before_parameter, before_sku = _first_product_code_snapshot(data)
    mapping_result = await restore_salesdrive_products_for_tabletki_outbound(
        session,
        payload={"data": [data]},
        branch=branch,
        enterprise_code=enterprise_code,
    )
    mapping_status = str(mapping_result.get("status") or "")

    if mapping_status == "mapping_error":
        logger.warning(
            "Store-aware outbound status mapping error: enterprise_code=%s branch=%s externalId=%s tabletkiOrder=%s code_mapping_mode=%s missing=%s",
            enterprise_code,
            branch,
            data.get("externalId"),
            data.get("tabletkiOrder") or data.get("TabletkiOrder"),
            mapping_result.get("code_mapping_mode"),
            mapping_result.get("missing_mappings"),
        )
        return None, mapping_result

    if mapping_status == "ok":
        transformed_payload = mapping_result.get("payload") if isinstance(mapping_result, dict) else None
        transformed_items = _extract_data_items(transformed_payload) if isinstance(transformed_payload, dict) else []
        transformed_data_for_log = transformed_items[0] if transformed_items else data
        after_parameter, after_sku = _first_product_code_snapshot(transformed_data_for_log)
        logger.info(
            "Store-aware outbound status mapping applied: enterprise_code=%s branch=%s externalId=%s store_id=%s store_code=%s code_mapping_mode=%s mapped_products=%s first_parameter_before=%s first_parameter_after=%s first_sku_before=%s first_sku_after=%s",
            enterprise_code,
            branch,
            data.get("externalId"),
            mapping_result.get("store_id"),
            mapping_result.get("store_code"),
            mapping_result.get("code_mapping_mode"),
            mapping_result.get("mapped_products"),
            before_parameter,
            after_parameter,
            before_sku,
            after_sku,
        )

    transformed_payload = mapping_result.get("payload") if isinstance(mapping_result, dict) else None
    transformed_data = None
    if isinstance(transformed_payload, dict):
        transformed_items = _extract_data_items(transformed_payload)
        if transformed_items:
            transformed_data = transformed_items[0]

    return transformed_data or data, mapping_result

async def process_salesdrive_webhook(payload: Dict[str, Any]) -> None:
    """Главная точка входа бизнес-логики вебхука SalesDrive."""
    data_items = _extract_data_items(payload)
    if not data_items:
        logger.warning("SalesDrive webhook ignored: payload.data is empty or invalid")
        return

    async with get_async_db() as session:
        processed_retry_enterprises: set[str] = set()

        for data in data_items:
            status_in: Optional[int] = data.get("statusId")
            mapped_status: int = STATUS_MAP.get(status_in, status_in)

            external_id = str(data.get("externalId") or "")
            order_id = str(data.get("id") or "")
            branch_value = data.get("branch")
            if branch_value is None:
                branch_value = data.get("utmSource")

            products = data.get("products") or []
            product_lines: List[str] = []
            if products and isinstance(products, list):
                for i, p in enumerate(products, start=1):
                    if not isinstance(p, dict):
                        continue
                    name = p.get("name") or p.get("documentName") or ""
                    qty = p.get("amount") or 1
                    if name:
                        product_lines.append(f"{i}. {name} (x{qty})")
            product_name = "\n".join(product_lines)
            order_date = str(data.get("orderTime") or "")

            if status_in == 9 and os.getenv("ENABLE_CALL_REQUEST_NOTIFY", "0") == "1":
                branch = str(branch_value) if branch_value is not None else ""
                raw_payment = data.get("paymentAmount")
                try:
                    payment_amount = float(raw_payment) if raw_payment is not None else 0.0
                except (TypeError, ValueError):
                    payment_amount = 0.0

                contacts = data.get("contacts") or []
                f_name = ""
                l_name = ""
                phone = None
                if contacts and isinstance(contacts, list):
                    first_contact = contacts[0] or {}
                    f_name = first_contact.get("fName") or ""
                    l_name = first_contact.get("lName") or ""
                    phone = first_contact.get("phone") or ""

                try:
                    await notify_call_request(
                        branch=branch,
                        id=order_id,
                        paymentAmount=payment_amount,
                        fName=f_name,
                        lName=l_name,
                        phone=str(phone) if phone is not None else "",
                        product_name=product_name,
                        order_date=order_date,
                    )
                    logger.info(
                        "📞 Отправлено уведомление о звонке: externalId=%s, branch=%s, amount=%s, fName=%s, lName=%s, phone=%s",
                        external_id,
                        branch,
                        payment_amount,
                        f_name,
                        l_name,
                        phone,
                    )
                except Exception as e:
                    logger.exception("❌ Ошибка при вызове notify_call_request: %s", e)

            enterprise_code = await _get_enterprise_code_by_branch(session, branch_value)
            if not enterprise_code:
                logger.error("⛔ enterprise_code не найден по branch=%s в MappingBranch", branch_value)
                continue

            await safe_upsert_salesdrive_order(
                session,
                order=data,
                enterprise_code=enterprise_code,
            )

            creds = await _get_tabletki_credentials(session, enterprise_code)
            if not creds:
                logger.error("⛔ tabletki_login/password не найдены для enterprise_code=%s", enterprise_code)
                continue
            tabletki_login, tabletki_password = creds

            if enterprise_code not in processed_retry_enterprises:
                retry_stats = await process_due_tabletki_cancel_retries(session, enterprise_code=enterprise_code)
                if retry_stats["due_found"]:
                    logger.info("Processed due Tabletki cancel retries from webhook path: %s", retry_stats)
                processed_retry_enterprises.add(enterprise_code)

            mapped_data, mapping_report = await _apply_store_aware_outbound_mapping_if_enabled(
                session=session,
                data=data,
                branch_value=branch_value,
                enterprise_code=enterprise_code,
            )

            order_obj = {
                "id": external_id,
                "tabletkiOrder": str(data.get("tabletkiOrder") or data.get("TabletkiOrder") or ""),
                "statusID": mapped_status,
                "branchID": str(branch_value) if branch_value is not None else "",
                "rows": _build_order_rows((mapped_data or data).get("products") or []),
            }
            orders: List[Dict[str, Any]] = [order_obj]

            if mapped_data is None:
                logger.warning(
                    "Store-aware outbound status send skipped due to mapping_error: enterprise_code=%s branch=%s externalId=%s tabletkiOrder=%s code_mapping_mode=%s",
                    enterprise_code,
                    branch_value,
                    external_id,
                    data.get("tabletkiOrder") or data.get("TabletkiOrder"),
                    mapping_report.get("code_mapping_mode") if isinstance(mapping_report, dict) else None,
                )
            elif status_in in (4, 10, 16):
                try:
                    await send_orders_to_tabletki(
                        session=session,
                        orders=orders,
                        tabletki_login=tabletki_login,
                        tabletki_password=tabletki_password,
                        cancel_reason=1,
                        enterprise_code=enterprise_code,
                    )
                    logger.info(
                        "✅ Подтверждение: id=%s, status_in=%s → statusID=%s, enterprise=%s",
                        external_id,
                        status_in,
                        mapped_status,
                        enterprise_code,
                    )
                except Exception as e:
                    logger.exception("❌ Ошибка send_orders_to_tabletki (confirm): %s", e)

            elif status_in == 6:
                raw_reason = data.get("rejectionReason")
                if isinstance(raw_reason, (str, int)):
                    cancel_reason = CANCEL_REASON.get(raw_reason, 1)
                else:
                    cancel_reason = 1
                    logger.warning("⚠️ rejectionReason=%r не сопоставлён, используем cancel_reason=1", raw_reason)

                try:
                    await send_orders_to_tabletki(
                        session=session,
                        orders=orders,
                        tabletki_login=tabletki_login,
                        tabletki_password=tabletki_password,
                        cancel_reason=cancel_reason,
                        enterprise_code=enterprise_code,
                    )
                    logger.info(
                        "✅ Отказ: id=%s, status_in=6 → statusID=%s, reason=%s, enterprise=%s",
                        external_id,
                        mapped_status,
                        cancel_reason,
                        enterprise_code,
                    )
                except Exception as e:
                    logger.exception("❌ Ошибка send_orders_to_tabletki (cancel): %s", e)
            else:
                logger.info("ℹ️ statusId=%s (map=%s) — не отправляем в Tabletki.", status_in, mapped_status)

            ttn, provider = _extract_ttn_block(data)
            if ttn:
                alias = DELIVERY_MAP.get((provider or "").lower())
                if not alias:
                    logger.warning("⚠️ Неизвестный provider=%r — deliveryServiceAlias не определён", provider)
                else:
                    phone_raw = _extract_phone(data)
                    phone_number = re.sub(r"\D+", "", str(phone_raw or ""))

                    try:
                        sent = await send_ttn(
                            session=session,
                            id=external_id,
                            enterprise_code=enterprise_code,
                            ttn=ttn,
                            deliveryServiceAlias=alias,
                            phoneNumber=phone_number
                        )
                        if sent:
                            logger.info(
                                "📦 TTN отправлен/обновлён: id=%s, ttn=%s, alias=%s, phone=%s",
                                external_id,
                                ttn,
                                alias,
                                phone_number,
                            )
                        else:
                            logger.info(
                                "ℹ️ TTN не отправлен: id=%s, ttn=%s (совпадает, пустой или была ошибка)",
                                external_id,
                                ttn,
                            )
                    except Exception as e:
                        logger.exception("❌ Ошибка send_ttn: %s", e)
            else:
                logger.debug("TTN отсутствует — пропускаем отправку трека.")

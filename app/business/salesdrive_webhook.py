# app/services/salesdrive_webhook.py
from __future__ import annotations

import re
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

# Импорты из проекта
from app.database import get_async_db, EnterpriseSettings
from app.models import MappingBranch
from app.services.order_sender import send_orders_to_tabletki
from app.services.send_TTN import send_ttn  # async def send_ttn(session, id, enterprise_code, ttn, deliveryServiceAlias, phoneNumber)

logger = logging.getLogger("salesdrive")
logger.setLevel(logging.INFO)

# === Справочники ===
STATUS_MAP = {2: 4, 3: 4, 4: 4, 5: 6, 6: 7}
CANCEL_REASON = {
    # поддержка как строк, так и кодов (пример: 24 -> "Відмова споживача")
    "Відмова споживача": 1,
    "Недостатня кількість": 5,
    24: 1,
}
DELIVERY_MAP = {"novaposhta": "NP", "ukrposhta": "UP"}  # ключи в нижнем регистре без пробелов

async def _get_enterprise_code_by_branch(session: AsyncSession, branch_value: Any) -> Optional[str]:
    """Возвращает enterprise_code по значению branch (берём из data.utmSource вебхука)."""
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

async def process_salesdrive_webhook(payload: Dict[str, Any]) -> None:
    """Главная точка входа бизнес-логики вебхука SalesDrive."""
    data = (payload.get("data") or {})
    status_in: Optional[int] = data.get("statusId")
    mapped_status: int = STATUS_MAP.get(status_in, status_in)

    external_id = str(data.get("externalId") or "")
    utm_source = data.get("utmSource")  # branch берём из utmSource
    products = data.get("products") or []

    order_obj = {
        "id": external_id,
        "statusID": mapped_status,
        "branchID": str(utm_source) if utm_source is not None else "",
        "rows": _build_order_rows(products),
    }
    orders: List[Dict[str, Any]] = [order_obj]

    async with get_async_db() as session:
        # enterprise_code ищем по utmSource
        enterprise_code = await _get_enterprise_code_by_branch(session, utm_source)
        if not enterprise_code:
            logger.error("⛔ enterprise_code не найден по utmSource=%s в MappingBranch", utm_source)
            return

        creds = await _get_tabletki_credentials(session, enterprise_code)
        if not creds:
            logger.error("⛔ tabletki_login/password не найдены для enterprise_code=%s", enterprise_code)
            return
        tabletki_login, tabletki_password = creds

        # === Отправка подтверждения / отказа ===
        if status_in == 4:
            # подтверждение
            try:
                await send_orders_to_tabletki(
                    session=session,
                    orders=orders,
                    tabletki_login=tabletki_login,
                    tabletki_password=tabletki_password,
                    cancel_reason=1
                )
                logger.info("✅ Подтверждение: id=%s, status_in=%s → statusID=%s, enterprise=%s",
                            external_id, status_in, mapped_status, enterprise_code)
            except Exception as e:
                logger.exception("❌ Ошибка send_orders_to_tabletki (confirm): %s", e)

        elif status_in == 6:
            # отказ
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
                    cancel_reason=cancel_reason
                )
                logger.info("✅ Отказ: id=%s, status_in=6 → statusID=%s, reason=%s, enterprise=%s",
                            external_id, mapped_status, cancel_reason, enterprise_code)
            except Exception as e:
                logger.exception("❌ Ошибка send_orders_to_tabletki (cancel): %s", e)
        else:
            logger.info("ℹ️ statusId=%s (map=%s) — не отправляем в Tabletki.", status_in, mapped_status)

        # === TTN: при наличии — отправляем трек ===
        ttn, provider = _extract_ttn_block(data)
        if ttn:
            alias = DELIVERY_MAP.get((provider or "").lower())
            if not alias:
                logger.warning("⚠️ Неизвестный provider=%r — deliveryServiceAlias не определён", provider)
            else:
                phone_raw = _extract_phone(data)
                # очищаем телефон до цифр (важно для валидного JSON у приёмника)
                phone_number = re.sub(r"\D+", "", str(phone_raw or ""))

                try:
                    await send_ttn(
                        session=session,
                        id=external_id,
                        enterprise_code=enterprise_code,
                        ttn=ttn,
                        deliveryServiceAlias=alias,
                        phoneNumber=phone_number
                    )
                    logger.info("📦 TTN отправлен: id=%s, ttn=%s, alias=%s, phone=%s",
                                external_id, ttn, alias, phone_number)
                except Exception as e:
                    logger.exception("❌ Ошибка send_ttn: %s", e)
        else:
            logger.debug("TTN отсутствует — пропускаем отправку трека.")
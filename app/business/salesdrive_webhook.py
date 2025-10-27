# app/services/salesdrive_webhook.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

# Импорты из проекта (как просил)
from app.database import get_async_db, EnterpriseSettings
from app.models import MappingBranch
from app.services.order_sender import send_orders_to_tabletki
# имя файла приёма TTN уточни: тут предполагаю app/services/send_TTN.py
from app.services.send_TTN import send_ttn  # async def send_ttn(session, id, enterprise_code, ttn, deliveryServiceAlias, phoneNumber)

logger = logging.getLogger("salesdrive")
logger.setLevel(logging.INFO)

# 1) Справочники
STATUS_MAP = {2: 4, 3: 4, 4: 4, 5: 6, 6: 7}
CANCEL_REASON = {"Відмова споживача": 1, "Недостатня кількість": 5}
DELIVERY_MAP = {"novaposhta": "NP", "ukrposhta": "UP"}  # без лишних пробелов, ключи в нижнем регистре

async def _get_enterprise_code_by_branch(session: AsyncSession, branch_value: Any) -> Optional[str]:
    """Возвращает enterprise_code по значению 'branch' (sajt из вебхука)."""
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
            "goodsCode": str(p.get("productId") or ""),     # productId -> goodsCode
            "goodsName": p.get("name") or "",               # name -> goodsName
            "goodsProducer": "",                            # пусто, как просил
            "qtyShip": p.get("amount") or 0,                # amount -> qtyShip
            "priceShip": p.get("price") or 0,               # price -> priceShip
        })
    return rows

def _extract_ttn_block(data: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """
    Возвращает (trackingNumber, provider) из data. Берём первую запись с TTN из ord_delivery_data[].
    """
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
    """
    Главная точка входа бизнес-логики вебхука SalesDrive.
    Выполняет:
      - маппинг статуса,
      - сбор orders (list) в требуемом формате,
      - поиск enterprise_code по sajt -> выдача credentials,
      - вызов send_orders_to_tabletki(),
      - при наличии TTN — вызов send_ttn().
    """
    data = (payload.get("data") or {})
    status_in: Optional[int] = data.get("statusId")
    mapped_status: int = STATUS_MAP.get(status_in, status_in)  # если нет в мапе — отдаём как есть

    external_id = str(data.get("externalId") or "")
    sajt = data.get("sajt")  # branchID = sajt (как просил)
    products = data.get("products") or []

    # Сформировать объект заказа + обернуть в список
    order_obj = {
        "id": external_id,                      # externalId из вебхука
        "statusID": mapped_status,              # результат маппинга
        "branchID": str(sajt) if sajt is not None else "",
        "rows": _build_order_rows(products),
    }
    orders: List[Dict[str, Any]] = [order_obj]

    # Подготовка к отправке: enterprise_code и креды
    async with get_async_db() as session:  # предполагается, что get_async_db работает как async CM
        enterprise_code = await _get_enterprise_code_by_branch(session, sajt)
        if not enterprise_code:
            logger.error("⛔ enterprise_code не найден по sajt=%s в MappingBranch", sajt)
            return

        creds = await _get_tabletki_credentials(session, enterprise_code)
        if not creds:
            logger.error("⛔ tabletki_login/password не найдены для enterprise_code=%s", enterprise_code)
            return
        tabletki_login, tabletki_password = creds

        # 2) Статусы 2/3/4 → подтверждение (cancel_reason=1)
        if status_in in (2, 3, 4):
            try:
                await send_orders_to_tabletki(
                    session=session,
                    orders=orders,
                    tabletki_login=tabletki_login,
                    tabletki_password=tabletki_password,
                    cancel_reason=1
                )
                logger.info("✅ Отправлен подтверждённый заказ: id=%s, status_in=%s → statusID=%s, enterprise=%s",
                            external_id, status_in, mapped_status, enterprise_code)
            except Exception as e:
                logger.exception("❌ Ошибка send_orders_to_tabletki для подтверждения: %s", e)

        # 3) Статус 6 → отказ (cancel_reason по словарю)
        elif status_in == 6:
            raw_reason = data.get("rejectionReason")
            # В вебхуке иногда приходит код/число; у тебя словарь по строке — делаем мягкую деградацию
            if isinstance(raw_reason, str):
                cancel_reason = CANCEL_REASON.get(raw_reason, 1)
            else:
                # если пришёл int-код или None — ставим по умолчанию 1 и предупреждаем
                cancel_reason = 1
                logger.warning("⚠️ rejectionReason=%r не сопоставлён со словарём, используем cancel_reason=1", raw_reason)

            try:
                await send_orders_to_tabletki(
                    session=session,
                    orders=orders,
                    tabletki_login=tabletki_login,
                    tabletki_password=tabletki_password,
                    cancel_reason=cancel_reason
                )
                logger.info("✅ Отправлен отказ: id=%s, status_in=6 → statusID=%s, reason=%s, enterprise=%s",
                            external_id, mapped_status, cancel_reason, enterprise_code)
            except Exception as e:
                logger.exception("❌ Ошибка send_orders_to_tabletki для отказа: %s", e)

        else:
            logger.info("ℹ️ statusId=%s не обрабатывается (map=%s). Ничего не отправляем.", status_in, mapped_status)

        # 4) TTN — отправка трека, если есть
        ttn, provider = _extract_ttn_block(data)
        if ttn:
            alias = DELIVERY_MAP.get((provider or "").lower())
            if not alias:
                logger.warning("⚠️ Неизвестный provider=%r, не можем маппить deliveryServiceAlias", provider)
            else:
                phone = _extract_phone(data)
                try:
                    await send_ttn(
                        session=session,
                        id=external_id,
                        enterprise_code=enterprise_code,
                        ttn=ttn,
                        deliveryServiceAlias=alias,
                        phoneNumber=phone or ""
                    )
                    logger.info("📦 TTN отправлен: id=%s, ttn=%s, alias=%s, phone=%s", external_id, ttn, alias, phone)
                except Exception as e:
                    logger.exception("❌ Ошибка send_ttn: %s", e)
        else:
            logger.debug("TTN отсутствует в ord_delivery_data — пропускаем отправку трека.")
import base64
import logging
import httpx
from sqlalchemy import select
from app.models import DeveloperSettings, EnterpriseSettings
from sqlalchemy.ext.asyncio import AsyncSession

def _normalize_ttn(value: str) -> str:
    return "".join(str(value or "").split()).upper()


async def send_ttn(session: AsyncSession, id: str, enterprise_code: str, ttn: str, deliveryServiceAlias: str, phoneNumber: str) -> bool:
    """
    Отправляет TTN в Tabletki.ua.
    Если текущий TTN совпадает с новым, отправка пропускается.
    Возвращает True, если запрос на отправку был выполнен успешно.
    """
    logging.info(f"📦 Проверка TTN для заказа {id}...")

    # Получение настроек разработчика (endpoint)
    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    # Получение настроек предприятия
    enterprise_q = await session.execute(
        select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    )
    enterprise = enterprise_q.scalar()

    if not enterprise:
        logging.error(f"❌ Не найдены настройки EnterpriseSettings для enterprise_code={enterprise_code}")
        return False

    auth_header = base64.b64encode(
        f"{enterprise.tabletki_login}:{enterprise.tabletki_password}".encode()
    ).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}"
    }

    async with httpx.AsyncClient() as client:
        # Проверка текущего TTN
        status_url = f"{endpoint_orders}/api/Delivery/status/{id}"
        status_resp = await client.get(status_url, headers=headers)
        if status_resp.status_code != 200:
            logging.error(f"❌ Ошибка получения статуса TTN: {status_resp.status_code}")
            return False

        status_data = status_resp.json()
        current_ttn = str(status_data.get("ttN_Number") or "").strip()
        incoming_ttn = str(ttn or "").strip()
        if not incoming_ttn:
            logging.info("ℹ️ Новый TTN пустой — отправка пропущена.")
            return False

        if current_ttn:
            if _normalize_ttn(current_ttn) == _normalize_ttn(incoming_ttn):
                logging.info(f"✅ TTN уже актуален: {current_ttn} (совпадает с входящим)")
                return False
            logging.info(f"🔄 TTN будет обновлён: {current_ttn} -> {incoming_ttn}")

        # Отправка нового/обновленного TTN
        post_headers = headers.copy()
        post_headers["Content-Type"] = "application/json-patch+json"
        payload = [
            {
                "id": id,
                "ttn": incoming_ttn,
                "deliveryServiceAlias": deliveryServiceAlias,
                "phoneNumber": phoneNumber
            }
        ]
        post_url = f"{endpoint_orders}/api/Orders/ttnForOrder"
        post_resp = await client.post(post_url, json=payload, headers=post_headers)

        if post_resp.status_code == 200:
            logging.info(f"📦 TTN успешно отправлен: {incoming_ttn}, {deliveryServiceAlias}, {phoneNumber}")
            return True
        else:
            logging.error(f"❌ Ошибка отправки TTN: {post_resp.status_code}, {post_resp.text}")
            return False

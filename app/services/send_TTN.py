import base64
import logging
import httpx
from sqlalchemy import select
from app.models import DeveloperSettings, EnterpriseSettings
from sqlalchemy.ext.asyncio import AsyncSession

async def send_ttn(session: AsyncSession, id: str, enterprise_code: str, ttn: str, deliveryServiceAlias: str, phoneNumber: str):
    """
    Отправляет TTN в Tabletki.ua, если он еще не зарегистрирован
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
        return

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
            return

        status_data = status_resp.json()
        if status_data.get("ttN_Number"):
            logging.info(f"✅ TTN уже зарегистрирован: {status_data['ttN_Number']}")
            return

        # Отправка TTN, если не зарегистрирован
        post_headers = headers.copy()
        post_headers["Content-Type"] = "application/json-patch+json"
        payload = [
            {
                "id": id,
                "ttn": ttn,
                "deliveryServiceAlias": deliveryServiceAlias,
                "phoneNumber": phoneNumber
            }
        ]
        post_url = f"{endpoint_orders}/api/Orders/ttnForOrder"
        post_resp = await client.post(post_url, json=payload, headers=post_headers)

        if post_resp.status_code == 200:
            logging.info(f"📦 TTN успешно отправлен: {ttn}, {deliveryServiceAlias}, {phoneNumber}")
        else:
            logging.error(f"❌ Ошибка отправки TTN: {post_resp.status_code}, {post_resp.text}")

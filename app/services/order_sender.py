# app/services/order_sender.py

import aiohttp
import base64
import json
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models import DeveloperSettings

async def send_orders_to_tabletki(session: AsyncSession, orders: list, tabletki_login: str, tabletki_password: str):
    """
    Отправляет заказы в Tabletki.ua по API в зависимости от статуса заказа:
    - статус 4 или 6: подтверждён (отправка в /api/orders),
    - статус 7: отказ (отправка в /api/Orders/cancelledOrders).
    """
    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    auth_header = base64.b64encode(f"{tabletki_login}:{tabletki_password}".encode()).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}"
    }

    async with aiohttp.ClientSession() as http_session:
        for order in orders:
            if order["statusID"] == 7 or all(item.get("qtyShip", 0) == 0 for item in order["rows"]):
                url = f"{endpoint_orders}/api/Orders/cancelledOrders"
                cancel_data = [{
                    "id": order["id"],
                    "id_CancelReason": 2,
                    "rows": [
                        {
                            "goodsCode": item["goodsCode"],
                            "qty": item.get("qty", item.get("qtyShip", 0))
                        } for item in order["rows"]
                    ]
                }]
                print(f"🚫 Отправка отказа заказа {order['id']}:")
                print(json.dumps(cancel_data, indent=2, ensure_ascii=False))
                async with http_session.post(url, json=cancel_data, headers=headers) as response:
                    print(f"📬 Ответ при отказе: {response.status}, {await response.text()}")

            elif order["statusID"] in [4, 6]:
                valid_rows = [item for item in order["rows"] if item.get("qtyShip", 0) > 0]
                if not valid_rows:
                    print(f"⚠️ Пропущен заказ {order['id']} — нет строк с qtyShip > 0")
                    continue

                order["rows"] = valid_rows
                url = f"{endpoint_orders}/api/orders"
                async with http_session.post(url, json=[order], headers=headers) as response:
                    print(f"✅ Заказ {order['id']} отправлен: {response.status}, {await response.text()}")
                    
					
async def send_single_order_status_2(session: AsyncSession, order: dict, tabletki_login: str, tabletki_password: str):
    """
    Отправляет заказ на Tabletki.ua со статусом 2.0 — ручное подтверждение без автопроверки остатков.
    """
    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    auth_header = base64.b64encode(f"{tabletki_login}:{tabletki_password}".encode()).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}"
    }

    url = f"{endpoint_orders}/api/orders"
    valid_rows = [item for item in order["rows"] if item.get("qty", 0) > 0 or item.get("qtyShip", 0) > 0]

    if not valid_rows:
        print(f"⚠️ Пропущен заказ {order['id']} — нет строк с qty или qtyShip > 0")
        return

    order["rows"] = valid_rows

    async with aiohttp.ClientSession() as http_session:
        async with http_session.post(url, json=[order], headers=headers) as response:
            response_text = await response.text()
            print(f"📤 Заказ {order['id']} отправлен на Tabletki со статусом 2.0. Статус: {response.status}")
            print(f"📨 Ответ: {response_text}")
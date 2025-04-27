import aiohttp
import datetime
from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings

async def send_order_to_key_crm(order: dict, enterprise_code: str, branch: str):
    print(f"📦 [KeyCRM] Передача нового замовлення {order.get('id')} для {enterprise_code}, філія {branch}")

    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        enterprise = result.scalars().first()
        if not enterprise or not enterprise.token:
            print("❌ API ключ не найден.")
            return

        token = enterprise.token
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        async with aiohttp.ClientSession() as http_session:
            # 1. Получаем source_id
            source_id = None
            async with http_session.get("https://openapi.keycrm.app/v1/order/source", headers=headers) as resp:
                data = await resp.json()
                for src in data.get("data", []):
                    if src.get("name") == "Tabletki.ua":
                        source_id = src.get("id")
                        break
            if not source_id:
                print("❌ source_id для 'Tabletki.ua' не найден.")
                return

            # 2. Получаем delivery_service_id
            delivery_service_id = None
            alias_map = {
                "NP": "Novaposhta",
                "UP": "UkrPoshta"
            }
            delivery_alias = next((item["value"] for item in order.get("deliveryData", []) if item["key"] == "DeliveryServiceAlias"), "")
            delivery_name = alias_map.get(delivery_alias)
            if delivery_name:
                async with http_session.get("https://openapi.keycrm.app/v1/order/delivery-service", headers=headers) as resp:
                    data = await resp.json()
                    for item in data.get("data", []):
                        if item.get("name") == delivery_name:
                            delivery_service_id = item.get("id")
                            break

            # 3. Формируем recipient full name
            full_name = " ".join([
                next((x["value"] for x in order["deliveryData"] if x["key"] == k), "") 
                for k in ["LastName", "Name", "MiddleName"]
            ])

            # 4. Секция shipping
            delivery = {
                "delivery_service_id": delivery_service_id,
                "tracking_code": "",
                "shipping_service": next((x["value"] for x in order["deliveryData"] if x["key"] == "DeliveryServiceName"), ""),
                "shipping_address_city": next((x["value"] for x in order["deliveryData"] if x["key"] == "CityReceiver"), ""),
                "shipping_address_country": "Ukraine",
                "shipping_address_region": "",
                "shipping_address_zip": "",
                "shipping_secondary_line": "string",
                "shipping_receive_point": next((x["value"] for x in order["deliveryData"] if x["key"] == "ReceiverWhs"), ""),
                "recipient_full_name": full_name,
                "recipient_phone": order.get("customerPhone", ""),
                "warehouse_ref": next((x["value"] for x in order["deliveryData"] if x["key"] == "ID_Whs"), ""),
                "shipping_date": datetime.date.today().isoformat()
            } if order.get("deliveryData") else {}

            # 5. Секция products
            products = [{
                "sku": row["goodsCode"],
                "price": row["price"],
                "purchased_price": row["price"],
                "discount_percent": 0,
                "discount_amount": 0,
                "quantity": row["qty"],
                "unit_type": "шт",
                "name": row["goodsName"],
                "comment": ""
            } for row in order["rows"]]

            # 6. Финальный payload
            payload = {
                "source_id": source_id,
                "source_uuid": order.get("id"),
                "buyer_comment": order.get("comment", ""),
                "manager_id": 1,
                "manager_comment": "",
                "promocode": "",
                "discount_percent": 0,
                "discount_amount": 0,
                "shipping_price": 0,
                "wrap_price": 0,
                "gift_message": "",
                "is_gift": False,
                "gift_wrap": False,
                "taxes": 0,
                "ordered_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "buyer": {
                    "full_name": full_name,
                    "email": "",
                    "phone": order.get("customerPhone", "")
                },
                "shipping": delivery,
                "products": products,
                "payments": [
                    {
                        "payment_method_id": 1,
                        "payment_method": "Наложенный платеж",
                        "amount": 0.01,
                        "description": "Наложенный платеж",
                        "payment_date": "",
                        "status": "not_paid"
                    }
                ],
                "custom_fields": []
            }

            # 7. Отправка
            async with http_session.post("https://openapi.keycrm.app/v1/order", json=payload, headers=headers) as resp:
                resp_text = await resp.text()
                print(f"📬 Відповідь від KeyCRM ({resp.status}): {resp_text}")
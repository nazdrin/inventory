import requests
import aiohttp
import datetime
from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings

def fetch_skus_by_product_ids(api_key, product_ids):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    sku_map = {}
    chunk_size = 50
    for i in range(0, len(product_ids), chunk_size):
        chunk = product_ids[i:i + chunk_size]
        filter_param = ",".join(map(str, chunk))
        params = {
            "limit": 100,
            "page": 1,
            "include": "product",
            "sort": "id",
            "filter[product_id]": filter_param
        }
        print(f"🔍 Запрос SKUs: {params}")
        response = requests.get("https://openapi.keycrm.app/v1/offers", headers=headers, params=params)
        print(f"📤 URL запроса: {response.url}")
        print(f"📩 Ответ от KeyCRM (status_code={response.status_code}): {response.text}")
        if response.status_code != 200:
            continue
        data = response.json().get("data", [])
        print(f"📩 Ответ от KeyCRM (status_code={response.status_code}): {response.text}")
        for offer in data:
            sku_map[str(offer.get("product_id"))] = offer.get("sku")
    return sku_map


async def send_order_to_key_crm(order: dict, enterprise_code: str, branch: str):
    """
    🟢 Зміни:
      - Додано нормалізацію вхідних даних: якщо order = [ {...} ], беремо перший елемент.
      - Передаємо order['code'] у поле manager_comment у payload (рядком).
    """
    # 0) Нормалізація вхідних даних (якщо раптом передали список)
    if isinstance(order, list):
        if not order:
            print("❌ Порожній список замовлень.")
            return
        order = order[0]

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
            # 1) Отримуємо source_id
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

            # 2) Отримуємо delivery_service_id за Alias
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

            # 3) Формуємо ПІБ отримувача
            full_name = " ".join([
                next((x["value"] for x in order.get("deliveryData", []) if x["key"] == k), "") 
                for k in ["LastName", "Name", "MiddleName"]
            ]).strip()

            # 4) Секція shipping
            delivery = {
                "delivery_service_id": delivery_service_id,
                "tracking_code": "",
                "shipping_service": next((x["value"] for x in order.get("deliveryData", []) if x["key"] == "DeliveryServiceName"), ""),
                "shipping_address_city": next((x["value"] for x in order.get("deliveryData", []) if x["key"] == "CityReceiver"), ""),
                "shipping_address_country": "Ukraine",
                "shipping_address_region": "",
                "shipping_address_zip": "",
                "shipping_secondary_line": "string",
                "shipping_receive_point": next((x["value"] for x in order.get("deliveryData", []) if x["key"] == "ReceiverWhs"), ""),
                "recipient_full_name": full_name,
                "recipient_phone": order.get("customerPhone", ""),
                "warehouse_ref": next((x["value"] for x in order.get("deliveryData", []) if x["key"] == "ID_Whs"), ""),
                "shipping_date": datetime.date.today().isoformat()
            } if order.get("deliveryData") else {}

            # 5) Секція products
            product_ids = [row["goodsCode"] for row in order["rows"]]
            sku_map = fetch_skus_by_product_ids(token, product_ids)
            products = [{
                "sku": sku_map.get(str(row["goodsCode"]), ""),
                "price": row["price"],
                "purchased_price": row["price"],
                "discount_percent": 0,
                "discount_amount": 0,
                "quantity": row["qty"],
                "unit_type": "шт",
                "name": row["goodsName"],
                "comment": ""
            } for row in order["rows"]]

            # 5.1) Значення для manager_comment з order['code']
            manager_comment_value = str(order.get("code", ""))
            print(f"📝 manager_comment (із order.code): {manager_comment_value}")

            # 6) Фінальний payload
            payload = {
                "source_id": source_id,
                "source_uuid": order.get("id"),
                "buyer_comment": order.get("comment", ""),
                "manager_id": 1,
                "manager_comment": manager_comment_value,  # <- тут передаємо code
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

            # 7) Відправка
            async with http_session.post("https://openapi.keycrm.app/v1/order", json=payload, headers=headers) as resp:
                resp_text = await resp.text()
                print(f"📬 Відповідь від KeyCRM ({resp.status}): {resp_text}")

import aiohttp
import logging
import datetime
from sqlalchemy.future import select
from app.database import get_async_db, EnterpriseSettings
from app.services.order_sender import send_orders_to_tabletki
from app.services.send_TTN import send_ttn

STATUS_MAP = {
    2: 4,
    3: 4,
    4: 4,
    5: 6,
    6: 7
}

ALIAS_REVERSE = {
    "Novaposhta": "NP",
    "Ukrposhta": "UP"
}

async def check_statuses_key_crm(order: dict, enterprise_code: str, branch: str):
    print(f"📦 [KeyCRM] Перевірка замовлення {order.get('id')} для {enterprise_code}")

    async with get_async_db() as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        enterprise = result.scalars().first()
        if not enterprise or not enterprise.token:
            logging.warning(f"❌ API token відсутній для {enterprise_code}")
            return

        token = enterprise.token
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}"
        }

        async with aiohttp.ClientSession() as http_session:
            url = f"https://openapi.keycrm.app/v1/order?filter[source_uuid]={order.get('id')}&include=products,shipping"
            async with http_session.get(url, headers=headers) as resp:
                data = await resp.json()
                seller_order = data.get("data", [])[0] if data.get("data") else None
                logging.info(f"📦 Ответ продавца: {seller_order}")

                if not seller_order:
                    logging.info("❗ Замовлення не знайдено у продавця.")
                    return

                seller_status_group_id = seller_order.get("status_group_id")
                mapped_status = STATUS_MAP.get(seller_status_group_id, 1)

                logging.info(
                    f"🔎 Сравнение статусов: статус продавця (група)={seller_status_group_id}, наш статус={order['statusID']}, результат сопоставления={mapped_status}"
                )
                cancel_reason = 1
                if mapped_status > order["statusID"]:
                    logging.info(f"📌 Статус продавця: {seller_status_group_id}, Внутрішній статус до зміни: {order['statusID']}, після зміни: {mapped_status}")

                    seller_products = seller_order.get("products", [])
                    for i, item in enumerate(order.get("rows", [])):
                        if i < len(seller_products):
                            seller_product = seller_products[i]
                            qty_seller = seller_product.get("quantity", item["qty"])
                            order["rows"][i]["qtyShip"] = qty_seller
                            order["rows"][i]["priceShip"] = seller_product.get("price", item["price"])
                            if qty_seller != item["qty"]:
                                logging.info(f"🔁 Змінено qty товару {item['goodsCode']}: старе={item['qty']}, нове={qty_seller}")
                                order["rows"][i]["qty"] = qty_seller
                            elif qty_seller == 0:
                                logging.warning(f"❌ У продавця відсутній товар {item['goodsCode']}, кількість = 0 — можливе скасування замовлення")
                        else:
                            logging.warning(f"⚠️ У продавця відсутня позиція {item['goodsCode']} — пропущено")

                    order["statusID"] = mapped_status
                    logging.info(f"📦 Підготовка до надсилання замовлення з товарами: {order['rows']}")

                    await send_orders_to_tabletki(session, [order], enterprise.tabletki_login, enterprise.tabletki_password,cancel_reason=cancel_reason,)
                    logging.info(f"✅ Оновлений статус до {mapped_status} та надіслано в Tabletki.ua")
                else:
                    logging.info(f"ℹ️ Статус продавця ({seller_status_group_id}) не вищий за наш ({order['statusID']}), оновлення не потрібно")

                tracking_code = seller_order.get("shipping", {}).get("tracking_code")
                if tracking_code:
                    delivery_service_id = seller_order.get("shipping", {}).get("delivery_service_id")
                    delivery_alias = ""
                    if delivery_service_id:
                        try:
                            async with http_session.get(
                                "https://openapi.keycrm.app/v1/order/delivery-service?limit=50&page=1&sort=id",
                                headers=headers
                            ) as delivery_resp:
                                delivery_data = await delivery_resp.json()
                                services = delivery_data.get("data", [])
                                matched_service = next((s for s in services if s["id"] == delivery_service_id), None)
                                if matched_service:
                                    delivery_name = matched_service.get("name")
                                    delivery_alias = ALIAS_REVERSE.get(delivery_name, "")
                        except Exception as e:
                            logging.warning(f"❌ Помилка при отриманні delivery_service: {e}")

                    phone_number = seller_order.get("shipping", {}).get("recipient_phone", order.get("customerPhone"))

                    sent = await send_ttn(
                        session=session,
                        id=order["id"],
                        enterprise_code=enterprise_code,
                        ttn=tracking_code,
                        deliveryServiceAlias=delivery_alias,
                        phoneNumber=phone_number
                    )
                    if sent:
                        logging.info(f"📦 Відправлено/оновлено TTN: {tracking_code}, {delivery_alias}, {phone_number}")
                    else:
                        logging.info(f"ℹ️ TTN не відправлено: {tracking_code} (співпадає, порожній або була помилка)")

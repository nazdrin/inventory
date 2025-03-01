import base64
import asyncio
import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings, MappingBranch, InventoryStock

# 2.1 Подключение к базе через get_async_db
async def get_enterprises_with_auto_booking(session: AsyncSession):
    result = await session.execute(
        select(EnterpriseSettings.enterprise_code).where(EnterpriseSettings.auto_confirm == True)
    )
    return [row[0] for row in result.fetchall()]

# 2.3 Получение филиалов по предприятиям
async def get_branches_for_enterprises(session: AsyncSession, enterprise_codes):
    result = await session.execute(
        select(MappingBranch.branch, MappingBranch.enterprise_code)
        .where(MappingBranch.enterprise_code.in_(enterprise_codes))
    )
    return result.fetchall()

# 2.4 Запрос данных о заказах
async def fetch_orders(session: AsyncSession, branch, enterprise_code):
    developer_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = developer_settings.scalar()
    
    enterprise_settings = await session.execute(
        select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    )
    enterprise = enterprise_settings.scalar()
    
    if not enterprise:
        print(f"Не найдены настройки для enterprise_code: {enterprise_code}")
        return None
    
    auth_header = base64.b64encode(f"{enterprise.tabletki_login}:{enterprise.tabletki_password}".encode()).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}"
    }
    
    orders = []
    async with aiohttp.ClientSession() as session:
        for status in [0, 1, 2]:
            url = f"{endpoint_orders}/api/Orders/{branch}/{status}"
            async with session.get(url, headers=headers) as response:
                response_text = await response.text()
                print(f"Ответ для branch {branch}, status {status}: {response_text}")
                
                if response.status == 200:
                    data = await response.json()
                    orders.extend(data)
                else:
                    print(f"Ошибка при получении заказов для branch {branch}: {response.status}")
    
    return orders

# 2.5 Обработка заказов
async def process_orders(session: AsyncSession, orders):
    processed_orders = []

    for order in orders:
        branch_id = order["branchID"]
        order_id = order["id"]

        items_status = []
        order_rows = []

        for item in order["rows"]:
            goods_code = item["goodsCode"]
            qty_requested = item["qty"]  # Теперь qty сохраняется
            
            stock_result = await session.execute(
                select(InventoryStock).where(
                    (InventoryStock.branch == branch_id) & (InventoryStock.code == goods_code)
                ).order_by(InventoryStock.updated_at.desc())
            )
            stock_entry = stock_result.scalars().first()

            if not stock_entry or stock_entry.qty == 0:
                items_status.append("not_available")
                order_rows.append({
                    "goodsCode": goods_code,
                    "qty": qty_requested  # Добавляем qty из заказа
                })
                continue

            if stock_entry.qty >= qty_requested:
                order_rows.append({
                    "goodsCode": goods_code,
                    "goodsName": item["goodsName"],
                    "goodsProducer": item["goodsProducer"],
                    "qtyShip": qty_requested,
                    "priceShip": item["price"]
                })
                items_status.append("available")
            else:
                order_rows.append({
                    "goodsCode": goods_code,
                    "goodsName": item["goodsName"],
                    "goodsProducer": item["goodsProducer"],
                    "qtyShip": stock_entry.qty,
                    "priceShip": item["price"]
                })
                items_status.append("partial")

        if all(status == "not_available" for status in items_status):
            status_id = 7
        elif "partial" in items_status or "available" in items_status:
            status_id = 4
        else:
            status_id = 4

        processed_orders.append({
            "id": order_id,
            "statusID": status_id,
            "branchID": branch_id,
            "rows": order_rows  # Теперь qty остается в заказе
        })

    return processed_orders

async def send_order_results(session: AsyncSession, processed_orders, auth_header):
    import json
    developer_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = developer_settings.scalar()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}"
    }
    
    async with aiohttp.ClientSession() as http_session:
        for order in processed_orders:
            if order["statusID"] == 7 or all(item.get("qtyShip", 0) == 0 for item in order["rows"]):
                url = f"{endpoint_orders}/api/Orders/cancelledOrders"
                cancel_data = [{
                    "id": order["id"],
                    "id_CancelReason": 2,
                    "rows": [{"goodsCode": item["goodsCode"], "qty": item.get("qty", item.get("qtyShip", 0))} for item in order["rows"]]
                }]
                print(f"Отправка запроса на отказ: {cancel_data}")
                print(f"Отправка запроса на отказ: {json.dumps(cancel_data, indent=2, ensure_ascii=False)}")
                async with http_session.post(url, json=cancel_data, headers=headers) as response:
                    response_text = await response.text()
                    print(f"Ответ от сервера при отказе заказа {order['id']}: {response_text} (Статус: {response.status})")
            else:
                valid_rows = [item for item in order["rows"] if item["qtyShip"] > 0]
                if not valid_rows:
                    print(f"Пропущен заказ {order['id']} из-за отсутствия доступных позиций.")
                    continue
                order["rows"] = valid_rows
                url = f"{endpoint_orders}/api/orders"
                async with http_session.post(url, json=[order], headers=headers) as response:
                    response_text = await response.text()
                    print(f"Ответ от сервера при отправке заказа {order['id']}: {response_text}")


# Основная асинхронная функция
async def main():
    async with get_async_db() as session:
        enterprise_codes = await get_enterprises_with_auto_booking(session)
        print(f"Найдено {len(enterprise_codes)} предприятий с авто-бронированием")
        
        branches = await get_branches_for_enterprises(session, enterprise_codes)
        print(f"Обнаружено {len(branches)} филиалов")
        
        for branch, enterprise_code in branches:
            orders = await fetch_orders(session, branch, enterprise_code)
            if orders:
                print(f"Получено {len(orders)} заказов для филиала {branch}")
                processed_orders = await process_orders(session, orders)
                print(f"Обработано {len(processed_orders)} заказов для филиала {branch}")
                enterprise_settings = await session.execute(
                    select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
                )
                enterprise = enterprise_settings.scalar()
                auth_header = base64.b64encode(f"{enterprise.tabletki_login}:{enterprise.tabletki_password}".encode()).decode()
                await send_order_results(session, processed_orders, auth_header)
            else:
                print(f"Заказы для филиала {branch} не найдены")

if __name__ == "__main__":
    asyncio.run(main())

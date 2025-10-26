# app/services/auto_confirm.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.database import InventoryStock

async def process_orders(session: AsyncSession, orders):
    """
    Проверка остатков для заказов, формирование подтвержденных и отклоненных строк.
    Возвращает список заказов с обновлёнными статусами.
    """
    processed_orders = []

    for order in orders:
        branch_id = order["branchID"]
        order_id = order["id"]
        items_status = []
        order_rows = []

        for item in order["rows"]:
            goods_code = item["goodsCode"]
            qty_requested = item["qty"]

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
                    "qty": qty_requested
                })
                continue

            if stock_entry.qty >= qty_requested:
                items_status.append("available")
                order_rows.append({
                    "goodsCode": goods_code,
                    "goodsName": item.get("goodsName"),
                    "goodsProducer": item.get("goodsProducer"),
                    "qtyShip": qty_requested,
                    "priceShip": item["price"]
                })
            else:
                items_status.append("partial")
                order_rows.append({
                    "goodsCode": goods_code,
                    "goodsName": item.get("goodsName"),
                    "goodsProducer": item.get("goodsProducer"),
                    "qtyShip": stock_entry.qty,
                    "priceShip": item["price"]
                })

        if all(s == "not_available" for s in items_status):
            status_id = 7
        else:
            status_id = 4

        processed_orders.append({
            "id": order_id,
            "statusID": status_id,
            "branchID": branch_id,
            "rows": order_rows
        })

    return processed_orders
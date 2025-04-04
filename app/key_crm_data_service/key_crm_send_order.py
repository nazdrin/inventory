async def send_order_to_key_crm(order: dict, enterprise_code: str, branch: str):
    """
    Заглушка: отправка заказа продавцу через KeyCRM.
    """
    print(f"📦 [KeyCRM] Отправка заказа ID {order.get('id')} для {enterprise_code}, филиал {branch}")
    # TODO: реализовать логику отправки заказа через API KeyCRM
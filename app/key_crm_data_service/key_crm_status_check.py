async def check_statuses_key_crm(order: dict, enterprise_code: str, branch: str):
    """
    Заглушка: отправка/проверка актуального статуса заказа в KeyCRM.
    """
    print(f"📦 [KeyCRM] Проверка/отправка статуса для заказа {order.get('id')} – предприятие: {enterprise_code}, филиал: {branch}")
    # TODO: реализовать логику запроса/обновления статуса в KeyCRM
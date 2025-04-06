import base64
import aiohttp
import json
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models import DeveloperSettings, EnterpriseSettings, MappingBranch
from app.services.auto_confirm import process_orders
from app.services.order_sender import send_orders_to_tabletki
from app.services.order_sender import send_single_order_status_2
from app.key_crm_data_service.key_crm_send_order import send_order_to_key_crm
from app.key_crm_data_service.key_crm_status_check import check_statuses_key_crm

ORDER_SEND_PROCESSORS = {
    "KeyCRM": send_order_to_key_crm,
    # Добавишь сюда новые форматы позже
}
ORDER_STATUS_CHECKERS = {
    "KeyCRM": check_statuses_key_crm,
    # Примеры для будущих форматов:
    # "Dntrade": check_statuses_dntrade,
    # "Prom": check_statuses_prom,
}

async def fetch_orders_for_enterprise(session: AsyncSession, enterprise_code: str):
    """
    Получает заказы для заданного предприятия (enterprise_code),
    если активирован флаг `order_fetcher = True` и найдены филиалы.
    ВРЕМЕННО выводит каждый полученный заказ в терминал.
    """
    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    enterprise_q = await session.execute(
        select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
    )
    enterprise = enterprise_q.scalar()

    if not enterprise:
        print(f"❌ Не найдены настройки EnterpriseSettings для enterprise_code={enterprise_code}")
        return []

    if not enterprise.order_fetcher:
        print(f"ℹ️ Флаг order_fetcher=False, пропуск: enterprise_code={enterprise_code}")
        return []

    auth_header = base64.b64encode(
        f"{enterprise.tabletki_login}:{enterprise.tabletki_password}".encode()
    ).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}"
    }

    branches_q = await session.execute(
        select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
    )
    branches = [row[0] for row in branches_q.fetchall()]
    auto_confirm_flag = enterprise.auto_confirm

    all_orders = []
    async with aiohttp.ClientSession() as http_session:
        for branch in branches:
            if auto_confirm_flag:
                # ===== Вариант с авто-подтверждением =====
                status = 2
                url = f"{endpoint_orders}/api/Orders/{branch}/{status}"
                try:
                    async with http_session.get(url, headers=headers) as response:
                        print(f"🌐 Запрос заказов: {url}")
                        if response.status == 200:
                            data = await response.json()
                            if isinstance(data, list):
                                for order in data:
                                    print("📦 Заказ:")
                                    print(json.dumps(order, indent=2, ensure_ascii=False))

                                order_codes = list(set(order["code"] for order in data if "code" in order))
                                if order_codes:
                                    from app.services.telegram_bot import notify_user
                                    await notify_user(branch, order_codes)

                                # 👉 ОБРАБОТКА
                                processed_orders = await process_orders(session, data)
                                print(f"🔁 Обработано {len(processed_orders)} заказов")

                                # 👉 ОТПРАВКА
                                await send_orders_to_tabletki(
                                    session,
                                    processed_orders,
                                    tabletki_login=enterprise.tabletki_login,
                                    tabletki_password=enterprise.tabletki_password
                                )
                        else:
                            print(f"❌ Ошибка при запросе заказов: {response.status} для branch={branch}")
                except Exception as e:
                    print(f"🚨 Ошибка при запросе заказов для branch={branch}, status={status}: {str(e)}")
            else:
                # ===== Вариант без авто-подтверждения =====
                for status in [0, 2, 4, 4.1]:
                    url = f"{endpoint_orders}/api/Orders/{branch}/{status}"
                    try:
                        async with http_session.get(url, headers=headers) as response:
                            print(f"🌐 Запрос заказов: {url}")
                            if response.status == 200:
                                data = await response.json()
                                if isinstance(data, list):
                                    for order in data:
                                        print("📦 Заказ:")
                                        print(json.dumps(order, indent=2, ensure_ascii=False))
                                        if status == 0:
                                            # TODO: передача заказов продавцу
                                            processor = ORDER_SEND_PROCESSORS.get(enterprise.data_format)
                                            if processor:
                                                await processor(order, enterprise_code, branch)
                                            else:
                                                print(f"⚠️ Нет функции отправки заказа для формата {enterprise.data_format}")
                                           
                                            # Отправка на Tabletki.ua со статусом 2.0
                                            order["statusID"] = 2.0
                                            await send_single_order_status_2(
                                                session=session,
                                                order=order,
                                                tabletki_login=enterprise.tabletki_login,
                                                tabletki_password=enterprise.tabletki_password
                                            )
                                        elif status in [2, 4, 4.1]:
                                            # TODO: передача статуса продавцу
                                            # Отправка актуального статуса продавцу через соответствующий обработчик
                                            status_checker = ORDER_STATUS_CHECKERS.get(enterprise.data_format)
                                            if status_checker:
                                                await status_checker(order, enterprise_code, branch)
                                            else:
                                                print(f"⚠️ Нет обработчика для проверки статуса формата {enterprise.data_format}")
                                        all_orders.append(order)
                                    if status == 0:
                                        order_codes = list(set(order["code"] for order in data if "code" in order))
                                        if order_codes:
                                            from app.services.telegram_bot import notify_user
                                            await notify_user(branch, order_codes)
                            else:
                                print(f"❌ Ошибка при запросе заказов: {response.status} для branch={branch}")
                    except Exception as e:
                        print(f"🚨 Ошибка при запросе заказов для branch={branch}, status={status}: {str(e)}")

    print(f"✅ Получено всего {len(all_orders)} заказов для предприятия {enterprise_code}")
    return all_orders
# services/order_fetcher.py

import base64
import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models import DeveloperSettings, EnterpriseSettings, MappingBranch

async def fetch_orders_for_enterprise(session: AsyncSession, enterprise_code: str):
    """
    Получает заказы для заданного предприятия (enterprise_code),
    если активирован флаг `order_fetcher = True` и найдены филиалы.

    Args:
        session (AsyncSession): активная сессия БД
        enterprise_code (str): код предприятия

    Returns:
        list: список заказов, если успешно получены
    """
    # Получаем URL API заказов из DeveloperSettings
    dev_settings = await session.execute(select(DeveloperSettings.endpoint_orders))
    endpoint_orders = dev_settings.scalar()

    # Получаем настройки конкретного предприятия
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

    # Авторизация через Basic Auth (логин и пароль из настроек)
    auth_header = base64.b64encode(
        f"{enterprise.tabletki_login}:{enterprise.tabletki_password}".encode()
    ).decode()
    headers = {
        "accept": "application/json",
        "Authorization": f"Basic {auth_header}"
    }

    # Получаем список филиалов, привязанных к предприятию
    branches_q = await session.execute(
        select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
    )
    branches = [row[0] for row in branches_q.fetchall()]
    if not branches:
        print(f"⚠️ Нет филиалов в MappingBranch для enterprise_code={enterprise_code}")
        return []

    all_orders = []
    async with aiohttp.ClientSession() as http_session:
        for branch in branches:
            for status in [0, 1, 2, 3, 4]:  # Список статус-кодов из бизнес-логики
                url = f"{endpoint_orders}/api/Orders/{branch}/{status}"
                try:
                    async with http_session.get(url, headers=headers) as response:
                        print(f"🌐 Запрос заказов: {url}")
                        if response.status == 200:
                            data = await response.json()
                            if isinstance(data, list):
                                all_orders.extend(data)
                            else:
                                print(f"⚠️ Ответ не является списком для branch={branch}, status={status}")
                        else:
                            print(f"❌ Ошибка при запросе заказов: {response.status} для branch={branch}")
                except Exception as e:
                    print(f"🚨 Ошибка при запросе заказов для branch={branch}, status={status}: {str(e)}")

    print(f"✅ Получено всего {len(all_orders)} заказов для предприятия {enterprise_code}")
    return all_orders
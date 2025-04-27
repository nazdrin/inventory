import aiohttp
import asyncio

API_URL = "https://openapi.keycrm.app/v1/order/source"
API_TOKEN = "YjFkODBhM2IxZWQyZGZkZjcwNWY5OGRjOGQ0ODUzODE5NDEwN2NjYw"

async def fetch_keycrm_order_sources(limit=15, page=1, sort="id"):
    url = f"{API_URL}?limit={limit}&page={page}&sort={sort}"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {API_TOKEN}"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            print(f"🔗 GET {url}")
            print(f"📡 Статус: {response.status}")
            
            response_json = await response.json()
            print("📤 Відповідь від KeyCRM:\n")

            for item in response_json.get("data", []):
                print(f"🆔 ID: {item.get('id')}")
                print(f"📦 Назва: {item.get('name')}")
                print(f"🔗 Alias: {item.get('alias')}")
                print(f"🚚 Driver: {item.get('driver')}")
                print(f"🌍 Source URL: {item.get('source_uuid')}")
                print(f"💰 Валюта: {item.get('currency_code')}")
                print(f"📅 Створено: {item.get('created_at')}")
                print(f"🛠 Оновлено: {item.get('updated_at')}")
                print("-" * 40)

async def fetch_keycrm_order_statuses(limit=15, page=1, sort="id"):
    url = f"https://openapi.keycrm.app/v1/order/status?limit={limit}&page={page}&sort={sort}"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {API_TOKEN}"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            print(f"\n🔗 GET {url}")
            print(f"📡 Статус: {response.status}")
            response_json = await response.json()
            print("📤 Статуси замовлень:\n")
            for item in response_json.get("data", []):
                print(f"🆔 ID: {item.get('id')}")
                print(f"📋 Назва: {item.get('name')}")
                print(f"🎯 Тип: {item.get('type')}")
                print(f"📅 Створено: {item.get('created_at')}")
                print(f"🛠 Оновлено: {item.get('updated_at')}")
                print("-" * 40)

async def fetch_keycrm_delivery_services(limit=15, page=1, sort="id"):
    url = f"https://openapi.keycrm.app/v1/order/delivery-service?limit={limit}&page={page}&sort={sort}"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {API_TOKEN}"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            print(f"\n🔗 GET {url}")
            print(f"📡 Статус: {response.status}")
            response_json = await response.json()
            print("📤 Служби доставки:\n")
            for item in response_json.get("data", []):
                print(f"🆔 ID: {item.get('id')}")
                print(f"🚚 Назва: {item.get('name')}")
                print(f"🔗 Код: {item.get('code')}")
                print(f"📅 Створено: {item.get('created_at')}")
                print(f"🛠 Оновлено: {item.get('updated_at')}")
                print("-" * 40)

if __name__ == "__main__":
    asyncio.run(fetch_keycrm_order_sources())
    asyncio.run(fetch_keycrm_order_statuses())
    asyncio.run(fetch_keycrm_delivery_services())
import requests
import os
import json

API_URL = "https://openapi.keycrm.app/v1/offers/stocks"
ENTERPRISE_CODE = "2"
LIMIT = 15


def fetch_all_stock(api_key):
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    all_items = []
    page = 1

    while True:
        params = {
            "limit": LIMIT,
            "page": page,
            "filter[details]": "true"
        }

        response = requests.get(API_URL, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Ошибка при запросе страницы {page}: {response.status_code}")
            break

        json_data = response.json()
        items = json_data.get("data", [])
        if not items:
            break

        all_items.extend(items)

        if not json_data.get("next_page_url"):
            break

        page += 1

    return all_items


def save_raw_data(data):
    os.makedirs("temp", exist_ok=True)
    file_path = os.path.join("temp", "raw_stock_data.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"✅ Данные сохранены в файл: {file_path}")


if __name__ == "__main__":
    # 🔐 Здесь укажи временно токен вручную
    API_KEY = "YjFkODBhM2IxZWQyZGZkZjcwNWY5OGRjOGQ0ODUzODE5NDEwN2NjYw"

    raw_data = fetch_all_stock(API_KEY)
    save_raw_data(raw_data)

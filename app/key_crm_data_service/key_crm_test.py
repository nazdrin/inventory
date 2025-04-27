import requests
import os
import json

API_URL = "https://openapi.keycrm.app/v1/order/status"
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


def fetch_order_statuses(api_key):
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
            "sort": "id"
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

    print(json.dumps(all_items, ensure_ascii=False, indent=4))


if __name__ == "__main__":
    API_KEY = "MmRlMThlZDM2YzI5MWZlOTE1YTEyOWMyZmI1YzY5ZDY1YWI2Yjc3OA"

    raw_data = fetch_all_stock(API_KEY)
    statuses = fetch_order_statuses(API_KEY)
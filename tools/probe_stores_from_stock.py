#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Сбор кодов и названий складов Ветменеджер через остатки товаров.

Почему так?
- /rest/api/Store на вашем инстансе отвечает 200, но пусто.
- В ответах /rest/api/Good/StockBalancesForProduct для товаров присутствуют поля store/store_id и title.
- Пройдёмся по первым N товарам и агрегируем уникальные склады.

Запуск:
  python tools/probe_stores_from_stock.py --domain vetdom.vetmanager.cloud --api-key 'KEY' --max-goods 300
"""

import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests

X_REST_TIME_ZONE = "Europe/Kiev"  # важно: 'Kyiv' на вашем инстансе ранее давал 500
LOG_PROGRESS_EVERY = 50           # прогресс-лог каждые N товаров


def setup_logger() -> logging.Logger:
    lg = logging.getLogger("probe_stores")
    if not lg.handlers:
        lg.setLevel(logging.INFO)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        lg.addHandler(h)
    return lg


def headers(api_key: str) -> Dict[str, str]:
    return {
        "X-REST-API-KEY": api_key,
        "Content-Type": "application/json",
        "X-REST-TIME-ZONE": X_REST_TIME_ZONE,
    }


def http_get_json(url: str, api_key: str, logger: logging.Logger) -> Dict[str, Any]:
    resp = requests.get(url, headers=headers(api_key), timeout=30)
    try:
        data = resp.json()
    except ValueError:
        data = {}
    if not resp.ok:
        body = (resp.text or "")[:400].replace("\n", "\\n")
        logger.error(f"HTTP {resp.status_code} for {url}: {body}")
        resp.raise_for_status()
    return data if isinstance(data, dict) else {"data": data}


def _extract_list(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Гибко достаём массив из разных форматов."""
    if isinstance(payload, list):
        return payload  # type: ignore[return-value]
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("good", "items", "list"):
            if isinstance(data.get(key), list):
                return data[key]
    if isinstance(payload.get("items"), list):
        return payload["items"]
    return []


def discover_user_id(domain: str, api_key: str, logger: logging.Logger) -> str:
    url = (f"https://{domain}/rest/api/User?filter="
           "[{\"property\":\"is_limited\",\"value\":0,\"operator\":\"=\"}]")
    payload = http_get_json(url, api_key, logger)
    users = (payload.get("data") or {}).get("user") or payload.get("items") or []
    if not isinstance(users, list) or not users:
        raise RuntimeError("Не удалось получить список пользователей для autodetect user_id")
    # берём активного, если есть
    for u in users:
        if str(u.get("is_active", "1")) == "1":
            return str(u.get("id"))
    return str(users[0].get("id"))


def discover_first_clinic(domain: str, api_key: str, user_id: str, logger: logging.Logger) -> str:
    url = f"https://{domain}/rest/api/user/allowedClinicsByUserId?user_id={user_id}"
    payload = http_get_json(url, api_key, logger)
    clinics = (payload.get("data") or {}).get("clinics") or payload.get("items") or []
    if not isinstance(clinics, list) or not clinics:
        raise RuntimeError("Не удалось получить список клиник")
    return str(clinics[0].get("id"))


def fetch_first_goods(domain: str, api_key: str, logger: logging.Logger, max_goods: int) -> List[Dict[str, Any]]:
    """Берём первые max_goods товаров из /rest/api/Good (как есть, без фильтров)."""
    base = f"https://{domain}/rest/api/Good"
    out: List[Dict[str, Any]] = []
    limit = 200
    offset = 0
    while len(out) < max_goods:
        url = f"{base}?limit={limit}&offset={offset}"
        payload = http_get_json(url, api_key, logger)
        items = _extract_list(payload)
        if not items:
            break
        take = items[: max_goods - len(out)]
        out.extend(take)
        if len(items) < limit:
            break
        offset += limit
    logger.info(f"Загружено товаров для опроса остатков: {len(out)} (запрошено {max_goods})")
    return out


def extract_store_id(row: Dict[str, Any]) -> Optional[str]:
    sid = row.get("store_id", None)
    if sid is None and isinstance(row.get("store"), dict):
        sid = row["store"].get("id", None)
    if sid is None:
        return None
    return str(sid)


def extract_store_title(row: Dict[str, Any]) -> str:
    if isinstance(row.get("store"), dict):
        title = row["store"].get("title") or row["store"].get("name") or ""
        if title:
            return str(title)
    # иногда заголовок кладут прямо в строку
    for k in ("store_title", "storeName", "title", "name"):
        if row.get(k):
            return str(row[k])
    return ""


def collect_stores_from_balances(domain: str, api_key: str, clinic_id: str, user_id: str,
                                 goods: List[Dict[str, Any]], logger: logging.Logger) -> Dict[str, str]:
    """
    Для каждого товара делаем GET /Good/StockBalancesForProduct?clinic_id=&good_id=&user_id=
    и собираем пары store_id -> title.
    """
    base = f"https://{domain}/rest/api/Good/StockBalancesForProduct"
    stores: Dict[str, str] = {}
    for idx, g in enumerate(goods, start=1):
        gid = g.get("id")
        if gid is None:
            continue
        url = f"{base}?clinic_id={clinic_id}&good_id={gid}&user_id={user_id}"
        try:
            payload = http_get_json(url, api_key, logger)
        except Exception as ex:
            logger.warning(f"Пропуск gid={gid}: {ex}")
            continue

        balances = (payload.get("data") or {}).get("stock_balances") or []
        if not isinstance(balances, list):
            balances = []

        for row in balances:
            sid = extract_store_id(row)
            if not sid:
                continue
            title = extract_store_title(row)
            if sid not in stores:
                stores[sid] = title
            else:
                # если названия различаются — залогируем
                if title and title != stores[sid]:
                    logger.warning(f"store_id={sid}: разные названия '{stores[sid]}' vs '{title}'")

        if idx % LOG_PROGRESS_EVERY == 0 or idx == len(goods):
            logger.info(f"[progress] обработано товаров: {idx}/{len(goods)}; найдено складов: {len(stores)}")

    return stores


def main():
    parser = argparse.ArgumentParser(description="Собрать список складов через StockBalancesForProduct")
    parser.add_argument("--domain", required=True, help="например: vetdom.vetmanager.cloud")
    parser.add_argument("--api-key", required=True, help="X-REST-API-KEY")
    parser.add_argument("--max-goods", type=int, default=300, help="сколько первых товаров опросить (по умолчанию 300)")
    args = parser.parse_args()

    logger = setup_logger()
    logger.info(f"Старт. Домен={args.domain}, ключ={args.api_key[:4]}***, max_goods={args.max_goods}")

    try:
        user_id = discover_user_id(args.domain, args.api_key, logger)
        clinic_id = discover_first_clinic(args.domain, args.api_key, user_id, logger)
        logger.info(f"Контекст: user_id={user_id}, clinic_id={clinic_id}")
    except Exception as ex:
        logger.error(f"Не удалось определить user/clinic: {ex}")
        sys.exit(1)

    goods = fetch_first_goods(args.domain, args.api_key, logger, max_goods=args.max_goods)
    if not goods:
        logger.error("Товары не получены — прерывание.")
        sys.exit(2)

    stores = collect_stores_from_balances(args.domain, args.api_key, clinic_id, user_id, goods, logger)
    if not stores:
        logger.warning("Не удалось обнаружить склады из остатков. Увеличьте --max-goods и повторите.")
        sys.exit(3)

    # Вывод в консоль: <id>\t<title>
    print("\nstore_id\ttitle")
    for sid in sorted(stores, key=lambda x: (int(x) if str(x).isdigit() else str(x))):
        print(f"{sid}\t{stores[sid]}")

    logger.info(f"Готово. Найдено складов: {len(stores)}")


if __name__ == "__main__":
    main()
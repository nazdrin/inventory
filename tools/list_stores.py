#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тестовый локальный скрипт для получения всех складов (Store) из Vetmanager и вывода:
<store_id>\t<title>

Логика:
1) Пытаемся /rest/api/Store (fallback: /rest/api/store), парсим data.store[]/items[]/top-level list.
2) Если не нашли, используем справочники:
   - GET /rest/api/ComboManualName, ищем запись, в которой title содержит "Склад"
   - Если в ответе уже есть comboManualItems — берём их
   - Иначе делаем GET /rest/api/ComboManualItem?filter=[{"property":"combo_manual_id","value":<id>,"operator":"="}]
     и печатаем value (как "код") и title (как название).
Заголовки: X-REST-API-KEY и X-REST-TIME-ZONE: Europe/Kiev
"""

import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("list_stores")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(h)
    return logger


def headers(api_key: str) -> Dict[str, str]:
    return {
        "X-REST-API-KEY": api_key,
        "Content-Type": "application/json",
        "X-REST-TIME-ZONE": "Europe/Kiev",  # важно: 'Kiev', а не 'Kyiv'
    }


def http_get_json(url: str, api_key: str, logger: logging.Logger) -> Dict[str, Any]:
    logger.info(f"HTTP GET → {url}")
    resp = requests.get(url, headers=headers(api_key), timeout=30)
    logger.info(f"HTTP {resp.status_code} ← {url}")
    try:
        data = resp.json()
    except ValueError:
        data = {}
    if not resp.ok:
        # покажем первые 400 символов ответа для диагностики
        body = resp.text[:400].replace("\n", "\\n")
        logger.error(f"HTTP error {resp.status_code}: {body}")
        resp.raise_for_status()
    return data if isinstance(data, dict) else {"data": data}


def _extract_list(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Универсальный извлекатель списка сущностей из разных форматов:
      {"data": {"store": [...]}}
      {"data": {"items": [...]}}
      {"items": [...]}
      {"data": [...]}
      [...]
    """
    if isinstance(payload, list):
        return payload  # type: ignore[return-value]
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("store", "stores", "items", "list"):
            if isinstance(data.get(key), list):
                return data[key]
    if isinstance(payload.get("items"), list):
        return payload["items"]
    return []


def try_list_stores_by_model(domain: str, api_key: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    for path in ("/rest/api/Store", "/rest/api/store"):
        try:
            url = f"https://{domain}{path}?limit=500&offset=0"
            payload = http_get_json(url, api_key, logger)
            items = _extract_list(payload)
            if items:
                logger.info(f"Найдено складов через {path}: {len(items)}")
                return items
            logger.warning(f"{path} вернул пустой список")
        except requests.RequestException:
            logger.warning(f"{path} недоступен, пробуем следующий вариант")
    return []


def try_list_stores_via_combo_manual(domain: str, api_key: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Резервный путь: берём справочник 'Склад' из ComboManualName/ComboManualItem.
    Возвращаем список словарей с полями id/value и title.
    """
    url = f"https://{domain}/rest/api/ComboManualName"
    payload = http_get_json(url, api_key, logger)
    names = _extract_list(payload)
    cm = None
    for n in names:
        title = (n.get("title") or "").lower()
        if "склад" in title:
            cm = n
            break
    if not cm:
        logger.warning("Не нашли справочник с названием, содержащим 'Склад' в ComboManualName")
        return []

    # Если в ComboManualName уже есть comboManualItems — используем их
    if isinstance(cm.get("comboManualItems"), list) and cm["comboManualItems"]:
        items = cm["comboManualItems"]
        logger.info(f"Нашли comboManualItems для 'Склад': {len(items)}")
        # нормализуем к виду {id, title}
        norm = []
        for it in items:
            norm.append({
                "id": it.get("value") or it.get("id"),
                "title": it.get("title"),
            })
        return norm

    # Иначе запрашиваем ComboManualItem по combo_manual_id
    cm_id = cm.get("id")
    if not cm_id:
        logger.warning("combo_manual_id не найден для 'Склад'")
        return []

    flt = json.dumps([{"property": "combo_manual_id", "value": str(cm_id), "operator": "="}], ensure_ascii=False)
    url_items = f"https://{domain}/rest/api/ComboManualItem?filter={flt}"
    payload_items = http_get_json(url_items, api_key, logger)
    items = _extract_list(payload_items)
    logger.info(f"Получили ComboManualItem по combo_manual_id={cm_id}: {len(items)}")
    norm = []
    for it in items:
        norm.append({
            "id": it.get("value") or it.get("id"),
            "title": it.get("title"),
        })
    return norm


def print_stores(stores: List[Dict[str, Any]], logger: logging.Logger) -> None:
    if not stores:
        logger.warning("Список складов пуст.")
        return

    logger.info("Склады:")
    for s in stores:
        # возможные поля: id, title, status, clinic_id (в модели Store), а также 'value' если через справочник
        sid = s.get("id") or s.get("value")
        title = s.get("title") or s.get("name") or ""
        print(f"{sid}\t{title}")


def main():
    parser = argparse.ArgumentParser(description="Вывести список складов Vetmanager: <id>\\t<title>")
    parser.add_argument("--domain", required=True, help="домен без схемы (например: vetdom.vetmanager.cloud)")
    parser.add_argument("--api-key", required=True, help="X-REST-API-KEY")
    args = parser.parse_args()

    logger = setup_logger()
    logger.info(f"Старт. Домен={args.domain}, ключ={args.api_key[:4]}***")

    # 1) основной путь — модель Store
    stores = try_list_stores_by_model(args.domain, args.api_key, logger)

    # 2) резерв — через справочник 'Склад'
    if not stores:
        logger.warning("Переходим к резервному пути через ComboManualName/ComboManualItem…")
        stores = try_list_stores_via_combo_manual(args.domain, args.api_key, logger)

    print_stores(stores, logger)
    logger.info("Готово.")


if __name__ == "__main__":
    main()
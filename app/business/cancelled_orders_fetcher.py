import asyncio
import base64
import logging
from typing import List, Dict

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

# используем готовые объекты проекта
from app.database import get_async_db, DeveloperSettings, EnterpriseSettings, MappingBranch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def _basic_auth(login: str, password: str) -> str:
    token = base64.b64encode(f"{login}:{password}".encode("utf-8")).decode("utf-8")
    return f"Basic {token}"

async def _fetch_cancelled_for_branch(
    client: httpx.AsyncClient,
    base_url: str,
    auth_header: str,
    branch_id: str,
    timeout: float = 20.0
) -> List[Dict]:
    url = f"{base_url}/api/Orders/cancelledOrdersByCustomer/{branch_id}"
    headers = {
        "Authorization": auth_header,
        "Accept": "application/json",
    }
    r = await client.get(url, headers=headers, timeout=timeout)
    if r.status_code == 204:
        return []
    if r.status_code != 200:
        raise RuntimeError(f"{url} -> {r.status_code}: {r.text[:300]}")
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Ожидался массив JSON, получено: {type(data)}")
    out = []
    for item in data:
        if not isinstance(item, dict):
            continue
        out.append({
            "id": str(item.get("id", "")).strip(),
            "branchID": str(item.get("branchID", "")).strip(),
            "cancelDateTime": str(item.get("cancelDateTime", "")).strip(),
            "cancelReason": str(item.get("cancelReason", "")).strip(),
        })
    return out

async def _get_base_url_from_devs(db: AsyncSession) -> str:
    dev: DeveloperSettings | None = (
        await db.execute(select(DeveloperSettings))
    ).scalar_one_or_none()
    if dev is None:
        raise ValueError("DeveloperSettings не найден. Укажите endpoint_reserve в настройках разработчика.")
    endpoint = getattr(dev, "endpoint_orders", None)
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ValueError("В DeveloperSettings отсутствует endpoint_orders. Заполните URL резерва (например, https://reserve.tabletki.ua).")
    return endpoint.strip().rstrip("/")

async def _get_creds_and_branches(db: AsyncSession, enterprise_code: str) -> tuple[str, str, List[str]]:
    # логин/пароль
    row = (
        await db.execute(
            select(EnterpriseSettings.tabletki_login, EnterpriseSettings.tabletki_password)
            .where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
    ).first()
    if not row or not row[0] or not row[1]:
        raise ValueError(f"Нет tabletki_login/tabletki_password для enterprise_code={enterprise_code} в EnterpriseSettings.")
    login, password = row[0], row[1]

    # филиалы
    rows = (
        await db.execute(
            select(MappingBranch.branch).where(MappingBranch.enterprise_code == enterprise_code)
        )
    ).all()
    branches = [str(r[0]) for r in rows if r and r[0]]
    if not branches:
        raise ValueError(f"Нет branch в MappingBranch для enterprise_code={enterprise_code}.")
    return login, password, branches

async def get_cancelled_orders(enterprise_code: str, verify_ssl: bool = True) -> List[Dict]:
    """
    Возвращает массив отмен:
    [
      {"id": "...", "branchID": "...", "cancelDateTime": "...", "cancelReason": "..."},
      ...
    ]
    """
    async with get_async_db() as db:
        assert isinstance(db, AsyncSession)
        base_url = await _get_base_url_from_devs(db)
        login, password, branches = await _get_creds_and_branches(db, enterprise_code)

    auth_header = _basic_auth(login, password)

    result: List[Dict] = []
    limits = httpx.Limits(max_connections=10)
    async with httpx.AsyncClient(verify=verify_ssl, limits=limits) as client:
        tasks = [
            _fetch_cancelled_for_branch(client, base_url, auth_header, b)
            for b in branches
        ]
        responses = await asyncio.gather(*tasks, return_exceptions=True)

    for b, data in zip(branches, responses):
        if isinstance(data, Exception):
            logging.error("Ошибка по branch %s: %s", b, data)
            continue
        result.extend(data)

    return result

async def acknowledge_cancelled_orders(
    enterprise_code: str,
    request_ids: List[str],
    verify_ssl: bool = True
) -> Dict:
    """
    Передать на API список обработанных отказов, чтобы следующие GET-запросы не возвращали старые данные.
    Вызывает POST /api/Orders/acceptedCancelledOrdersByCustomer

    :param enterprise_code: код предприятия для получения логина/пароля
    :param request_ids: список UUID (строк) отмен для подтверждения обработки
    :param verify_ssl: проверять SSL-сертификат
    :return: словарь с результатом {"ok": bool, "status": int, "response": Any}
    """
    # Быстрый выход, если список пустой или состоит из пустых строк
    if not request_ids:
        return {"ok": True, "status": 204, "response": []}

    payload = [str(x).strip() for x in request_ids if str(x).strip()]
    if not payload:
        return {"ok": True, "status": 204, "response": []}

    # Берем base_url из DeveloperSettings и креды из EnterpriseSettings
    async with get_async_db() as db:
        assert isinstance(db, AsyncSession)
        base_url = await _get_base_url_from_devs(db)
        login, password, _ = await _get_creds_and_branches(db, enterprise_code)

    auth_header = _basic_auth(login, password)
    url = f"{base_url}/api/Orders/acceptedCancelledOrdersByCustomer"
    headers = {
        "Authorization": auth_header,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(verify=verify_ssl) as client:
        r = await client.post(url, headers=headers, json=payload, timeout=30.0)

    ok = 200 <= r.status_code < 300

    # Пытаемся вернуть тело ответа, если оно есть (иначе вернем текст или None)
    response_body = None
    try:
        if r.text:
            response_body = r.json()
    except Exception:
        response_body = r.text[:500]

    if not ok:
        raise RuntimeError(f"POST {url} -> {r.status_code}: {r.text[:500]}")

    return {"ok": ok, "status": r.status_code, "response": response_body}

# --- CLI для локального запуска ---
def _parse_cli():
    import argparse
    p = argparse.ArgumentParser(description="Fetch 'cancelledOrdersByCustomer' by enterprise_code")
    p.add_argument("--enterprise-code", required=True, help="enterprise_code из EnterpriseSettings/MappingBranch")
    p.add_argument("--no-ssl-verify", action="store_true", help="Отключить проверку SSL при необходимости")
    return p.parse_args()

if __name__ == "__main__":
    args = _parse_cli()
    data = asyncio.run(get_cancelled_orders(enterprise_code=args.enterprise_code, verify_ssl=not args.no_ssl_verify))
    import json
    print(json.dumps(data, ensure_ascii=False, indent=2))
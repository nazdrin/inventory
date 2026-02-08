import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_async_db
from app.models import EnterpriseSettings

SALESDRIVE_BASE_URL = "https://petrenko.salesdrive.me"
NP_BASE_URL = "https://api.novaposhta.ua/v2.0/json/"
NEW_STATUS_ID = 1
TARGET_STATUS_ID = 21

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def _parse_salesdrive_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
    return None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning("Неверное значение %s=%r, используется default=%s", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.warning("Неверное значение %s=%r, используется default=%s", name, raw, default)
        return default


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw


def _seat_dimensions_cm(volume_m3: float) -> Tuple[int, int, int]:
    if volume_m3 <= 0:
        return 1, 1, 1
    side_m = volume_m3 ** (1 / 3)
    side_cm = max(1, int(round(side_m * 100)))
    return side_cm, side_cm, side_cm


def _buyout_ok(client_rating: Any) -> bool:
    if not client_rating or not isinstance(client_rating, dict):
        return True  # если нет clientRating — считаем OK
    value = client_rating.get("buyoutPercent", None)
    if value is None:
        return True
    try:
        num = float(value)
        if num == 0:
            return True
        return num > 74
    except (TypeError, ValueError):
        return False


async def _get_salesdrive_api_key(db: AsyncSession, enterprise_code: str) -> str:
    q = (
        select(EnterpriseSettings.token)
        .where(EnterpriseSettings.enterprise_code == str(enterprise_code))
        .limit(1)
    )
    res = await db.execute(q)
    token = res.scalar_one_or_none()
    if not token:
        raise ValueError(
            f"Не найден token в EnterpriseSettings для enterprise_code={enterprise_code}"
        )
    return token


async def _fetch_new_orders(api_key: str, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    url = f"{SALESDRIVE_BASE_URL.rstrip('/')}/api/order/list/"
    params = {"filter[statusId]": NEW_STATUS_ID}
    headers = {
        "Accept": "application/json",
        "X-Api-Key": api_key,
    }
    resp = await client.get(url, params=params, headers=headers, timeout=30.0)
    if resp.status_code != 200:
        raise RuntimeError(f"GET {url} -> {resp.status_code}: {resp.text[:500]}")
    data = resp.json()
    items = data.get("data", [])
    if not isinstance(items, list):
        raise RuntimeError("Ожидался массив data в ответе SalesDrive")
    return items


async def _update_status(
    api_key: str,
    order_id: Any,
    ttn_number: str,
    client: httpx.AsyncClient,
) -> None:
    url = f"{SALESDRIVE_BASE_URL.rstrip('/')}/api/order/update/"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }
    payload = {
        "id": order_id,
        "data": {
            "statusId": TARGET_STATUS_ID,
            "novaposhta": {"ttn": ttn_number},
        },
    }
    resp = await client.post(url, headers=headers, json=payload, timeout=30.0)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"POST {url} -> {resp.status_code}: {resp.text[:500]}")


def _compute_time_window_minutes(min_age_minutes: Optional[int], now_kyiv: datetime) -> Tuple[int, str]:
    if min_age_minutes is not None:
        return min_age_minutes, "override"
    default_minutes = _env_int("BIOTUS_TIME_DEFAULT_MINUTES", 30)
    switch_hour = _env_int("BIOTUS_TIME_SWITCH_HOUR", 12)
    after_switch_minutes = _env_int("BIOTUS_TIME_AFTER_SWITCH_MINUTES", 15)
    if now_kyiv.hour >= switch_hour:
        return after_switch_minutes, "after_switch"
    return default_minutes, "default"


def _to_kyiv(dt: datetime, tz: ZoneInfo) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def _extract_contact(order: Dict[str, Any]) -> Tuple[str, str, str]:
    contacts = order.get("contacts") or []
    contact = None
    if isinstance(contacts, list) and contacts:
        contact = contacts[0] or {}
    if not contact:
        contact = order.get("primaryContact") or {}

    phone = ""
    phone_raw = contact.get("phone")
    if isinstance(phone_raw, list) and phone_raw:
        phone = str(phone_raw[0] or "")
    elif isinstance(phone_raw, str):
        phone = phone_raw

    first_name = str(contact.get("fName") or "")
    last_name = str(contact.get("lName") or "")
    return phone, first_name, last_name


def _extract_delivery(order: Dict[str, Any]) -> Dict[str, Any]:
    raw = order.get("ord_delivery_data")
    if isinstance(raw, list) and raw:
        return raw[0] or {}
    if isinstance(raw, dict):
        return raw
    return {}


async def _np_call(
    client: httpx.AsyncClient,
    api_key: str,
    model_name: str,
    called_method: str,
    method_props: Dict[str, Any],
) -> Dict[str, Any]:
    payload = {
        "apiKey": api_key,
        "modelName": model_name,
        "calledMethod": called_method,
        "methodProperties": method_props,
    }
    resp = await client.post(NP_BASE_URL, json=payload, timeout=30.0)
    if resp.status_code != 200:
        raise RuntimeError(f"POST {NP_BASE_URL} -> {resp.status_code}: {resp.text[:500]}")
    return resp.json()


async def np_create_recipient(
    client: httpx.AsyncClient,
    api_key: str,
    first_name: str,
    last_name: str,
    phone: str,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    data = await _np_call(
        client,
        api_key,
        model_name="Counterparty",
        called_method="save",
        method_props={
            "CounterpartyType": "PrivatePerson",
            "CounterpartyProperty": "Recipient",
            "FirstName": first_name,
            "LastName": last_name,
            "Phone": phone,
        },
    )

    if not data.get("success") or data.get("errors"):
        return None, None, f"NP Counterparty/save error: {data.get('errors') or data}"

    items = data.get("data") or []
    if not items:
        return None, None, "NP Counterparty/save: пустой data"

    recipient_ref = items[0].get("Ref")
    contact_ref = None
    contact_block = items[0].get("ContactPerson") or {}
    contact_data = contact_block.get("data") or []
    if contact_data:
        contact_ref = contact_data[0].get("Ref")

    if recipient_ref and contact_ref:
        return recipient_ref, contact_ref, None

    if not recipient_ref:
        return None, None, "NP Counterparty/save: нет Ref получателя"

    data = await _np_call(
        client,
        api_key,
        model_name="Counterparty",
        called_method="getCounterpartyContactPersons",
        method_props={
            "Ref": recipient_ref,
            "Page": "1",
        },
    )
    if not data.get("success") or data.get("errors"):
        return recipient_ref, None, f"NP getCounterpartyContactPersons error: {data.get('errors') or data}"

    items = data.get("data") or []
    if not items:
        return recipient_ref, None, "NP getCounterpartyContactPersons: пустой data"

    contact_ref = items[0].get("Ref")
    if not contact_ref:
        return recipient_ref, None, "NP getCounterpartyContactPersons: нет Ref контакта"

    return recipient_ref, contact_ref, None


async def np_create_ttn(
    client: httpx.AsyncClient,
    api_key: str,
    recipient_ref: str,
    contact_recipient_ref: str,
    recipient_phone: str,
    city_recipient_ref: str,
    recipient_address_ref: str,
    service_type: str,
    has_postpay: bool,
    postpay_sum: Optional[float],
) -> Tuple[Optional[str], Optional[str]]:
    sender_ref = _env_str("NP_SENDER_REF", "2fbfd260-ba45-11f0-a1d5-48df37b921da")
    contact_sender_ref = _env_str("NP_CONTACT_SENDER_REF", "77739776-ba74-11f0-a1d5-48df37b921da")
    sender_address_ref = _env_str("NP_SENDER_ADDRESS_REF", "9e6adf20-8502-11e4-acce-0050568002cf")
    city_sender_ref = _env_str("NP_CITY_SENDER_REF", "8d5a980d-391c-11dd-90d9-001a92567626")
    sender_phone = _env_str("NP_SENDER_PHONE", "380672399124")

    seats = _env_int("NP_DEFAULT_SEATS", 1)
    weight = _env_float("NP_DEFAULT_WEIGHT_KG", 1.6)
    volume = _env_float("NP_DEFAULT_VOLUME_M3", 0.0076)
    description = _env_str("NP_DEFAULT_DESCRIPTION", "Дієтичні добавки, спортивне харчування та зоотовари")
    if seats <= 0:
        seats = 1

    method_props: Dict[str, Any] = {
        "PayerType": "Recipient",
        "PaymentMethod": "Cash",
        "CargoType": "Cargo",
        "VolumeGeneral": str(volume),
        "Weight": str(weight),
        "ServiceType": service_type or "WarehouseWarehouse",
        "SeatsAmount": str(seats),
        "Description": description,
        "CitySender": city_sender_ref,
        "Sender": sender_ref,
        "SenderAddress": sender_address_ref,
        "ContactSender": contact_sender_ref,
        "SendersPhone": sender_phone,
        "CityRecipient": city_recipient_ref,
        "Recipient": recipient_ref,
        "RecipientAddress": recipient_address_ref,
        "ContactRecipient": contact_recipient_ref,
        "RecipientsPhone": recipient_phone,
    }

    per_seat_weight = weight / seats
    per_seat_volume = volume / seats
    width_cm, length_cm, height_cm = _seat_dimensions_cm(per_seat_volume)
    method_props["OptionsSeat"] = [
        {
            "volumetricVolume": round(per_seat_volume, 6),
            "volumetricWidth": width_cm,
            "volumetricLength": length_cm,
            "volumetricHeight": height_cm,
            "weight": round(per_seat_weight, 3),
        }
        for _ in range(seats)
    ]

    if has_postpay and postpay_sum is not None:
        # Контроль оплаты (AfterpaymentOnGoodsCost) вместо наложенного платежа (BackwardDeliveryData)
        method_props["AfterpaymentOnGoodsCost"] = str(postpay_sum)

    data = await _np_call(
        client,
        api_key,
        model_name="InternetDocument",
        called_method="save",
        method_props=method_props,
    )

    if not data.get("success") or data.get("errors"):
        return None, f"NP InternetDocument/save error: {data.get('errors') or data}"

    items = data.get("data") or []
    if not items:
        return None, "NP InternetDocument/save: пустой data"

    ttn_number = items[0].get("IntDocNumber")
    if not ttn_number:
        return None, "NP InternetDocument/save: нет IntDocNumber"
    return ttn_number, None


async def process_biotus_orders(
    enterprise_code: str = "2547",
    min_age_minutes: Optional[int] = None,
    verify_ssl: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    async with get_async_db() as db:
        assert isinstance(db, AsyncSession)
        api_key = await _get_salesdrive_api_key(db, enterprise_code)

    tz_name = _env_str("BIOTUS_TZ", "Europe/Kyiv")
    kyiv_tz = ZoneInfo(tz_name)
    now_kyiv = datetime.now(tz=kyiv_tz)
    window_minutes, window_source = _compute_time_window_minutes(min_age_minutes, now_kyiv)
    cutoff = now_kyiv - timedelta(minutes=window_minutes)

    updated = 0
    np_api_key = _env_str("NP_API_KEY", "")
    matched: List[Dict[str, Any]] = []
    debug_samples: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(verify=verify_ssl) as client:
        orders = await _fetch_new_orders(api_key, client)

        for order in orders:
            order_id = order.get("id")
            supplier = order.get("supplier")
            if supplier != "Biotus":
                if len(debug_samples) < 2:
                    debug_samples.append(
                        {
                            "id": order_id,
                            "reason": "supplier_not_biotus",
                            "supplier": supplier,
                            "createTime": order.get("createTime"),
                        }
                    )
                continue

            created_at = _parse_salesdrive_dt(order.get("orderTime"))
            if not created_at:
                if len(debug_samples) < 2:
                    debug_samples.append(
                        {
                            "id": order_id,
                            "reason": "missing_createTime",
                            "supplier": supplier,
                            "orderTime": order.get("orderTime"),
                        }
                    )
                continue
            created_at_kyiv = _to_kyiv(created_at, kyiv_tz)
            if created_at_kyiv > now_kyiv:
                if len(debug_samples) < 2:
                    debug_samples.append(
                        {
                            "id": order_id,
                            "reason": "created_in_future",
                            "supplier": supplier,
                            "orderTime": created_at_kyiv.strftime("%Y-%m-%d %H:%M:%S"),
                            "now_kyiv": now_kyiv.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
                continue
            if created_at_kyiv.date() == now_kyiv.date() and created_at_kyiv > cutoff:
                if len(debug_samples) < 2:
                    debug_samples.append(
                        {
                            "id": order_id,
                            "reason": "created_too_new_today",
                            "supplier": supplier,
                            "orderTime": created_at_kyiv.strftime("%Y-%m-%d %H:%M:%S"),
                            "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )
                continue

            primary_contact = order.get("primaryContact") or {}
            client_rating = primary_contact.get("clientRating")
            if not _buyout_ok(client_rating):
                if len(debug_samples) < 2:
                    debug_samples.append(
                        {
                            "id": order_id,
                            "reason": "buyout_not_ok",
                            "supplier": supplier,
                            "buyoutPercent": (
                                client_rating.get("buyoutPercent")
                                if isinstance(client_rating, dict)
                                else None
                            ),
                        }
                    )
                continue

            matched.append(order)

        if dry_run:
            logger.info(
                "DRY RUN: окно=%s минут (source=%s) cutoff=%s now_kyiv=%s",
                window_minutes,
                window_source,
                cutoff.strftime("%Y-%m-%d %H:%M:%S"),
                now_kyiv.strftime("%Y-%m-%d %H:%M:%S"),
            )

        if debug_samples:
            for sample in debug_samples[:2]:
                logger.info("DEBUG SAMPLE: %s", sample)

        for order in matched:
            order_id = order.get("id")
            if not order_id:
                logger.warning("Пропуск заказа без id: %s", order)
                continue

            if dry_run:
                logger.info(
                    "DRY RUN: создал бы ТТН и обновил бы статус заказа %s -> %s",
                    order_id,
                    TARGET_STATUS_ID,
                )
                updated += 1
                continue

            if not np_api_key:
                logger.error("NP_API_KEY не задан, пропуск заказа %s", order_id)
                continue

            phone, first_name, last_name = _extract_contact(order)
            delivery = _extract_delivery(order)
            city_ref = str(delivery.get("cityRef") or "")
            branch_ref = str(delivery.get("branchRef") or "")
            service_type = str(delivery.get("type") or "WarehouseWarehouse")
            has_postpay = str(delivery.get("hasPostpay") or "0") == "1"
            postpay_sum_raw = delivery.get("postpaySum")
            try:
                postpay_sum = float(postpay_sum_raw) if postpay_sum_raw is not None else None
            except (TypeError, ValueError):
                postpay_sum = None

            if not (phone and first_name and last_name and city_ref and branch_ref):
                logger.error(
                    "Недостаточно данных для ТТН заказа %s: phone=%r first=%r last=%r cityRef=%r branchRef=%r",
                    order_id,
                    phone,
                    first_name,
                    last_name,
                    city_ref,
                    branch_ref,
                )
                continue

            recipient_ref, contact_recipient_ref, err = await np_create_recipient(
                client,
                np_api_key,
                first_name=first_name,
                last_name=last_name,
                phone=phone,
            )
            if err or not recipient_ref or not contact_recipient_ref:
                logger.error("Не удалось создать получателя для заказа %s: %s", order_id, err)
                continue

            ttn_number, err = await np_create_ttn(
                client,
                np_api_key,
                recipient_ref=recipient_ref,
                contact_recipient_ref=contact_recipient_ref,
                recipient_phone=phone,
                city_recipient_ref=city_ref,
                recipient_address_ref=branch_ref,
                service_type=service_type,
                has_postpay=has_postpay,
                postpay_sum=postpay_sum,
            )
            if err or not ttn_number:
                logger.error("Не удалось создать ТТН для заказа %s: %s", order_id, err)
                continue

            await _update_status(api_key, order_id, ttn_number, client=client)
            updated += 1
            logger.info("Статус заказа %s обновлен -> %s, ТТН=%s", order_id, TARGET_STATUS_ID, ttn_number)

    return {
        "total": len(orders),
        "matched": len(matched),
        "updated": updated,
        "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "window_minutes": window_minutes,
        "window_source": window_source,
        "tz": tz_name,
    }


def _parse_cli():
    import argparse
    p = argparse.ArgumentParser(description="Check Biotus orders in SalesDrive and update status")
    p.add_argument("--enterprise-code", default="2547", help="enterprise_code для token в EnterpriseSettings")
    p.add_argument(
        "--min-age-minutes",
        type=int,
        default=None,
        help="Override для окна отбора (минуты от текущего времени)",
    )
    p.add_argument("--dry-run", action="store_true", help="Только показать, что бы обновили")
    p.add_argument("--no-ssl-verify", action="store_true", help="Отключить проверку SSL")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_cli()
    result = asyncio.run(
        process_biotus_orders(
            enterprise_code=args.enterprise_code,
            min_age_minutes=args.min_age_minutes,
            verify_ssl=not args.no_ssl_verify,
            dry_run=args.dry_run,
        )
    )
    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))

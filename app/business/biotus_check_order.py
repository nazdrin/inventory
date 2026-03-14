import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_async_db
from app.models import EnterpriseSettings
from app.services.order_sender import send_orders_to_tabletki

SALESDRIVE_BASE_URL = "https://petrenko.salesdrive.me"
NP_BASE_URL = "https://api.novaposhta.ua/v2.0/json/"
NEW_STATUS_ID = 1
TARGET_STATUS_ID = 21
DEFAULT_DUPLICATE_STATUS_ID = 20
DEFAULT_FALLBACK_ADDITIONAL_STATUS_IDS = [9, 19, 18, 20]

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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int_list(name: str, default: List[int]) -> List[int]:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default

    parts = [item.strip() for item in str(raw).replace(";", ",").split(",")]
    parsed: List[int] = []
    for part in parts:
        if not part:
            continue
        try:
            parsed.append(int(part))
        except (TypeError, ValueError):
            logger.warning("Неверное значение в %s: %r (пропущено)", name, part)

    if parsed:
        return parsed

    logger.warning("Неверное значение %s=%r, используется default=%s", name, raw, default)
    return default


def _parse_csv_items(value: str) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_allowed_supplier_ids(suppliers: Optional[str]) -> List[int]:
    raw = suppliers
    if raw is None:
        raw = _env_str("ALLOWED_SUPPLIERS", "38;41")
    parts = str(raw).replace(",", ";").split(";")
    parsed: List[int] = []
    for part in parts:
        item = part.strip()
        if not item:
            continue
        try:
            parsed.append(int(item))
        except (TypeError, ValueError):
            continue
    if parsed:
        return parsed
    logger.warning(
        "ALLOWED_SUPPLIERS пустой после разбора (%r), используется default=%s",
        raw,
        "38;41",
    )
    return [38, 41]


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
    return await _fetch_orders_by_status(api_key=api_key, status_id=NEW_STATUS_ID, client=client)


async def _fetch_orders_by_status(
    api_key: str,
    status_id: int,
    client: httpx.AsyncClient,
) -> List[Dict[str, Any]]:
    url = f"{SALESDRIVE_BASE_URL.rstrip('/')}/api/order/list/"
    params = {"filter[statusId]": status_id}
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
    status_id: int,
    ttn_number: Optional[str],
    client: httpx.AsyncClient,
) -> None:
    url = f"{SALESDRIVE_BASE_URL.rstrip('/')}/api/order/update/"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }
    data: Dict[str, Any] = {"statusId": status_id}
    if ttn_number:
        data["novaposhta"] = {"ttn": ttn_number}
    payload = {"id": order_id, "data": data}
    resp = await client.post(url, headers=headers, json=payload, timeout=30.0)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"POST {url} -> {resp.status_code}: {resp.text[:500]}")


async def _update_obrabotano_only(
    api_key: str,
    order_id: Any,
    value: int,
    client: httpx.AsyncClient,
) -> None:
    """
    Обновляет только поле obrabotano в SalesDrive, без смены statusId.
    """
    url = f"{SALESDRIVE_BASE_URL.rstrip('/')}/api/order/update/"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }
    payload = {
        "id": order_id,
        "data": {
            "obrabotano": value,
        },
    }
    resp = await client.post(url, headers=headers, json=payload, timeout=30.0)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"POST {url} -> {resp.status_code}: {resp.text[:500]}")


def _compute_time_window_minutes(min_age_minutes: Optional[int], now_kyiv: datetime) -> Tuple[int, str]:
    if min_age_minutes is not None:
        logger.info("Orders time window mode=override min_age_minutes=%s", min_age_minutes)
        return min_age_minutes, "override"
    default_minutes = _env_int("BIOTUS_TIME_DEFAULT_MINUTES", 30)
    switch_hour = _env_int("BIOTUS_TIME_SWITCH_HOUR", 12)
    end_hour = _env_int("BIOTUS_TIME_SWITCH_END_HOUR", 13)
    after_switch_minutes = _env_int("BIOTUS_TIME_AFTER_SWITCH_MINUTES", 15)
    if end_hour <= switch_hour:
        logger.warning(
            "Неверная конфигурация BIOTUS_TIME_SWITCH_END_HOUR=%s <= BIOTUS_TIME_SWITCH_HOUR=%s; "
            "используется fallback: after_switch до конца дня",
            end_hour,
            switch_hour,
        )
        if now_kyiv.hour >= switch_hour:
            logger.info(
                "Orders time window mode=after_switch reason=fallback_invalid_end_hour now_hour=%s switch_hour=%s",
                now_kyiv.hour,
                switch_hour,
            )
            return after_switch_minutes, "after_switch"
        logger.info(
            "Orders time window mode=default reason=fallback_invalid_end_hour now_hour=%s switch_hour=%s",
            now_kyiv.hour,
            switch_hour,
        )
        return default_minutes, "default"
    if switch_hour <= now_kyiv.hour < end_hour:
        logger.info(
            "Orders time window mode=after_switch_window reason=within_window now_hour=%s window=%s-%s",
            now_kyiv.hour,
            switch_hour,
            end_hour,
        )
        return after_switch_minutes, "after_switch_window"
    logger.info(
        "Orders time window mode=default reason=outside_window now_hour=%s window=%s-%s",
        now_kyiv.hour,
        switch_hour,
        end_hour,
    )
    return default_minutes, "default"


def _to_kyiv(dt: datetime, tz: ZoneInfo) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def _parse_supplier_id(order: Dict[str, Any]) -> Optional[int]:
    supplier_id_raw = order.get("supplierlist")
    if isinstance(supplier_id_raw, int):
        return supplier_id_raw
    if isinstance(supplier_id_raw, str):
        try:
            return int(supplier_id_raw.strip())
        except ValueError:
            return None
    return None


def _is_obrabotano_marked(order: Dict[str, Any]) -> bool:
    value = order.get("obrabotano")
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            return int(value) == 1
        except (TypeError, ValueError):
            return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "yes", "y", "on"}
    return False


def _to_qty_ship(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _to_price_ship(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _build_tabletki_order_payload(order: Dict[str, Any]) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    products = order.get("products") or []
    if isinstance(products, list):
        for p in products:
            if not isinstance(p, dict):
                continue
            goods_code = str(
                p.get("parameter")
                or p.get("productId")
                or p.get("id")
                or ""
            ).strip()
            qty_ship = _to_qty_ship(p.get("amount"))
            if not goods_code or qty_ship <= 0:
                continue
            rows.append(
                {
                    "goodsCode": goods_code,
                    "goodsName": str(p.get("name") or ""),
                    "goodsProducer": "",
                    "qtyShip": qty_ship,
                    "priceShip": _to_price_ship(p.get("price")),
                }
            )

    order_id = str(order.get("externalId") or order.get("id") or "").strip()
    branch_id = str(order.get("branch") or order.get("utmSource") or "").strip()
    return {
        "id": order_id,
        "statusID": 4,
        "branchID": branch_id,
        "rows": rows,
    }


def _classify_for_main_flow(
    order: Dict[str, Any],
    allowed_supplier_ids_set: set[int],
    now_kyiv: datetime,
    cutoff: datetime,
    kyiv_tz: ZoneInfo,
) -> Tuple[bool, str]:
    supplier_id = _parse_supplier_id(order)
    if supplier_id is None:
        return False, "supplier_missing"
    if supplier_id not in allowed_supplier_ids_set:
        return False, "supplier_not_allowed"

    created_at = _parse_salesdrive_dt(order.get("orderTime"))
    if not created_at:
        return False, "missing_orderTime"

    created_at_kyiv = _to_kyiv(created_at, kyiv_tz)
    if created_at_kyiv > now_kyiv:
        return False, "created_in_future"
    if created_at_kyiv.date() == now_kyiv.date() and created_at_kyiv > cutoff:
        return False, "too_new_for_main"

    primary_contact = order.get("primaryContact") or {}
    client_rating = primary_contact.get("clientRating")
    if not _buyout_ok(client_rating):
        return False, "buyout_not_ok"

    return True, "matched_for_main"


def _eligible_for_fallback_by_timeout(
    order: Dict[str, Any],
    now_kyiv: datetime,
    kyiv_tz: ZoneInfo,
    timeout_minutes: int,
) -> Tuple[bool, str]:
    if timeout_minutes <= 0:
        return True, "timeout_disabled"
    created_at = _parse_salesdrive_dt(order.get("orderTime"))
    if not created_at:
        return False, "missing_orderTime_for_fallback"
    created_at_kyiv = _to_kyiv(created_at, kyiv_tz)
    if created_at_kyiv > now_kyiv:
        return False, "created_in_future_for_fallback"
    age_minutes = (now_kyiv - created_at_kyiv).total_seconds() / 60.0
    if age_minutes < timeout_minutes:
        return False, "too_new_for_fallback"
    return True, "eligible_by_timeout"


async def _get_tabletki_credentials_by_enterprise(enterprise_code: str) -> Tuple[str, str]:
    async with get_async_db() as db:
        assert isinstance(db, AsyncSession)
        q = (
            select(EnterpriseSettings.tabletki_login, EnterpriseSettings.tabletki_password)
            .where(EnterpriseSettings.enterprise_code == str(enterprise_code))
            .limit(1)
        )
        row = (await db.execute(q)).first()
        if not row:
            return "", ""
        return str(row[0] or ""), str(row[1] or "")


async def _process_fallback_orders_batch(
    *,
    source: str,
    orders: List[Dict[str, Any]],
    api_key: str,
    client: httpx.AsyncClient,
    now_kyiv: datetime,
    kyiv_tz: ZoneInfo,
    timeout_minutes: int,
    dry_run: bool,
    tabletki_login: str,
    tabletki_password: str,
    tabletki_cancel_reason_default: int,
) -> Dict[str, int]:
    counters = {
        "total_candidates": 0,
        "eligible_timeout": 0,
        "skipped_already_processed": 0,
        "skipped_too_new": 0,
        "sent_tabletki": 0,
        "obrabotano_updated": 0,
        "errors": 0,
    }

    for order in orders:
        order_id = order.get("id")
        if not order_id:
            continue
        counters["total_candidates"] += 1

        if _is_obrabotano_marked(order):
            counters["skipped_already_processed"] += 1
            continue

        eligible, timeout_reason = _eligible_for_fallback_by_timeout(
            order=order,
            now_kyiv=now_kyiv,
            kyiv_tz=kyiv_tz,
            timeout_minutes=timeout_minutes,
        )
        if not eligible:
            if timeout_reason in {
                "too_new_for_fallback",
                "missing_orderTime_for_fallback",
                "created_in_future_for_fallback",
            }:
                counters["skipped_too_new"] += 1
            continue
        counters["eligible_timeout"] += 1

        tabletki_order = _build_tabletki_order_payload(order)
        if not tabletki_order.get("id"):
            counters["errors"] += 1
            logger.warning("Fallback[%s] skip order without externalId/id: order_id=%s", source, order_id)
            continue
        if not tabletki_order.get("rows"):
            counters["errors"] += 1
            logger.warning("Fallback[%s] skip order without valid rows: order_id=%s", source, order_id)
            continue

        if dry_run:
            logger.info(
                "DRY RUN fallback[%s]: would send to Tabletki and set SalesDrive obrabotano=1 for order=%s",
                source,
                order_id,
            )
            counters["sent_tabletki"] += 1
            continue

        try:
            async with get_async_db() as db:
                assert isinstance(db, AsyncSession)
                await send_orders_to_tabletki(
                    session=db,
                    orders=[tabletki_order],
                    tabletki_login=tabletki_login,
                    tabletki_password=tabletki_password,
                    cancel_reason=tabletki_cancel_reason_default,
                )
            counters["sent_tabletki"] += 1
        except Exception as exc:
            counters["errors"] += 1
            logger.exception("Fallback[%s] send to Tabletki failed for order=%s: %s", source, order_id, exc)
            continue

        try:
            await _update_obrabotano_only(
                api_key=api_key,
                order_id=order_id,
                value=1,
                client=client,
            )
            counters["obrabotano_updated"] += 1
        except Exception as exc:
            counters["errors"] += 1
            logger.exception("Fallback[%s] obrabotano update failed for order=%s: %s", source, order_id, exc)

    logger.info(
        "Fallback batch[%s]: candidates=%s eligible_timeout=%s sent=%s obrabotano_updated=%s "
        "skipped_already_processed=%s skipped_too_new=%s errors=%s",
        source,
        counters["total_candidates"],
        counters["eligible_timeout"],
        counters["sent_tabletki"],
        counters["obrabotano_updated"],
        counters["skipped_already_processed"],
        counters["skipped_too_new"],
        counters["errors"],
    )
    return counters


def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    if not isinstance(phone, str):
        phone = str(phone)
    return re.sub(r"\D+", "", phone)


def _mask_phone(phone: str) -> str:
    if not phone:
        return "***"
    return "***" + phone[-4:]


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
    enterprise_code: Optional[str] = None,
    min_age_minutes: Optional[int] = None,
    verify_ssl: bool = True,
    dry_run: bool = False,
    suppliers: Optional[str] = None,
) -> Dict[str, Any]:
    if not enterprise_code:
        enterprise_code = _env_str("BIOTUS_ENTERPRISE_CODE", "2547")
    async with get_async_db() as db:
        assert isinstance(db, AsyncSession)
        api_key = await _get_salesdrive_api_key(db, enterprise_code)

    tz_name = _env_str("BIOTUS_TZ", "Europe/Kyiv")
    kyiv_tz = ZoneInfo(tz_name)
    now_kyiv = datetime.now(tz=kyiv_tz)
    window_minutes, window_source = _compute_time_window_minutes(min_age_minutes, now_kyiv)
    cutoff = now_kyiv - timedelta(minutes=window_minutes)

    updated = 0
    skipped = 0
    duplicate_marked = 0
    main_processed = 0
    np_api_key = _env_str("NP_API_KEY", "")
    duplicate_status_id = _env_int("BIOTUS_DUPLICATE_STATUS_ID", DEFAULT_DUPLICATE_STATUS_ID)
    fallback_enabled = _env_bool("BIOTUS_ENABLE_UNHANDLED_FALLBACK", True)
    fallback_timeout_minutes = _env_int("BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES", 60)
    fallback_additional_status_ids = _env_int_list(
        "BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS",
        DEFAULT_FALLBACK_ADDITIONAL_STATUS_IDS,
    )
    tabletki_cancel_reason_default = _env_int("TABLETKI_CANCEL_REASON_DEFAULT", 18)
    allowed_supplier_ids = _resolve_allowed_supplier_ids(suppliers)
    allowed_supplier_ids_set = set(allowed_supplier_ids)
    matched: List[Dict[str, Any]] = []
    unhandled_by_main: List[Dict[str, Any]] = []
    debug_samples: List[Dict[str, Any]] = []
    supplier_seen_counts: Dict[int, int] = {}
    supplier_matched_counts: Dict[int, int] = {}
    unhandled_reason_counts: Dict[str, int] = {}

    fallback_total_candidates = 0
    fallback_eligible_timeout = 0
    fallback_skipped_already_processed = 0
    fallback_skipped_too_new = 0
    fallback_sent_tabletki = 0
    fallback_obrabotano_updated = 0
    fallback_errors = 0
    fallback_additional_total_orders = 0

    async with httpx.AsyncClient(verify=verify_ssl) as client:
        orders = await _fetch_new_orders(api_key, client)

        for order in orders:
            order_id = order.get("id")
            supplier_id_raw = order.get("supplierlist")
            supplier_id = _parse_supplier_id(order)

            if supplier_id is not None:
                supplier_seen_counts[supplier_id] = supplier_seen_counts.get(supplier_id, 0) + 1
            is_main_eligible, reason = _classify_for_main_flow(
                order=order,
                allowed_supplier_ids_set=allowed_supplier_ids_set,
                now_kyiv=now_kyiv,
                cutoff=cutoff,
                kyiv_tz=kyiv_tz,
            )
            if not is_main_eligible:
                unhandled_by_main.append({"order": order, "reason": reason})
                unhandled_reason_counts[reason] = unhandled_reason_counts.get(reason, 0) + 1
                if len(debug_samples) < 2:
                    debug_samples.append(
                        {
                            "id": order_id,
                            "reason": reason,
                            "supplierlist": supplier_id_raw,
                            "allowed_suppliers": allowed_supplier_ids,
                            "orderTime": order.get("orderTime"),
                        }
                    )
                continue

            matched.append(order)
            supplier_matched_counts[supplier_id] = supplier_matched_counts.get(supplier_id, 0) + 1

        if debug_samples:
            for sample in debug_samples[:2]:
                logger.info("DEBUG SAMPLE: %s", sample)

        phone_counts: Dict[str, int] = {}
        for order in matched:
            phone, _, _ = _extract_contact(order)
            normalized_phone = normalize_phone(phone)
            if not normalized_phone:
                continue
            phone_counts[normalized_phone] = phone_counts.get(normalized_phone, 0) + 1

        duplicate_phones = {phone for phone, count in phone_counts.items() if count > 1}
        duplicate_orders = sum(
            1
            for order in matched
            if normalize_phone(_extract_contact(order)[0]) in duplicate_phones
        )
        logger.info(
            "Duplicate phones found in batch: %s (orders affected: %s)",
            len(duplicate_phones),
            duplicate_orders,
        )

        if dry_run:
            logger.info(
                "DRY RUN: окно=%s минут (source=%s) cutoff=%s now_kyiv=%s",
                window_minutes,
                window_source,
                cutoff.strftime("%Y-%m-%d %H:%M:%S"),
                now_kyiv.strftime("%Y-%m-%d %H:%M:%S"),
            )
            logger.info(
                "DRY RUN supplier filter: allowed=%s seen=%s matched=%s",
                allowed_supplier_ids,
                supplier_seen_counts,
                supplier_matched_counts,
            )

        for order in matched:
            order_id = order.get("id")
            if not order_id:
                logger.warning("Пропуск заказа без id: %s", order)
                continue

            supplier_id_raw = order.get("supplierlist")
            supplier_id: Optional[int]
            if isinstance(supplier_id_raw, int):
                supplier_id = supplier_id_raw
            elif isinstance(supplier_id_raw, str):
                try:
                    supplier_id = int(supplier_id_raw.strip())
                except ValueError:
                    supplier_id = None
            else:
                supplier_id = None

            phone, _, _ = _extract_contact(order)
            normalized_phone = normalize_phone(phone)
            if normalized_phone in duplicate_phones:
                logger.info(
                    "Duplicate phone -> set status %s, skip TTN (order=%s, phone=%s)",
                    duplicate_status_id,
                    order_id,
                    _mask_phone(normalized_phone),
                )
                duplicate_marked += 1
                if dry_run:
                    continue
                await _update_status(
                    api_key,
                    order_id,
                    status_id=duplicate_status_id,
                    ttn_number=None,
                    client=client,
                )
                main_processed += 1
                continue

            if supplier_id == 40:
                if dry_run:
                    logger.info(
                        "DRY RUN: supplier=40, обновил бы статус заказа %s -> %s без ТТН",
                        order_id,
                        TARGET_STATUS_ID,
                    )
                    updated += 1
                    continue

                await _update_status(
                    api_key,
                    order_id,
                    status_id=TARGET_STATUS_ID,
                    ttn_number=None,
                    client=client,
                )
                updated += 1
                main_processed += 1
                logger.info(
                    "Заказ %s (supplier=40) обновлен -> %s без формирования ТТН",
                    order_id,
                    TARGET_STATUS_ID,
                )
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
                unhandled_by_main.append({"order": order, "reason": "main_np_api_key_missing"})
                unhandled_reason_counts["main_np_api_key_missing"] = (
                    unhandled_reason_counts.get("main_np_api_key_missing", 0) + 1
                )
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
                unhandled_by_main.append({"order": order, "reason": "main_missing_ttn_data"})
                unhandled_reason_counts["main_missing_ttn_data"] = (
                    unhandled_reason_counts.get("main_missing_ttn_data", 0) + 1
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
                unhandled_by_main.append({"order": order, "reason": "main_np_recipient_error"})
                unhandled_reason_counts["main_np_recipient_error"] = (
                    unhandled_reason_counts.get("main_np_recipient_error", 0) + 1
                )
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
                unhandled_by_main.append({"order": order, "reason": "main_np_ttn_error"})
                unhandled_reason_counts["main_np_ttn_error"] = (
                    unhandled_reason_counts.get("main_np_ttn_error", 0) + 1
                )
                continue

            await _update_status(
                api_key,
                order_id,
                status_id=TARGET_STATUS_ID,
                ttn_number=ttn_number,
                client=client,
            )
            updated += 1
            main_processed += 1
            logger.info("Статус заказа %s обновлен -> %s, ТТН=%s", order_id, TARGET_STATUS_ID, ttn_number)

        if fallback_enabled:
            tabletki_login, tabletki_password = await _get_tabletki_credentials_by_enterprise(str(enterprise_code))
            if not tabletki_login or not tabletki_password:
                logger.warning(
                    "Fallback disabled in run due to missing tabletki_login/password for enterprise=%s",
                    enterprise_code,
                )
            else:
                # 1) существующая fallback-ветка: необработанные заказы из status=1
                from_status1_orders = [item.get("order") or {} for item in unhandled_by_main]
                c_main = await _process_fallback_orders_batch(
                    source="status1_unhandled",
                    orders=from_status1_orders,
                    api_key=api_key,
                    client=client,
                    now_kyiv=now_kyiv,
                    kyiv_tz=kyiv_tz,
                    timeout_minutes=fallback_timeout_minutes,
                    dry_run=dry_run,
                    tabletki_login=tabletki_login,
                    tabletki_password=tabletki_password,
                    tabletki_cancel_reason_default=tabletki_cancel_reason_default,
                )

                # 2) новая ветка: отдельные статусы из env BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS
                additional_orders: List[Dict[str, Any]] = []
                for status_id in fallback_additional_status_ids:
                    status_orders = await _fetch_orders_by_status(
                        api_key=api_key,
                        status_id=status_id,
                        client=client,
                    )
                    fallback_additional_total_orders += len(status_orders)
                    additional_orders.extend(status_orders)

                c_additional = await _process_fallback_orders_batch(
                    source="status_9_19_18_20",
                    orders=additional_orders,
                    api_key=api_key,
                    client=client,
                    now_kyiv=now_kyiv,
                    kyiv_tz=kyiv_tz,
                    timeout_minutes=fallback_timeout_minutes,
                    dry_run=dry_run,
                    tabletki_login=tabletki_login,
                    tabletki_password=tabletki_password,
                    tabletki_cancel_reason_default=tabletki_cancel_reason_default,
                )

                fallback_total_candidates = c_main["total_candidates"] + c_additional["total_candidates"]
                fallback_eligible_timeout = c_main["eligible_timeout"] + c_additional["eligible_timeout"]
                fallback_skipped_already_processed = c_main["skipped_already_processed"] + c_additional["skipped_already_processed"]
                fallback_skipped_too_new = c_main["skipped_too_new"] + c_additional["skipped_too_new"]
                fallback_sent_tabletki = c_main["sent_tabletki"] + c_additional["sent_tabletki"]
                fallback_obrabotano_updated = c_main["obrabotano_updated"] + c_additional["obrabotano_updated"]
                fallback_errors = c_main["errors"] + c_additional["errors"]
        else:
            fallback_total_candidates = len(unhandled_by_main)

        logger.info(
            "Biotus summary: total_status1=%s main_matched=%s main_processed=%s unhandled_by_main=%s "
            "fallback_enabled=%s fallback_additional_total_orders=%s fallback_candidates=%s fallback_eligible_timeout=%s fallback_sent=%s "
            "fallback_obrabotano_updated=%s fallback_skipped_already_processed=%s fallback_skipped_too_new=%s fallback_errors=%s",
            len(orders),
            len(matched),
            main_processed,
            len(unhandled_by_main),
            fallback_enabled,
            fallback_additional_total_orders,
            fallback_total_candidates,
            fallback_eligible_timeout,
            fallback_sent_tabletki,
            fallback_obrabotano_updated,
            fallback_skipped_already_processed,
            fallback_skipped_too_new,
            fallback_errors,
        )
        if unhandled_reason_counts:
            logger.info("Biotus unhandled reasons: %s", unhandled_reason_counts)

        logger.info("Skipped orders due to duplicate phones: %s", skipped)
        logger.info("Orders marked as duplicate: %s", duplicate_marked)

    return {
        "total": len(orders),
        "matched": len(matched),
        "updated": updated,
        "main_processed": main_processed,
        "skipped": skipped,
        "duplicate_marked": duplicate_marked,
        "unhandled_by_main": len(unhandled_by_main),
        "unhandled_reason_counts": unhandled_reason_counts,
        "fallback_enabled": fallback_enabled,
        "fallback_timeout_minutes": fallback_timeout_minutes,
        "fallback_additional_total_orders": fallback_additional_total_orders,
        "fallback_total_candidates": fallback_total_candidates,
        "fallback_eligible_timeout": fallback_eligible_timeout,
        "fallback_skipped_already_processed": fallback_skipped_already_processed,
        "fallback_skipped_too_new": fallback_skipped_too_new,
        "fallback_sent_tabletki": fallback_sent_tabletki,
        "fallback_obrabotano_updated": fallback_obrabotano_updated,
        "fallback_errors": fallback_errors,
        # Backward-compatible aliases for external consumers of old keys.
        "fallback_skipped_note_marker": fallback_skipped_already_processed,
        "fallback_note_updated": fallback_obrabotano_updated,
        "allowed_suppliers": allowed_supplier_ids,
        "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
        "window_minutes": window_minutes,
        "window_source": window_source,
        "tz": tz_name,
    }


def _parse_cli():
    import argparse
    p = argparse.ArgumentParser(description="Check orders in SalesDrive and update status")
    p.add_argument(
        "--enterprise-code",
        default=_env_str("BIOTUS_ENTERPRISE_CODE", "2547"),
        help="enterprise_code для token в EnterpriseSettings",
    )
    p.add_argument(
        "--min-age-minutes",
        type=int,
        default=None,
        help="Override для окна отбора (минуты от текущего времени)",
    )
    p.add_argument("--dry-run", action="store_true", help="Только показать, что бы обновили")
    p.add_argument("--no-ssl-verify", action="store_true", help="Отключить проверку SSL")
    p.add_argument(
        "--suppliers",
        default=None,
        help='Список кодов supplierlist; переопределяет ALLOWED_SUPPLIERS (например "38;41")',
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_cli()
    result = asyncio.run(
        process_biotus_orders(
            enterprise_code=args.enterprise_code,
            min_age_minutes=args.min_age_minutes,
            verify_ssl=not args.no_ssl_verify,
            dry_run=args.dry_run,
            suppliers=args.suppliers,
        )
    )
    import json
    print(json.dumps(result, ensure_ascii=False, indent=2))

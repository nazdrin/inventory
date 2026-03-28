import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

from app.dntrade_data_service.client import DEFAULT_LIMIT, fetch_products_page
from app.dntrade_data_service.runtime import (
    fetch_enterprise_settings,
    maybe_dump_raw_json,
    save_to_json,
)
from app.services.database_service import process_database_service

load_dotenv()

DEFAULT_VAT = 20
LIMIT = DEFAULT_LIMIT
DELTA_LIMIT = int(os.getenv("DNTRADE_CATALOG_DELTA_LIMIT", "100"))
MAX_PAGES = int(os.getenv("DNTRADE_CATALOG_MAX_PAGES", "2000"))
MAX_REPEAT_PAGES = int(os.getenv("DNTRADE_CATALOG_MAX_REPEAT_PAGES", "3"))
INCREMENTAL_ENABLED = (os.getenv("DNTRADE_CATALOG_INCREMENTAL_ENABLED", "1") or "").strip().lower() in {"1", "true", "yes", "on"}
DELTA_SAFETY_WINDOW_MIN = int(os.getenv("DNTRADE_CATALOG_DELTA_SAFETY_WINDOW_MIN", "10"))
FULL_SYNC_MAX_AGE_HOURS = int(os.getenv("DNTRADE_CATALOG_FULL_SYNC_MAX_AGE_HOURS", "24"))
STATE_DIR = Path(os.getenv("DNTRADE_STATE_DIR", "state_cache"))
logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _format_api_datetime(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _format_state_datetime(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_state_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _state_path(enterprise_code: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"dntrade_{enterprise_code}_catalog_sync_state.json"


def _snapshot_path(enterprise_code: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"dntrade_{enterprise_code}_catalog_snapshot.json"


def _inflight_path(enterprise_code: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"dntrade_{enterprise_code}_catalog_sync_inflight.json"


def _write_json_atomically(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)


@dataclass
class CatalogSyncState:
    last_successful_catalog_sync_at: Optional[str] = None
    last_full_catalog_sync_at: Optional[str] = None
    last_sync_mode: Optional[str] = None
    last_sync_status: Optional[str] = None
    last_modified_from: Optional[str] = None
    last_modified_to: Optional[str] = None

    @classmethod
    def load(cls, enterprise_code: str) -> Tuple["CatalogSyncState", Optional[str]]:
        path = _state_path(enterprise_code)
        if not path.exists():
            return cls(), "state_missing"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return cls(), "state_invalid_payload"
            return cls(
                last_successful_catalog_sync_at=payload.get("last_successful_catalog_sync_at"),
                last_full_catalog_sync_at=payload.get("last_full_catalog_sync_at"),
                last_sync_mode=payload.get("last_sync_mode"),
                last_sync_status=payload.get("last_sync_status"),
                last_modified_from=payload.get("last_modified_from"),
                last_modified_to=payload.get("last_modified_to"),
            ), None
        except Exception:
            logger.exception("Dntrade catalog state read failed: enterprise_code=%s path=%s", enterprise_code, path)
            return cls(), "state_read_failed"

    def save(self, enterprise_code: str) -> None:
        _write_json_atomically(
            _state_path(enterprise_code),
            {
                "last_successful_catalog_sync_at": self.last_successful_catalog_sync_at,
                "last_full_catalog_sync_at": self.last_full_catalog_sync_at,
                "last_sync_mode": self.last_sync_mode,
                "last_sync_status": self.last_sync_status,
                "last_modified_from": self.last_modified_from,
                "last_modified_to": self.last_modified_to,
            },
        )


@dataclass
class SyncPlan:
    sync_mode: str
    modified_from: Optional[str] = None
    modified_to: Optional[str] = None
    fallback_reason: Optional[str] = None


def _load_snapshot(enterprise_code: str) -> Optional[list[dict]]:
    path = _snapshot_path(enterprise_code)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Dntrade catalog snapshot read failed: enterprise_code=%s path=%s", enterprise_code, path)
        return None
    if not isinstance(payload, list):
        logger.warning("Dntrade catalog snapshot has invalid type: enterprise_code=%s path=%s", enterprise_code, path)
        return None
    return payload


def _save_snapshot(enterprise_code: str, payload: list[dict]) -> None:
    _write_json_atomically(_snapshot_path(enterprise_code), payload)


def _write_inflight_marker(enterprise_code: str, payload: Dict[str, Any]) -> None:
    _write_json_atomically(_inflight_path(enterprise_code), payload)


def _clear_inflight_marker(enterprise_code: str) -> None:
    path = _inflight_path(enterprise_code)
    if path.exists():
        path.unlink()


def _build_sync_plan(enterprise_code: str) -> Tuple[SyncPlan, CatalogSyncState]:
    now = _utcnow()
    state, state_error = CatalogSyncState.load(enterprise_code)
    snapshot = _load_snapshot(enterprise_code)

    if not INCREMENTAL_ENABLED:
        return SyncPlan(sync_mode="full", fallback_reason="incremental_disabled"), state

    if _inflight_path(enterprise_code).exists():
        return SyncPlan(sync_mode="fallback_full", fallback_reason="stale_inflight_marker"), state

    if state_error:
        return SyncPlan(sync_mode="fallback_full", fallback_reason=state_error), state

    if state.last_sync_status != "success":
        return SyncPlan(sync_mode="fallback_full", fallback_reason="previous_sync_not_success"), state

    if snapshot is None:
        return SyncPlan(sync_mode="fallback_full", fallback_reason="snapshot_missing_or_invalid"), state

    last_successful = _parse_state_datetime(state.last_successful_catalog_sync_at)
    last_full = _parse_state_datetime(state.last_full_catalog_sync_at)
    if last_successful is None or last_full is None:
        return SyncPlan(sync_mode="fallback_full", fallback_reason="state_missing_timestamps"), state

    if now - last_full > timedelta(hours=FULL_SYNC_MAX_AGE_HOURS):
        return SyncPlan(sync_mode="fallback_full", fallback_reason="full_sync_too_old"), state

    modified_from_dt = last_successful - timedelta(minutes=DELTA_SAFETY_WINDOW_MIN)
    modified_to_dt = now
    if modified_from_dt >= modified_to_dt:
        return SyncPlan(sync_mode="fallback_full", fallback_reason="invalid_delta_window"), state

    return SyncPlan(
        sync_mode="delta",
        modified_from=_format_api_datetime(modified_from_dt),
        modified_to=_format_api_datetime(modified_to_dt),
    ), state


def _merge_delta_into_snapshot(snapshot: list[dict], delta_records: list[dict]) -> list[dict]:
    merged_by_code: Dict[str, dict] = {}
    for record in snapshot:
        code = str(record.get("code") or "").strip()
        if code:
            merged_by_code[code] = dict(record)
    for record in delta_records:
        code = str(record.get("code") or "").strip()
        if code:
            merged_by_code[code] = dict(record)
    return list(merged_by_code.values())


def _estimate_delta_filter_effective(
    *,
    sync_mode: str,
    modified_from: Optional[str],
    modified_to: Optional[str],
    pages_fetched: int,
    total_products: int,
) -> str:
    if sync_mode != "delta":
        return "unknown"
    if not modified_from or not modified_to:
        return "no"
    if total_products == 0:
        return "yes"
    if pages_fetched <= 1 and total_products <= LIMIT:
        return "yes"
    return "unknown"


async def _fetch_catalog_products(
    *,
    session: aiohttp.ClientSession,
    api_key: str,
    enterprise_code: str,
    sync_mode: str,
    modified_from: Optional[str] = None,
    modified_to: Optional[str] = None,
) -> Tuple[list[dict], Dict[str, Any]]:
    all_products = []
    offset = 0
    request_limit = DELTA_LIMIT if sync_mode == "delta" else LIMIT
    pages_fetched = 0
    repeated_page_count = 0
    last_fingerprint: Optional[Tuple[int, str, str]] = None
    fetch_errors = 0
    max_pages_hit = False
    repeated_page_stop = False

    while True:
        if pages_fetched >= MAX_PAGES:
            max_pages_hit = True
            logger.warning(
                "Dntrade catalog stop: max pages reached (%s) for enterprise_code=%s sync_mode=%s",
                MAX_PAGES,
                enterprise_code,
                sync_mode,
            )
            break

        response = await fetch_products_page(
            session=session,
            api_key=api_key,
            offset=offset,
            limit=request_limit,
            modified_from=modified_from,
            modified_to=modified_to,
        )

        if response is None:
            fetch_errors += 1
            logger.warning(
                "Dntrade catalog stop: empty/invalid response at offset=%s enterprise_code=%s sync_mode=%s modified_from=%s modified_to=%s",
                offset,
                enterprise_code,
                sync_mode,
                modified_from,
                modified_to,
            )
            break

        products = response.get("products", [])
        if not products:
            logger.info(
                "Dntrade catalog stop: no more products at offset=%s enterprise_code=%s sync_mode=%s",
                offset,
                enterprise_code,
                sync_mode,
            )
            break

        if sync_mode == "delta" and (pages_fetched < 3 or (pages_fetched + 1) % 10 == 0):
            logger.info(
                "Dntrade catalog delta page: enterprise_code=%s offset=%s limit=%s page_items=%s modified_from=%s modified_to=%s",
                enterprise_code,
                offset,
                request_limit,
                len(products),
                modified_from,
                modified_to,
            )

        first_id = str(products[0].get("product_id", ""))
        last_id = str(products[-1].get("product_id", ""))
        current_fingerprint = (len(products), first_id, last_id)
        if current_fingerprint == last_fingerprint:
            repeated_page_count += 1
            if repeated_page_count >= MAX_REPEAT_PAGES:
                repeated_page_stop = True
                logger.warning(
                    "Dntrade catalog stop: repeating page detected %s times at offset=%s enterprise_code=%s sync_mode=%s",
                    repeated_page_count,
                    offset,
                    enterprise_code,
                    sync_mode,
                )
                break
        else:
            repeated_page_count = 0
        last_fingerprint = current_fingerprint

        all_products.extend(products)
        offset += len(products)
        pages_fetched += 1

        if pages_fetched % 10 == 0:
            logger.info(
                "Dntrade catalog progress: enterprise_code=%s sync_mode=%s pages=%s products=%s offset=%s modified_from=%s modified_to=%s",
                enterprise_code,
                sync_mode,
                pages_fetched,
                len(all_products),
                offset,
                modified_from,
                modified_to,
            )

    return all_products, {
        "pages_fetched": pages_fetched,
        "fetch_errors": fetch_errors,
        "max_pages_hit": max_pages_hit,
        "repeated_page_stop": repeated_page_stop,
    }


def transform_products(products):
    """Трансформация данных продуктов в целевой формат."""
    transformed = []
    seen_product_ids = set()
    skipped_missing_product_id = 0
    skipped_duplicates = 0
    normalized_empty_producer = 0

    for product in products:
        product_id = product.get("product_id")
        if not product_id:
            skipped_missing_product_id += 1
            continue
        if product_id in seen_product_ids:
            skipped_duplicates += 1
            continue  # Пропускаем дублирующийся product_id

        producer = product.get("short_description")
        if not producer or producer in [None, "", 0]:  # Фильтрация некорректных значений
            producer = ""
            normalized_empty_producer += 1
        transformed.append({
            "code": product_id,
            "name": product.get("title"),
            "vat": DEFAULT_VAT,
            "producer": producer,
            "barcode": product.get("barcode"),
        })
        seen_product_ids.add(product_id)  # Запоминаем обработанный product_id
    stats = {
        "skipped_missing_product_id": skipped_missing_product_id,
        "skipped_duplicates": skipped_duplicates,
        "normalized_empty_producer": normalized_empty_producer,
    }
    return transformed, stats


def _persist_success_state(
    *,
    enterprise_code: str,
    sync_mode: str,
    state: CatalogSyncState,
    modified_from: Optional[str],
    modified_to: Optional[str],
    snapshot_payload: Optional[list[dict]] = None,
) -> None:
    now = _utcnow()
    if snapshot_payload is not None:
        _save_snapshot(enterprise_code, snapshot_payload)

    state.last_successful_catalog_sync_at = _format_state_datetime(now)
    if sync_mode in {"full", "fallback_full"}:
        state.last_full_catalog_sync_at = state.last_successful_catalog_sync_at
    state.last_sync_mode = sync_mode
    state.last_sync_status = "success"
    state.last_modified_from = modified_from
    state.last_modified_to = modified_to
    state.save(enterprise_code)

async def run_service(enterprise_code, file_type):
    """Основной сервис выполнения задачи."""
    started = perf_counter()
    logger.info("Dntrade catalog start: enterprise_code=%s", enterprise_code)
    if not enterprise_code:
        logger.warning("Dntrade catalog stop: empty enterprise_code")
        return

    enterprise_settings = await fetch_enterprise_settings(enterprise_code)
    if not enterprise_settings:
        logger.warning("Dntrade catalog stop: enterprise settings not found for %s", enterprise_code)
        return

    api_key = enterprise_settings.token
    if not api_key:
        logger.warning("Dntrade catalog stop: empty token for %s", enterprise_code)
        return

    plan, state = _build_sync_plan(enterprise_code)
    effective_mode = plan.sync_mode
    fallback_reason = plan.fallback_reason
    state_updated = False

    _write_inflight_marker(
        enterprise_code,
        {
            "started_at": _format_state_datetime(_utcnow()),
            "planned_sync_mode": plan.sync_mode,
            "modified_from": plan.modified_from,
            "modified_to": plan.modified_to,
            "fallback_reason": plan.fallback_reason,
        },
    )

    try:
        async with aiohttp.ClientSession() as session:
            all_products, fetch_stats = await _fetch_catalog_products(
                session=session,
                api_key=api_key,
                enterprise_code=enterprise_code,
                sync_mode=plan.sync_mode,
                modified_from=plan.modified_from,
                modified_to=plan.modified_to,
            )

            if plan.sync_mode == "delta" and (
                fetch_stats["fetch_errors"] > 0
                or fetch_stats["max_pages_hit"]
                or fetch_stats["repeated_page_stop"]
            ):
                effective_mode = "fallback_full"
                if fetch_stats["fetch_errors"] > 0:
                    fallback_reason = "delta_fetch_error"
                elif fetch_stats["max_pages_hit"]:
                    fallback_reason = "delta_max_pages_hit"
                else:
                    fallback_reason = "delta_repeated_page_detected"
                logger.warning(
                    "Dntrade catalog delta fallback to full: enterprise_code=%s reason=%s",
                    enterprise_code,
                    fallback_reason,
                )
                all_products, fetch_stats = await _fetch_catalog_products(
                    session=session,
                    api_key=api_key,
                    enterprise_code=enterprise_code,
                    sync_mode=effective_mode,
                )

        if not all_products:
            if effective_mode == "delta":
                _persist_success_state(
                    enterprise_code=enterprise_code,
                    sync_mode=effective_mode,
                    state=state,
                    modified_from=plan.modified_from,
                    modified_to=plan.modified_to,
                    snapshot_payload=None,
                )
                state_updated = True
                logger.info(
                    "Dntrade catalog done: enterprise_code=%s sync_mode=%s modified_from=%s modified_to=%s "
                    "fetched_products=0 transformed=0 deduped=0 skipped=0 elapsed=%.3fs "
                    "state_updated=%s delta_filter_effective=%s fallback_reason=%s",
                    enterprise_code,
                    effective_mode,
                    plan.modified_from,
                    plan.modified_to,
                    perf_counter() - started,
                    "yes",
                    _estimate_delta_filter_effective(
                        sync_mode=effective_mode,
                        modified_from=plan.modified_from,
                        modified_to=plan.modified_to,
                        pages_fetched=0,
                        total_products=0,
                    ),
                    fallback_reason,
                )
                return
            logger.warning("Dntrade catalog stop: no products fetched for %s", enterprise_code)
            return

        maybe_dump_raw_json(all_products, enterprise_code, "catalog", label="raw_input")

        transformed_data, transform_stats = transform_products(all_products)
        if effective_mode == "delta" and all_products and not transformed_data:
            fallback_reason = "delta_transformed_empty"
            logger.warning(
                "Dntrade catalog delta fallback to full: enterprise_code=%s reason=%s",
                enterprise_code,
                fallback_reason,
            )
            effective_mode = "fallback_full"
            async with aiohttp.ClientSession() as fallback_session:
                all_products, fetch_stats = await _fetch_catalog_products(
                    session=fallback_session,
                    api_key=api_key,
                    enterprise_code=enterprise_code,
                    sync_mode=effective_mode,
                )
            if not all_products:
                logger.warning("Dntrade catalog stop: fallback full returned no products for %s", enterprise_code)
                return
            maybe_dump_raw_json(all_products, enterprise_code, "catalog", label="raw_input")
            transformed_data, transform_stats = transform_products(all_products)

        deduped_count = transform_stats["skipped_duplicates"]
        skipped_total = transform_stats["skipped_missing_product_id"]

        if effective_mode == "delta":
            snapshot = _load_snapshot(enterprise_code)
            if snapshot is None:
                logger.warning(
                    "Dntrade catalog delta fallback to full: enterprise_code=%s reason=%s",
                    enterprise_code,
                    "snapshot_missing_before_merge",
                )
                effective_mode = "fallback_full"
                fallback_reason = "snapshot_missing_before_merge"
                async with aiohttp.ClientSession() as fallback_session:
                    all_products, fetch_stats = await _fetch_catalog_products(
                        session=fallback_session,
                        api_key=api_key,
                        enterprise_code=enterprise_code,
                        sync_mode=effective_mode,
                    )
                if not all_products:
                    logger.warning("Dntrade catalog stop: fallback full returned no products for %s", enterprise_code)
                    return
                maybe_dump_raw_json(all_products, enterprise_code, "catalog", label="raw_input")
                transformed_data, transform_stats = transform_products(all_products)
                deduped_count = transform_stats["skipped_duplicates"]
                skipped_total = transform_stats["skipped_missing_product_id"]
                merged_catalog = transformed_data
            else:
                merged_catalog = _merge_delta_into_snapshot(snapshot, transformed_data)
        else:
            merged_catalog = transformed_data

        file_type = "catalog"
        json_file_path = save_to_json(merged_catalog, enterprise_code, file_type)

        if not json_file_path:
            logger.error("Dntrade catalog stop: failed to write json for %s", enterprise_code)
            return

        await process_database_service(json_file_path, file_type, enterprise_code)
        _persist_success_state(
            enterprise_code=enterprise_code,
            sync_mode=effective_mode,
            state=state,
            modified_from=plan.modified_from if effective_mode == "delta" else None,
            modified_to=plan.modified_to if effective_mode == "delta" else None,
            snapshot_payload=merged_catalog,
        )
        state_updated = True
        logger.info(
            "Dntrade catalog done: enterprise_code=%s sync_mode=%s modified_from=%s modified_to=%s "
            "fetched_products=%s transformed=%s persistence_records=%s deduped=%s skipped=%s "
            "elapsed=%.3fs state_updated=%s delta_filter_effective=%s fallback_reason=%s",
            enterprise_code,
            effective_mode,
            plan.modified_from if effective_mode == "delta" else None,
            plan.modified_to if effective_mode == "delta" else None,
            len(all_products),
            len(transformed_data),
            len(merged_catalog),
            deduped_count,
            skipped_total,
            perf_counter() - started,
            "yes" if state_updated else "no",
            _estimate_delta_filter_effective(
                sync_mode=effective_mode,
                modified_from=plan.modified_from if effective_mode == "delta" else None,
                modified_to=plan.modified_to if effective_mode == "delta" else None,
                pages_fetched=fetch_stats["pages_fetched"],
                total_products=len(all_products),
            ),
            fallback_reason,
        )
    finally:
        _clear_inflight_marker(enterprise_code)

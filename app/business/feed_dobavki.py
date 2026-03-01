from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Literal
from urllib.parse import urlparse, parse_qs

import math
import httpx
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DRIVE_SCOPE_READONLY = ["https://www.googleapis.com/auth/drive.readonly"]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOOGLE_SET_DIR = PROJECT_ROOT / "google_set"
GDRIVE_FOLDER_ENV = "DOBAVKI_GDRIVE_FOLDER_ID"
GDRIVE_FILE_NAME_ENV = "DOBAVKI_GDRIVE_FILE_NAME"
_gdrive_last_failure_reason = "not_initialized"

# --- Колонки в Google Sheet для поставщика D4 ---
COL_ARTIKUL = "Артикул"
COL_NAME = "Назва"
COL_BARCODE = "Штрихкод"
COL_AVAILABLE = "Наявність"
COL_QTY = "Кількість на складі"
COL_PRICE_USD = "Ціна зі знижкою"   # как на скрине, без звёздочки


# ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================

def _convert_gsheet_link_to_csv_url(feed_url: str) -> str:
    """
    Преобразует обычную ссылку вида:
      https://docs.google.com/spreadsheets/d/<ID>/edit?gid=<GID>
    в CSV-export:
      https://docs.google.com/spreadsheets/d/<ID>/export?format=csv&gid=<GID>

    Если формат другой, возвращаем исходный feed_url (на случай, если уже CSV).
    """
    try:
        parsed = urlparse(feed_url)
        path_parts = parsed.path.split("/")
        if "d" not in path_parts:
            return feed_url

        idx = path_parts.index("d")
        file_id = path_parts[idx + 1]

        qs = parse_qs(parsed.query)
        gid = qs.get("gid", ["0"])[0]

        csv_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}"
        return csv_url
    except Exception as e:
        logger.warning("Не удалось преобразовать ссылку Google Sheet в CSV: %s", e)
        return feed_url


async def _get_feed_settings_by_code(code: str) -> tuple[Optional[str], Optional[float], Optional[float]]:
    """
    Достаёт из dropship_enterprises:
      - feed_url
      - gdrive_folder (используем как курс USD→UAH)
      - retail_markup (наценка в процентах, например 25 = +25%)
    """
    async with get_async_db() as session:
        res = await session.execute(
            text(
                """
                SELECT feed_url, gdrive_folder, retail_markup
                FROM dropship_enterprises
                WHERE code = :code
                LIMIT 1
                """
            ),
            {"code": code},
        )
        row = res.first()
        if not row:
            return None, None, None

        feed_url, rate, markup = row

        try:
            rate_val = float(rate) if rate is not None else None
        except (TypeError, ValueError):
            rate_val = None

        try:
            # В БД retail_markup хранится как процент (например 25),
            # здесь конвертируем в долю (0.25)
            markup_val = float(markup) / 100.0 if markup is not None else None
        except (TypeError, ValueError):
            markup_val = None

        return feed_url, rate_val, markup_val


def _parse_csv_text(csv_text: str) -> List[Dict[str, str]]:
    """
    Парсинг CSV-текста:
      - пропускаем первую строку (надпись "Система знижок ...");
      - со второй строки берём заголовки и данные.
    """
    lines = csv_text.splitlines()
    if len(lines) <= 1:
        return []

    # отрезаем первую строку с надписью "Система знижок..."
    body = "\n".join(lines[1:])

    sample = body[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(body), dialect=dialect)
    return list(reader)


async def _load_sheet_rows(*, code: str, timeout: int) -> Optional[List[Dict[str, str]]]:
    """
    Рабочий режим:
        1) берём feed_url, gdrive_folder, retail_markup по code из dropship_enterprises;
        2) конвертируем ссылку в CSV-export;
        3) скачиваем CSV по HTTP;
        4) возвращаем список строк (dict по именам колонок).
    """
    feed_url, _, _ = await _get_feed_settings_by_code(code)
    if not feed_url:
        msg = f"Не найден feed_url в dropship_enterprises для code='{code}'"
        logger.error(msg)
        send_notification(msg, "Розробник")
        return None

    csv_url = _convert_gsheet_link_to_csv_url(feed_url)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with httpx.AsyncClient(
            headers=headers,
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            resp = await client.get(csv_url)
            resp.raise_for_status()
            csv_text = resp.text
    except Exception as e:
        msg = f"Ошибка загрузки CSV по ссылке {csv_url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Розробник")
        return None

    rows = _parse_csv_text(csv_text)
    if not rows:
        msg = f"CSV для code='{code}' пустой или без данных"
        logger.error(msg)
        send_notification(msg, "Розробник")
        return None

    logger.info("D4 CSV: прочитано строк (code=%s): %d", code, len(rows))
    return rows


def _is_available(row: Dict[str, str]) -> bool:
    """
    Фильтр по колонке 'Наявність'.
    TRUE: 'TRUE', 'True', 'true', '1', 'так', 'yes'.
    """
    val = (row.get(COL_AVAILABLE) or "").strip().lower()
    return val in {"true", "1", "так", "yes"}


def _parse_qty(value: Optional[str]) -> int:
    """
    Из 'Кількість на складі' вида '30' / '30+' / '10 шт' берём только цифровую часть.
    """
    if not value:
        return 0
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return 0
    try:
        return int(digits)
    except ValueError:
        return 0


def _to_float(val: Optional[str]) -> float:
    if val is None:
        return 0.0
    s = str(val).strip().replace(" ", "").replace("\u00A0", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _get_d4_snapshot_path() -> Path:
    temp_root = Path(os.getenv("TEMP_FILE_PATH", tempfile.gettempdir()))
    return temp_root / "dobavki" / "D4_catalog_snapshot.json"


def _load_catalog_snapshot(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Dobavki D4: failed to read snapshot %s: %s", path, exc)
        return {}

    rows = raw if isinstance(raw, list) else []
    snapshot: Dict[str, Dict[str, str]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        sku = str(item.get("id") or "").strip()
        if not sku:
            continue
        snapshot[sku] = {
            "name": str(item.get("name") or "").strip(),
            "barcode": str(item.get("barcode") or "").strip(),
        }
    return snapshot


def _save_catalog_snapshot(path: Path, catalog_items: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(catalog_items, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_catalog_delta_and_update_snapshot(catalog_items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    snapshot_path = _get_d4_snapshot_path()
    prev_snapshot = _load_catalog_snapshot(snapshot_path)

    if not prev_snapshot:
        delta_items = catalog_items
    else:
        delta_items = []
        for item in catalog_items:
            sku = str(item.get("id") or "").strip()
            if not sku:
                continue
            prev = prev_snapshot.get(sku)
            if not prev:
                delta_items.append(item)
                continue
            name = str(item.get("name") or "").strip()
            barcode = str(item.get("barcode") or "").strip()
            if prev.get("name") != name or prev.get("barcode") != barcode:
                delta_items.append(item)

    try:
        _save_catalog_snapshot(snapshot_path, catalog_items)
    except Exception as exc:
        logger.warning("Dobavki D4: failed to save snapshot %s: %s", snapshot_path, exc)
    return delta_items


def _credentials_candidates() -> List[Path]:
    candidates: List[Path] = [
        GOOGLE_SET_DIR / "service_account.json",
        GOOGLE_SET_DIR / "credentials.json",
    ]
    if GOOGLE_SET_DIR.exists():
        other_jsons = sorted(
            p for p in GOOGLE_SET_DIR.glob("*.json")
            if p not in candidates
        )
        candidates.extend(other_jsons)
    return candidates


def _build_drive_service():
    global _gdrive_last_failure_reason
    _gdrive_last_failure_reason = "unknown"

    try:
        from googleapiclient.discovery import build
    except Exception as exc:
        _gdrive_last_failure_reason = f"googleapiclient_unavailable:{exc}"
        logger.warning("Dobavki D4: %s", _gdrive_last_failure_reason)
        return None

    for cred_path in _credentials_candidates():
        if not cred_path.exists():
            continue
        try:
            raw = json.loads(cred_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Dobavki D4: failed to read credentials file %s: %s", cred_path, exc)
            continue

        cred_type = str(raw.get("type") or "").strip()
        try:
            if cred_type == "service_account":
                from google.oauth2.service_account import Credentials as SACredentials

                credentials = SACredentials.from_service_account_file(
                    str(cred_path),
                    scopes=DRIVE_SCOPE_READONLY,
                )
                return build("drive", "v3", credentials=credentials, cache_discovery=False)

            if cred_type in {"authorized_user"}:
                from google.oauth2.credentials import Credentials as UserCredentials

                credentials = UserCredentials.from_authorized_user_file(
                    str(cred_path),
                    scopes=DRIVE_SCOPE_READONLY,
                )
                return build("drive", "v3", credentials=credentials, cache_discovery=False)

            if cred_type in {"installed", "web"}:
                token_path = GOOGLE_SET_DIR / "token.json"
                if not token_path.exists():
                    _gdrive_last_failure_reason = f"oauth_token_missing:{token_path}"
                    logger.warning("Dobavki D4: %s", _gdrive_last_failure_reason)
                    return None
                from google.oauth2.credentials import Credentials as UserCredentials

                credentials = UserCredentials.from_authorized_user_file(
                    str(token_path),
                    scopes=DRIVE_SCOPE_READONLY,
                )
                return build("drive", "v3", credentials=credentials, cache_discovery=False)
        except Exception as exc:
            logger.warning("Dobavki D4: failed to create Drive credentials from %s: %s", cred_path, exc)
            _gdrive_last_failure_reason = f"credentials_error:{exc}"
            continue

    _gdrive_last_failure_reason = f"credentials_not_found_in:{GOOGLE_SET_DIR}"
    logger.warning("Dobavki D4: %s", _gdrive_last_failure_reason)
    return None


def _find_json_file_in_folder(service, folder_id: str, preferred_name: Optional[str] = None) -> Optional[str]:
    try:
        base_q = f"'{folder_id}' in parents and trashed = false"
        if preferred_name:
            escaped_name = preferred_name.replace("'", "\\'")
            q = f"{base_q} and name = '{escaped_name}'"
            resp = (
                service.files()
                .list(
                    q=q,
                    fields="files(id,name,modifiedTime,mimeType)",
                    pageSize=1,
                )
                .execute()
            )
            files = resp.get("files", [])
            if files:
                return files[0].get("id")
            return None

        q = (
            f"{base_q} and "
            "(mimeType = 'application/json' or name contains '.json')"
        )
        resp = (
            service.files()
            .list(
                q=q,
                fields="files(id,name,modifiedTime,mimeType)",
                orderBy="modifiedTime desc",
                pageSize=1,
            )
            .execute()
        )
        files = resp.get("files", [])
        if files:
            return files[0].get("id")
        return None
    except Exception as exc:
        logger.warning("Dobavki D4: failed to find JSON file in folder: %s", exc)
        return None


def _download_file_text(service, file_id: str) -> str:
    from googleapiclient.http import MediaIoBaseDownload

    request = service.files().get_media(fileId=file_id)
    stream = io.BytesIO()
    downloader = MediaIoBaseDownload(stream, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return stream.getvalue().decode("utf-8")


async def _try_load_products_json_from_gdrive() -> Optional[List[Dict]]:
    global _gdrive_last_failure_reason

    folder_id = (os.getenv(GDRIVE_FOLDER_ENV) or "").strip()
    preferred_name = (os.getenv(GDRIVE_FILE_NAME_ENV) or "").strip() or None
    if not folder_id:
        _gdrive_last_failure_reason = f"missing_env:{GDRIVE_FOLDER_ENV}"
        logger.warning("Dobavki D4: %s", _gdrive_last_failure_reason)
        return None

    service = _build_drive_service()
    if not service:
        return None

    file_id = _find_json_file_in_folder(service, folder_id, preferred_name=preferred_name)
    if not file_id:
        _gdrive_last_failure_reason = "json_file_not_found"
        logger.warning("Dobavki D4: %s", _gdrive_last_failure_reason)
        return None

    try:
        text_content = _download_file_text(service, file_id)
        payload = json.loads(text_content)
    except Exception as exc:
        _gdrive_last_failure_reason = f"json_download_parse_error:{exc}"
        logger.warning("Dobavki D4: %s", _gdrive_last_failure_reason)
        return None

    if not isinstance(payload, list):
        _gdrive_last_failure_reason = "json_root_not_list"
        logger.warning("Dobavki D4: %s", _gdrive_last_failure_reason)
        return None

    products = [item for item in payload if isinstance(item, dict)]
    if not products:
        _gdrive_last_failure_reason = "json_list_empty"
        logger.warning("Dobavki D4: %s", _gdrive_last_failure_reason)
        return None
    return products


def _build_catalog_from_products(products: List[Dict]) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for row in products:
        sku = str(row.get("articul") or "").strip()
        name = str(row.get("name") or "").strip()
        barcode = str(row.get("barcode") or "").strip()
        if not (sku and name):
            continue
        items.append({
            "id": sku,
            "name": " ".join(name.split()),
            "barcode": barcode,
        })
    return items


def _parse_qty_any(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return max(0, int(value))
    return _parse_qty(str(value))


async def _build_stock_from_products(*, products: List[Dict], code: str) -> List[Dict[str, object]]:
    _, _, markup = await _get_feed_settings_by_code(code)
    if markup is None:
        msg = f"Для code='{code}' не задан корректный retail_markup в dropship_enterprises"
        logger.error(msg)
        send_notification(msg, "Розробник")
        return []

    rows_out: List[Dict[str, object]] = []
    for row in products:
        sku = str(row.get("articul") or "").strip()
        if not sku:
            continue

        qty = _parse_qty_any(row.get("qty"))
        price_opt = round(_to_float(row.get("price")), 2)
        if price_opt < 0:
            price_opt = 0.0
        price_retail = round(price_opt * (1.0 + markup), 2)
        rows_out.append({
            "code_sup": sku,
            "qty": qty,
            "price_retail": price_retail,
            "price_opt": price_opt,
        })
    return rows_out


# ===================== ПАРСЕР CATALOG =====================

async def parse_d4_catalog_to_json(
    *,
    code: str = "D4",
    timeout: int = 30,
) -> str:
    """
    Каталог (D4 Google Sheet) → JSON:
    [
      {"id": "<Артикул>", "name": "<Назва>", "barcode": "<Штрихкод>"}
    ]

    Только строки с Наявність == TRUE.
    """
    rows = await _load_sheet_rows(code=code, timeout=timeout)
    if rows is None:
        return "[]"

    items: List[Dict[str, str]] = []
    for row in rows:
        if not _is_available(row):
            continue

        sku = (row.get(COL_ARTIKUL) or "").strip()
        name = (row.get(COL_NAME) or "").strip()
        barcode = (row.get(COL_BARCODE) or "").strip()

        if not (sku and name):
            continue

        items.append({
            "id": sku,
            "name": " ".join(name.split()),
            "barcode": barcode,
        })

    logger.info("D4 каталог: собрано позиций (code=%s): %d", code, len(items))
    return json.dumps(items, ensure_ascii=False, indent=2)


# ===================== ПАРСЕР STOCK =====================

async def parse_d4_stock_to_json(
    *,
    code: str = "D4",
    timeout: int = 30,
) -> str:
    """
    Сток (D4 Google Sheet) → JSON:
    [
      {
        "code_sup": "<Артикул>",
        "qty": <int>,
        "price_opt": Ціна зі знижкою (оптова ціна з фіду, грн),
        "price_retail": price_opt * (1 + retail_markup)
      }
    ]

    price_opt = Ціна зі знижкою (оптова ціна з фіду, грн)
    price_retail = price_opt * (1 + retail_markup)
    """
    rows = await _load_sheet_rows(code=code, timeout=timeout)
    if rows is None:
        return "[]"

    # наценку берём из БД (курс для D4 больше не используется)
    _, _, markup = await _get_feed_settings_by_code(code)
    if markup is None:
        msg = (
            f"Для code='{code}' не задан корректный retail_markup в dropship_enterprises"
        )
        logger.error(msg)
        send_notification(msg, "Розробник")
        return "[]"

    rows_out: List[Dict[str, object]] = []
    for row in rows:
        if not _is_available(row):
            continue

        sku = (row.get(COL_ARTIKUL) or "").strip()
        if not sku:
            continue

        qty = _parse_qty(row.get(COL_QTY))
        wholesale_raw = _to_float(row.get(COL_PRICE_USD))

        # COL_PRICE_USD ("Ціна зі знижкою") — це оптова ціна з фіду (грн)
        price_opt = round(wholesale_raw, 2)
        if price_opt < 0:
            price_opt = 0.0

        # Роздріб рахуємо від опту + націнка
        price_retail_raw = price_opt * (1.0 + markup)
        price_retail = round(price_retail_raw, 2)

        rows_out.append({
            "code_sup": sku,
            "qty": qty,
            "price_retail": price_retail,
            "price_opt": price_opt,
        })

    logger.info("D4 сток: собрано позиций (code=%s): %d", code, len(rows_out))
    return json.dumps(rows_out, ensure_ascii=False, indent=2)


# ===================== ОБЩАЯ ОБГОРТКА =====================

async def parse_d4_feed_to_json(
    *,
    mode: Literal["catalog", "stock"] = "catalog",
    code: str = "D4",
    timeout: int = 30,
) -> str:
    """
    Унифицированная обёртка для поставщика D4 (Google Sheet):
      mode = "catalog" → каталог
      mode = "stock"   → сток
    """
    if mode not in {"catalog", "stock"}:
        raise ValueError("mode must be 'catalog' or 'stock'")

    products = await _try_load_products_json_from_gdrive()
    if products:
        logger.info("Dobavki D4: using Google Drive JSON source")
        if mode == "catalog":
            catalog_items = _build_catalog_from_products(products)
            delta_items = _build_catalog_delta_and_update_snapshot(catalog_items)
            logger.info("D4 каталог (Google Drive): delta позиций (code=%s): %d", code, len(delta_items))
            return json.dumps(delta_items, ensure_ascii=False, indent=2)

        stock_rows = await _build_stock_from_products(products=products, code=code)
        logger.info("D4 сток (Google Drive): собрано позиций (code=%s): %d", code, len(stock_rows))
        return json.dumps(stock_rows, ensure_ascii=False, indent=2)

    logger.info(
        "Dobavki D4: fallback to Google Sheet source (reason=%s)",
        _gdrive_last_failure_reason,
    )
    if mode == "catalog":
        return await parse_d4_catalog_to_json(code=code, timeout=timeout)
    return await parse_d4_stock_to_json(code=code, timeout=timeout)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Парсер фида D4 (Google Sheet): "
            "режимы 'catalog' (каталог) и 'stock' (остатки/цены). "
            "URL и параметры берутся из БД по dropship_enterprises.code"
        )
    )
    parser.add_argument(
        "--mode",
        choices=["catalog", "stock"],
        default="catalog",
        help="Режим: catalog | stock (по умолчанию catalog)",
    )
    parser.add_argument(
        "--code",
        default="D4",
        help="значение поля code в dropship_enterprises (по умолчанию D4)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="таймаут HTTP-запроса, сек.",
    )

    args = parser.parse_args()
    out = asyncio.run(
        parse_d4_feed_to_json(
            mode=args.mode,
            code=args.code,
            timeout=args.timeout,
        )
    )
    print(out)

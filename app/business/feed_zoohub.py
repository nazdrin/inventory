from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import httpx
import xml.etree.ElementTree as ET
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

# Google Drive
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# Excel
from openpyxl import load_workbook

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === D10 (ZooHub) ===
D10_CODE_DEFAULT = "D10"

# Excel: заголовки на 4-й строке (1-indexed)
D10_HEADER_ROW = 4

# Кэш/стейт (для дельты и drop-цен)
# Можно переопределить env: D10_STATE_DIR=/path/to/state
DEFAULT_STATE_DIR = Path(os.getenv("D10_STATE_DIR") or "state_cache")


# ──────────────────────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ──────────────────────────────────────────────────────────────────────────────

def _strip_ns(tag: str) -> str:
    if not tag:
        return tag
    return tag.split("}")[-1]


def _to_float(val: Optional[str]) -> float:
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s:
        return 0.0
    # Уберём валюты/текст, оставим цифры/точку/запятую/минус
    s = s.replace("\u00A0", " ").strip()
    # Частый кейс: "235 UAH"
    m = re.search(r"-?\d+(?:[.,]\d+)?", s)
    if not m:
        return 0.0
    num = m.group(0).replace(",", ".")
    try:
        return float(num)
    except Exception:
        return 0.0


def _norm_str(v: Any) -> str:
    s = ("" if v is None else str(v)).strip()
    # иногда Excel даёт "None" текстом — прибьём
    if s.lower() == "none":
        return ""
    return s


def _is_probably_junk_row(values: Dict[str, str]) -> bool:
    """
    Пропускаем строки-категории/мусор:
    - нет артикула
    - или заполнено только одно поле (например, "антибиотики"), без ШК и цены
    """
    art = values.get("id", "")
    name = values.get("name", "")
    barcode = values.get("barcode", "")
    drop_price = values.get("drop_price_raw", "")

    if not art:
        return True

    # если “артикул” похож на текстовую категорию, иногда так бывает — отсеем
    # (если артикул не содержит цифр и слишком длинный)
    if not re.search(r"\d", art) and len(art) > 12:
        return True

    # если это “раздел”, часто: есть только name, а barcode/цены пустые
    if name and not barcode and not drop_price and len(name) > 2:
        # но если есть артикул — всё же может быть товар; оставим мягко:
        # считаем мусором только если name == art (часто у категорий)
        if name.strip().lower() == art.strip().lower():
            return True

    return False


# ──────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ──────────────────────────────────────────────────────────────────────────────

async def _get_gdrive_folder_by_code(code: str) -> Optional[str]:
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT gdrive_folder FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


async def _get_feed_url_by_code(code: str) -> Optional[str]:
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


# ──────────────────────────────────────────────────────────────────────────────
# GOOGLE DRIVE
# ──────────────────────────────────────────────────────────────────────────────

async def _connect_to_google_drive():
    creds_path = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH")
    if not creds_path or not os.path.exists(creds_path):
        msg = f"Неверный путь к учетным данным Google Drive: {creds_path}"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        raise FileNotFoundError(msg)

    credentials = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)
    logger.info("D10: Подключено к Google Drive")
    return service


async def _fetch_single_file_metadata(drive_service, folder_id: str) -> Dict[str, Any]:
    try:
        results = (
            drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id, name, modifiedTime)",
                pageSize=10,
            )
            .execute()
        )
        files = results.get("files", []) or []
        if not files:
            raise FileNotFoundError(f"В папке {folder_id} нет файлов")
        return files[0]
    except HttpError as e:
        msg = f"D10: HTTP ошибка при получении файлов из папки {folder_id}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        raise


async def _download_file_bytes(drive_service, file_id: str) -> bytes:
    try:
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()
    except HttpError as e:
        msg = f"D10: HTTP ошибка при загрузке файла {file_id}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        raise


# ──────────────────────────────────────────────────────────────────────────────
# STATE CACHE (для дельты и drop-цен)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class D10State:
    # ключ: id (артикул) -> signature
    sig_by_id: Dict[str, str]
    # drop price cache
    drop_by_id: Dict[str, float]
    drop_by_barcode: Dict[str, float]

    @classmethod
    def empty(cls) -> "D10State":
        return cls(sig_by_id={}, drop_by_id={}, drop_by_barcode={})

    @staticmethod
    def _state_paths(code: str) -> Tuple[Path, Path]:
        DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)
        state_path = DEFAULT_STATE_DIR / f"{code.lower()}_catalog_state.json"
        drop_path = DEFAULT_STATE_DIR / f"{code.lower()}_drop_prices.json"
        return state_path, drop_path

    @classmethod
    def load(cls, code: str) -> "D10State":
        state_path, drop_path = cls._state_paths(code)

        sig_by_id: Dict[str, str] = {}
        drop_by_id: Dict[str, float] = {}
        drop_by_barcode: Dict[str, float] = {}

        if state_path.exists():
            try:
                sig_by_id = json.loads(state_path.read_text(encoding="utf-8"))
                if not isinstance(sig_by_id, dict):
                    sig_by_id = {}
            except Exception:
                sig_by_id = {}

        if drop_path.exists():
            try:
                payload = json.loads(drop_path.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    drop_by_id = payload.get("drop_by_id") or {}
                    drop_by_barcode = payload.get("drop_by_barcode") or {}
                    # нормализация типов
                    drop_by_id = {str(k): float(v) for k, v in drop_by_id.items() if str(k).strip()}
                    drop_by_barcode = {str(k): float(v) for k, v in drop_by_barcode.items() if str(k).strip()}
            except Exception:
                drop_by_id = {}
                drop_by_barcode = {}

        return cls(sig_by_id=sig_by_id, drop_by_id=drop_by_id, drop_by_barcode=drop_by_barcode)

    def save(self, code: str) -> None:
        state_path, drop_path = self._state_paths(code)

        tmp_state = state_path.with_suffix(".json.partial")
        tmp_drop = drop_path.with_suffix(".json.partial")

        tmp_state.write_text(json.dumps(self.sig_by_id, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_drop.write_text(
            json.dumps(
                {"drop_by_id": self.drop_by_id, "drop_by_barcode": self.drop_by_barcode},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        os.replace(tmp_state, state_path)
        os.replace(tmp_drop, drop_path)


def _make_signature(*, item_id: str, name: str, barcode: str, drop_price: float) -> str:
    # сигнатура должна меняться при любом значимом изменении
    return f"{item_id}|{name}|{barcode}|{drop_price:.4f}"


# ──────────────────────────────────────────────────────────────────────────────
# CATALOG: Excel parsing (строка заголовков = 4)
# ──────────────────────────────────────────────────────────────────────────────

D10_REQUIRED_HEADERS = {
    "Артикул": "id",
    "Найменування": "name",
    "Штрихкод": "barcode",
    "drop, грн.": "drop_price_raw",
}

def _parse_d10_catalog_excel_bytes(data: bytes) -> List[Dict[str, Any]]:
    """
    Возвращает список записей:
    [
      {"id": "...", "name": "...", "barcode": "...", "drop_price": 12.34},
      ...
    ]
    """
    wb = load_workbook(io.BytesIO(data), data_only=True)
    sheet = wb.active

    # Заголовки в строке 4
    header_row = list(sheet.iter_rows(min_row=D10_HEADER_ROW, max_row=D10_HEADER_ROW, values_only=True))[0]
    headers: Dict[str, int] = {
        str(value).strip(): idx
        for idx, value in enumerate(header_row)
        if value is not None and str(value).strip()
    }

    # Проверка обязательных колонок
    for col in D10_REQUIRED_HEADERS.keys():
        if col not in headers:
            raise ValueError(f"D10: В Excel не найден обязательный столбец '{col}' (ожидаем заголовки в строке {D10_HEADER_ROW})")

    idx_map = {k: headers[k] for k in D10_REQUIRED_HEADERS.keys()}

    items: List[Dict[str, Any]] = []
    for row in sheet.iter_rows(min_row=D10_HEADER_ROW + 1, values_only=True):
        raw_id = row[idx_map["Артикул"]] if idx_map["Артикул"] < len(row) else None
        raw_name = row[idx_map["Найменування"]] if idx_map["Найменування"] < len(row) else None
        raw_barcode = row[idx_map["Штрихкод"]] if idx_map["Штрихкод"] < len(row) else None
        raw_drop = row[idx_map["drop, грн."]] if idx_map["drop, грн."] < len(row) else None

        item_id = _norm_str(raw_id)
        name = _norm_str(raw_name)
        barcode = _norm_str(raw_barcode)
        drop_price = _to_float(_norm_str(raw_drop))

        row_obj = {
            "id": item_id,
            "name": name,
            "barcode": barcode,
            "drop_price": drop_price,
            "drop_price_raw": _norm_str(raw_drop),
        }

        if _is_probably_junk_row(row_obj):
            continue

        # Минимальная валидация: должен быть артикул и название (по опыту)
        if not item_id or not name:
            continue

        items.append(row_obj)

    logger.info("D10: Каталог Excel распарсен, позиций: %d", len(items))
    return items


def _apply_delta_and_update_state(
    *,
    code: str,
    items: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """
    Возвращает только изменившиеся позиции для передачи в import_catalog.py:
      [{"id": ..., "name": ..., "barcode": ...}, ...]

    И обновляет стейт:
      - sig_by_id
      - drop_by_id, drop_by_barcode
    """
    state = D10State.load(code)
    is_first_run = len(state.sig_by_id) == 0

    changed: List[Dict[str, str]] = []

    new_sig_by_id: Dict[str, str] = dict(state.sig_by_id)
    new_drop_by_id: Dict[str, float] = dict(state.drop_by_id)
    new_drop_by_barcode: Dict[str, float] = dict(state.drop_by_barcode)

    for it in items:
        item_id = _norm_str(it.get("id"))
        name = _norm_str(it.get("name"))
        barcode = _norm_str(it.get("barcode"))
        drop_price = float(it.get("drop_price") or 0.0)

        sig = _make_signature(item_id=item_id, name=name, barcode=barcode, drop_price=drop_price)
        prev_sig = state.sig_by_id.get(item_id)

        # первый запуск: отдаём всё
        if is_first_run or (prev_sig != sig):
            changed.append({"id": item_id, "name": name, "barcode": barcode})

        # обновляем стейт
        new_sig_by_id[item_id] = sig
        new_drop_by_id[item_id] = drop_price
        if barcode:
            new_drop_by_barcode[barcode] = drop_price

    # сохраняем
    D10State(sig_by_id=new_sig_by_id, drop_by_id=new_drop_by_id, drop_by_barcode=new_drop_by_barcode).save(code)

    logger.info(
        "D10: Delta: first_run=%s, total=%d, changed=%d",
        is_first_run,
        len(items),
        len(changed),
    )
    return changed


# ──────────────────────────────────────────────────────────────────────────────
# STOCK: XML feed from dropship_enterprises.feed_url
# ──────────────────────────────────────────────────────────────────────────────

async def _download_xml_feed(*, url: str, timeout: int) -> Optional[bytes]:
    headers = {"User-Agent": "Mozilla/5.0", "accept": "application/xml,text/xml,*/*"}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=timeout) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content
    except Exception as e:
        msg = f"D10: Ошибка загрузки XML фида {url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return None


def _find_child_text(elem: ET.Element, wanted_local_names: List[str]) -> str:
    """
    Ищет текст дочернего элемента по localname (без namespace), например:
      wanted_local_names=["mpn"] найдёт <g:mpn>...</g:mpn>
    """
    for ch in list(elem):
        if _strip_ns(ch.tag) in wanted_local_names:
            return (ch.text or "").strip()
    return ""


def _collect_item_nodes(root: ET.Element) -> List[ET.Element]:
    items = root.findall(".//item")
    if items:
        return items
    # fallback: любые item/offer
    return [el for el in root.iter() if _strip_ns(el.tag).lower() in ("item", "offer")]


def _get_drop_price_for_code_sup(state: D10State, code_sup: str) -> float:
    code_sup = (code_sup or "").strip()
    if not code_sup:
        return 0.0
    # основной кейс: g:mpn совпадает с Артикул
    if code_sup in state.drop_by_id:
        return float(state.drop_by_id.get(code_sup) or 0.0)
    # fallback: иногда mpn может совпасть с barcode (редко, но пусть будет)
    if code_sup in state.drop_by_barcode:
        return float(state.drop_by_barcode.get(code_sup) or 0.0)
    return 0.0


async def parse_feed_stock_to_json(
    *,
    code: str = D10_CODE_DEFAULT,
    timeout: int = 60,
) -> str:
    """
    Сток D10:
      - feed_url берём из dropship_enterprises.feed_url
      - парсим <item>
      - g:mpn -> code_sup
      - g:availability == "in stock" -> qty=1 (иначе пропускаем)
      - g:price -> price_retail (int)
      - price_opt -> из сохранённого drop, грн. (по code_sup как по Артикулу)
    """
    feed_url = await _get_feed_url_by_code(code)
    if not feed_url:
        msg = f"D10: Не найден feed_url в dropship_enterprises для code='{code}'"
        logger.error(msg)
        send_notification(msg, "Разработчик")
        return "[]"

    xml_bytes = await _download_xml_feed(url=feed_url, timeout=timeout)
    if not xml_bytes:
        return "[]"

    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        msg = f"D10: Ошибка парсинга XML: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return "[]"

    state = D10State.load(code)

    rows: List[Dict[str, Any]] = []
    items = _collect_item_nodes(root)

    for it in items:
        # availability
        availability = _find_child_text(it, ["availability"]).strip().lower()
        if availability != "in stock":
            continue

        code_sup = _find_child_text(it, ["mpn"]).strip()
        if not code_sup:
            # fallback: иногда могут класть в g:id, но по ТЗ нужен mpn
            code_sup = _find_child_text(it, ["id"]).strip()
        if not code_sup:
            continue

        price_raw = _find_child_text(it, ["price"])
        price_retail = int(_to_float(price_raw))

        qty = 1  # строго по ТЗ
        price_opt = _get_drop_price_for_code_sup(state, code_sup)
        price_opt = round(float(price_opt), 2)

        rows.append(
            {
                "code_sup": code_sup,
                "qty": qty,
                "price_retail": price_retail,
                "price_opt": price_opt,
            }
        )

    logger.info("D10: Сток распарсен, позиций in stock: %d", len(rows))
    return json.dumps(rows, ensure_ascii=False, indent=2)


# ──────────────────────────────────────────────────────────────────────────────
# CATALOG: Google Drive Excel -> delta JSON
# ──────────────────────────────────────────────────────────────────────────────

async def parse_feed_catalog_to_json(
    *,
    code: str = D10_CODE_DEFAULT,
    timeout: int = 30,  # совместимость
) -> str:
    """
    Каталог D10:
      1) gdrive_folder берём из dropship_enterprises по code
      2) в папке 1 Excel файл
      3) заголовки в строке 4
      4) выдаём: первый раз все, далее только изменения
      5) сохраняем drop, грн. в локальный кэш для стока
    """
    try:
        folder_id = await _get_gdrive_folder_by_code(code)
        if not folder_id:
            raise RuntimeError(f"D10: Не найден gdrive_folder в dropship_enterprises для code='{code}'")

        drive_service = await _connect_to_google_drive()
        file_meta = await _fetch_single_file_metadata(drive_service, folder_id)
        file_id = file_meta["id"]
        file_name = file_meta.get("name") or "catalog.xlsx"
        logger.info("D10: Найден файл каталога: %s (%s)", file_name, file_id)

        file_bytes = await _download_file_bytes(drive_service, file_id)
        all_items = _parse_d10_catalog_excel_bytes(file_bytes)

        changed_items = _apply_delta_and_update_state(code=code, items=all_items)

        return json.dumps(changed_items, ensure_ascii=False, indent=2)

    except Exception as e:
        msg = f"D10: Ошибка обработки каталога из Google Drive: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return "[]"


async def parse_feed_to_json(
    *,
    mode: Literal["catalog", "stock"] = "catalog",
    code: str = D10_CODE_DEFAULT,
    timeout: int = 60,
) -> str:
    if mode == "catalog":
        return await parse_feed_catalog_to_json(code=code, timeout=timeout)
    if mode == "stock":
        return await parse_feed_stock_to_json(code=code, timeout=timeout)
    raise ValueError("mode must be 'catalog' or 'stock'")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Парсер поставщика D10 (ZooHub): "
            "catalog (Excel из Google Drive, заголовки в строке 4, first full then delta, cache drop price) "
            "и stock (XML feed_url из dropship_enterprises, mpn->code_sup, price_opt from cached drop)."
        )
    )
    parser.add_argument("--mode", choices=["catalog", "stock"], default="catalog")
    parser.add_argument("--code", default=D10_CODE_DEFAULT)
    parser.add_argument("--timeout", type=int, default=60)

    args = parser.parse_args()
    out = asyncio.run(parse_feed_to_json(mode=args.mode, code=args.code, timeout=args.timeout))
    print(out)
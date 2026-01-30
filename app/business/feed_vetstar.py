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
from sqlalchemy import text

from app.database import get_async_db
from app.services.notification_service import send_notification

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === D12 (VetStar) ===
D12_CODE_DEFAULT = "D12"

# Кэш дельты каталога
DEFAULT_STATE_DIR = Path(os.getenv("D12_STATE_DIR") or "state_cache")


# ──────────────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────────────

def _norm_str(v: Any) -> str:
    s = ("" if v is None else str(v)).strip()
    if s.lower() == "none":
        return ""
    return s


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    s = str(v).replace("\u00A0", " ").strip()
    if not s:
        return 0.0
    m = re.search(r"-?\d+(?:[.,]\d+)?", s)
    if not m:
        return 0.0
    try:
        return float(m.group(0).replace(",", "."))
    except Exception:
        return 0.0


def _make_signature(*, item_id: str, name: str, barcode: str) -> str:
    return f"{item_id}|{name}|{barcode}"


def _parse_qty_plus(value: Any) -> int:
    """
    D12 qty rule:
      '+' => 1, '++' => 2, '+++' => 3
      everything else => 0 (skip row)
    """
    s = _norm_str(value)
    if re.fullmatch(r"\+{1,3}", s):
        return len(s)
    return 0


def _is_header_like(v: str) -> bool:
    s = (v or "").strip().lower()
    return s in {"артикул", "найменування", "штрихкод", "вільний залишок", "залишок"}


# ──────────────────────────────────────────────────────────────────────────────
# DB
# ──────────────────────────────────────────────────────────────────────────────

async def _get_feed_url_by_code(code: str) -> Optional[str]:
    async with get_async_db() as session:
        res = await session.execute(
            text("SELECT feed_url FROM dropship_enterprises WHERE code = :code LIMIT 1"),
            {"code": code},
        )
        return res.scalar_one_or_none()


# ──────────────────────────────────────────────────────────────────────────────
# download
# ──────────────────────────────────────────────────────────────────────────────

async def _download_excel_bytes(*, url: str, timeout: int) -> Optional[bytes]:
    headers = {"User-Agent": "Mozilla/5.0", "accept": "*/*"}
    try:
        async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.content
    except Exception as e:
        msg = f"D12: Ошибка загрузки Excel по url={url}: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return None


# ──────────────────────────────────────────────────────────────────────────────
# XLS reader (xlrd)
# ──────────────────────────────────────────────────────────────────────────────

def _read_xls_first_sheet_rows(data: bytes) -> List[List[Any]]:
    """
    Returns rows as list-of-lists from the first sheet.
    Requires xlrd for .xls.
    """
    try:
        import xlrd  # type: ignore
    except Exception:
        raise RuntimeError(
            "D12: Для чтения .xls нужен пакет xlrd. Установи: pip install xlrd==2.0.1"
        )

    book = xlrd.open_workbook(file_contents=data)
    sheet = book.sheet_by_index(0)

    rows: List[List[Any]] = []
    for r in range(sheet.nrows):
        rows.append(sheet.row_values(r))
    return rows


def _get_cell(row: List[Any], idx_0: int) -> Any:
    return row[idx_0] if idx_0 < len(row) else None


# ──────────────────────────────────────────────────────────────────────────────
# STATE (delta catalog)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class D12State:
    sig_by_id: Dict[str, str]

    @classmethod
    def empty(cls) -> "D12State":
        return cls(sig_by_id={})

    @staticmethod
    def _state_path(code: str) -> Path:
        DEFAULT_STATE_DIR.mkdir(parents=True, exist_ok=True)
        return DEFAULT_STATE_DIR / f"{code.lower()}_catalog_state.json"

    @classmethod
    def load(cls, code: str) -> "D12State":
        p = cls._state_path(code)
        if not p.exists():
            return cls.empty()
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                payload = {}
            payload = {str(k): str(v) for k, v in payload.items() if str(k).strip()}
            return cls(sig_by_id=payload)
        except Exception:
            return cls.empty()

    def save(self, code: str) -> None:
        p = self._state_path(code)
        tmp = p.with_suffix(".json.partial")
        tmp.write_text(json.dumps(self.sig_by_id, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, p)


def _apply_delta_and_update_state(*, code: str, items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    state = D12State.load(code)
    is_first = len(state.sig_by_id) == 0

    changed: List[Dict[str, str]] = []
    new_sig = dict(state.sig_by_id)

    for it in items:
        item_id = _norm_str(it.get("id"))
        name = _norm_str(it.get("name"))
        barcode = _norm_str(it.get("barcode"))

        if not item_id or not name:
            continue

        sig = _make_signature(item_id=item_id, name=name, barcode=barcode)
        prev = state.sig_by_id.get(item_id)

        if is_first or prev != sig:
            changed.append({"id": item_id, "name": name, "barcode": barcode})

        new_sig[item_id] = sig

    D12State(sig_by_id=new_sig).save(code)
    logger.info("D12: Delta catalog: first_run=%s total=%d changed=%d", is_first, len(items), len(changed))
    return changed


# ──────────────────────────────────────────────────────────────────────────────
# PARSE: catalog / stock from same XLS
# ──────────────────────────────────────────────────────────────────────────────

def _parse_catalog_from_rows(rows: List[List[Any]]) -> List[Dict[str, str]]:
    """
    колонка B (idx=1): Артикул
    колонка C (idx=2): Найменування
    колонка E (idx=4): Штрихкод
    """
    out: List[Dict[str, str]] = []

    for row in rows:
        item_id = _norm_str(_get_cell(row, 1))
        name = _norm_str(_get_cell(row, 2))
        barcode = _norm_str(_get_cell(row, 4))

        if not item_id or _is_header_like(item_id):
            continue
        if not name or _is_header_like(name):
            continue

        out.append({"id": item_id, "name": name, "barcode": barcode})

    logger.info("D12: Catalog parsed: %d rows", len(out))
    return out


def _parse_stock_from_rows(rows: List[List[Any]]) -> List[Dict[str, Any]]:
    """
    B (idx=1): Артикул -> code_sup
    D (idx=3): Вільний залишок -> qty mapping by '+' count (1..3)
    G (idx=6): price_opt
    I (idx=8): price_retail
    """
    out: List[Dict[str, Any]] = []

    for row in rows:
        code_sup = _norm_str(_get_cell(row, 1))
        if not code_sup or _is_header_like(code_sup):
            continue

        qty_raw = _get_cell(row, 3)
        qty = _parse_qty_plus(qty_raw)
        if qty <= 0:
            continue

        price_opt = round(float(_to_float(_get_cell(row, 6))), 2)
        price_retail = int(_to_float(_get_cell(row, 8)))

        out.append(
            {
                "code_sup": code_sup,
                "qty": qty,
                "price_retail": price_retail,
                "price_opt": price_opt,
            }
        )

    logger.info("D12: Stock parsed (qty>0): %d rows", len(out))
    return out


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ──────────────────────────────────────────────────────────────────────────────

async def parse_feed_catalog_to_json(*, code: str = D12_CODE_DEFAULT, timeout: int = 60) -> str:
    """
    1) feed_url из dropship_enterprises
    2) скачиваем .xls
    3) парсим catalog
    4) first run => all, далее => delta
    """
    try:
        url = await _get_feed_url_by_code(code)
        if not url:
            raise RuntimeError(f"D12: Не найден feed_url в dropship_enterprises для code='{code}'")

        data = await _download_excel_bytes(url=url, timeout=timeout)
        if not data:
            return "[]"

        rows = _read_xls_first_sheet_rows(data)
        items = _parse_catalog_from_rows(rows)
        changed = _apply_delta_and_update_state(code=code, items=items)

        return json.dumps(changed, ensure_ascii=False, indent=2)
    except Exception as e:
        msg = f"D12: Ошибка обработки catalog: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return "[]"


async def parse_feed_stock_to_json(*, code: str = D12_CODE_DEFAULT, timeout: int = 60) -> str:
    """
    1) feed_url из dropship_enterprises
    2) скачиваем .xls
    3) парсим stock (qty по '+')
    """
    try:
        url = await _get_feed_url_by_code(code)
        if not url:
            raise RuntimeError(f"D12: Не найден feed_url в dropship_enterprises для code='{code}'")

        data = await _download_excel_bytes(url=url, timeout=timeout)
        if not data:
            return "[]"

        rows = _read_xls_first_sheet_rows(data)
        items = _parse_stock_from_rows(rows)

        return json.dumps(items, ensure_ascii=False, indent=2)
    except Exception as e:
        msg = f"D12: Ошибка обработки stock: {e}"
        logger.exception(msg)
        send_notification(msg, "Разработчик")
        return "[]"


async def parse_feed_to_json(
    *,
    mode: Literal["catalog", "stock"] = "catalog",
    code: str = D12_CODE_DEFAULT,
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
            "D12 (VetStar) parser: one XLS for both catalog+stock. "
            "Catalog: B=id, C=name, E=barcode with delta cache. "
            "Stock: B=code_sup, D='+' qty, G=price_opt, I=price_retail."
        )
    )
    parser.add_argument("--mode", choices=["catalog", "stock"], default="catalog")
    parser.add_argument("--code", default=D12_CODE_DEFAULT)
    parser.add_argument("--timeout", type=int, default=60)

    args = parser.parse_args()
    print(asyncio.run(parse_feed_to_json(mode=args.mode, code=args.code, timeout=args.timeout)))
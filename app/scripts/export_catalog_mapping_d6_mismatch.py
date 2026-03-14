"""Скрипт для поиска грубых расхождений между Name и Name_<SUPPLIER_CODE> в catalog_mapping."""

from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

try:
    import pandas as pd
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: pandas. Install with: pip install pandas") from exc

try:
    from rapidfuzz import fuzz
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: rapidfuzz. Install with: pip install rapidfuzz") from exc


BASE_OUTPUT_DIR = Path("/Users/dmitrijnazdrin/inventory_service_1")


def normalize_text(value: Any) -> str:
    text_val = "" if value is None else str(value)
    text_val = text_val.lower().strip()
    text_val = re.sub(r"[\u2013\u2014]", "-", text_val)
    text_val = re.sub(r"[^\w\s-]", " ", text_val)
    text_val = text_val.replace("_", " ").replace("-", " ")
    text_val = re.sub(r"\s+", " ", text_val).strip()
    return text_val


def token_overlap(name1: str, name2: str) -> float:
    tokens1 = set(normalize_text(name1).split())
    tokens2 = set(normalize_text(name2).split())
    if not tokens1 or not tokens2:
        return 0.0
    inter = len(tokens1 & tokens2)
    base = max(len(tokens1), len(tokens2))
    return inter / base if base else 0.0


def _norm_num(num_str: str) -> str:
    val = num_str.replace(",", ".").strip()
    if "." in val:
        val = val.rstrip("0").rstrip(".")
    return val


def _extract_form_markers(name: Any) -> set[str]:
    """
    Вытаскивает числовые маркеры формы выпуска:
    - вес/объем (300 г, 500 мл, 0.5 кг, 1000 мг)
    - количество в упаковке (60 капсул, 80 табл, №60, 30 шт)
    """
    s = normalize_text(name)
    if not s:
        return set()

    markers: set[str] = set()

    # Вес/объем
    unit_map = {
        "мг": "mg",
        "mg": "mg",
        "г": "g",
        "гр": "g",
        "g": "g",
        "кг": "kg",
        "kg": "kg",
        "мл": "ml",
        "ml": "ml",
        "л": "l",
        "l": "l",
    }
    for n, unit in re.findall(r"(\\d+(?:[.,]\\d+)?)\\s*(мг|mg|г|гр|g|кг|kg|мл|ml|л|l)\\b", s, flags=re.IGNORECASE):
        key = f"{_norm_num(n)}_{unit_map.get(unit.lower(), unit.lower())}"
        markers.add(key)

    # Количество единиц
    for n, _unit in re.findall(
        r"(\\d+)\\s*(капсул(?:а|ы)?|caps?|таблет(?:ка|ки)?|табл|саше|шт|pcs?)\\b",
        s,
        flags=re.IGNORECASE,
    ):
        markers.add(f"{int(n)}_count")

    for n in re.findall(r"№\\s*(\\d+)", s):
        markers.add(f"{int(n)}_count")

    return markers


def _has_form_mismatch(name1: Any, name2: Any) -> bool:
    m1 = _extract_form_markers(name1)
    m2 = _extract_form_markers(name2)
    if not m1 or not m2:
        return False
    return len(m1 & m2) == 0


def is_strong_mismatch(name1: Any, name2: Any, score_threshold: float = 45.0, overlap_threshold: float = 0.20) -> bool:
    n1_raw = "" if name1 is None else str(name1).strip()
    n2_raw = "" if name2 is None else str(name2).strip()

    # Пустые названия считаем подозрительными
    if not n1_raw or not n2_raw:
        return True

    n1 = normalize_text(n1_raw)
    n2 = normalize_text(n2_raw)

    if not n1 or not n2:
        return True

    score = max(fuzz.token_sort_ratio(n1, n2), fuzz.ratio(n1, n2))
    overlap = token_overlap(n1, n2)

    if _has_form_mismatch(n1_raw, n2_raw):
        return True

    return score < score_threshold and overlap < overlap_threshold


async def fetch_rows(database_url: str, supplier_code: str) -> List[Dict[str, Any]]:
    code_col = f"Code_{supplier_code}"
    name_col = f"Name_{supplier_code}"

    engine = create_async_engine(database_url, future=True)
    try:
        async with engine.connect() as conn:
            query = text(
                f'''
                SELECT "ID", "Name", "Barcode", "{name_col}" AS "Name_supplier", "{code_col}" AS "Code_supplier"
                FROM catalog_mapping
                WHERE "{code_col}" IS NOT NULL
                  AND btrim("{code_col}") <> ''
                '''
            )
            result = await conn.execute(query)
            return [dict(row) for row in result.mappings().all()]
    finally:
        await engine.dispose()


def export_to_excel(rows: List[Dict[str, Any]], output_path: Path, supplier_code: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    columns = ["ID", "Name", "Barcode", f"Name_{supplier_code}", f"Code_{supplier_code}"]
    df = pd.DataFrame(rows, columns=columns)
    df.to_excel(output_path, index=False, engine="openpyxl")


async def main() -> None:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        print("ERROR: DATABASE_URL is not set in .env")
        sys.exit(1)

    supplier_code = (os.getenv("SUPPLIER_CODE") or os.getenv("MISMATCH_SUPPLIER_CODE") or "D6").strip().upper()
    if not re.fullmatch(r"[A-Z]\d{1,3}", supplier_code):
        print(f"ERROR: Invalid SUPPLIER_CODE={supplier_code!r}. Expected format like D2, D3, D10.")
        sys.exit(1)

    output_path = BASE_OUTPUT_DIR / f"catalog_mapping_{supplier_code.lower()}_mismatch.xlsx"

    checked = 0
    mismatches: List[Dict[str, Any]] = []

    try:
        rows = await fetch_rows(database_url, supplier_code)
        checked = len(rows)

        for row in rows:
            name = row.get("Name")
            name_supplier = row.get("Name_supplier")
            if is_strong_mismatch(name, name_supplier):
                mismatches.append(
                    {
                        "ID": row.get("ID"),
                        "Name": name,
                        "Barcode": row.get("Barcode"),
                        f"Name_{supplier_code}": name_supplier,
                        f"Code_{supplier_code}": row.get("Code_supplier"),
                    }
                )

        export_to_excel(mismatches, output_path, supplier_code)

        print(f"Supplier code: {supplier_code}")
        print(f"Checked rows: {checked}")
        print(f"Mismatch rows: {len(mismatches)}")
        print(f"Output file: {output_path}")

    except ModuleNotFoundError as exc:
        missing = exc.name or "unknown"
        print(f"ERROR: Missing dependency: {missing}")
        if missing in {"openpyxl", "pandas", "rapidfuzz"}:
            print(f"Install with: pip install {missing}")
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

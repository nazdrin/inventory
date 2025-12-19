import io
import json
import os
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd

# Google Drive
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


# === НАСТРОЙКИ КОЛОНОК (как и раньше) ===
CODE_COL = "Код товара Tabletki.ua"
PRICE_RETAIL_COL = "Цена розн."
FALLBACK_PRICE_COL = "Цена"  # на всякий случай, если "Цена розн." нет


# === ENV НАСТРОЙКИ ===
# Можно оставить старый ROOT_DIR хардкодом, но лучше через env
ROOT_DIR = Path(os.getenv("COMPETITORS_ROOT_DIR", "/Users/dmitrijnazdrin/Documents/Competitors"))

# Список городов для раскладки Total (формат env: COMPETITOR_CITIES=Kyiv,Lviv,Odessa)
COMPETITOR_CITIES = [c.strip() for c in os.getenv("COMPETITOR_CITIES", "").split(",") if c.strip()]

# Google Drive folder id
COMPETITOR_GDRIVE_FOLDER_ID = os.getenv("COMPETITOR_GDRIVE_FOLDER_ID", "").strip()

# Service account credentials path
GOOGLE_DRIVE_CREDENTIALS_PATH = os.getenv("GOOGLE_DRIVE_CREDENTIALS_PATH", "").strip()

# Имена файлов на Google Drive
TOTAL_FILENAME = os.getenv("COMPETITOR_TOTAL_FILENAME", "competitors_delivery_total.json").strip()
CITY_FILENAME_TEMPLATE = os.getenv("COMPETITOR_CITY_FILENAME_TEMPLATE", "competitors_delivery_{city}.json").strip()


def _is_valid_drive_folder_id(folder_id: str) -> bool:
    """Basic sanity-check for Google Drive folder id (avoid placeholders/typos)."""
    if not folder_id:
        return False
    # common placeholder patterns
    low = folder_id.lower()
    if "твой" in low or "your" in low or "id_" in low:
        return False
    # must not contain spaces or quotes
    if any(ch.isspace() for ch in folder_id) or "'" in folder_id or '"' in folder_id:
        return False
    # Google IDs are typically URL-safe base64-like (letters/digits/_-)
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" )
    if any(ch not in allowed for ch in folder_id):
        return False
    # minimal length
    return len(folder_id) >= 10


def _normalize_code_series(s: pd.Series) -> pd.Series:
    """Приводим коду к строковому виду без .0 и лишних пробелов."""
    # Сначала в numeric, чтобы убрать .0, потом обратно в Int/str
    # Но часть кодов может быть строкой — делаем максимально безопасно.
    def norm_one(v: Any) -> Optional[str]:
        if pd.isna(v):
            return None
        if isinstance(v, (int,)):
            return str(v)
        if isinstance(v, float):
            # 1080612.0 -> 1080612
            if v.is_integer():
                return str(int(v))
            return str(v)
        # string
        t = str(v).strip()
        if t.endswith(".0"):
            t = t[:-2]
        return t

    return s.map(norm_one)


def _normalize_price_series(s: pd.Series) -> pd.Series:
    """Приводим цену к float, поддерживаем '123,45'."""
    # приводим к str, заменяем запятую на точку, затем to_numeric
    s2 = s.astype(str).str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s2, errors="coerce")


def read_excels_min_price(folder: Path) -> pd.DataFrame:
    """Читает все xlsx в папке и возвращает DataFrame [code, price] c min price по code."""
    excel_files = list(folder.glob("*.xlsx"))
    if not excel_files:
        return pd.DataFrame(columns=["code", "price"])

    frames: List[pd.DataFrame] = []

    for file_path in excel_files:
        print(f"  - читаю файл: {file_path.name}")
        try:
            df = pd.read_excel(file_path)
        except Exception as e:
            print(f"    ❌ Ошибка чтения {file_path.name}: {e}")
            continue

        if CODE_COL not in df.columns:
            print(f"    ⚠ Нет колонки '{CODE_COL}', файл пропускаю.")
            continue

        if PRICE_RETAIL_COL in df.columns:
            price_col = PRICE_RETAIL_COL
        elif FALLBACK_PRICE_COL in df.columns:
            price_col = FALLBACK_PRICE_COL
        else:
            print(
                f"    ⚠ Нет колонок '{PRICE_RETAIL_COL}' или '{FALLBACK_PRICE_COL}', файл пропускаю."
            )
            continue

        tmp = df[[CODE_COL, price_col]].copy()
        tmp.rename(columns={CODE_COL: "code", price_col: "price"}, inplace=True)

        tmp["code"] = _normalize_code_series(tmp["code"])
        tmp["price"] = _normalize_price_series(tmp["price"])

        tmp = tmp.dropna(subset=["code", "price"])

        frames.append(tmp)

    if not frames:
        return pd.DataFrame(columns=["code", "price"])

    all_data = pd.concat(frames, ignore_index=True)

    # min price by code
    result = all_data.groupby("code", as_index=False)["price"].min()
    return result


def delete_local_excels(folder: Path) -> None:
    excel_files = list(folder.glob("*.xlsx"))
    for file_path in excel_files:
        try:
            file_path.unlink()
            print(f"  🗑 Удалён файл: {file_path.name}")
        except Exception as e:
            print(f"  ❌ Не удалось удалить {file_path.name}: {e}")


def build_drive_service(credentials_path: str):
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=scopes)
    return build("drive", "v3", credentials=creds)


def gdrive_find_files_by_name(service, folder_id: str, filename: str) -> List[Dict[str, str]]:
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = service.files().list(q=q, fields="files(id,name)", pageSize=100).execute()
    return resp.get("files", [])


def gdrive_delete_file(service, file_id: str) -> None:
    service.files().delete(fileId=file_id).execute()


def gdrive_download_json(service, file_id: str) -> List[Dict[str, Any]]:
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    raw = fh.read().decode("utf-8")
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def gdrive_create_json(service, folder_id: str, filename: str, rows: List[Dict[str, Any]]) -> str:
    payload = json.dumps(rows, ensure_ascii=False)
    media = MediaIoBaseUpload(io.BytesIO(payload.encode("utf-8")), mimetype="application/json", resumable=False)
    metadata = {"name": filename, "parents": [folder_id]}
    created = service.files().create(body=metadata, media_body=media, fields="id").execute()
    return created["id"]


def gdrive_update_json(service, file_id: str, rows: List[Dict[str, Any]]) -> None:
    payload = json.dumps(rows, ensure_ascii=False)
    media = MediaIoBaseUpload(io.BytesIO(payload.encode("utf-8")), mimetype="application/json", resumable=False)
    service.files().update(fileId=file_id, media_body=media).execute()


def gdrive_upsert_total_by_city(
    service,
    folder_id: str,
    total_filename: str,
    city: str,
    incoming_rows: List[Dict[str, Any]],
) -> None:
    """Create TOTAL file if missing; otherwise update only rows for the given city inside TOTAL."""
    existing_files = gdrive_find_files_by_name(service, folder_id, total_filename)

    if not existing_files:
        # Если TOTAL еще не создан — создаем файл только с данным городом
        new_id = gdrive_create_json(service, folder_id, total_filename, incoming_rows)
        print(f"  ✅ Total JSON создан (не было файла) и содержит обновления для города {city}: {total_filename} ({new_id})")
        return

    file_id = existing_files[0]["id"]
    existing_rows = gdrive_download_json(service, file_id)
    merged = merge_city_rows(existing_rows, incoming_rows, city)
    gdrive_update_json(service, file_id, merged)
    print(f"  ✅ Total JSON обновлён по городу {city}: {total_filename} ({file_id})")


def expand_total_to_cities(min_prices_df: pd.DataFrame, cities: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, r in min_prices_df.iterrows():
        code = str(r["code"])  # уже строка
        price = float(r["price"])  # float
        for city in cities:
            rows.append({"code": code, "city": city, "delivery_price": price})
    return rows


def city_rows_from_df(min_prices_df: pd.DataFrame, city: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, r in min_prices_df.iterrows():
        rows.append({"code": str(r["code"]), "city": city, "delivery_price": float(r["price"])})
    return rows


def merge_city_rows(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]], city: str) -> List[Dict[str, Any]]:
    """Обновляем/добавляем записи только для указанного города (ключ: code+city)."""
    # Индекс существующих
    index: Dict[tuple, Dict[str, Any]] = {}
    kept: List[Dict[str, Any]] = []

    for row in existing:
        try:
            c = str(row.get("code", "")).strip()
            ct = str(row.get("city", "")).strip()
            if not c or not ct:
                continue
            key = (c, ct)
            index[key] = {"code": c, "city": ct, "delivery_price": float(row.get("delivery_price"))}
        except Exception:
            continue

    # Применяем входящие обновления
    for row in incoming:
        c = str(row.get("code", "")).strip()
        ct = str(row.get("city", "")).strip()
        if not c or not ct:
            continue
        if ct != city:
            continue
        try:
            p = float(row.get("delivery_price"))
        except Exception:
            continue
        index[(c, ct)] = {"code": c, "city": ct, "delivery_price": p}

    # Собираем обратно список
    for _, v in index.items():
        kept.append(v)

    # Стабильная сортировка для удобства
    kept.sort(key=lambda x: (x["city"], x["code"]))
    return kept


def process_total(service) -> None:
    total_dir = ROOT_DIR / "Total"
    if not total_dir.exists() or not total_dir.is_dir():
        print("▶ Папка Total не найдена — пропускаю Total")
        return

    print("\n▶ Обработка папки: Total")

    if not COMPETITOR_CITIES:
        print("  ⚠ COMPETITOR_CITIES пустой. Total обработать нельзя (нужно список городов для раскладки).")
        return

    min_df = read_excels_min_price(total_dir)
    if min_df.empty:
        print("  ⚠ В Total нет данных (xlsx пустые/непрочитаны) — пропускаю.")
        # всё равно чистим xlsx
        delete_local_excels(total_dir)
        return

    rows = expand_total_to_cities(min_df, COMPETITOR_CITIES)

    # На Total: удаляем старый файл в GDrive папке и грузим новый
    if not COMPETITOR_GDRIVE_FOLDER_ID:
        print("  ❌ Не задан COMPETITOR_GDRIVE_FOLDER_ID — не могу загрузить Total JSON.")
    else:
        existing_files = gdrive_find_files_by_name(service, COMPETITOR_GDRIVE_FOLDER_ID, TOTAL_FILENAME)
        for f in existing_files:
            try:
                gdrive_delete_file(service, f["id"])
                print(f"  🗑 Удалён старый файл на Google Drive: {f['name']} ({f['id']})")
            except Exception as e:
                print(f"  ❌ Не удалось удалить файл {f['name']} ({f['id']}): {e}")

        try:
            new_id = gdrive_create_json(service, COMPETITOR_GDRIVE_FOLDER_ID, TOTAL_FILENAME, rows)
            print(f"  ✅ Total JSON загружен на Google Drive: {TOTAL_FILENAME} ({new_id})")
        except Exception as e:
            print(f"  ❌ Ошибка загрузки Total JSON на Google Drive: {e}")

    # Удаляем локальные excel
    delete_local_excels(total_dir)


def process_city_folder(service, city_dir: Path) -> None:
    city = city_dir.name
    print(f"\n▶ Обработка папки: {city}")

    min_df = read_excels_min_price(city_dir)
    if min_df.empty:
        print("  ⚠ Нет данных для города (xlsx пустые/непрочитаны) — пропускаю.")
        delete_local_excels(city_dir)
        return

    incoming_rows = city_rows_from_df(min_df, city)

    if not _is_valid_drive_folder_id(COMPETITOR_GDRIVE_FOLDER_ID):
        print(
            "  ❌ COMPETITOR_GDRIVE_FOLDER_ID не задан или некорректен. "
            "Укажи реальный ID папки Google Drive (строка из URL папки)."
        )
        delete_local_excels(city_dir)
        return

    # Требование: данные из папок городов должны дополнять/перезаписывать TOTAL-файл
    try:
        gdrive_upsert_total_by_city(
            service,
            COMPETITOR_GDRIVE_FOLDER_ID,
            TOTAL_FILENAME,
            city,
            incoming_rows,
        )
    except Exception as e:
        print(f"  ❌ Ошибка обновления TOTAL JSON по городу {city}: {e}")

    # Удаляем локальные excel
    delete_local_excels(city_dir)


def main():
    if not ROOT_DIR.exists():
        print(f"❌ Папка не найдена: {ROOT_DIR}")
        return

    if not GOOGLE_DRIVE_CREDENTIALS_PATH:
        print("❌ Не задан GOOGLE_DRIVE_CREDENTIALS_PATH (путь к service account json)")
        return

    service = build_drive_service(GOOGLE_DRIVE_CREDENTIALS_PATH)

    # 1) Total
    process_total(service)

    # 2) Города (все подпапки кроме Total)
    for item in ROOT_DIR.iterdir():
        if not item.is_dir():
            continue
        if item.name == "Total":
            continue

        # Обрабатываем только если есть xlsx
        if list(item.glob("*.xlsx")):
            process_city_folder(service, item)


if __name__ == "__main__":
    main()
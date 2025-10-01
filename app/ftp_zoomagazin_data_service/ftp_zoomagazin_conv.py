# app/ftp_zoomagazin_data_service/ftp_zoomagazin_conv.py
# -*- coding: utf-8 -*-

import os
import logging
from datetime import datetime
from ftplib import FTP, error_perm
from io import BytesIO

try:
    import chardet
except Exception:
    chardet = None

# =========================
# НАСТРОЙКИ
# =========================
FTP_HOST = os.getenv("ZOOMAGAZIN_FTP_HOST", "164.92.213.254")
FTP_PORT = int(os.getenv("ZOOMAGAZIN_FTP_PORT", "21"))
FTP_USER = os.getenv("ZOOMAGAZIN_FTP_USER", "anonymous")
FTP_PASS = os.getenv("ZOOMAGAZIN_FTP_PASS", "")

# Кандидаты входящей папки (первый доступный будет использован)
INCOMING_DIR_CANDIDATES = [
    os.getenv("ZOOMAGAZIN_INCOMING_DIR", "/tabletki-uploads"),
    "/upload",
]

DEFAULT_FILE_TYPE = "catalog"  # или "stock"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================
# УТИЛИТЫ
# =========================
def _join_ftp(*parts: str) -> str:
    """Нормализация FTP-пути."""
    cleaned = [str(p).strip("/") for p in parts if p]
    return "/" if not cleaned else ("/" + "/".join(cleaned))


def _safe_cwd(ftp: FTP, path: str) -> bool:
    try:
        ftp.cwd(path)
        return True
    except Exception:
        return False


def _connect() -> FTP:
    """
    Создание FTP-сессии с режимом имён latin1 и попыткой выключить UTF8.
    Это критично для имён, загруженных не в UTF-8.
    """
    ftp = FTP()
    ftp.encoding = "latin1"
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    # ключевой момент: просим сервер не использовать UTF8 для имён
    try:
        ftp.sendcmd("OPTS UTF8 OFF")
    except Exception:
        pass
    return ftp


def _mdtm_or_none(ftp: FTP, name: str):
    """Читает MDTM (дату/время на сервере) или None, если не поддерживается."""
    try:
        resp = ftp.sendcmd(f"MDTM {name}")
        if resp.startswith("213 "):
            return datetime.strptime(resp[4:].strip(), "%Y%m%d%H%M%S")
    except Exception:
        pass
    return None


def _list_json_files_with_mtime(ftp: FTP, incoming_abs: str):
    """
    Возвращает список [(name, mtime)], отсортированный по убыванию времени.
    Имена — как вернул сервер (latin1).
    """
    if not _safe_cwd(ftp, incoming_abs):
        raise RuntimeError(f"Не удалось открыть входящую папку: {incoming_abs}")

    try:
        names = ftp.nlst()
    except error_perm as e:
        if "No files found" in str(e):
            names = []
        else:
            raise

    json_names = [n for n in names if n.lower().endswith(".json")]
    pairs = []
    for n in json_names:
        pairs.append((n, _mdtm_or_none(ftp, n)))

    pairs.sort(key=lambda t: (t[1] or datetime.min), reverse=True)
    return pairs


def _decode_bytes(raw: bytes) -> str:
    """Декодирование содержимого JSON с многоступенчатым fallback."""
    for enc in ("utf-8", "utf-8-sig", "cp1251", "latin1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            pass
    if chardet:
        enc = chardet.detect(raw).get("encoding") or "utf-8"
        try:
            return raw.decode(enc, errors="replace")
        except Exception:
            pass
    return raw.decode("latin1", errors="replace")


def _download_to_string(ftp: FTP, directory: str, filename: str) -> str:
    """
    Скачивает файл бинарно и возвращает текст.
    Не делаем cwd; RETR идёт по абсолютному пути.
    Делаем несколько попыток имени (кавычки, экранирование пробелов, перекодировки).
    """
    abs_name = _join_ftp(directory, filename)

    def _retr(name: str) -> bytes:
        buf = BytesIO()
        ftp.sendcmd("TYPE I")
        ftp.retrbinary("RETR " + name, buf.write)
        return buf.getvalue()

    # Кандидаты имён (в порядке приоритетов)
    candidates = [abs_name]

    # В кавычках
    if not (abs_name.startswith('"') and abs_name.endswith('"')):
        candidates.append(f'"{abs_name}"')

    # С экранированием пробелов
    if " " in abs_name and r"\ " not in abs_name:
        candidates.append(abs_name.replace(" ", r"\ "))

    # Перекодированные варианты
    try:
        candidates.append(abs_name.encode("latin1", "ignore").decode("cp1251", "ignore"))
    except Exception:
        pass
    try:
        candidates.append(abs_name.encode("latin1", "ignore").decode("utf-8", "ignore"))
    except Exception:
        pass

    # Убираем дубли и пустые
    seen = set()
    candidates = [c for c in candidates if c and not (c in seen or seen.add(c))]

    last_exc = None
    for cand in candidates:
        try:
            raw = _retr(cand)
            return _decode_bytes(raw)
        except Exception as e:
            last_exc = e
            continue

    # Ничего не вышло
    raise last_exc if last_exc else error_perm("550 Failed to open file.")



def _normalize_dst_name(file_type: str, filename: str) -> str:
    """
    Локальное имя результата: 'catalog-<basename>.json' / 'stock-<basename>.json'
    без повторов префиксов и двойного .json.
    """
    base, _ext = os.path.splitext(filename)
    low = base.lower()
    if low.startswith("catalog-"):
        base = base[8:]
    elif low.startswith("stock-"):
        base = base[6:]
    prefix = "catalog" if file_type == "catalog" else "stock"
    return f"{prefix}-{base}.json"


def _delete_all_except(ftp: FTP, directory: str, keep_name: str):
    """Удаляет все .json файлы, кроме keep_name, в заданной директории на FTP."""
    if not _safe_cwd(ftp, directory):
        logger.warning("Не удалось зайти в директорию для очистки: %s", directory)
        return
    try:
        names = ftp.nlst()
    except Exception as e:
        logger.warning("Не удалось прочитать список для очистки: %s", e)
        return

    for n in names:
        if n == keep_name:
            continue
        if not n.lower().endswith(".json"):
            continue
        try:
            ftp.delete(n)
            logger.info("🗑 Удалён файл: %s", _join_ftp(directory, n))
        except Exception as e:
            logger.warning("Не удалось удалить %s: %s", n, e)


# =========================
# ХУКИ ОТПРАВКИ ДАННЫХ ДАЛЬШЕ
# =========================
from app.services.database_service import process_database_service  # noqa: E402


async def send_catalog_data(file_path: str, enterprise_code: int):
    await process_database_service(file_path, "catalog", enterprise_code)


async def send_stock_data(file_path: str, enterprise_code: int):
    await process_database_service(file_path, "stock", enterprise_code)


# =========================
# ОСНОВНОЙ СЦЕНАРИЙ
# =========================
def run_service(enterprise_code: int, file_type: str = DEFAULT_FILE_TYPE) -> bool:
    """
    1) Находим рабочую входящую папку на FTP.
    2) Берём самый свежий .json.
    3) Скачиваем → декодируем → сохраняем локально (./temp/<code>/...json).
    4) Передаём дальше (catalog/stock).
    5) Удаляем на FTP все другие .json-файлы, оставляя только обработанный.
    """
    ftp = None
    try:
        ftp = _connect()

        incoming_abs = None
        for cand in INCOMING_DIR_CANDIDATES:
            if _safe_cwd(ftp, cand):
                incoming_abs = cand
                break
        if not incoming_abs:
            raise RuntimeError("Не найдена входящая папка среди: " + ", ".join(INCOMING_DIR_CANDIDATES))

        files = _list_json_files_with_mtime(ftp, incoming_abs)
        if not files:
            logger.info("Нет JSON-файлов во входящей папке.")
            return True

        latest_name, latest_mtime = files[0]
        logger.info("Обработка файла: %s (mtime=%s)", _join_ftp(incoming_abs, latest_name), latest_mtime or "—")

        text = _download_to_string(ftp, incoming_abs, latest_name)

        # локальный результат
        temp_dir = os.path.join(".", "temp", str(enterprise_code))
        os.makedirs(temp_dir, exist_ok=True)
        out_name = _normalize_dst_name(file_type, latest_name)
        out_path = os.path.join(temp_dir, out_name)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
        logger.info("✅ Сохранено: %s", out_path)

        # передача дальше
        import asyncio
        if file_type == "catalog":
            logger.info("Начало обработки catalog для предприятия %s", enterprise_code)
            asyncio.run(send_catalog_data(out_path, enterprise_code))
        else:
            logger.info("Начало обработки stock для предприятия %s", enterprise_code)
            asyncio.run(send_stock_data(out_path, enterprise_code))
        logger.info("Данные %s успешно записаны для предприятия %s", file_type, enterprise_code)

        # очистка FTP: оставить только последний файл
        _delete_all_except(ftp, incoming_abs, latest_name)
        logger.info("🧹 Очистка завершена (оставлен только последний файл).")

        return True

    except Exception as e:
        logger.error("❌ Критическая ошибка: %s", e, exc_info=True)
        return False
    finally:
        try:
            if ftp:
                ftp.quit()
        except Exception:
            pass


# =========================
# CLI
# =========================
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="FtpZoomagazin processor")
    parser.add_argument("--enterprise", type=int, required=True, help="enterprise_code")
    parser.add_argument("--type", choices=["catalog", "stock"], default=DEFAULT_FILE_TYPE, help="file type to process")
    args = parser.parse_args()

    ok = run_service(args.enterprise, args.type)
    raise SystemExit(0 if ok else 1)

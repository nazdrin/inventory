# app/ftp_zoomagazin_data_service/ftp_zoomagazin_conv.py
# -*- coding: utf-8 -*-

import os
import logging
from datetime import datetime
from ftplib import FTP, error_perm
from io import BytesIO

try:
    import chardet
except Exception:  # chardet может быть не установлен
    chardet = None

# =========================
# НАСТРОЙКИ (можно вынести в .env)
# =========================
FTP_HOST = os.getenv("ZOOMAGAZIN_FTP_HOST", "164.92.213.254")
FTP_PORT = int(os.getenv("ZOOMAGAZIN_FTP_PORT", "21"))
FTP_USER = os.getenv("ZOOMAGAZIN_FTP_USER", "anonymous")
FTP_PASS = os.getenv("ZOOMAGAZIN_FTP_PASS", "")

# Основная входящая директория. Скрипт попробует и альтернативу, если первая недоступна.
INCOMING_DIR_CANDIDATES = [
    os.getenv("ZOOMAGAZIN_INCOMING_DIR", "/tabletki-uploads"),
    "/upload",
]

# Тип файла: "catalog" или "stock" — сюда придёт из планировщика
DEFAULT_FILE_TYPE = "catalog"

# Логгер
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================
# УТИЛИТЫ
# =========================
def _join_ftp(*parts: str) -> str:
    """Безопасно соединяет компонент пути для FTP (без двойных слэш)."""
    cleaned = []
    for p in parts:
        if not p:
            continue
        cleaned.append(str(p).strip("/"))
    if not cleaned:
        return "/"
    return "/" + "/".join(cleaned)


def _safe_cwd(ftp: FTP, path: str) -> bool:
    """Проверяет возможность зайти в каталог."""
    try:
        ftp.cwd(path)
        return True
    except Exception:
        return False


def _connect() -> FTP:
    """Устанавливает FTP-сессию. Для имён файлов — кодировка latin1 (без ошибок)."""
    ftp = FTP()
    ftp.encoding = "latin1"  # критично для «битых» имён
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    return ftp


def _mdtm_or_none(ftp: FTP, name: str):
    """Читает MDTM (время модификации на сервере), либо None, если не поддерживается."""
    try:
        resp = ftp.sendcmd(f"MDTM {name}")
        # Ответ вид: "213 YYYYMMDDHHMMSS"
        if resp.startswith("213 "):
            dt = datetime.strptime(resp[4:].strip(), "%Y%m%d%H%M%S")
            return dt
    except Exception:
        pass
    return None


def _list_json_files_with_mtime(ftp: FTP, incoming_abs: str):
    """
    Возвращает список [(name, mtime)], где name — как вернул сервер (latin1),
    mtime — datetime или None. Сортирует по mtime (свежие сверху).
    """
    # обязательно перейти в папку
    if not _safe_cwd(ftp, incoming_abs):
        raise RuntimeError(f"Не удалось открыть входящую папку: {incoming_abs}")

    try:
        names = ftp.nlst()
    except error_perm as e:
        # Пустая папка может дать "550 No files found"
        if "No files found" in str(e):
            names = []
        else:
            raise

    json_names = [n for n in names if n.lower().endswith(".json")]
    files = []
    for n in json_names:
        mtime = _mdtm_or_none(ftp, n)
        files.append((n, mtime))

    # Сортировка: сперва с mtime (по убыванию), затем по имени
    files.sort(key=lambda t: (t[1] or datetime.min), reverse=True)
    return files


def _decode_bytes(raw: bytes) -> str:
    """Декодирует содержимое JSON с безопасными фоллбэками."""
    # 1) чистый UTF-8
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pass
    # 2) UTF-8 с BOM
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        pass
    # 3) cp1251
    try:
        return raw.decode("cp1251")
    except UnicodeDecodeError:
        pass
    # 4) latin1 как «последняя соломинка»
    try:
        return raw.decode("latin1")
    except UnicodeDecodeError:
        pass
    # 5) chardet (если есть)
    if chardet:
        enc = chardet.detect(raw).get("encoding") or "utf-8"
        try:
            return raw.decode(enc, errors="replace")
        except Exception:
            return raw.decode("latin1", errors="replace")
    # финальный фоллбэк
    return raw.decode("latin1", errors="replace")


def _download_to_string(ftp: FTP, directory: str, filename: str) -> str:
    """Скачивает файл бинарно и возвращает строку с авто-декодированием."""
    # cwd делаем каждый раз явно — иначе RETR с кириллицей работает нестабильно
    if not _safe_cwd(ftp, directory):
        raise RuntimeError(f"Не удалось перейти в {directory}")

    buf = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buf.write)
    raw = buf.getvalue()
    return _decode_bytes(raw)


def _normalize_dst_name(file_type: str, filename: str) -> str:
    """
    Приводит имя к виду: 'catalog-<basename>.json' или 'stock-<basename>.json'
    без двойных префиксов/суффиксов.
    """
    base, ext = os.path.splitext(filename)  # ext может быть '.json' уже
    # убрать повторы префикса
    low = base.lower()
    if low.startswith("catalog-"):
        base = base[8:]
    if low.startswith("stock-"):
        base = base[6:]
    # собрать обратно
    prefix = "catalog" if file_type == "catalog" else "stock"
    return f"{prefix}-{base}.json"


def _delete_all_except(ftp: FTP, directory: str, keep_name: str):
    """
    Удаляет в папке все .json файлы, кроме keep_name.
    Ошибки по каждому файлу логируем, но процесс не прерываем.
    """
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
# ВАШИ ВНУТРЕННИЕ ХУКИ ОТПРАВКИ
# =========================
# В проекте эти функции уже есть, просто импортируем
from app.services.database_service import process_database_service  # noqa: E402


async def send_catalog_data(file_path: str, enterprise_code: int):
    await process_database_service(file_path, "catalog", enterprise_code)


async def send_stock_data(file_path: str, enterprise_code: int):
    await process_database_service(file_path, "stock", enterprise_code)


# =========================
# ОСНОВНОЙ ЗАПУСК ДЛЯ ОДНОГО ПРЕДПРИЯТИЯ
# =========================
def run_service(enterprise_code: int, file_type: str = DEFAULT_FILE_TYPE) -> bool:
    """
    Основной сценарий:
      1) соединяемся с FTP и находим рабочую входящую папку;
      2) ищем список .json, берём самый новый;
      3) скачиваем его, декодируем, пишем во временный файл;
      4) отправляем дальше (catalog/stock);
      5) удаляем ВСЕ остальные .json файлы в папке, последний (обработанный) оставляем;
    Возвращает True/False — выполнено без критических ошибок.
    """
    ftp = None
    try:
        ftp = _connect()

        # найти рабочую входящую папку
        incoming_abs = None
        for cand in INCOMING_DIR_CANDIDATES:
            if _safe_cwd(ftp, cand):
                incoming_abs = cand
                break
        if not incoming_abs:
            raise RuntimeError("Не найдена входящая папка среди кандидатов: " + ", ".join(INCOMING_DIR_CANDIDATES))

        files = _list_json_files_with_mtime(ftp, incoming_abs)
        if not files:
            logger.info("Нет JSON-файлов во входящей папке.")
            return True

        latest_name, latest_mtime = files[0]
        logger.info("Обработка файла: %s (mtime=%s)", _join_ftp(incoming_abs, latest_name), latest_mtime or "—")

        # скачать и декодировать содержимое
        text = _download_to_string(ftp, incoming_abs, latest_name)

        # сохранить во временный локальный файл (для ваших downstream-сервисов)
        temp_dir = os.path.join(".", "temp", str(enterprise_code))
        os.makedirs(temp_dir, exist_ok=True)
        safe_out_name = _normalize_dst_name(file_type, latest_name)
        out_path = os.path.join(temp_dir, safe_out_name)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)

        logger.info("✅ Сохранено: %s", out_path)

        # отправка дальше
        import asyncio
        if file_type == "catalog":
            logger.info("Начало обработки catalog для предприятия %s", enterprise_code)
            asyncio.run(send_catalog_data(out_path, enterprise_code))
        else:
            logger.info("Начало обработки stock для предприятия %s", enterprise_code)
            asyncio.run(send_stock_data(out_path, enterprise_code))

        logger.info("Данные %s успешно записаны в БД для предприятия %s", file_type, enterprise_code)

        # очистка: оставляем только самый свежий из .json, остальные удаляем
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
# ЛОКАЛЬНЫЙ ЗАПУСК
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

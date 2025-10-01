# app/ftp_zoomagazin_data_service/ftp_zoomagazin_conv.py
# -*- coding: utf-8 -*-

import os
import re
import stat
import logging
import asyncio
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

# Кандидаты входящей папки на FTP
INCOMING_DIR_CANDIDATES = [
    os.getenv("ZOOMAGAZIN_INCOMING_DIR", "/tabletki-uploads"),
    "/upload",
]

# Корень FTP на ФС (для анонимного vsftpd обычно /var/ftp)
FTP_FS_ROOT = os.getenv("ZOOMAGAZIN_FTP_FS_ROOT", "/var/ftp")

DEFAULT_FILE_TYPE = "catalog"  # или "stock"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =========================
# УТИЛИТЫ: пути, кодировки
# =========================
def _join_ftp(*parts: str) -> str:
    cleaned = [str(p).strip("/") for p in parts if p]
    return "/" if not cleaned else ("/" + "/".join(cleaned))

def _ftp_to_fs_path(ftp_abs: str) -> str:
    """Мэппинг FTP-пути в абсолютный путь на ФС: /tabletki-uploads -> /var/ftp/tabletki-uploads"""
    return os.path.join(FTP_FS_ROOT, ftp_abs.lstrip("/"))

def _decode_bytes(raw: bytes) -> str:
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

def _normalize_dst_name(file_type: str, filename: str) -> str:
    base, _ext = os.path.splitext(filename)
    low = base.lower()
    if low.startswith("catalog-"):
        base = base[8:]
    elif low.startswith("stock-"):
        base = base[6:]
    prefix = "catalog" if file_type == "catalog" else "stock"
    return f"{prefix}-{base}.json"

# =========================
# ЛОКАЛЬНАЯ НОРМАЛИЗАЦИЯ (ФАЙЛОВАЯ СИСТЕМА)
# =========================

# Простейшая транслитерация для slug
TRANSLIT = {
    ord('А'): 'A', ord('Б'): 'B', ord('В'): 'V', ord('Г'): 'G', ord('Д'): 'D',
    ord('Е'): 'E', ord('Ё'): 'E', ord('Ж'): 'Zh', ord('З'): 'Z', ord('И'): 'I',
    ord('Й'): 'Y', ord('К'): 'K', ord('Л'): 'L', ord('М'): 'M', ord('Н'): 'N',
    ord('О'): 'O', ord('П'): 'P', ord('Р'): 'R', ord('С'): 'S', ord('Т'): 'T',
    ord('У'): 'U', ord('Ф'): 'F', ord('Х'): 'H', ord('Ц'): 'C', ord('Ч'): 'Ch',
    ord('Ш'): 'Sh', ord('Щ'): 'Sch', ord('Ъ'): '',  ord('Ы'): 'Y', ord('Ь'): '',
    ord('Э'): 'E', ord('Ю'): 'Yu', ord('Я'): 'Ya',
    ord('а'): 'a', ord('б'): 'b', ord('в'): 'v', ord('г'): 'g', ord('д'): 'd',
    ord('е'): 'e', ord('ё'): 'e', ord('ж'): 'zh', ord('з'): 'z', ord('и'): 'i',
    ord('й'): 'y', ord('к'): 'k', ord('л'): 'l', ord('м'): 'm', ord('н'): 'n',
    ord('о'): 'o', ord('п'): 'p', ord('р'): 'r', ord('с'): 's', ord('т'): 't',
    ord('у'): 'u', ord('ф'): 'f', ord('х'): 'h', ord('ц'): 'c', ord('ч'): 'ch',
    ord('ш'): 'sh', ord('щ'): 'sch', ord('ъ'): '',  ord('ы'): 'y', ord('ь'): '',
    ord('э'): 'e', ord('ю'): 'yu', ord('я'): 'ya',
}
_slug_re = re.compile(r"[^A-Za-z0-9]+")

def _try_decode_name(name_bytes: bytes) -> str:
    for enc in ("utf-8", "cp1251", "latin1"):
        try:
            return name_bytes.decode(enc)
        except UnicodeDecodeError:
            continue
    return name_bytes.decode("latin1", errors="replace")

def _slugify(text: str) -> str:
    text = text.translate(TRANSLIT)
    text = _slug_re.sub("-", text).strip("-")
    text = re.sub(r"-{2,}", "-", text)
    return text or "file"

def _derive_prefix_and_base(decoded: str) -> tuple[str, str]:
    name = decoded
    if name.lower().endswith(".json"):
        name = name[:-5]
    low = name.lower()
    if low.startswith("catalog-"):
        prefix = "catalog"; name = name[8:]
    elif low.startswith("stock-"):
        prefix = "stock"; name = name[6:]
    else:
        prefix = "catalog"
    base = _slugify(name)
    return prefix, base

def _ensure_0644(full_path_bytes: bytes):
    try:
        st = os.stat(full_path_bytes)
        mode = stat.S_IMODE(st.st_mode)
        if mode != 0o644:
            os.chmod(full_path_bytes, 0o644)
            try:
                human = full_path_bytes.decode("utf-8")
            except Exception:
                human = str(full_path_bytes)
            logger.info("chmod 0644: %s", human)
    except Exception as e:
        logger.warning("Не удалось выставить 0644: %s (%s)", full_path_bytes, e)

def _unique_ascii_target(dir_bytes: bytes, prefix: str, base: str) -> bytes:
    cand = f"{prefix}-{base}.json".encode("ascii", "ignore")
    tgt = os.path.join(dir_bytes, cand)
    if not os.path.exists(tgt):
        return tgt
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    cand2 = f"{prefix}-{base}-{ts}.json".encode("ascii", "ignore")
    return os.path.join(dir_bytes, cand2)

def _normalize_dir_fs(fs_abs: str):
    """
    Локально (на ФС) привести все *.json в каталоге к читабельному виду:
    - права 0644,
    - имя ASCII: catalog-/stock- + slug + .json.
    """
    dir_bytes = os.fsencode(fs_abs)  # bytes
    try:
        entries = os.listdir(dir_bytes)
    except Exception as e:
        logger.warning("Не удалось прочитать каталог %s: %s", fs_abs, e)
        return

    for name_b in entries:
        full_b = os.path.join(dir_bytes, name_b)
        if not os.path.isfile(full_b):
            continue
        if not name_b.lower().endswith(b".json"):
            continue

        # права
        _ensure_0644(full_b)

        # check если уже ASCII-норм
        try:
            ascii_name = name_b.decode("ascii")
            if ascii_name.lower().endswith(".json"):
                base = ascii_name[:-5].lower()
                if base.startswith("catalog-") or base.startswith("stock-"):
                    # выглядит ок — пропустим
                    continue
        except UnicodeDecodeError:
            pass

        decoded = _try_decode_name(name_b)
        prefix, base = _derive_prefix_and_base(decoded)
        target_b = _unique_ascii_target(dir_bytes, prefix, base)
        try:
            os.rename(full_b, target_b)
            logger.info("REN: %s -> %s", decoded, os.path.basename(target_b).decode("ascii", "ignore"))
        except Exception as e:
            logger.warning("Не удалось переименовать '%s': %s", decoded, e)

# =========================
# СИНХРОННЫЕ FTP ФУНКЦИИ (в to_thread)
# =========================
def _connect_sync() -> FTP:
    ftp = FTP()
    ftp.encoding = "latin1"
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    try:
        ftp.sendcmd("OPTS UTF8 OFF")
    except Exception:
        pass
    return ftp

def _safe_cwd_sync(ftp: FTP, path: str) -> bool:
    try:
        ftp.cwd(path)
        return True
    except Exception:
        return False

def _mdtm_sync(ftp: FTP, name: str):
    try:
        resp = ftp.sendcmd(f"MDTM {name}")
        if resp.startswith("213 "):
            return datetime.strptime(resp[4:].strip(), "%Y%m%d%H%M%S")
    except Exception:
        pass
    return None

def _list_json_with_mtime_sync(ftp: FTP, incoming_abs: str):
    if not _safe_cwd_sync(ftp, incoming_abs):
        raise RuntimeError(f"Не удалось открыть входящую папку: {incoming_abs}")
    try:
        names = ftp.nlst()
    except error_perm as e:
        if "No files found" in str(e):
            names = []
        else:
            raise
    files = [(n, _mdtm_sync(ftp, n)) for n in names if n.lower().endswith(".json")]
    files.sort(key=lambda t: (t[1] or datetime.min), reverse=True)
    return files

def _retr_text_sync(ftp: FTP, directory: str, filename: str) -> str:
    """RETR относительно current dir (+ варианты имени), после cwd(directory)."""
    if not _safe_cwd_sync(ftp, directory):
        raise RuntimeError(f"Не удалось перейти в {directory}")

    def _retr_rel(name: str) -> bytes:
        buf = BytesIO()
        ftp.sendcmd("TYPE I")
        ftp.retrbinary("RETR " + name, buf.write)
        return buf.getvalue()

    candidates = [filename]
    if not filename.startswith("./"):
        candidates.append("./" + filename)
    try:
        candidates.append(filename.encode("latin1", "ignore").decode("cp1251", "ignore"))
    except Exception:
        pass
    try:
        candidates.append(filename.encode("latin1", "ignore").decode("utf-8", "ignore"))
    except Exception:
        pass

    seen = set()
    candidates = [c for c in candidates if c and not (c in seen or seen.add(c))]

    last_exc = None
    for cand in candidates:
        try:
            raw = _retr_rel(cand)
            return _decode_bytes(raw)
        except Exception as e:
            last_exc = e
            continue

    raise last_exc if last_exc else error_perm("550 Failed to open file.")

def _delete_all_except_sync(ftp: FTP, directory: str, keep_name: str):
    if not _safe_cwd_sync(ftp, directory):
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

def _fetch_latest_text_and_name_sync(incoming_abs: str):
    """
    Возвращает (text, latest_name) или (None, None).
    Перебирает кандидатов по дате и «скипает» те, что не читаются (550).
    """
    ftp = None
    try:
        ftp = _connect_sync()
        files = _list_json_with_mtime_sync(ftp, incoming_abs)
        if not files:
            return (None, None)
        for name, mtime in files:
            logger.info("Кандидат: %s (mtime=%s)", _join_ftp(incoming_abs, name), mtime or "—")
            try:
                txt = _retr_text_sync(ftp, incoming_abs, name)
                logger.info("Выбран файл для обработки: %s", name)
                return (txt, name)
            except error_perm as e:
                if "550" in str(e):
                    logger.warning("Пропуск (не читается через RETR): %s", name)
                    continue
                raise
            except Exception as e:
                logger.warning("Пропуск (ошибка RETR): %s (%s)", name, e)
                continue
        return (None, None)
    finally:
        try:
            if ftp:
                ftp.quit()
        except Exception:
            pass

def _delete_all_except_wrapper_sync(incoming_abs: str, latest_name: str):
    ftp = None
    try:
        ftp = _connect_sync()
        _delete_all_except_sync(ftp, incoming_abs, latest_name)
    finally:
        try:
            if ftp:
                ftp.quit()
        except Exception:
            pass

# =========================
# ХУКИ ОТПРАВКИ ДАЛЬШЕ (ваши async-функции)
# =========================
from app.services.database_service import process_database_service  # noqa: E402

async def send_catalog_data(file_path: str, enterprise_code: int):
    await process_database_service(file_path, "catalog", enterprise_code)

async def send_stock_data(file_path: str, enterprise_code: int):
    await process_database_service(file_path, "stock", enterprise_code)

# =========================
# ОСНОВНОЙ АСИНХРОННЫЙ СЦЕНАРИЙ
# =========================
async def run_service(enterprise_code: int, file_type: str = DEFAULT_FILE_TYPE) -> bool:
    """
    Порядок:
      0) Локальная нормализация ФС (права 0644 + ASCII-имена) для всех JSON в найденной входящей папке.
      1) через FTP берём самый свежий читабельный JSON,
      2) сохраняем локально ./temp/<code>/<normalized>.json,
      3) await отправляем в ваш downstream (catalog/stock),
      4) чистим FTP: удаляем все .json, кроме обработанного.
    """
    try:
        # Найти существующую входящую папку на FTP
        incoming_abs = None
        # Попробуем через FTP (быстро)
        def _pick_dir_sync():
            ftp = None
            try:
                ftp = _connect_sync()
                for cand in INCOMING_DIR_CANDIDATES:
                    if _safe_cwd_sync(ftp, cand):
                        return cand
                return None
            finally:
                try:
                    if ftp:
                        ftp.quit()
                except Exception:
                    pass

        incoming_abs = await asyncio.to_thread(_pick_dir_sync)
        if not incoming_abs:
            raise RuntimeError("Не найдена входящая папка среди: " + ", ".join(INCOMING_DIR_CANDIDATES))

        # 0) Локальная нормализация в соответствующей ФС-папке
        fs_path = _ftp_to_fs_path(incoming_abs)
        await asyncio.to_thread(_normalize_dir_fs, fs_path)

        # 1) Получаем самый свежий читабельный файл
        text, latest_name = await asyncio.to_thread(_fetch_latest_text_and_name_sync, incoming_abs)
        if not latest_name:
            logger.info("Нет доступных JSON-файлов (после нормализации).")
            return True

        # 2) Локальный результат
        temp_dir = os.path.join(".", "temp", str(enterprise_code))
        os.makedirs(temp_dir, exist_ok=True)
        out_name = _normalize_dst_name(file_type, latest_name)
        out_path = os.path.join(temp_dir, out_name)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
        logger.info("✅ Сохранено: %s", out_path)

        # 3) Передача дальше
        if file_type == "catalog":
            logger.info("Начало обработки catalog для предприятия %s", enterprise_code)
            await send_catalog_data(out_path, enterprise_code)
        else:
            logger.info("Начало обработки stock для предприятия %s", enterprise_code)
            await send_stock_data(out_path, enterprise_code)
        logger.info("Данные %s успешно записаны для предприятия %s", file_type, enterprise_code)

        # 4) Очистка: оставить только обработанный
        await asyncio.to_thread(_delete_all_except_wrapper_sync, incoming_abs, latest_name)
        logger.info("🧹 Очистка завершена (оставлен только последний файл).")

        return True

    except Exception as e:
        logger.error("❌ Критическая ошибка в run_service: %s", e, exc_info=True)
        return False

# =========================
# CLI (ручной тест)
# =========================
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="FtpZoomagazin processor (async with FS normalization)")
    parser.add_argument("--enterprise", type=int, required=True, help="enterprise_code")
    parser.add_argument("--type", choices=["catalog", "stock"], default=DEFAULT_FILE_TYPE, help="file type to process")
    args = parser.parse_args()

    async def _amain():
        ok = await run_service(args.enterprise, args.type)
        raise SystemExit(0 if ok else 1)

    asyncio.run(_amain())

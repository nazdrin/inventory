import os
import requests
from typing import Optional, Tuple, Union

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------
# ENV / Константи
# ---------------------------------------
load_dotenv()
INFO_TOKEN = os.getenv("TELEGRAM_DEVELOP") or os.getenv("TELEGRAM_BOT_TOKEN")
ERROR_TOKEN = os.getenv("TELEGRAM_ERROR_BOT_TOKEN")

DEFAULT_CHAT_IDS = [807661373, 1041598119]
ERROR_MARKERS = (
    "error",
    "failed",
    "failure",
    "exception",
    "traceback",
    "critical",
    "ошибка",
    "помилка",
    "критичес",
    "не задан",
    "не задана",
    "не найден",
    "не найдены",
    "нет настроек",
    "остановлен",
    "❌",
    "🔴",
    "🔥",
)


def _parse_chat_ids(raw: Optional[str]) -> list[Union[int, str]]:
    if not raw or not raw.strip():
        return DEFAULT_CHAT_IDS

    values: list[Union[int, str]] = []
    for item in raw.split(","):
        value = item.strip()
        if not value:
            continue
        try:
            values.append(int(value))
        except ValueError:
            values.append(value)
    return values or DEFAULT_CHAT_IDS


CHAT_IDS = _parse_chat_ids(os.getenv("TELEGRAM_CHAT_IDS"))
ERROR_CHAT_IDS = _parse_chat_ids(os.getenv("TELEGRAM_ERROR_CHAT_IDS") or os.getenv("TELEGRAM_CHAT_IDS"))


def _is_error_message(message: str) -> bool:
    normalized = str(message or "").strip().lower()
    return any(marker in normalized for marker in ERROR_MARKERS)


def _resolve_notification_target(message: str) -> tuple[Optional[str], list[Union[int, str]], str]:
    if _is_error_message(message):
        return ERROR_TOKEN or INFO_TOKEN, ERROR_CHAT_IDS, "error"
    return INFO_TOKEN, CHAT_IDS, "info"


# ---------------------------------------
# Допоміжне: отримаємо sync-URL БД
# ---------------------------------------
def _normalize_to_sync_url(url: str) -> str:
    """
    Перетворює async URL на sync URL для PostgreSQL.
    Напр.: postgresql+asyncpg:// -> postgresql+psycopg2://
    """
    if not url:
        return url
    if url.startswith("postgresql+asyncpg://"):
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
    # Якщо просто 'postgresql://' — це вже ок для psycopg2
    return url


def _resolve_sync_db_url() -> Optional[str]:
    """
    Порядок пріоритету:
    1) SYNC_DATABASE_URL
    2) DATABASE_SYNC_URL
    3) DATABASE_URL (конвертуємо до sync, якщо async)
    4) DB_URL (часто так називають)
    """
    candidates = [
        os.getenv("SYNC_DATABASE_URL"),
        os.getenv("DATABASE_SYNC_URL"),
        os.getenv("DATABASE_URL"),
        os.getenv("DB_URL"),
    ]
    for raw in candidates:
        if raw and raw.strip():
            return _normalize_to_sync_url(raw.strip())
    return None


_SYNC_DB_URL = _resolve_sync_db_url()
_SYNC_ENGINE = create_engine(_SYNC_DB_URL, pool_pre_ping=True) if _SYNC_DB_URL else None


# ---------------------------------------
# Доступ до БД (синхронно)
# ---------------------------------------
def _fetch_enterprise_meta_sync(enterprise_code: str) -> Optional[Tuple[Optional[str], Optional[str]]]:
    """
    СИНХРОННО дістає (enterprise_name, data_format) з таблиці enterprise_settings
    за enterprise_code. Повертає None, якщо нічого не знайдено або немає доступної БД.
    """
    if not _SYNC_ENGINE:
        return None

    sql = text("""
        SELECT enterprise_name, data_format
        FROM enterprise_settings
        WHERE CAST(enterprise_code AS TEXT) = :code
        LIMIT 1
    """)
    try:
        with _SYNC_ENGINE.connect() as conn:
            row = conn.execute(sql, {"code": str(enterprise_code)}).first()
            if not row:
                return None
            # row[0] -> enterprise_name, row[1] -> data_format
            return row[0], row[1]
    except Exception:
        # За потреби додайте логування
        return None


# ---------------------------------------
# Формування тексту
# ---------------------------------------
def _build_text(
    message: str,
    enterprise_code: Optional[str],
    meta: Optional[Tuple[Optional[str], Optional[str]]],
) -> str:
    """
    - Без enterprise_code → лише message.
    - З enterprise_code → завжди додаємо:
        Enterprise Code, Название предприятия, Формат.
      Якщо метаданих немає, підставляємо '—'.
    """
    if not enterprise_code:
        return f"{message}"

    enterprise_name = meta[0] if meta else None
    data_format = meta[1] if meta else None

    lines = [
        message,
        "",
        f"Enterprise Code: {enterprise_code}",
        f"Название предприятия: {enterprise_name or '—'}",
        f"Формат: {data_format or '—'}",
    ]
    return "\n".join(lines)


# ---------------------------------------
# Публічна функція
# ---------------------------------------
def send_notification(message: str, enterprise_code: Optional[Union[str, int]] = None) -> None:
    """
    Синхронна відправка повідомлення у Telegram.

    - Якщо enterprise_code не передано → надсилається лише message.
    - Якщо enterprise_code передано → додаються Enterprise Code, Название предприятия і Формат.
    """
    token, chat_ids, _channel = _resolve_notification_target(message)
    if not token:
        # Немає токена — пропускаємо відправку
        return

    meta: Optional[Tuple[Optional[str], Optional[str]]] = None
    code_str: Optional[str] = None

    if enterprise_code is not None and str(enterprise_code).strip():
        code_str = str(enterprise_code).strip()
        meta = _fetch_enterprise_meta_sync(code_str)

    text = _build_text(message, code_str, meta)
    telegram_api = f"https://api.telegram.org/bot{token}/sendMessage"

    for chat_id in chat_ids:
        payload = {"chat_id": chat_id, "text": text}
        try:
            requests.post(telegram_api, data=payload, timeout=15)
        except Exception:
            # За потреби додайте логування
            pass


# Локальний тест:
# if __name__ == "__main__":
#     send_notification("🟡 Каталог успешно отправлен!", 328)
#     send_notification("✅ Сток успешно отправлен!", 320)
#     send_notification("ℹ️ Системне повідомлення без прив'язки до підприємства")

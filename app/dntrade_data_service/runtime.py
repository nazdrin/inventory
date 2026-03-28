import json
import logging
import os
import tempfile
from typing import Any, Optional

from sqlalchemy.future import select

from app.database import EnterpriseSettings, get_async_db

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _resolve_temp_dir() -> str:
    temp_dir = os.getenv("TEMP_FILE_PATH", tempfile.gettempdir())
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir


async def fetch_enterprise_settings(enterprise_code: str) -> Optional[EnterpriseSettings]:
    """Read enterprise settings in a short-lived read-only session."""
    async with get_async_db(commit_on_exit=False) as session:
        result = await session.execute(
            select(EnterpriseSettings).where(EnterpriseSettings.enterprise_code == enterprise_code)
        )
        return result.scalars().first()


def save_to_json(data: Any, enterprise_code: str, file_type: str, *, suffix: str = "data") -> Optional[str]:
    """Persist payload into temp storage for downstream database_service usage."""
    try:
        temp_dir = _resolve_temp_dir()
        json_file_path = os.path.join(temp_dir, f"{enterprise_code}_{file_type}_{suffix}.json")

        with open(json_file_path, "w", encoding="utf-8") as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)

        logger.info("DNTrade JSON written: path=%s", json_file_path)
        return json_file_path
    except IOError as exc:
        logger.error("DNTrade JSON write failed: enterprise_code=%s file_type=%s error=%s", enterprise_code, file_type, exc)
        return None


def maybe_dump_raw_json(data: Any, enterprise_code: str, file_type: str, *, label: str) -> Optional[str]:
    """Optionally dump raw/debug payload when explicitly enabled via env."""
    if not _env_flag("DNTRADE_DEBUG_RAW_JSON", default=False):
        return None
    path = save_to_json(data, enterprise_code, file_type, suffix=label)
    if path:
        logger.info(
            "DNTrade raw/debug dump written: enterprise_code=%s file_type=%s label=%s path=%s",
            enterprise_code,
            file_type,
            label,
            path,
        )
    return path

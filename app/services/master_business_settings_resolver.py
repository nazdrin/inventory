import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import select

from app.database import get_async_db
from app.models import BusinessSettings, EnterpriseSettings


load_dotenv()

logger = logging.getLogger("master_business_settings_resolver")


def _env_bool(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    return int((os.getenv(name) or str(default)).strip())


def _env_optional(name: str) -> Optional[str]:
    value = (os.getenv(name) or "").strip()
    return value or None


def _env_int_list(name: str, default: list[int]) -> tuple[int, ...]:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return tuple(default)

    parsed: list[int] = []
    for part in str(raw).replace(";", ",").split(","):
        item = part.strip()
        if not item:
            continue
        try:
            parsed.append(int(item))
        except (TypeError, ValueError):
            logger.warning("Invalid %s list item=%r skipped", name, item)

    if parsed:
        return tuple(parsed)

    logger.warning("Invalid %s=%r, using default=%s", name, raw, default)
    return tuple(default)


@dataclass(frozen=True)
class MasterBusinessSettingsSnapshot:
    source: str
    business_settings_exists: bool
    business_enterprise_code: Optional[str]
    daily_publish_enterprise_code_override: Optional[str]
    weekly_salesdrive_enterprise_code_override: Optional[str]
    biotus_enterprise_code_override: Optional[str]
    master_weekly_enabled: bool
    master_weekly_day: str
    master_weekly_hour: int
    master_weekly_minute: int
    master_daily_publish_enabled: bool
    master_daily_publish_hour: int
    master_daily_publish_minute: int
    master_daily_publish_limit: int
    master_archive_enabled: bool
    master_archive_every_minutes: int
    primary_enterprise_exists: bool
    biotus_effective_enterprise_exists: bool
    biotus_enable_unhandled_fallback: bool
    biotus_unhandled_order_timeout_minutes: int
    biotus_fallback_additional_status_ids: tuple[int, ...]
    biotus_duplicate_status_id: int
    biotus_time_default_minutes: int
    biotus_time_switch_hour: int
    biotus_time_switch_end_hour: int
    biotus_time_after_switch_minutes: int
    biotus_tz: str
    inconsistency: Optional[str] = None
    biotus_inconsistency: Optional[str] = None

    @property
    def effective_daily_publish_enterprise_code(self) -> Optional[str]:
        return self.daily_publish_enterprise_code_override or self.business_enterprise_code

    @property
    def effective_weekly_salesdrive_enterprise_code(self) -> Optional[str]:
        return self.weekly_salesdrive_enterprise_code_override or self.business_enterprise_code

    @property
    def biotus_enterprise_code_effective(self) -> Optional[str]:
        return self.biotus_enterprise_code_override or self.business_enterprise_code

    def resolve_publish_enterprise(self, explicit_enterprise: Optional[str] = None) -> str:
        explicit = str(explicit_enterprise or "").strip()
        if explicit:
            return explicit

        value = self.effective_daily_publish_enterprise_code
        if value:
            return value

        if self.business_settings_exists:
            raise RuntimeError(
                "business_settings row exists, but daily publish target cannot be resolved from "
                "business_enterprise_code + daily_publish_enterprise_code_override"
            )
        raise RuntimeError("MASTER_DAILY_PUBLISH_ENTERPRISE or MASTER_CATALOG_ENTERPRISE_CODE is required for daily publish")

    def resolve_weekly_salesdrive_enterprise(self, explicit_enterprise: Optional[str] = None) -> str:
        explicit = str(explicit_enterprise or "").strip()
        if explicit:
            return explicit

        value = self.effective_weekly_salesdrive_enterprise_code
        if value:
            return value

        if self.business_settings_exists:
            raise RuntimeError(
                "business_settings row exists, but weekly SalesDrive target cannot be resolved from "
                "business_enterprise_code + weekly_salesdrive_enterprise_code_override"
            )
        raise RuntimeError("MASTER_WEEKLY_SALESDRIVE_ENTERPRISE or MASTER_CATALOG_ENTERPRISE_CODE is required for weekly salesdrive export")

    def resolve_primary_business_enterprise(self, explicit_enterprise: Optional[str] = None, *, purpose: str) -> str:
        explicit = str(explicit_enterprise or "").strip()
        if explicit:
            return explicit

        value = self.business_enterprise_code
        if value:
            return value

        if self.business_settings_exists:
            raise RuntimeError(
                f"business_settings row exists, but business_enterprise_code is missing for {purpose}"
            )
        raise RuntimeError(f"Для режима {purpose} требуется --enterprise или MASTER_CATALOG_ENTERPRISE_CODE")

    def resolve_biotus_enterprise(self, explicit_enterprise: Optional[str] = None) -> str:
        explicit = str(explicit_enterprise or "").strip()
        if explicit:
            return explicit

        value = self.biotus_enterprise_code_effective
        if value:
            return value

        if self.business_settings_exists:
            raise RuntimeError(
                "business_settings row exists, but Biotus target cannot be resolved from "
                "business_enterprise_code + biotus_enterprise_code_override"
            )
        raise RuntimeError("BIOTUS_ENTERPRISE_CODE is required for Biotus contour")


async def load_master_business_settings_snapshot() -> MasterBusinessSettingsSnapshot:
    async with get_async_db(commit_on_exit=False) as session:
        row = (
            await session.execute(
                select(BusinessSettings)
                .order_by(BusinessSettings.id)
                .limit(1)
            )
        ).scalar_one_or_none()

        if row is None:
            snapshot = MasterBusinessSettingsSnapshot(
                source="env-fallback",
                business_settings_exists=False,
                business_enterprise_code=_env_optional("MASTER_CATALOG_ENTERPRISE_CODE"),
                daily_publish_enterprise_code_override=_env_optional("MASTER_DAILY_PUBLISH_ENTERPRISE"),
                weekly_salesdrive_enterprise_code_override=_env_optional("MASTER_WEEKLY_SALESDRIVE_ENTERPRISE"),
                biotus_enterprise_code_override=_env_optional("BIOTUS_ENTERPRISE_CODE"),
                master_weekly_enabled=_env_bool("MASTER_WEEKLY_ENABLED", "1"),
                master_weekly_day=(_env_optional("MASTER_WEEKLY_DAY") or "SUN").upper(),
                master_weekly_hour=_env_int("MASTER_WEEKLY_HOUR", 3),
                master_weekly_minute=_env_int("MASTER_WEEKLY_MINUTE", 0),
                master_daily_publish_enabled=_env_bool("MASTER_DAILY_PUBLISH_ENABLED", "1"),
                master_daily_publish_hour=_env_int("MASTER_DAILY_PUBLISH_HOUR", 9),
                master_daily_publish_minute=_env_int("MASTER_DAILY_PUBLISH_MINUTE", 0),
                master_daily_publish_limit=_env_int("MASTER_DAILY_PUBLISH_LIMIT", 0),
                master_archive_enabled=_env_bool("MASTER_ARCHIVE_ENABLED", "1"),
                master_archive_every_minutes=max(1, _env_int("MASTER_ARCHIVE_EVERY_MINUTES", 60)),
                primary_enterprise_exists=bool(_env_optional("MASTER_CATALOG_ENTERPRISE_CODE")),
                biotus_effective_enterprise_exists=bool(_env_optional("BIOTUS_ENTERPRISE_CODE")),
                biotus_enable_unhandled_fallback=_env_bool("BIOTUS_ENABLE_UNHANDLED_FALLBACK", "1"),
                biotus_unhandled_order_timeout_minutes=_env_int("BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES", 60),
                biotus_fallback_additional_status_ids=_env_int_list(
                    "BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS",
                    [9, 19, 18, 20],
                ),
                biotus_duplicate_status_id=_env_int("BIOTUS_DUPLICATE_STATUS_ID", 20),
                biotus_time_default_minutes=_env_int("BIOTUS_TIME_DEFAULT_MINUTES", 30),
                biotus_time_switch_hour=_env_int("BIOTUS_TIME_SWITCH_HOUR", 12),
                biotus_time_switch_end_hour=_env_int("BIOTUS_TIME_SWITCH_END_HOUR", 13),
                biotus_time_after_switch_minutes=_env_int("BIOTUS_TIME_AFTER_SWITCH_MINUTES", 15),
                biotus_tz=_env_optional("BIOTUS_TZ") or "Europe/Kyiv",
            )
            return snapshot

        primary_code = str(row.business_enterprise_code or "").strip() or None
        primary_exists = False
        inconsistency = None
        biotus_code = str(row.biotus_enterprise_code_override or "").strip() or None
        biotus_effective_code = biotus_code or primary_code
        biotus_effective_exists = False
        biotus_inconsistency = None

        if primary_code:
            primary_exists = bool(
                (
                    await session.execute(
                        select(EnterpriseSettings.enterprise_code)
                        .where(EnterpriseSettings.enterprise_code == primary_code)
                        .limit(1)
                    )
                ).scalar_one_or_none()
            )
        if not primary_code:
            inconsistency = "business_settings row exists, but business_enterprise_code is empty."
            logger.warning(inconsistency)
        elif not primary_exists:
            inconsistency = (
                "business_settings row exists, but business_enterprise_code=%s is missing in EnterpriseSettings."
                % primary_code
            )
            logger.warning(inconsistency)

        if biotus_effective_code:
            biotus_effective_exists = bool(
                (
                    await session.execute(
                        select(EnterpriseSettings.enterprise_code)
                        .where(EnterpriseSettings.enterprise_code == biotus_effective_code)
                        .limit(1)
                    )
                ).scalar_one_or_none()
            )
        if not biotus_effective_code:
            biotus_inconsistency = (
                "business_settings row exists, but Biotus target is empty after "
                "business_enterprise_code + biotus_enterprise_code_override resolution."
            )
            logger.warning(biotus_inconsistency)
        elif not biotus_effective_exists:
            biotus_inconsistency = (
                "business_settings row exists, but Biotus effective enterprise_code=%s is missing in EnterpriseSettings."
                % biotus_effective_code
            )
            logger.warning(biotus_inconsistency)

        return MasterBusinessSettingsSnapshot(
            source="db",
            business_settings_exists=True,
            business_enterprise_code=primary_code,
            daily_publish_enterprise_code_override=(
                str(row.daily_publish_enterprise_code_override or "").strip() or None
            ),
            weekly_salesdrive_enterprise_code_override=(
                str(row.weekly_salesdrive_enterprise_code_override or "").strip() or None
            ),
            biotus_enterprise_code_override=biotus_code,
            master_weekly_enabled=bool(row.master_weekly_enabled),
            master_weekly_day=str(row.master_weekly_day or "SUN").upper(),
            master_weekly_hour=int(row.master_weekly_hour),
            master_weekly_minute=int(row.master_weekly_minute),
            master_daily_publish_enabled=bool(row.master_daily_publish_enabled),
            master_daily_publish_hour=int(row.master_daily_publish_hour),
            master_daily_publish_minute=int(row.master_daily_publish_minute),
            master_daily_publish_limit=int(row.master_daily_publish_limit),
            master_archive_enabled=bool(row.master_archive_enabled),
            master_archive_every_minutes=max(1, int(row.master_archive_every_minutes)),
            primary_enterprise_exists=primary_exists,
            biotus_effective_enterprise_exists=biotus_effective_exists,
            biotus_enable_unhandled_fallback=bool(row.biotus_enable_unhandled_fallback),
            biotus_unhandled_order_timeout_minutes=int(row.biotus_unhandled_order_timeout_minutes),
            biotus_fallback_additional_status_ids=tuple(
                int(item) for item in (row.biotus_fallback_additional_status_ids or [9, 19, 18, 20])
            ),
            biotus_duplicate_status_id=int(row.biotus_duplicate_status_id),
            biotus_time_default_minutes=_env_int("BIOTUS_TIME_DEFAULT_MINUTES", 30),
            biotus_time_switch_hour=_env_int("BIOTUS_TIME_SWITCH_HOUR", 12),
            biotus_time_switch_end_hour=_env_int("BIOTUS_TIME_SWITCH_END_HOUR", 13),
            biotus_time_after_switch_minutes=_env_int("BIOTUS_TIME_AFTER_SWITCH_MINUTES", 15),
            biotus_tz=_env_optional("BIOTUS_TZ") or "Europe/Kyiv",
            inconsistency=inconsistency,
            biotus_inconsistency=biotus_inconsistency,
        )

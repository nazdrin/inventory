"""add business settings table

Revision ID: 6f4d2b7c9a11
Revises: 2a6b7c8d9e10
Create Date: 2026-04-10 12:00:00.000000

"""

from typing import Sequence, Union
import os

from alembic import op
import sqlalchemy as sa


revision: str = "6f4d2b7c9a11"
down_revision: Union[str, None] = "2a6b7c8d9e10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


WEEKDAY_VALUES = {"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"}


def _env_bool(name: str, default: str) -> bool:
    raw = (os.getenv(name) or default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or str(default)).strip()
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_str(name: str) -> str | None:
    value = (os.getenv(name) or "").strip()
    return value or None


def _normalize_weekday(raw: str | None, default: str = "SUN") -> str:
    value = (raw or default).strip().upper()
    return value if value in WEEKDAY_VALUES else default


def _normalize_override(value: str | None, primary_code: str) -> str | None:
    normalized = (value or "").strip()
    if not normalized or normalized == primary_code:
        return None
    return normalized


def upgrade() -> None:
    op.create_table(
        "business_settings",
        sa.Column("id", sa.SmallInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("business_enterprise_code", sa.String(), nullable=False),
        sa.Column("daily_publish_enterprise_code_override", sa.String(), nullable=True),
        sa.Column("weekly_salesdrive_enterprise_code_override", sa.String(), nullable=True),
        sa.Column("biotus_enterprise_code_override", sa.String(), nullable=True),
        sa.Column("master_weekly_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("master_weekly_day", sa.String(length=3), server_default=sa.text("'SUN'"), nullable=False),
        sa.Column("master_weekly_hour", sa.SmallInteger(), server_default=sa.text("3"), nullable=False),
        sa.Column("master_weekly_minute", sa.SmallInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("master_daily_publish_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("master_daily_publish_hour", sa.SmallInteger(), server_default=sa.text("9"), nullable=False),
        sa.Column("master_daily_publish_minute", sa.SmallInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("master_daily_publish_limit", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("master_archive_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("master_archive_every_minutes", sa.Integer(), server_default=sa.text("60"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.CheckConstraint("id = 1", name="ck_business_settings_singleton_id"),
        sa.CheckConstraint(
            "master_weekly_day IN ('MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN')",
            name="ck_business_settings_weekly_day",
        ),
        sa.CheckConstraint("master_weekly_hour BETWEEN 0 AND 23", name="ck_business_settings_weekly_hour"),
        sa.CheckConstraint("master_weekly_minute BETWEEN 0 AND 59", name="ck_business_settings_weekly_minute"),
        sa.CheckConstraint(
            "master_daily_publish_hour BETWEEN 0 AND 23",
            name="ck_business_settings_daily_publish_hour",
        ),
        sa.CheckConstraint(
            "master_daily_publish_minute BETWEEN 0 AND 59",
            name="ck_business_settings_daily_publish_minute",
        ),
        sa.CheckConstraint(
            "master_daily_publish_limit >= 0",
            name="ck_business_settings_daily_publish_limit_non_negative",
        ),
        sa.CheckConstraint(
            "master_archive_every_minutes >= 1",
            name="ck_business_settings_archive_every_minutes_positive",
        ),
        sa.ForeignKeyConstraint(
            ["business_enterprise_code"],
            ["enterprise_settings.enterprise_code"],
            name="fk_business_settings_business_enterprise_code",
        ),
        sa.ForeignKeyConstraint(
            ["daily_publish_enterprise_code_override"],
            ["enterprise_settings.enterprise_code"],
            name="fk_business_settings_daily_publish_enterprise_code_override",
        ),
        sa.ForeignKeyConstraint(
            ["weekly_salesdrive_enterprise_code_override"],
            ["enterprise_settings.enterprise_code"],
            name="fk_business_settings_weekly_salesdrive_enterprise_code_override",
        ),
        sa.ForeignKeyConstraint(
            ["biotus_enterprise_code_override"],
            ["enterprise_settings.enterprise_code"],
            name="fk_business_settings_biotus_enterprise_code_override",
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    bind = op.get_bind()
    business_rows = bind.execute(
        sa.text(
            """
            SELECT enterprise_code
            FROM enterprise_settings
            WHERE lower(coalesce(data_format, '')) = 'business'
            ORDER BY enterprise_name, enterprise_code
            """
        )
    ).fetchall()

    if len(business_rows) != 1:
        return

    business_enterprise_code = str(business_rows[0][0]).strip()
    if not business_enterprise_code:
        return

    bind.execute(
        sa.text(
            """
            INSERT INTO business_settings (
                id,
                business_enterprise_code,
                daily_publish_enterprise_code_override,
                weekly_salesdrive_enterprise_code_override,
                biotus_enterprise_code_override,
                master_weekly_enabled,
                master_weekly_day,
                master_weekly_hour,
                master_weekly_minute,
                master_daily_publish_enabled,
                master_daily_publish_hour,
                master_daily_publish_minute,
                master_daily_publish_limit,
                master_archive_enabled,
                master_archive_every_minutes
            ) VALUES (
                :id,
                :business_enterprise_code,
                :daily_publish_enterprise_code_override,
                :weekly_salesdrive_enterprise_code_override,
                :biotus_enterprise_code_override,
                :master_weekly_enabled,
                :master_weekly_day,
                :master_weekly_hour,
                :master_weekly_minute,
                :master_daily_publish_enabled,
                :master_daily_publish_hour,
                :master_daily_publish_minute,
                :master_daily_publish_limit,
                :master_archive_enabled,
                :master_archive_every_minutes
            )
            """
        ),
        {
            "id": 1,
            "business_enterprise_code": business_enterprise_code,
            "daily_publish_enterprise_code_override": _normalize_override(
                _env_str("MASTER_DAILY_PUBLISH_ENTERPRISE"),
                business_enterprise_code,
            ),
            "weekly_salesdrive_enterprise_code_override": _normalize_override(
                _env_str("MASTER_WEEKLY_SALESDRIVE_ENTERPRISE"),
                business_enterprise_code,
            ),
            "biotus_enterprise_code_override": _normalize_override(
                _env_str("BIOTUS_ENTERPRISE_CODE"),
                business_enterprise_code,
            ),
            "master_weekly_enabled": _env_bool("MASTER_WEEKLY_ENABLED", "1"),
            "master_weekly_day": _normalize_weekday(_env_str("MASTER_WEEKLY_DAY"), "SUN"),
            "master_weekly_hour": _env_int("MASTER_WEEKLY_HOUR", 3),
            "master_weekly_minute": _env_int("MASTER_WEEKLY_MINUTE", 0),
            "master_daily_publish_enabled": _env_bool("MASTER_DAILY_PUBLISH_ENABLED", "1"),
            "master_daily_publish_hour": _env_int("MASTER_DAILY_PUBLISH_HOUR", 9),
            "master_daily_publish_minute": _env_int("MASTER_DAILY_PUBLISH_MINUTE", 0),
            "master_daily_publish_limit": max(0, _env_int("MASTER_DAILY_PUBLISH_LIMIT", 0)),
            "master_archive_enabled": _env_bool("MASTER_ARCHIVE_ENABLED", "1"),
            "master_archive_every_minutes": max(1, _env_int("MASTER_ARCHIVE_EVERY_MINUTES", 60)),
        },
    )


def downgrade() -> None:
    op.drop_table("business_settings")

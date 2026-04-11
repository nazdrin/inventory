"""add biotus policy fields to business_settings

Revision ID: 73b2c4d5e6f7
Revises: 6f4d2b7c9a11
Create Date: 2026-04-11 08:00:00.000000

"""

from typing import Sequence, Union
import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "73b2c4d5e6f7"
down_revision: Union[str, None] = "6f4d2b7c9a11"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _env_bool(name: str, default: str) -> bool:
    raw = (os.getenv(name) or default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or str(default)).strip()
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_int_list(name: str, default: list[int]) -> list[int]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return list(default)

    parsed: list[int] = []
    for part in raw.replace(";", ",").split(","):
        item = part.strip()
        if not item:
            continue
        try:
            parsed.append(int(item))
        except (TypeError, ValueError):
            continue
    return parsed or list(default)


def upgrade() -> None:
    op.add_column(
        "business_settings",
        sa.Column("biotus_enable_unhandled_fallback", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("biotus_unhandled_order_timeout_minutes", sa.Integer(), nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column(
            "biotus_fallback_additional_status_ids",
            postgresql.ARRAY(sa.Integer()),
            nullable=True,
        ),
    )
    op.add_column(
        "business_settings",
        sa.Column("biotus_duplicate_status_id", sa.Integer(), nullable=True),
    )

    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            UPDATE business_settings
            SET biotus_enable_unhandled_fallback = :enabled,
                biotus_unhandled_order_timeout_minutes = :timeout_minutes,
                biotus_fallback_additional_status_ids = :status_ids,
                biotus_duplicate_status_id = :duplicate_status_id
            """
        ),
        {
            "enabled": _env_bool("BIOTUS_ENABLE_UNHANDLED_FALLBACK", "1"),
            "timeout_minutes": max(0, _env_int("BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES", 60)),
            "status_ids": _env_int_list("BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS", [9, 19, 18, 20]),
            "duplicate_status_id": max(1, _env_int("BIOTUS_DUPLICATE_STATUS_ID", 20)),
        },
    )

    op.alter_column(
        "business_settings",
        "biotus_enable_unhandled_fallback",
        existing_type=sa.Boolean(),
        server_default=sa.text("true"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "biotus_unhandled_order_timeout_minutes",
        existing_type=sa.Integer(),
        server_default=sa.text("60"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "biotus_fallback_additional_status_ids",
        existing_type=postgresql.ARRAY(sa.Integer()),
        server_default=sa.text("ARRAY[9,19,18,20]"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "biotus_duplicate_status_id",
        existing_type=sa.Integer(),
        server_default=sa.text("20"),
        nullable=False,
    )

    op.create_check_constraint(
        "ck_business_settings_biotus_timeout_non_negative",
        "business_settings",
        "biotus_unhandled_order_timeout_minutes >= 0",
    )
    op.create_check_constraint(
        "ck_business_settings_biotus_duplicate_status_positive",
        "business_settings",
        "biotus_duplicate_status_id >= 1",
    )
    op.create_check_constraint(
        "ck_business_settings_biotus_additional_status_ids_non_empty",
        "business_settings",
        "coalesce(array_length(biotus_fallback_additional_status_ids, 1), 0) >= 1",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_business_settings_biotus_additional_status_ids_non_empty",
        "business_settings",
        type_="check",
    )
    op.drop_constraint(
        "ck_business_settings_biotus_duplicate_status_positive",
        "business_settings",
        type_="check",
    )
    op.drop_constraint(
        "ck_business_settings_biotus_timeout_non_negative",
        "business_settings",
        type_="check",
    )
    op.drop_column("business_settings", "biotus_duplicate_status_id")
    op.drop_column("business_settings", "biotus_fallback_additional_status_ids")
    op.drop_column("business_settings", "biotus_unhandled_order_timeout_minutes")
    op.drop_column("business_settings", "biotus_enable_unhandled_fallback")

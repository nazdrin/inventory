"""add supplier schedule window fields

Revision ID: 2a6b7c8d9e10
Revises: 1c3d4e5f6a70
Create Date: 2026-04-09 12:00:00.000000

"""

from typing import Sequence, Union
import os

from alembic import op
import sqlalchemy as sa


revision: str = "2a6b7c8d9e10"
down_revision: Union[str, None] = "1c3d4e5f6a70"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _parse_day(raw: str | None) -> int | None:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        day = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if 1 <= day <= 7:
        return day
    return None


def _parse_time(raw: str | None) -> str | None:
    value = str(raw or "").strip()
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (TypeError, ValueError):
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def upgrade() -> None:
    op.add_column(
        "dropship_enterprises",
        sa.Column("schedule_enabled", sa.Boolean(), nullable=True, server_default=sa.text("false")),
    )
    op.add_column(
        "dropship_enterprises",
        sa.Column("block_start_day", sa.SmallInteger(), nullable=True),
    )
    op.add_column(
        "dropship_enterprises",
        sa.Column("block_start_time", sa.String(length=5), nullable=True),
    )
    op.add_column(
        "dropship_enterprises",
        sa.Column("block_end_day", sa.SmallInteger(), nullable=True),
    )
    op.add_column(
        "dropship_enterprises",
        sa.Column("block_end_time", sa.String(length=5), nullable=True),
    )

    bind = op.get_bind()
    schedule_globally_enabled = os.getenv("SUPPLIER_SCHEDULE_ENABLED", "").strip().lower() == "true"

    rows = bind.execute(sa.text("SELECT code FROM dropship_enterprises")).fetchall()
    update_stmt = sa.text(
        """
        UPDATE dropship_enterprises
        SET schedule_enabled = :schedule_enabled,
            block_start_day = :block_start_day,
            block_start_time = :block_start_time,
            block_end_day = :block_end_day,
            block_end_time = :block_end_time
        WHERE code = :supplier_code
        """
    )

    for row in rows:
        supplier_code = str(row[0] or "").strip().upper()
        if not supplier_code:
            continue

        prefix = f"SUPPLIER_{supplier_code}_BLOCK_"
        start_day = _parse_day(os.getenv(f"{prefix}START_DAY"))
        start_time = _parse_time(os.getenv(f"{prefix}START_TIME"))
        end_day = _parse_day(os.getenv(f"{prefix}END_DAY"))
        end_time = _parse_time(os.getenv(f"{prefix}END_TIME"))

        is_complete_window = all(value is not None for value in (start_day, start_time, end_day, end_time))

        bind.execute(
            update_stmt,
            {
                "supplier_code": supplier_code,
                "schedule_enabled": bool(schedule_globally_enabled and is_complete_window),
                "block_start_day": start_day if is_complete_window else None,
                "block_start_time": start_time if is_complete_window else None,
                "block_end_day": end_day if is_complete_window else None,
                "block_end_time": end_time if is_complete_window else None,
            },
        )

    bind.execute(sa.text("UPDATE dropship_enterprises SET schedule_enabled = FALSE WHERE schedule_enabled IS NULL"))

    op.alter_column(
        "dropship_enterprises",
        "schedule_enabled",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("false"),
    )


def downgrade() -> None:
    op.drop_column("dropship_enterprises", "block_end_time")
    op.drop_column("dropship_enterprises", "block_end_day")
    op.drop_column("dropship_enterprises", "block_start_time")
    op.drop_column("dropship_enterprises", "block_start_day")
    op.drop_column("dropship_enterprises", "schedule_enabled")

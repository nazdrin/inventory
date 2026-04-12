"""add business stock control fields

Revision ID: 84c1f0e2a9b3
Revises: 73b2c4d5e6f7
Create Date: 2026-04-11 16:20:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "84c1f0e2a9b3"
down_revision: Union[str, None] = "73b2c4d5e6f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "business_settings",
        sa.Column("business_stock_enabled", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "business_settings",
        sa.Column("business_stock_interval_seconds", sa.Integer(), nullable=True),
    )

    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            WITH business_candidates AS (
                SELECT stock_upload_frequency
                FROM enterprise_settings
                WHERE lower(coalesce(data_format, '')) = 'business'
            ),
            resolved_interval AS (
                SELECT CASE
                    WHEN (SELECT count(*) FROM business_candidates) = 1 THEN
                        GREATEST(
                            COALESCE((SELECT stock_upload_frequency FROM business_candidates LIMIT 1), 1),
                            1
                        ) * 60
                    ELSE 60
                END AS interval_seconds
            )
            UPDATE business_settings
            SET business_stock_enabled = true,
                business_stock_interval_seconds = (SELECT interval_seconds FROM resolved_interval)
            """
        )
    )

    op.alter_column(
        "business_settings",
        "business_stock_enabled",
        existing_type=sa.Boolean(),
        server_default=sa.text("true"),
        nullable=False,
    )
    op.alter_column(
        "business_settings",
        "business_stock_interval_seconds",
        existing_type=sa.Integer(),
        server_default=sa.text("60"),
        nullable=False,
    )

    op.create_check_constraint(
        "ck_business_settings_stock_interval_positive",
        "business_settings",
        "business_stock_interval_seconds >= 1",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_business_settings_stock_interval_positive",
        "business_settings",
        type_="check",
    )
    op.drop_column("business_settings", "business_stock_interval_seconds")
    op.drop_column("business_settings", "business_stock_enabled")

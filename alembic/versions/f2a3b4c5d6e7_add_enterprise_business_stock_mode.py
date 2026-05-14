"""add enterprise business stock mode

Revision ID: f2a3b4c5d6e7
Revises: e1b2c3d4f5a6
Create Date: 2026-04-23 14:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "f2a3b4c5d6e7"
down_revision: Union[str, None] = "e1b2c3d4f5a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(table_name: str, column_name: str) -> bool:
    inspector = inspect(op.get_bind())
    return any(item.get("name") == column_name for item in inspector.get_columns(table_name))


def _has_check_constraint(table_name: str, constraint_name: str) -> bool:
    inspector = inspect(op.get_bind())
    return any(item.get("name") == constraint_name for item in inspector.get_check_constraints(table_name))


def upgrade() -> None:
    if not _has_column("enterprise_settings", "business_stock_mode"):
        op.add_column(
            "enterprise_settings",
            sa.Column(
                "business_stock_mode",
                sa.String(length=32),
                server_default=sa.text("'baseline_legacy'"),
                nullable=False,
            ),
        )

    if not _has_check_constraint("enterprise_settings", "ck_enterprise_settings_business_stock_mode"):
        op.create_check_constraint(
            "ck_enterprise_settings_business_stock_mode",
            "enterprise_settings",
            "business_stock_mode IN ('baseline_legacy', 'store_aware')",
        )


def downgrade() -> None:
    if _has_check_constraint("enterprise_settings", "ck_enterprise_settings_business_stock_mode"):
        op.drop_constraint(
            "ck_enterprise_settings_business_stock_mode",
            "enterprise_settings",
            type_="check",
        )
    if _has_column("enterprise_settings", "business_stock_mode"):
        op.drop_column("enterprise_settings", "business_stock_mode")

"""add catalog_enabled and stock_enabled to enterprise_settings

Revision ID: 5b7f1c2d9e10
Revises: 9f2b6c4d8a11
Create Date: 2026-04-05 14:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5b7f1c2d9e10"
down_revision: Union[str, None] = "9f2b6c4d8a11"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "enterprise_settings",
        sa.Column("catalog_enabled", sa.Boolean(), nullable=True, server_default=sa.text("true")),
    )
    op.add_column(
        "enterprise_settings",
        sa.Column("stock_enabled", sa.Boolean(), nullable=True, server_default=sa.text("true")),
    )

    op.execute("UPDATE enterprise_settings SET catalog_enabled = TRUE WHERE catalog_enabled IS NULL")
    op.execute("UPDATE enterprise_settings SET stock_enabled = TRUE WHERE stock_enabled IS NULL")

    op.alter_column(
        "enterprise_settings",
        "catalog_enabled",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("true"),
    )
    op.alter_column(
        "enterprise_settings",
        "stock_enabled",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("true"),
    )


def downgrade() -> None:
    op.drop_column("enterprise_settings", "stock_enabled")
    op.drop_column("enterprise_settings", "catalog_enabled")

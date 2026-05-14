"""extend business_stores for stage 1.1

Revision ID: bb7c8d9e0f31
Revises: aa4b5c6d7e20
Create Date: 2026-04-19 14:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "bb7c8d9e0f31"
down_revision: Union[str, None] = "aa4b5c6d7e20"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "business_stores",
        sa.Column("takes_over_legacy_scope", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column(
        "business_stores",
        sa.Column("migration_status", sa.String(length=64), server_default=sa.text("'draft'"), nullable=False),
    )
    op.add_column(
        "business_stores",
        sa.Column("salesdrive_enterprise_id", sa.Integer(), nullable=True),
    )

    op.create_index(
        "ix_business_stores_takes_over_legacy_scope",
        "business_stores",
        ["takes_over_legacy_scope"],
        unique=False,
    )
    op.create_index(
        "ix_business_stores_migration_status",
        "business_stores",
        ["migration_status"],
        unique=False,
    )
    op.create_index(
        "ix_business_stores_salesdrive_enterprise_id",
        "business_stores",
        ["salesdrive_enterprise_id"],
        unique=False,
    )
    op.create_check_constraint(
        "ck_business_stores_migration_status",
        "business_stores",
        "migration_status IN ('draft', 'dry_run', 'stock_live', 'catalog_stock_live', 'orders_live', 'disabled')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_business_stores_migration_status", "business_stores", type_="check")
    op.drop_index("ix_business_stores_salesdrive_enterprise_id", table_name="business_stores")
    op.drop_index("ix_business_stores_migration_status", table_name="business_stores")
    op.drop_index("ix_business_stores_takes_over_legacy_scope", table_name="business_stores")
    op.drop_column("business_stores", "salesdrive_enterprise_id")
    op.drop_column("business_stores", "migration_status")
    op.drop_column("business_stores", "takes_over_legacy_scope")

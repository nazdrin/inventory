"""add offer block rules table

Revision ID: 9f2b6c4d8a11
Revises: 7d3c2b1a9f4e
Create Date: 2026-03-21 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9f2b6c4d8a11"
down_revision: Union[str, None] = "7d3c2b1a9f4e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "offer_block_rules",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("product_code", sa.String(), nullable=False),
        sa.Column("supplier_code", sa.String(), nullable=True),
        sa.Column("blocked_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("reason", sa.String(length=500), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_offer_block_rules_product_code", "offer_block_rules", ["product_code"], unique=False)
    op.create_index("ix_offer_block_rules_supplier_code", "offer_block_rules", ["supplier_code"], unique=False)
    op.create_index("ix_offer_block_rules_blocked_until", "offer_block_rules", ["blocked_until"], unique=False)
    op.create_index(
        "ix_offer_block_rules_product_supplier_active",
        "offer_block_rules",
        ["product_code", "supplier_code", "is_active"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_offer_block_rules_product_supplier_active", table_name="offer_block_rules")
    op.drop_index("ix_offer_block_rules_blocked_until", table_name="offer_block_rules")
    op.drop_index("ix_offer_block_rules_supplier_code", table_name="offer_block_rules")
    op.drop_index("ix_offer_block_rules_product_code", table_name="offer_block_rules")
    op.drop_table("offer_block_rules")

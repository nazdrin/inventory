"""add store native pricing foundation

Revision ID: d4e5f6a7b8c9
Revises: a7b8c9d0e1f2
Create Date: 2026-04-29 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, None] = "a7b8c9d0e1f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(table_name: str) -> bool:
    inspector = inspect(op.get_bind())
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    if not _has_table("business_store_supplier_settings"):
        op.create_table(
            "business_store_supplier_settings",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("store_id", sa.BigInteger(), nullable=False),
            sa.Column("supplier_code", sa.String(length=255), nullable=False),
            sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
            sa.Column("priority_override", sa.Integer(), nullable=True),
            sa.Column("extra_markup_enabled", sa.Boolean(), server_default=sa.text("false"), nullable=False),
            sa.Column("extra_markup_mode", sa.String(length=32), nullable=True),
            sa.Column("extra_markup_value", sa.Numeric(12, 4), nullable=True),
            sa.Column("extra_markup_min", sa.Numeric(12, 4), nullable=True),
            sa.Column("extra_markup_max", sa.Numeric(12, 4), nullable=True),
            sa.Column("dumping_mode", sa.Boolean(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.ForeignKeyConstraint(["store_id"], ["business_stores.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "store_id",
                "supplier_code",
                name="uq_business_store_supplier_settings_store_supplier",
            ),
            sa.CheckConstraint(
                "(priority_override IS NULL) OR (priority_override >= 0)",
                name="ck_bsss_priority_nn",
            ),
            sa.CheckConstraint(
                "(extra_markup_mode IS NULL) OR (extra_markup_mode IN ('percent'))",
                name="ck_bsss_markup_mode",
            ),
            sa.CheckConstraint(
                "(extra_markup_value IS NULL) OR (extra_markup_value >= 0)",
                name="ck_bsss_markup_value_nn",
            ),
            sa.CheckConstraint(
                "(extra_markup_min IS NULL) OR (extra_markup_min >= 0)",
                name="ck_bsss_markup_min_nn",
            ),
            sa.CheckConstraint(
                "(extra_markup_max IS NULL) OR (extra_markup_max >= 0)",
                name="ck_bsss_markup_max_nn",
            ),
            sa.CheckConstraint(
                "(extra_markup_min IS NULL) OR (extra_markup_max IS NULL) OR (extra_markup_max >= extra_markup_min)",
                name="ck_bsss_markup_max_ge_min",
            ),
        )
        op.create_index("ix_bsss_store_id", "business_store_supplier_settings", ["store_id"], unique=False)
        op.create_index("ix_bsss_supplier_code", "business_store_supplier_settings", ["supplier_code"], unique=False)
        op.create_index("ix_bsss_store_active", "business_store_supplier_settings", ["store_id", "is_active"], unique=False)

    if not _has_table("business_store_offers"):
        op.create_table(
            "business_store_offers",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("store_id", sa.BigInteger(), nullable=False),
            sa.Column("enterprise_code", sa.String(), nullable=False),
            sa.Column("tabletki_branch", sa.String(length=255), nullable=False),
            sa.Column("supplier_code", sa.String(length=255), nullable=False),
            sa.Column("product_code", sa.String(length=255), nullable=False),
            sa.Column("market_scope_key", sa.String(length=255), nullable=True),
            sa.Column("base_price", sa.Numeric(12, 2), nullable=True),
            sa.Column("effective_price", sa.Numeric(12, 2), nullable=False),
            sa.Column("wholesale_price", sa.Numeric(12, 2), nullable=True),
            sa.Column("stock", sa.Integer(), server_default=sa.text("0"), nullable=False),
            sa.Column("priority_used", sa.Integer(), nullable=True),
            sa.Column("price_source", sa.String(length=128), nullable=True),
            sa.Column("pricing_context", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
            sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
            sa.ForeignKeyConstraint(["store_id"], ["business_stores.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "store_id",
                "supplier_code",
                "product_code",
                name="uq_business_store_offers_store_supplier_product",
            ),
            sa.CheckConstraint(
                "effective_price >= 0",
                name="ck_bso_effective_price_nn",
            ),
            sa.CheckConstraint(
                "(base_price IS NULL) OR (base_price >= 0)",
                name="ck_bso_base_price_nn",
            ),
            sa.CheckConstraint(
                "(wholesale_price IS NULL) OR (wholesale_price >= 0)",
                name="ck_bso_wholesale_price_nn",
            ),
            sa.CheckConstraint(
                "stock >= 0",
                name="ck_bso_stock_nn",
            ),
            sa.CheckConstraint(
                "(priority_used IS NULL) OR (priority_used >= 0)",
                name="ck_bso_priority_nn",
            ),
        )
        op.create_index("ix_bso_store_id", "business_store_offers", ["store_id"], unique=False)
        op.create_index("ix_bso_enterprise_code", "business_store_offers", ["enterprise_code"], unique=False)
        op.create_index("ix_bso_tabletki_branch", "business_store_offers", ["tabletki_branch"], unique=False)
        op.create_index("ix_bso_supplier_code", "business_store_offers", ["supplier_code"], unique=False)
        op.create_index("ix_bso_product_code", "business_store_offers", ["product_code"], unique=False)
        op.create_index("ix_bso_store_product", "business_store_offers", ["store_id", "product_code"], unique=False)
        op.create_index("ix_bso_enterprise_branch", "business_store_offers", ["enterprise_code", "tabletki_branch"], unique=False)


def downgrade() -> None:
    if _has_table("business_store_offers"):
        op.drop_index("ix_bso_enterprise_branch", table_name="business_store_offers")
        op.drop_index("ix_bso_store_product", table_name="business_store_offers")
        op.drop_index("ix_bso_product_code", table_name="business_store_offers")
        op.drop_index("ix_bso_supplier_code", table_name="business_store_offers")
        op.drop_index("ix_bso_tabletki_branch", table_name="business_store_offers")
        op.drop_index("ix_bso_enterprise_code", table_name="business_store_offers")
        op.drop_index("ix_bso_store_id", table_name="business_store_offers")
        op.drop_table("business_store_offers")

    if _has_table("business_store_supplier_settings"):
        op.drop_index("ix_bsss_store_active", table_name="business_store_supplier_settings")
        op.drop_index("ix_bsss_supplier_code", table_name="business_store_supplier_settings")
        op.drop_index("ix_bsss_store_id", table_name="business_store_supplier_settings")
        op.drop_table("business_store_supplier_settings")

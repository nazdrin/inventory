"""add business store names and markup foundation

Revision ID: c4d5e6f7a8b9
Revises: bb7c8d9e0f31
Create Date: 2026-04-20 13:20:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c4d5e6f7a8b9"
down_revision: Union[str, None] = "bb7c8d9e0f31"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "business_stores",
        sa.Column("name_strategy", sa.String(length=64), server_default=sa.text("'base'"), nullable=False),
    )
    op.add_column(
        "business_stores",
        sa.Column("extra_markup_enabled", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column(
        "business_stores",
        sa.Column("extra_markup_mode", sa.String(length=32), server_default=sa.text("'percent'"), nullable=False),
    )
    op.add_column(
        "business_stores",
        sa.Column("extra_markup_min", sa.Numeric(precision=12, scale=2), nullable=True),
    )
    op.add_column(
        "business_stores",
        sa.Column("extra_markup_max", sa.Numeric(precision=12, scale=2), nullable=True),
    )
    op.add_column(
        "business_stores",
        sa.Column(
            "extra_markup_strategy",
            sa.String(length=32),
            server_default=sa.text("'stable_per_product'"),
            nullable=False,
        ),
    )

    op.create_index(
        "ix_business_stores_name_strategy",
        "business_stores",
        ["name_strategy"],
        unique=False,
    )
    op.create_index(
        "ix_business_stores_extra_markup_enabled",
        "business_stores",
        ["extra_markup_enabled"],
        unique=False,
    )
    op.create_check_constraint(
        "ck_business_stores_name_strategy",
        "business_stores",
        "name_strategy IN ('base', 'supplier_random')",
    )
    op.create_check_constraint(
        "ck_business_stores_extra_markup_mode",
        "business_stores",
        "extra_markup_mode IN ('percent')",
    )
    op.create_check_constraint(
        "ck_business_stores_extra_markup_strategy",
        "business_stores",
        "extra_markup_strategy IN ('stable_per_product')",
    )
    op.create_check_constraint(
        "ck_business_stores_extra_markup_min_non_negative",
        "business_stores",
        "(extra_markup_min IS NULL) OR (extra_markup_min >= 0)",
    )
    op.create_check_constraint(
        "ck_business_stores_extra_markup_max_non_negative",
        "business_stores",
        "(extra_markup_max IS NULL) OR (extra_markup_max >= 0)",
    )
    op.create_check_constraint(
        "ck_business_stores_extra_markup_max_ge_min",
        "business_stores",
        "(extra_markup_min IS NULL) OR (extra_markup_max IS NULL) OR (extra_markup_max >= extra_markup_min)",
    )

    op.create_table(
        "business_store_product_names",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("store_id", sa.BigInteger(), nullable=False),
        sa.Column("internal_product_code", sa.String(length=255), nullable=False),
        sa.Column("external_product_name", sa.String(length=500), nullable=False),
        sa.Column(
            "name_source",
            sa.String(length=64),
            server_default=sa.text("'catalog_supplier_mapping'"),
            nullable=False,
        ),
        sa.Column("source_supplier_id", sa.BigInteger(), nullable=True),
        sa.Column("source_supplier_code", sa.String(length=255), nullable=True),
        sa.Column("source_supplier_product_id", sa.String(length=255), nullable=True),
        sa.Column("source_supplier_product_name_raw", sa.String(length=500), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["store_id"], ["business_stores.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "store_id",
            "internal_product_code",
            name="uq_business_store_product_names_store_internal",
        ),
        sa.CheckConstraint(
            "name_source IN ('catalog_supplier_mapping', 'manual', 'cleaned')",
            name="ck_business_store_product_names_name_source",
        ),
    )
    op.create_index(
        "ix_bspn_store_id",
        "business_store_product_names",
        ["store_id"],
        unique=False,
    )
    op.create_index(
        "ix_bspn_internal_code",
        "business_store_product_names",
        ["internal_product_code"],
        unique=False,
    )
    op.create_index(
        "ix_bspn_source_supplier",
        "business_store_product_names",
        ["source_supplier_id", "source_supplier_code"],
        unique=False,
    )
    op.create_index(
        "ix_bspn_is_active",
        "business_store_product_names",
        ["is_active"],
        unique=False,
    )

    op.create_table(
        "business_store_product_price_adjustments",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("store_id", sa.BigInteger(), nullable=False),
        sa.Column("internal_product_code", sa.String(length=255), nullable=False),
        sa.Column("markup_percent", sa.Numeric(precision=12, scale=4), nullable=False),
        sa.Column(
            "strategy",
            sa.String(length=32),
            server_default=sa.text("'stable_per_product'"),
            nullable=False,
        ),
        sa.Column("source", sa.String(length=64), server_default=sa.text("'generated'"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["store_id"], ["business_stores.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "store_id",
            "internal_product_code",
            name="uq_business_store_product_price_adjustments_store_internal",
        ),
        sa.CheckConstraint(
            "markup_percent >= 0",
            name="ck_business_store_product_price_adjustments_markup_non_negative",
        ),
        sa.CheckConstraint(
            "strategy IN ('stable_per_product')",
            name="ck_business_store_product_price_adjustments_strategy",
        ),
    )
    op.create_index(
        "ix_bspa_store_id",
        "business_store_product_price_adjustments",
        ["store_id"],
        unique=False,
    )
    op.create_index(
        "ix_bspa_internal_code",
        "business_store_product_price_adjustments",
        ["internal_product_code"],
        unique=False,
    )
    op.create_index(
        "ix_bspa_is_active",
        "business_store_product_price_adjustments",
        ["is_active"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_bspa_is_active",
        table_name="business_store_product_price_adjustments",
    )
    op.drop_index(
        "ix_bspa_internal_code",
        table_name="business_store_product_price_adjustments",
    )
    op.drop_index(
        "ix_bspa_store_id",
        table_name="business_store_product_price_adjustments",
    )
    op.drop_table("business_store_product_price_adjustments")

    op.drop_index(
        "ix_bspn_is_active",
        table_name="business_store_product_names",
    )
    op.drop_index(
        "ix_bspn_source_supplier",
        table_name="business_store_product_names",
    )
    op.drop_index(
        "ix_bspn_internal_code",
        table_name="business_store_product_names",
    )
    op.drop_index(
        "ix_bspn_store_id",
        table_name="business_store_product_names",
    )
    op.drop_table("business_store_product_names")

    op.drop_constraint("ck_business_stores_extra_markup_max_ge_min", "business_stores", type_="check")
    op.drop_constraint("ck_business_stores_extra_markup_max_non_negative", "business_stores", type_="check")
    op.drop_constraint("ck_business_stores_extra_markup_min_non_negative", "business_stores", type_="check")
    op.drop_constraint("ck_business_stores_extra_markup_strategy", "business_stores", type_="check")
    op.drop_constraint("ck_business_stores_extra_markup_mode", "business_stores", type_="check")
    op.drop_constraint("ck_business_stores_name_strategy", "business_stores", type_="check")
    op.drop_index("ix_business_stores_extra_markup_enabled", table_name="business_stores")
    op.drop_index("ix_business_stores_name_strategy", table_name="business_stores")
    op.drop_column("business_stores", "extra_markup_strategy")
    op.drop_column("business_stores", "extra_markup_max")
    op.drop_column("business_stores", "extra_markup_min")
    op.drop_column("business_stores", "extra_markup_mode")
    op.drop_column("business_stores", "extra_markup_enabled")
    op.drop_column("business_stores", "name_strategy")

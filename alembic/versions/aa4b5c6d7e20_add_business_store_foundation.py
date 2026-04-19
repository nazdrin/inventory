"""add business store foundation

Revision ID: aa4b5c6d7e20
Revises: 9c4a7f8b2d11
Create Date: 2026-04-19 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "aa4b5c6d7e20"
down_revision: Union[str, None] = "9c4a7f8b2d11"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "business_stores",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("store_code", sa.String(length=255), nullable=False),
        sa.Column("store_name", sa.String(length=500), nullable=False),
        sa.Column("legal_entity_name", sa.String(length=500), nullable=True),
        sa.Column("tax_identifier", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_legacy_default", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("enterprise_code", sa.String(), nullable=True),
        sa.Column("legacy_scope_key", sa.String(length=255), nullable=True),
        sa.Column("tabletki_enterprise_code", sa.String(length=255), nullable=True),
        sa.Column("tabletki_branch", sa.String(length=255), nullable=True),
        sa.Column("salesdrive_enterprise_code", sa.String(length=255), nullable=True),
        sa.Column("salesdrive_store_name", sa.String(length=500), nullable=True),
        sa.Column("catalog_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("stock_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("orders_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("catalog_only_in_stock", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("code_strategy", sa.String(length=64), server_default=sa.text("'opaque_mapping'"), nullable=False),
        sa.Column("code_prefix", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("store_code", name="uq_business_stores_store_code"),
        sa.CheckConstraint(
            "code_strategy IN ('opaque_mapping', 'legacy_same', 'prefix_mapping')",
            name="ck_business_stores_code_strategy",
        ),
    )
    op.create_index("ix_business_stores_enterprise_code", "business_stores", ["enterprise_code"], unique=False)
    op.create_index("ix_business_stores_legacy_scope_key", "business_stores", ["legacy_scope_key"], unique=False)
    op.create_index("ix_business_stores_is_active", "business_stores", ["is_active"], unique=False)
    op.create_index(
        "ix_business_stores_salesdrive_enterprise_code",
        "business_stores",
        ["salesdrive_enterprise_code"],
        unique=False,
    )
    op.create_index(
        "uq_business_stores_tabletki_identity",
        "business_stores",
        ["tabletki_enterprise_code", "tabletki_branch"],
        unique=True,
        postgresql_where=sa.text(
            "tabletki_enterprise_code IS NOT NULL AND tabletki_branch IS NOT NULL"
        ),
    )

    op.create_table(
        "business_store_product_codes",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("store_id", sa.BigInteger(), nullable=False),
        sa.Column("internal_product_code", sa.String(length=255), nullable=False),
        sa.Column("external_product_code", sa.String(length=255), nullable=False),
        sa.Column("code_source", sa.String(length=64), server_default=sa.text("'generated'"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["store_id"], ["business_stores.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "store_id",
            "internal_product_code",
            name="uq_business_store_product_codes_store_internal",
        ),
        sa.UniqueConstraint(
            "store_id",
            "external_product_code",
            name="uq_business_store_product_codes_store_external",
        ),
        sa.CheckConstraint(
            "code_source IN ('generated', 'legacy_same', 'prefix_mapping')",
            name="ck_business_store_product_codes_code_source",
        ),
    )
    op.create_index(
        "ix_business_store_product_codes_internal_product_code",
        "business_store_product_codes",
        ["internal_product_code"],
        unique=False,
    )
    op.create_index(
        "ix_business_store_product_codes_external_product_code",
        "business_store_product_codes",
        ["external_product_code"],
        unique=False,
    )
    op.create_index(
        "ix_business_store_product_codes_is_active",
        "business_store_product_codes",
        ["is_active"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_business_store_product_codes_is_active",
        table_name="business_store_product_codes",
    )
    op.drop_index(
        "ix_business_store_product_codes_external_product_code",
        table_name="business_store_product_codes",
    )
    op.drop_index(
        "ix_business_store_product_codes_internal_product_code",
        table_name="business_store_product_codes",
    )
    op.drop_table("business_store_product_codes")

    op.drop_index("uq_business_stores_tabletki_identity", table_name="business_stores")
    op.drop_index(
        "ix_business_stores_salesdrive_enterprise_code",
        table_name="business_stores",
    )
    op.drop_index("ix_business_stores_is_active", table_name="business_stores")
    op.drop_index("ix_business_stores_legacy_scope_key", table_name="business_stores")
    op.drop_index("ix_business_stores_enterprise_code", table_name="business_stores")
    op.drop_table("business_stores")

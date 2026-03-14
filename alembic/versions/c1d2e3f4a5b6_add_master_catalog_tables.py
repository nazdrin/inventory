"""add master catalog tables

Revision ID: c1d2e3f4a5b6
Revises: bfdcbfc70e58
Create Date: 2026-03-14 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "c1d2e3f4a5b6"
down_revision: Union[str, None] = "bfdcbfc70e58"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "master_catalog",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("sku", sa.String(length=500), nullable=False),
        sa.Column("barcode", sa.String(length=500), nullable=True),
        sa.Column("manufacturer", sa.String(length=500), nullable=True),
        sa.Column("name_ua", sa.String(length=500), nullable=True),
        sa.Column("name_ru", sa.String(length=500), nullable=True),
        sa.Column("category_l1_code", sa.String(length=500), nullable=True),
        sa.Column("category_l2_code", sa.String(length=500), nullable=True),
        sa.Column("weight_g", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("length_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("width_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("height_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("volume_ml", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("description_ua", sa.Text(), nullable=True),
        sa.Column("description_ru", sa.Text(), nullable=True),
        sa.Column("main_image_url", sa.String(length=500), nullable=True),
        sa.Column("is_archived", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("archived_reason", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sku"),
    )
    op.create_index("ix_master_catalog_barcode", "master_catalog", ["barcode"], unique=False)
    op.create_index("ix_master_catalog_is_archived", "master_catalog", ["is_archived"], unique=False)
    op.create_index("ix_master_catalog_category_l1_code", "master_catalog", ["category_l1_code"], unique=False)
    op.create_index("ix_master_catalog_category_l2_code", "master_catalog", ["category_l2_code"], unique=False)

    op.create_table(
        "catalog_categories",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("category_code", sa.String(length=500), nullable=False),
        sa.Column("parent_category_code", sa.String(length=500), nullable=True),
        sa.Column("name_ua", sa.String(length=500), nullable=False),
        sa.Column("name_ru", sa.String(length=500), nullable=True),
        sa.Column("level_no", sa.Integer(), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("category_code"),
    )
    op.create_index(
        "ix_catalog_categories_parent_category_code",
        "catalog_categories",
        ["parent_category_code"],
        unique=False,
    )
    op.create_index("ix_catalog_categories_level_no", "catalog_categories", ["level_no"], unique=False)
    op.create_index("ix_catalog_categories_is_active", "catalog_categories", ["is_active"], unique=False)

    op.create_table(
        "raw_tabletki_catalog",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("tabletki_guid", sa.String(length=500), nullable=True),
        sa.Column("sku", sa.String(length=500), nullable=False),
        sa.Column("barcode", sa.String(length=500), nullable=True),
        sa.Column("manufacturer", sa.String(length=500), nullable=True),
        sa.Column("name_ua", sa.String(length=500), nullable=True),
        sa.Column("name_ru", sa.String(length=500), nullable=True),
        sa.Column("category_l1_code", sa.String(length=500), nullable=True),
        sa.Column("category_l1_name", sa.String(length=500), nullable=True),
        sa.Column("category_l2_code", sa.String(length=500), nullable=True),
        sa.Column("category_l2_name", sa.String(length=500), nullable=True),
        sa.Column("weight_g", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("length_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("width_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("height_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("volume_ml", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("description_ua", sa.Text(), nullable=True),
        sa.Column("description_ru", sa.Text(), nullable=True),
        sa.Column("source_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("source_hash", sa.String(length=500), nullable=True),
        sa.Column("loaded_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_raw_tabletki_catalog_sku", "raw_tabletki_catalog", ["sku"], unique=False)
    op.create_index(
        "ix_raw_tabletki_catalog_tabletki_guid",
        "raw_tabletki_catalog",
        ["tabletki_guid"],
        unique=False,
    )
    op.create_index(
        "ix_raw_tabletki_catalog_source_hash",
        "raw_tabletki_catalog",
        ["source_hash"],
        unique=False,
    )

    op.create_table(
        "raw_supplier_feed_products",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("supplier_id", sa.BigInteger(), nullable=False),
        sa.Column("feed_product_id", sa.String(length=500), nullable=True),
        sa.Column("supplier_code", sa.String(length=500), nullable=True),
        sa.Column("name_raw", sa.String(length=500), nullable=True),
        sa.Column("manufacturer_raw", sa.String(length=500), nullable=True),
        sa.Column("barcode", sa.String(length=500), nullable=True),
        sa.Column("description_raw", sa.Text(), nullable=True),
        sa.Column("weight_g", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("length_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("width_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("height_mm", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("volume_ml", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("category_raw", sa.String(length=500), nullable=True),
        sa.Column("source_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("source_hash", sa.String(length=500), nullable=True),
        sa.Column("loaded_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_raw_supplier_feed_products_supplier_id",
        "raw_supplier_feed_products",
        ["supplier_id"],
        unique=False,
    )
    op.create_index(
        "ix_raw_supplier_feed_products_supplier_code",
        "raw_supplier_feed_products",
        ["supplier_code"],
        unique=False,
    )
    op.create_index(
        "ix_raw_supplier_feed_products_source_hash",
        "raw_supplier_feed_products",
        ["source_hash"],
        unique=False,
    )

    op.create_table(
        "catalog_supplier_mapping",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("sku", sa.String(length=500), nullable=False),
        sa.Column("supplier_id", sa.BigInteger(), nullable=False),
        sa.Column("supplier_code", sa.String(length=500), nullable=False),
        sa.Column("supplier_product_id", sa.String(length=500), nullable=True),
        sa.Column("supplier_product_name_raw", sa.String(length=500), nullable=True),
        sa.Column("barcode", sa.String(length=500), nullable=True),
        sa.Column("is_confirmed", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("match_source", sa.String(length=500), server_default=sa.text("'auto'"), nullable=False),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["sku"], ["master_catalog.sku"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("supplier_id", "supplier_code", name="uq_catalog_supplier_mapping_supplier_code"),
    )
    op.create_index("ix_catalog_supplier_mapping_sku", "catalog_supplier_mapping", ["sku"], unique=False)
    op.create_index(
        "ix_catalog_supplier_mapping_supplier_id",
        "catalog_supplier_mapping",
        ["supplier_id"],
        unique=False,
    )
    op.create_index(
        "ix_catalog_supplier_mapping_sku_is_active",
        "catalog_supplier_mapping",
        ["sku", "is_active"],
        unique=False,
    )

    op.create_table(
        "catalog_images",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("sku", sa.String(length=500), nullable=False),
        sa.Column("supplier_id", sa.BigInteger(), nullable=True),
        sa.Column("source_type", sa.String(length=500), nullable=True),
        sa.Column("image_url", sa.String(length=500), nullable=False),
        sa.Column("sort_order", sa.Integer(), server_default=sa.text("0"), nullable=True),
        sa.Column("is_main", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["sku"], ["master_catalog.sku"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_catalog_images_sku", "catalog_images", ["sku"], unique=False)
    op.create_index("ix_catalog_images_sku_is_active", "catalog_images", ["sku", "is_active"], unique=False)

    op.create_table(
        "catalog_content",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("sku", sa.String(length=500), nullable=False),
        sa.Column("language_code", sa.String(length=500), nullable=False),
        sa.Column("source_type", sa.String(length=500), nullable=True),
        sa.Column("supplier_id", sa.BigInteger(), nullable=True),
        sa.Column("title", sa.String(length=500), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_selected", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["sku"], ["master_catalog.sku"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_catalog_content_sku", "catalog_content", ["sku"], unique=False)
    op.create_index(
        "ix_catalog_content_sku_language_code",
        "catalog_content",
        ["sku", "language_code"],
        unique=False,
    )
    op.create_index(
        "ix_catalog_content_sku_is_selected",
        "catalog_content",
        ["sku", "is_selected"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_table("catalog_content")
    op.drop_table("catalog_images")
    op.drop_table("catalog_supplier_mapping")
    op.drop_table("raw_supplier_feed_products")
    op.drop_table("raw_tabletki_catalog")
    op.drop_table("catalog_categories")
    op.drop_table("master_catalog")

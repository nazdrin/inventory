"""add business enterprise catalog identity tables

Revision ID: e1b2c3d4f5a6
Revises: c4d5e6f7a8b9
Create Date: 2026-04-22 14:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "e1b2c3d4f5a6"
down_revision: Union[str, None] = "c4d5e6f7a8b9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(table_name: str) -> bool:
    return inspect(op.get_bind()).has_table(table_name)


def _has_index(table_name: str, index_name: str) -> bool:
    inspector = inspect(op.get_bind())
    return any(item.get("name") == index_name for item in inspector.get_indexes(table_name))


def upgrade() -> None:
    if not _has_table("business_enterprise_product_codes"):
        op.create_table(
            "business_enterprise_product_codes",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("enterprise_code", sa.String(), nullable=False),
            sa.Column("internal_product_code", sa.String(length=255), nullable=False),
            sa.Column("external_product_code", sa.String(length=255), nullable=False),
            sa.Column("code_source", sa.String(length=64), server_default=sa.text("'generated'"), nullable=False),
            sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
            sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "enterprise_code",
                "internal_product_code",
                name="uq_business_enterprise_product_codes_enterprise_internal",
            ),
            sa.UniqueConstraint(
                "enterprise_code",
                "external_product_code",
                name="uq_business_enterprise_product_codes_enterprise_external",
            ),
            sa.CheckConstraint(
                "code_source IN ('generated', 'legacy_same', 'prefix_mapping', 'backfilled_from_store')",
                name="ck_business_enterprise_product_codes_code_source",
            ),
        )
    if _has_table("business_enterprise_product_codes") and not _has_index("business_enterprise_product_codes", "ix_bepc_enterprise_code"):
        op.create_index("ix_bepc_enterprise_code", "business_enterprise_product_codes", ["enterprise_code"], unique=False)
    if _has_table("business_enterprise_product_codes") and not _has_index("business_enterprise_product_codes", "ix_bepc_internal_code"):
        op.create_index("ix_bepc_internal_code", "business_enterprise_product_codes", ["internal_product_code"], unique=False)
    if _has_table("business_enterprise_product_codes") and not _has_index("business_enterprise_product_codes", "ix_bepc_external_code"):
        op.create_index("ix_bepc_external_code", "business_enterprise_product_codes", ["external_product_code"], unique=False)
    if _has_table("business_enterprise_product_codes") and not _has_index("business_enterprise_product_codes", "ix_bepc_is_active"):
        op.create_index("ix_bepc_is_active", "business_enterprise_product_codes", ["is_active"], unique=False)

    if not _has_table("business_enterprise_product_names"):
        op.create_table(
            "business_enterprise_product_names",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("enterprise_code", sa.String(), nullable=False),
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
            sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "enterprise_code",
                "internal_product_code",
                name="uq_business_enterprise_product_names_enterprise_internal",
            ),
            sa.CheckConstraint(
                "name_source IN ('catalog_supplier_mapping', 'manual', 'cleaned', 'backfilled_from_store')",
                name="ck_business_enterprise_product_names_name_source",
            ),
        )
    if _has_table("business_enterprise_product_names") and not _has_index("business_enterprise_product_names", "ix_bepn_enterprise_code"):
        op.create_index("ix_bepn_enterprise_code", "business_enterprise_product_names", ["enterprise_code"], unique=False)
    if _has_table("business_enterprise_product_names") and not _has_index("business_enterprise_product_names", "ix_bepn_internal_code"):
        op.create_index("ix_bepn_internal_code", "business_enterprise_product_names", ["internal_product_code"], unique=False)
    if _has_table("business_enterprise_product_names") and not _has_index("business_enterprise_product_names", "ix_bepn_is_active"):
        op.create_index("ix_bepn_is_active", "business_enterprise_product_names", ["is_active"], unique=False)
    if _has_table("business_enterprise_product_names") and not _has_index("business_enterprise_product_names", "ix_bepn_source_supplier"):
        op.create_index("ix_bepn_source_supplier", "business_enterprise_product_names", ["source_supplier_id", "source_supplier_code"], unique=False)


def downgrade() -> None:
    if _has_table("business_enterprise_product_names"):
        if _has_index("business_enterprise_product_names", "ix_bepn_source_supplier"):
            op.drop_index("ix_bepn_source_supplier", table_name="business_enterprise_product_names")
        if _has_index("business_enterprise_product_names", "ix_bepn_is_active"):
            op.drop_index("ix_bepn_is_active", table_name="business_enterprise_product_names")
        if _has_index("business_enterprise_product_names", "ix_bepn_internal_code"):
            op.drop_index("ix_bepn_internal_code", table_name="business_enterprise_product_names")
        if _has_index("business_enterprise_product_names", "ix_bepn_enterprise_code"):
            op.drop_index("ix_bepn_enterprise_code", table_name="business_enterprise_product_names")
        op.drop_table("business_enterprise_product_names")

    if _has_table("business_enterprise_product_codes"):
        if _has_index("business_enterprise_product_codes", "ix_bepc_is_active"):
            op.drop_index("ix_bepc_is_active", table_name="business_enterprise_product_codes")
        if _has_index("business_enterprise_product_codes", "ix_bepc_external_code"):
            op.drop_index("ix_bepc_external_code", table_name="business_enterprise_product_codes")
        if _has_index("business_enterprise_product_codes", "ix_bepc_internal_code"):
            op.drop_index("ix_bepc_internal_code", table_name="business_enterprise_product_codes")
        if _has_index("business_enterprise_product_codes", "ix_bepc_enterprise_code"):
            op.drop_index("ix_bepc_enterprise_code", table_name="business_enterprise_product_codes")
        op.drop_table("business_enterprise_product_codes")

"""add order reporting foundation

Revision ID: 9f1a2b3c4d5e
Revises: a1b2c3d4e5f6
Create Date: 2026-05-08 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision: str = "9f1a2b3c4d5e"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(table_name: str) -> bool:
    return table_name in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_table("report_orders"):
        op.create_table(
            "report_orders",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("source", sa.String(length=64), server_default=sa.text("'salesdrive'"), nullable=False),
            sa.Column("enterprise_code", sa.String(), nullable=False),
            sa.Column("business_store_id", sa.BigInteger(), nullable=True),
            sa.Column("branch", sa.String(length=255), nullable=True),
            sa.Column("external_order_id", sa.String(length=255), nullable=False),
            sa.Column("salesdrive_order_id", sa.String(length=255), nullable=True),
            sa.Column("tabletki_order_id", sa.String(length=255), nullable=True),
            sa.Column("order_number", sa.String(length=255), nullable=True),
            sa.Column("order_created_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("order_updated_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("sale_date", sa.DateTime(timezone=True), nullable=True),
            sa.Column("status_id", sa.Integer(), nullable=True),
            sa.Column("status_name", sa.String(length=255), nullable=True),
            sa.Column("status_group", sa.String(length=64), server_default=sa.text("'active'"), nullable=False),
            sa.Column("is_order", sa.Boolean(), server_default=sa.text("true"), nullable=False),
            sa.Column("is_sale", sa.Boolean(), server_default=sa.text("false"), nullable=False),
            sa.Column("is_return", sa.Boolean(), server_default=sa.text("false"), nullable=False),
            sa.Column("is_cancelled", sa.Boolean(), server_default=sa.text("false"), nullable=False),
            sa.Column("is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False),
            sa.Column("customer_city", sa.String(length=255), nullable=True),
            sa.Column("payment_type", sa.String(length=255), nullable=True),
            sa.Column("delivery_type", sa.String(length=255), nullable=True),
            sa.Column("order_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("sale_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("items_quantity", sa.Numeric(14, 3), server_default=sa.text("0"), nullable=False),
            sa.Column("sale_quantity", sa.Numeric(14, 3), server_default=sa.text("0"), nullable=False),
            sa.Column("supplier_cost_total", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("gross_profit_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("expense_percent", sa.Numeric(8, 4), server_default=sa.text("0"), nullable=False),
            sa.Column("expense_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("net_profit_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("raw_hash", sa.String(length=64), nullable=True),
            sa.Column("raw_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.ForeignKeyConstraint(["business_store_id"], ["business_stores.id"]),
            sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("source", "enterprise_code", "external_order_id", name="uq_report_orders_source_enterprise_external"),
            sa.CheckConstraint("order_amount >= 0", name="ck_report_orders_order_amount_nonneg"),
            sa.CheckConstraint("sale_amount >= 0", name="ck_report_orders_sale_amount_nonneg"),
            sa.CheckConstraint("supplier_cost_total >= 0", name="ck_report_orders_cost_nonneg"),
            sa.CheckConstraint("expense_percent >= 0", name="ck_report_orders_expense_percent_nonneg"),
        )
        op.create_index("ix_report_orders_enterprise_created", "report_orders", ["enterprise_code", "order_created_at"])
        op.create_index("ix_report_orders_enterprise_sale_date", "report_orders", ["enterprise_code", "sale_date"])
        op.create_index("ix_report_orders_status_group", "report_orders", ["status_group"])
        op.create_index("ix_report_orders_salesdrive_order_id", "report_orders", ["salesdrive_order_id"])
        op.create_index("ix_report_orders_tabletki_order_id", "report_orders", ["tabletki_order_id"])

    if not _has_table("report_order_items"):
        op.create_table(
            "report_order_items",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("report_order_id", sa.BigInteger(), nullable=False),
            sa.Column("line_index", sa.Integer(), nullable=False),
            sa.Column("source_product_id", sa.String(length=255), nullable=True),
            sa.Column("sku", sa.String(length=255), nullable=True),
            sa.Column("barcode", sa.String(length=255), nullable=True),
            sa.Column("product_name", sa.String(length=1000), nullable=True),
            sa.Column("supplier_name", sa.String(length=500), nullable=True),
            sa.Column("supplier_code", sa.String(length=255), nullable=True),
            sa.Column("quantity", sa.Numeric(14, 3), server_default=sa.text("0"), nullable=False),
            sa.Column("sale_price", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("sale_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("cost_price", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("cost_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("gross_profit_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
            sa.Column("margin_percent", sa.Numeric(8, 4), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.ForeignKeyConstraint(["report_order_id"], ["report_orders.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("report_order_id", "line_index", name="uq_report_order_items_order_line"),
            sa.CheckConstraint("quantity >= 0", name="ck_report_order_items_quantity_nonneg"),
            sa.CheckConstraint("sale_amount >= 0", name="ck_report_order_items_sale_nonneg"),
            sa.CheckConstraint("cost_amount >= 0", name="ck_report_order_items_cost_nonneg"),
        )
        op.create_index("ix_report_order_items_order_id", "report_order_items", ["report_order_id"])
        op.create_index("ix_report_order_items_supplier_code", "report_order_items", ["supplier_code"])
        op.create_index("ix_report_order_items_sku", "report_order_items", ["sku"])

    if not _has_table("report_enterprise_expense_settings"):
        op.create_table(
            "report_enterprise_expense_settings",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("enterprise_code", sa.String(), nullable=False),
            sa.Column("expense_percent", sa.Numeric(8, 4), server_default=sa.text("0"), nullable=False),
            sa.Column("active_from", sa.Date(), nullable=False),
            sa.Column("active_to", sa.Date(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
            sa.PrimaryKeyConstraint("id"),
            sa.CheckConstraint("expense_percent >= 0", name="ck_report_expense_settings_percent_nonneg"),
        )
        op.create_index(
            "ix_report_expense_settings_enterprise",
            "report_enterprise_expense_settings",
            ["enterprise_code", "active_from", "active_to"],
        )

    if not _has_table("report_order_sync_state"):
        op.create_table(
            "report_order_sync_state",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("source", sa.String(length=64), server_default=sa.text("'salesdrive'"), nullable=False),
            sa.Column("enterprise_code", sa.String(), nullable=True),
            sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_sync_from", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_sync_to", sa.DateTime(timezone=True), nullable=True),
            sa.Column("status", sa.String(length=32), server_default=sa.text("'running'"), nullable=False),
            sa.Column("error_message", sa.Text(), nullable=True),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
            sa.Column("updated_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
            sa.Column("failed_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
            sa.Column("request_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
            sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
            sa.PrimaryKeyConstraint("id"),
            sa.CheckConstraint("status IN ('running', 'success', 'failed', 'partial')", name="ck_report_order_sync_state_status"),
        )
        op.create_index("ix_report_order_sync_state_source_enterprise", "report_order_sync_state", ["source", "enterprise_code"])
        op.create_index("ix_report_order_sync_state_status", "report_order_sync_state", ["status"])


def downgrade() -> None:
    for index_name, table_name in (
        ("ix_report_order_sync_state_status", "report_order_sync_state"),
        ("ix_report_order_sync_state_source_enterprise", "report_order_sync_state"),
        ("ix_report_expense_settings_enterprise", "report_enterprise_expense_settings"),
        ("ix_report_order_items_sku", "report_order_items"),
        ("ix_report_order_items_supplier_code", "report_order_items"),
        ("ix_report_order_items_order_id", "report_order_items"),
        ("ix_report_orders_tabletki_order_id", "report_orders"),
        ("ix_report_orders_salesdrive_order_id", "report_orders"),
        ("ix_report_orders_status_group", "report_orders"),
        ("ix_report_orders_enterprise_sale_date", "report_orders"),
        ("ix_report_orders_enterprise_created", "report_orders"),
    ):
        if _has_table(table_name):
            op.drop_index(index_name, table_name=table_name)
    for table_name in (
        "report_order_sync_state",
        "report_enterprise_expense_settings",
        "report_order_items",
        "report_orders",
    ):
        if _has_table(table_name):
            op.drop_table(table_name)

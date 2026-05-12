"""add checkbox fiscalization tables

Revision ID: b6c7d8e9f012
Revises: 9f1a2b3c4d5e
Create Date: 2026-05-09 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "b6c7d8e9f012"
down_revision: Union[str, None] = "9f1a2b3c4d5e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "checkbox_receipts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("salesdrive_order_id", sa.String(length=255), nullable=False),
        sa.Column("salesdrive_external_id", sa.String(length=255), nullable=True),
        sa.Column("enterprise_code", sa.String(), nullable=False),
        sa.Column("cash_register_code", sa.String(length=255), nullable=True),
        sa.Column("salesdrive_status_id", sa.Integer(), nullable=True),
        sa.Column("checkbox_receipt_id", sa.String(length=255), nullable=True),
        sa.Column("checkbox_order_id", sa.String(length=255), nullable=True),
        sa.Column("checkbox_shift_id", sa.String(length=255), nullable=True),
        sa.Column("checkbox_status", sa.String(length=32), server_default=sa.text("'draft'"), nullable=False),
        sa.Column("fiscal_code", sa.String(length=255), nullable=True),
        sa.Column("receipt_url", sa.String(length=1000), nullable=True),
        sa.Column("total_amount", sa.Numeric(14, 2), nullable=True),
        sa.Column("items_count", sa.Integer(), nullable=True),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("response_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("fiscalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "checkbox_status IN ('draft', 'pending', 'fiscalized', 'failed', 'cancelled', 'skipped')",
            name="ck_checkbox_receipts_status",
        ),
        sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "enterprise_code",
            "salesdrive_order_id",
            name="uq_checkbox_receipts_enterprise_salesdrive_order",
        ),
    )
    op.create_index(
        "ix_checkbox_receipts_enterprise_created",
        "checkbox_receipts",
        ["enterprise_code", "created_at"],
    )
    op.create_index("ix_checkbox_receipts_receipt_id", "checkbox_receipts", ["checkbox_receipt_id"])
    op.create_index(
        "ix_checkbox_receipts_status_retry",
        "checkbox_receipts",
        ["checkbox_status", "next_retry_at"],
    )

    op.create_table(
        "checkbox_shifts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("enterprise_code", sa.String(), nullable=False),
        sa.Column("cash_register_code", sa.String(length=255), nullable=True),
        sa.Column("checkbox_shift_id", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=32), server_default=sa.text("'opening'"), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("receipts_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("receipts_total_amount", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
        sa.Column("response_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint(
            "status IN ('opening', 'opened', 'closing', 'closed', 'failed')",
            name="ck_checkbox_shifts_status",
        ),
        sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "enterprise_code",
            "cash_register_code",
            "checkbox_shift_id",
            name="uq_checkbox_shifts_enterprise_cash_register_shift",
        ),
    )
    op.create_index("ix_checkbox_shifts_enterprise_status", "checkbox_shifts", ["enterprise_code", "status"])
    op.create_index("ix_checkbox_shifts_cash_register_status", "checkbox_shifts", ["cash_register_code", "status"])


def downgrade() -> None:
    op.drop_index("ix_checkbox_shifts_cash_register_status", table_name="checkbox_shifts")
    op.drop_index("ix_checkbox_shifts_enterprise_status", table_name="checkbox_shifts")
    op.drop_table("checkbox_shifts")

    op.drop_index("ix_checkbox_receipts_status_retry", table_name="checkbox_receipts")
    op.drop_index("ix_checkbox_receipts_receipt_id", table_name="checkbox_receipts")
    op.drop_index("ix_checkbox_receipts_enterprise_created", table_name="checkbox_receipts")
    op.drop_table("checkbox_receipts")

"""add payment reporting foundation

Revision ID: f8a9b0c1d2e3
Revises: e6f7a8b9c0d1
Create Date: 2026-05-03 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "f8a9b0c1d2e3"
down_revision: Union[str, None] = "e6f7a8b9c0d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


PAYMENT_CATEGORIES = [
    {"code": "customer_receipt", "name": "Customer receipt", "direction": "incoming", "sort_order": 10},
    {"code": "other_receipt", "name": "Other receipt", "direction": "incoming", "sort_order": 20},
    {"code": "excluded_receipt", "name": "Excluded receipt", "direction": "incoming", "sort_order": 30},
    {"code": "internal_transfer", "name": "Internal transfer", "direction": "both", "sort_order": 40},
    {"code": "unknown_incoming", "name": "Unknown incoming", "direction": "incoming", "sort_order": 50},
    {"code": "supplier_payment", "name": "Supplier payment", "direction": "outgoing", "sort_order": 110},
    {"code": "bank_fee", "name": "Bank fee", "direction": "outgoing", "sort_order": 120},
    {"code": "tax_payment", "name": "Tax payment", "direction": "outgoing", "sort_order": 130},
    {"code": "refund_to_customer", "name": "Refund to customer", "direction": "outgoing", "sort_order": 140},
    {"code": "salary_or_personal", "name": "Salary or personal expense", "direction": "outgoing", "sort_order": 150},
    {"code": "owner_withdrawal", "name": "Owner withdrawal", "direction": "outgoing", "sort_order": 155},
    {"code": "logistics_expense", "name": "Logistics expense", "direction": "outgoing", "sort_order": 160},
    {"code": "platform_fee", "name": "Platform fee", "direction": "outgoing", "sort_order": 165},
    {"code": "payment_service_fee", "name": "Payment service fee", "direction": "outgoing", "sort_order": 170},
    {"code": "other_expense", "name": "Other expense", "direction": "outgoing", "sort_order": 180},
    {"code": "unknown_outgoing", "name": "Unknown outgoing", "direction": "outgoing", "sort_order": 190},
]


def upgrade() -> None:
    op.create_table(
        "payment_business_entities",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("salesdrive_organization_id", sa.String(length=128), nullable=True),
        sa.Column("short_name", sa.String(length=500), nullable=False),
        sa.Column("full_name", sa.String(length=1000), nullable=True),
        sa.Column("normalized_name", sa.String(length=1000), nullable=True),
        sa.Column("tax_id", sa.String(length=64), nullable=True),
        sa.Column("entity_type", sa.String(length=32), server_default=sa.text("'other'"), nullable=False),
        sa.Column("verification_status", sa.String(length=32), server_default=sa.text("'needs_review'"), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verified_by", sa.String(length=255), nullable=True),
        sa.Column("vat_enabled", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("vat_payer", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("without_stamp", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("signer_name", sa.String(length=500), nullable=True),
        sa.Column("signer_position", sa.String(length=500), nullable=True),
        sa.Column("chief_accountant_name", sa.String(length=500), nullable=True),
        sa.Column("cashier_name", sa.String(length=500), nullable=True),
        sa.Column("signature_stamp_image_url", sa.String(length=1000), nullable=True),
        sa.Column("logo_image_url", sa.String(length=1000), nullable=True),
        sa.Column("address", sa.String(length=1000), nullable=True),
        sa.Column("postal_code", sa.String(length=64), nullable=True),
        sa.Column("city", sa.String(length=255), nullable=True),
        sa.Column("region", sa.String(length=255), nullable=True),
        sa.Column("country", sa.String(length=255), nullable=True),
        sa.Column("phone", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint(
            "entity_type IN ('fop', 'company', 'individual', 'other')",
            name="ck_payment_business_entities_entity_type",
        ),
        sa.CheckConstraint(
            "verification_status IN ('draft', 'needs_review', 'verified', 'archived')",
            name="ck_payment_business_entities_verification_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("salesdrive_organization_id", name="uq_payment_business_entities_salesdrive_org_id"),
    )
    op.create_index("ix_payment_business_entities_tax_id", "payment_business_entities", ["tax_id"], unique=False)
    op.create_index(
        "ix_payment_business_entities_verification_status",
        "payment_business_entities",
        ["verification_status"],
        unique=False,
    )

    op.create_table(
        "payment_business_accounts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("business_entity_id", sa.BigInteger(), nullable=False),
        sa.Column("salesdrive_account_id", sa.String(length=128), nullable=True),
        sa.Column("account_number", sa.String(length=128), nullable=False),
        sa.Column("account_title", sa.String(length=500), nullable=True),
        sa.Column("label", sa.String(length=255), nullable=True),
        sa.Column("card_mask", sa.String(length=64), nullable=True),
        sa.Column("currency", sa.String(length=16), server_default=sa.text("'UAH'"), nullable=False),
        sa.Column("bank_name", sa.String(length=255), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.ForeignKeyConstraint(["business_entity_id"], ["payment_business_entities.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_number", name="uq_payment_business_accounts_account_number"),
        sa.UniqueConstraint("salesdrive_account_id", name="uq_payment_business_accounts_salesdrive_account_id"),
    )
    op.create_index("ix_payment_business_accounts_entity_id", "payment_business_accounts", ["business_entity_id"])
    op.create_index("ix_payment_business_accounts_is_active", "payment_business_accounts", ["is_active"])

    op.create_table(
        "payment_import_runs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_system", sa.String(length=64), server_default=sa.text("'salesdrive'"), nullable=False),
        sa.Column("period_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payment_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), server_default=sa.text("'running'"), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("incoming_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("outcoming_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("created_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("updated_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("request_params", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint("payment_type IN ('incoming', 'outcoming', 'all')", name="ck_payment_import_runs_payment_type"),
        sa.CheckConstraint("status IN ('running', 'success', 'failed', 'partial')", name="ck_payment_import_runs_status"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_payment_import_runs_period", "payment_import_runs", ["period_from", "period_to"])
    op.create_index("ix_payment_import_runs_status", "payment_import_runs", ["status"])

    op.create_table(
        "payment_categories",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("direction", sa.String(length=16), nullable=False),
        sa.Column("is_system", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("sort_order", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint("direction IN ('incoming', 'outgoing', 'both')", name="ck_payment_categories_direction"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code", name="uq_payment_categories_code"),
    )
    op.create_index("ix_payment_categories_direction", "payment_categories", ["direction"])

    payment_categories = sa.table(
        "payment_categories",
        sa.column("code", sa.String()),
        sa.column("name", sa.String()),
        sa.column("direction", sa.String()),
        sa.column("is_system", sa.Boolean()),
        sa.column("is_active", sa.Boolean()),
        sa.column("sort_order", sa.Integer()),
    )
    op.bulk_insert(
        payment_categories,
        [{**item, "is_system": True, "is_active": True} for item in PAYMENT_CATEGORIES],
    )

    op.create_table(
        "payment_counterparty_supplier_mappings",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("supplier_code", sa.String(), nullable=False),
        sa.Column("supplier_salesdrive_id", sa.Integer(), nullable=True),
        sa.Column("match_type", sa.String(length=32), nullable=False),
        sa.Column("field_scope", sa.String(length=32), nullable=False),
        sa.Column("counterparty_pattern", sa.String(length=1000), nullable=True),
        sa.Column("normalized_pattern", sa.String(length=1000), nullable=True),
        sa.Column("counterparty_tax_id", sa.String(length=64), nullable=True),
        sa.Column("priority", sa.Integer(), server_default=sa.text("100"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("valid_from", sa.Date(), nullable=True),
        sa.Column("valid_to", sa.Date(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column("updated_by", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint(
            "match_type IN ('tax_id', 'exact', 'contains', 'search_text_contains')",
            name="ck_payment_counterparty_supplier_mappings_match_type",
        ),
        sa.CheckConstraint(
            "field_scope IN ('tax_id', 'counterparty_name', 'purpose', 'comment', 'search_text')",
            name="ck_payment_counterparty_supplier_mappings_field_scope",
        ),
        sa.ForeignKeyConstraint(["supplier_code"], ["dropship_enterprises.code"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_payment_counterparty_supplier_mappings_active_priority",
        "payment_counterparty_supplier_mappings",
        ["is_active", "priority"],
    )
    op.create_index(
        "ix_payment_counterparty_supplier_mappings_supplier_code",
        "payment_counterparty_supplier_mappings",
        ["supplier_code"],
    )
    op.create_index(
        "ix_payment_counterparty_supplier_mappings_tax_id",
        "payment_counterparty_supplier_mappings",
        ["counterparty_tax_id"],
    )

    op.create_table(
        "salesdrive_payments",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_system", sa.String(length=64), server_default=sa.text("'salesdrive'"), nullable=False),
        sa.Column("source_payment_id", sa.String(length=128), nullable=False),
        sa.Column("payment_type", sa.String(length=32), nullable=False),
        sa.Column("payment_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("currency", sa.String(length=16), server_default=sa.text("'UAH'"), nullable=False),
        sa.Column("counterparty_source_id", sa.String(length=128), nullable=True),
        sa.Column("counterparty_name", sa.String(length=1000), nullable=True),
        sa.Column("counterparty_normalized_name", sa.String(length=1000), nullable=True),
        sa.Column("counterparty_tax_id", sa.String(length=64), nullable=True),
        sa.Column("organization_source_id", sa.String(length=128), nullable=True),
        sa.Column("organization_name", sa.String(length=1000), nullable=True),
        sa.Column("organization_tax_id", sa.String(length=64), nullable=True),
        sa.Column("organization_account_source_id", sa.String(length=128), nullable=True),
        sa.Column("account_reference", sa.String(length=128), nullable=True),
        sa.Column("business_entity_id", sa.BigInteger(), nullable=True),
        sa.Column("business_account_id", sa.BigInteger(), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("purpose", sa.Text(), nullable=True),
        sa.Column("search_text", sa.Text(), nullable=True),
        sa.Column("incoming_category", sa.String(length=64), nullable=True),
        sa.Column("outgoing_category", sa.String(length=64), nullable=True),
        sa.Column("payment_category", sa.String(length=64), nullable=True),
        sa.Column("is_internal_transfer", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("internal_transfer_pair_id", sa.BigInteger(), nullable=True),
        sa.Column("internal_transfer_reason", sa.Text(), nullable=True),
        sa.Column("supplier_code", sa.String(), nullable=True),
        sa.Column("supplier_salesdrive_id", sa.Integer(), nullable=True),
        sa.Column("counterparty_supplier_mapping_id", sa.BigInteger(), nullable=True),
        sa.Column("mapping_source", sa.String(length=64), nullable=True),
        sa.Column("mapping_status", sa.String(length=32), server_default=sa.text("'not_applicable'"), nullable=False),
        sa.Column("raw_status", sa.String(length=64), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("import_run_id", sa.BigInteger(), nullable=True),
        sa.Column("is_locked", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint("payment_type IN ('incoming', 'outcoming')", name="ck_salesdrive_payments_payment_type"),
        sa.CheckConstraint(
            "mapping_status IN ('mapped', 'unmapped', 'not_applicable', 'ignored')",
            name="ck_salesdrive_payments_mapping_status",
        ),
        sa.ForeignKeyConstraint(["business_account_id"], ["payment_business_accounts.id"]),
        sa.ForeignKeyConstraint(["business_entity_id"], ["payment_business_entities.id"]),
        sa.ForeignKeyConstraint(["counterparty_supplier_mapping_id"], ["payment_counterparty_supplier_mappings.id"]),
        sa.ForeignKeyConstraint(["import_run_id"], ["payment_import_runs.id"]),
        sa.ForeignKeyConstraint(["incoming_category"], ["payment_categories.code"]),
        sa.ForeignKeyConstraint(["outgoing_category"], ["payment_categories.code"]),
        sa.ForeignKeyConstraint(["payment_category"], ["payment_categories.code"]),
        sa.ForeignKeyConstraint(["supplier_code"], ["dropship_enterprises.code"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_system", "source_payment_id", "payment_type", name="uq_salesdrive_payments_source_payment_type"),
    )
    op.create_index("ix_salesdrive_payments_business_account_date", "salesdrive_payments", ["business_account_id", "payment_date"])
    op.create_index("ix_salesdrive_payments_counterparty_normalized_name", "salesdrive_payments", ["counterparty_normalized_name"])
    op.create_index("ix_salesdrive_payments_counterparty_tax_id", "salesdrive_payments", ["counterparty_tax_id"])
    op.create_index("ix_salesdrive_payments_internal_transfer", "salesdrive_payments", ["is_internal_transfer"])
    op.create_index("ix_salesdrive_payments_mapping_status", "salesdrive_payments", ["mapping_status"])
    op.create_index("ix_salesdrive_payments_payment_date", "salesdrive_payments", ["payment_date"])
    op.create_index("ix_salesdrive_payments_supplier_date", "salesdrive_payments", ["supplier_code", "payment_date"])
    op.create_index("ix_salesdrive_payments_type_date", "salesdrive_payments", ["payment_type", "payment_date"])

    op.create_table(
        "internal_transfer_pairs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("pair_key", sa.String(length=255), nullable=False),
        sa.Column("outcoming_payment_id", sa.BigInteger(), nullable=False),
        sa.Column("incoming_payment_id", sa.BigInteger(), nullable=False),
        sa.Column("amount", sa.Numeric(14, 2), nullable=False),
        sa.Column("outcoming_account_id", sa.BigInteger(), nullable=False),
        sa.Column("incoming_account_id", sa.BigInteger(), nullable=False),
        sa.Column("outcoming_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("incoming_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("match_confidence", sa.Numeric(5, 4), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.ForeignKeyConstraint(["incoming_account_id"], ["payment_business_accounts.id"]),
        sa.ForeignKeyConstraint(["incoming_payment_id"], ["salesdrive_payments.id"]),
        sa.ForeignKeyConstraint(["outcoming_account_id"], ["payment_business_accounts.id"]),
        sa.ForeignKeyConstraint(["outcoming_payment_id"], ["salesdrive_payments.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("incoming_payment_id", name="uq_internal_transfer_pairs_incoming_payment_id"),
        sa.UniqueConstraint("outcoming_payment_id", name="uq_internal_transfer_pairs_outcoming_payment_id"),
        sa.UniqueConstraint("pair_key", name="uq_internal_transfer_pairs_pair_key"),
    )
    op.create_index("ix_internal_transfer_pairs_incoming_account_id", "internal_transfer_pairs", ["incoming_account_id"])
    op.create_index("ix_internal_transfer_pairs_outcoming_account_id", "internal_transfer_pairs", ["outcoming_account_id"])
    op.create_foreign_key(
        "fk_salesdrive_payments_internal_transfer_pair_id",
        "salesdrive_payments",
        "internal_transfer_pairs",
        ["internal_transfer_pair_id"],
        ["id"],
    )

    op.create_table(
        "internal_transfer_rules",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("business_entity_id", sa.BigInteger(), nullable=False),
        sa.Column("from_account_id", sa.BigInteger(), nullable=False),
        sa.Column("to_account_id", sa.BigInteger(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("pairing_window_minutes", sa.Integer(), server_default=sa.text("5"), nullable=False),
        sa.Column("require_exact_amount", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("allow_direct_self_marker", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint("from_account_id <> to_account_id", name="ck_internal_transfer_rules_different_accounts"),
        sa.ForeignKeyConstraint(["business_entity_id"], ["payment_business_entities.id"]),
        sa.ForeignKeyConstraint(["from_account_id"], ["payment_business_accounts.id"]),
        sa.ForeignKeyConstraint(["to_account_id"], ["payment_business_accounts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("from_account_id", "to_account_id", name="uq_internal_transfer_rules_account_pair"),
    )
    op.create_index("ix_internal_transfer_rules_entity_id", "internal_transfer_rules", ["business_entity_id"])
    op.create_index("ix_internal_transfer_rules_is_active", "internal_transfer_rules", ["is_active"])

    op.create_table(
        "account_balance_adjustments",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("period_month", sa.Date(), nullable=False),
        sa.Column("opening_balance_adjustment", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
        sa.Column("closing_balance_adjustment", sa.Numeric(14, 2), server_default=sa.text("0"), nullable=False),
        sa.Column("actual_opening_balance", sa.Numeric(14, 2), nullable=True),
        sa.Column("actual_closing_balance", sa.Numeric(14, 2), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=255), nullable=True),
        sa.Column("approved_by", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["payment_business_accounts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_id", "period_month", name="uq_account_balance_adjustments_account_month"),
    )
    op.create_index("ix_account_balance_adjustments_period_month", "account_balance_adjustments", ["period_month"])

    op.create_table(
        "payment_period_locks",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("period_month", sa.Date(), nullable=False),
        sa.Column("business_entity_id", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(length=32), server_default=sa.text("'open'"), nullable=False),
        sa.Column("closed_by", sa.String(length=255), nullable=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint("status IN ('open', 'closed')", name="ck_payment_period_locks_status"),
        sa.ForeignKeyConstraint(["business_entity_id"], ["payment_business_entities.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("period_month", "business_entity_id", name="uq_payment_period_locks_month_entity"),
    )
    op.create_index("ix_payment_period_locks_status", "payment_period_locks", ["status"])


def downgrade() -> None:
    op.drop_index("ix_payment_period_locks_status", table_name="payment_period_locks")
    op.drop_table("payment_period_locks")
    op.drop_index("ix_account_balance_adjustments_period_month", table_name="account_balance_adjustments")
    op.drop_table("account_balance_adjustments")
    op.drop_index("ix_internal_transfer_rules_is_active", table_name="internal_transfer_rules")
    op.drop_index("ix_internal_transfer_rules_entity_id", table_name="internal_transfer_rules")
    op.drop_table("internal_transfer_rules")
    op.drop_constraint("fk_salesdrive_payments_internal_transfer_pair_id", "salesdrive_payments", type_="foreignkey")
    op.drop_index("ix_internal_transfer_pairs_outcoming_account_id", table_name="internal_transfer_pairs")
    op.drop_index("ix_internal_transfer_pairs_incoming_account_id", table_name="internal_transfer_pairs")
    op.drop_table("internal_transfer_pairs")
    op.drop_index("ix_salesdrive_payments_type_date", table_name="salesdrive_payments")
    op.drop_index("ix_salesdrive_payments_supplier_date", table_name="salesdrive_payments")
    op.drop_index("ix_salesdrive_payments_payment_date", table_name="salesdrive_payments")
    op.drop_index("ix_salesdrive_payments_mapping_status", table_name="salesdrive_payments")
    op.drop_index("ix_salesdrive_payments_internal_transfer", table_name="salesdrive_payments")
    op.drop_index("ix_salesdrive_payments_counterparty_tax_id", table_name="salesdrive_payments")
    op.drop_index("ix_salesdrive_payments_counterparty_normalized_name", table_name="salesdrive_payments")
    op.drop_index("ix_salesdrive_payments_business_account_date", table_name="salesdrive_payments")
    op.drop_table("salesdrive_payments")
    op.drop_index("ix_payment_counterparty_supplier_mappings_tax_id", table_name="payment_counterparty_supplier_mappings")
    op.drop_index("ix_payment_counterparty_supplier_mappings_supplier_code", table_name="payment_counterparty_supplier_mappings")
    op.drop_index("ix_payment_counterparty_supplier_mappings_active_priority", table_name="payment_counterparty_supplier_mappings")
    op.drop_table("payment_counterparty_supplier_mappings")
    op.drop_index("ix_payment_categories_direction", table_name="payment_categories")
    op.drop_table("payment_categories")
    op.drop_index("ix_payment_import_runs_status", table_name="payment_import_runs")
    op.drop_index("ix_payment_import_runs_period", table_name="payment_import_runs")
    op.drop_table("payment_import_runs")
    op.drop_index("ix_payment_business_accounts_is_active", table_name="payment_business_accounts")
    op.drop_index("ix_payment_business_accounts_entity_id", table_name="payment_business_accounts")
    op.drop_table("payment_business_accounts")
    op.drop_index("ix_payment_business_entities_verification_status", table_name="payment_business_entities")
    op.drop_index("ix_payment_business_entities_tax_id", table_name="payment_business_entities")
    op.drop_table("payment_business_entities")

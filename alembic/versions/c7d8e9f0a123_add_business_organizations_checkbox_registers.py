"""add business organization links and checkbox registers

Revision ID: c7d8e9f0a123
Revises: b6c7d8e9f012
Create Date: 2026-05-09 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c7d8e9f0a123"
down_revision: Union[str, None] = "b6c7d8e9f012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("payment_business_accounts", sa.Column("mfo", sa.String(length=32), nullable=True))

    op.add_column(
        "business_stores",
        sa.Column("business_organization_id", sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        "fk_business_stores_business_organization_id",
        "business_stores",
        "payment_business_entities",
        ["business_organization_id"],
        ["id"],
    )
    op.create_index(
        "ix_business_stores_business_organization_id",
        "business_stores",
        ["business_organization_id"],
    )

    op.create_table(
        "checkbox_cash_registers",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("business_organization_id", sa.BigInteger(), nullable=False),
        sa.Column("business_store_id", sa.BigInteger(), nullable=True),
        sa.Column("enterprise_code", sa.String(), nullable=True),
        sa.Column("register_name", sa.String(length=500), nullable=False),
        sa.Column("cash_register_code", sa.String(length=255), nullable=False),
        sa.Column("checkbox_license_key", sa.String(length=1000), nullable=True),
        sa.Column("cashier_login", sa.String(length=500), nullable=True),
        sa.Column("cashier_password", sa.String(length=1000), nullable=True),
        sa.Column("cashier_pin", sa.String(length=255), nullable=True),
        sa.Column("api_base_url", sa.String(length=1000), nullable=True),
        sa.Column("is_test_mode", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("is_default", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("shift_open_mode", sa.String(length=32), server_default=sa.text("'on_fiscalization'"), nullable=False),
        sa.Column("shift_open_time", sa.String(length=8), nullable=True),
        sa.Column("shift_close_time", sa.String(length=8), nullable=True),
        sa.Column("timezone", sa.String(length=64), server_default=sa.text("'Europe/Kiev'"), nullable=False),
        sa.Column("receipt_notifications_enabled", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("shift_notifications_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.CheckConstraint(
            "shift_open_mode IN ('manual', 'scheduled', 'first_status_4', 'on_fiscalization')",
            name="ck_checkbox_cash_registers_shift_open_mode",
        ),
        sa.ForeignKeyConstraint(["business_organization_id"], ["payment_business_entities.id"]),
        sa.ForeignKeyConstraint(["business_store_id"], ["business_stores.id"]),
        sa.ForeignKeyConstraint(["enterprise_code"], ["enterprise_settings.enterprise_code"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "business_organization_id",
            "cash_register_code",
            name="uq_checkbox_cash_registers_org_code",
        ),
    )
    op.create_index(
        "ix_checkbox_cash_registers_org_active",
        "checkbox_cash_registers",
        ["business_organization_id", "is_active"],
    )
    op.create_index("ix_checkbox_cash_registers_store_id", "checkbox_cash_registers", ["business_store_id"])
    op.create_index("ix_checkbox_cash_registers_enterprise_code", "checkbox_cash_registers", ["enterprise_code"])

    op.create_table(
        "checkbox_receipt_exclusions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("business_organization_id", sa.BigInteger(), nullable=False),
        sa.Column("cash_register_id", sa.BigInteger(), nullable=True),
        sa.Column("supplier_code", sa.String(length=255), nullable=False),
        sa.Column("supplier_name", sa.String(length=500), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.ForeignKeyConstraint(["business_organization_id"], ["payment_business_entities.id"]),
        sa.ForeignKeyConstraint(["cash_register_id"], ["checkbox_cash_registers.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "business_organization_id",
            "cash_register_id",
            "supplier_code",
            name="uq_checkbox_receipt_exclusions_org_register_supplier",
        ),
    )
    op.create_index(
        "ix_checkbox_receipt_exclusions_org_active",
        "checkbox_receipt_exclusions",
        ["business_organization_id", "is_active"],
    )
    op.create_index(
        "ix_checkbox_receipt_exclusions_register_id",
        "checkbox_receipt_exclusions",
        ["cash_register_id"],
    )

    op.add_column("checkbox_receipts", sa.Column("business_store_id", sa.BigInteger(), nullable=True))
    op.add_column("checkbox_receipts", sa.Column("business_organization_id", sa.BigInteger(), nullable=True))
    op.add_column("checkbox_receipts", sa.Column("cash_register_id", sa.BigInteger(), nullable=True))
    op.create_foreign_key(
        "fk_checkbox_receipts_business_store_id",
        "checkbox_receipts",
        "business_stores",
        ["business_store_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_checkbox_receipts_business_organization_id",
        "checkbox_receipts",
        "payment_business_entities",
        ["business_organization_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_checkbox_receipts_cash_register_id",
        "checkbox_receipts",
        "checkbox_cash_registers",
        ["cash_register_id"],
        ["id"],
    )
    op.create_index("ix_checkbox_receipts_store_id", "checkbox_receipts", ["business_store_id"])
    op.create_index("ix_checkbox_receipts_organization_id", "checkbox_receipts", ["business_organization_id"])
    op.create_index("ix_checkbox_receipts_cash_register_id", "checkbox_receipts", ["cash_register_id"])

    op.add_column("checkbox_shifts", sa.Column("business_organization_id", sa.BigInteger(), nullable=True))
    op.add_column("checkbox_shifts", sa.Column("cash_register_id", sa.BigInteger(), nullable=True))
    op.create_foreign_key(
        "fk_checkbox_shifts_business_organization_id",
        "checkbox_shifts",
        "payment_business_entities",
        ["business_organization_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_checkbox_shifts_cash_register_id",
        "checkbox_shifts",
        "checkbox_cash_registers",
        ["cash_register_id"],
        ["id"],
    )
    op.create_index(
        "ix_checkbox_shifts_organization_status",
        "checkbox_shifts",
        ["business_organization_id", "status"],
    )
    op.create_index(
        "ix_checkbox_shifts_cash_register_id_status",
        "checkbox_shifts",
        ["cash_register_id", "status"],
    )

    # Conservative backfill: reuse an existing payment business entity by tax id
    # or SalesDrive organization id; otherwise create a draft entity from store data.
    op.execute(
        """
        WITH source AS (
            SELECT
                MIN(id) AS store_id,
                NULLIF(BTRIM(COALESCE(legal_entity_name, '')), '') AS legal_entity_name,
                NULLIF(BTRIM(COALESCE(tax_identifier, '')), '') AS tax_identifier,
                salesdrive_enterprise_id
            FROM business_stores
            WHERE business_organization_id IS NULL
              AND (
                NULLIF(BTRIM(COALESCE(legal_entity_name, '')), '') IS NOT NULL
                OR NULLIF(BTRIM(COALESCE(tax_identifier, '')), '') IS NOT NULL
                OR salesdrive_enterprise_id IS NOT NULL
              )
            GROUP BY
                NULLIF(BTRIM(COALESCE(legal_entity_name, '')), ''),
                NULLIF(BTRIM(COALESCE(tax_identifier, '')), ''),
                salesdrive_enterprise_id
        ),
        inserted AS (
            INSERT INTO payment_business_entities (
                salesdrive_organization_id,
                short_name,
                full_name,
                tax_id,
                entity_type,
                verification_status,
                is_active
            )
            SELECT
                source.salesdrive_enterprise_id::text,
                COALESCE(source.legal_entity_name, 'Business organization ' || source.store_id::text),
                source.legal_entity_name,
                source.tax_identifier,
                'other',
                'needs_review',
                true
            FROM source
            WHERE NOT EXISTS (
                SELECT 1
                FROM payment_business_entities e
                WHERE
                    (source.tax_identifier IS NOT NULL AND e.tax_id = source.tax_identifier)
                    OR (
                        source.salesdrive_enterprise_id IS NOT NULL
                        AND e.salesdrive_organization_id = source.salesdrive_enterprise_id::text
                    )
            )
            RETURNING id, salesdrive_organization_id, tax_id
        )
        UPDATE business_stores s
        SET business_organization_id = e.id
        FROM payment_business_entities e
        WHERE s.business_organization_id IS NULL
          AND (
            (NULLIF(BTRIM(COALESCE(s.tax_identifier, '')), '') IS NOT NULL AND e.tax_id = NULLIF(BTRIM(COALESCE(s.tax_identifier, '')), ''))
            OR (
                s.salesdrive_enterprise_id IS NOT NULL
                AND e.salesdrive_organization_id = s.salesdrive_enterprise_id::text
            )
          );
        """
    )


def downgrade() -> None:
    op.drop_index("ix_checkbox_shifts_cash_register_id_status", table_name="checkbox_shifts")
    op.drop_index("ix_checkbox_shifts_organization_status", table_name="checkbox_shifts")
    op.drop_constraint("fk_checkbox_shifts_cash_register_id", "checkbox_shifts", type_="foreignkey")
    op.drop_constraint("fk_checkbox_shifts_business_organization_id", "checkbox_shifts", type_="foreignkey")
    op.drop_column("checkbox_shifts", "cash_register_id")
    op.drop_column("checkbox_shifts", "business_organization_id")

    op.drop_index("ix_checkbox_receipts_cash_register_id", table_name="checkbox_receipts")
    op.drop_index("ix_checkbox_receipts_organization_id", table_name="checkbox_receipts")
    op.drop_index("ix_checkbox_receipts_store_id", table_name="checkbox_receipts")
    op.drop_constraint("fk_checkbox_receipts_cash_register_id", "checkbox_receipts", type_="foreignkey")
    op.drop_constraint("fk_checkbox_receipts_business_organization_id", "checkbox_receipts", type_="foreignkey")
    op.drop_constraint("fk_checkbox_receipts_business_store_id", "checkbox_receipts", type_="foreignkey")
    op.drop_column("checkbox_receipts", "cash_register_id")
    op.drop_column("checkbox_receipts", "business_organization_id")
    op.drop_column("checkbox_receipts", "business_store_id")

    op.drop_index("ix_checkbox_receipt_exclusions_register_id", table_name="checkbox_receipt_exclusions")
    op.drop_index("ix_checkbox_receipt_exclusions_org_active", table_name="checkbox_receipt_exclusions")
    op.drop_table("checkbox_receipt_exclusions")

    op.drop_index("ix_checkbox_cash_registers_enterprise_code", table_name="checkbox_cash_registers")
    op.drop_index("ix_checkbox_cash_registers_store_id", table_name="checkbox_cash_registers")
    op.drop_index("ix_checkbox_cash_registers_org_active", table_name="checkbox_cash_registers")
    op.drop_table("checkbox_cash_registers")

    op.drop_index("ix_business_stores_business_organization_id", table_name="business_stores")
    op.drop_constraint("fk_business_stores_business_organization_id", "business_stores", type_="foreignkey")
    op.drop_column("business_stores", "business_organization_id")
    op.drop_column("payment_business_accounts", "mfo")

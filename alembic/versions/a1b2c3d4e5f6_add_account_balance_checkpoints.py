"""add account balance checkpoints

Revision ID: a1b2c3d4e5f6
Revises: f8a9b0c1d2e3
Create Date: 2026-05-03 15:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "f8a9b0c1d2e3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("account_balance_adjustments", sa.Column("balance_date", sa.Date(), nullable=True))
    op.add_column("account_balance_adjustments", sa.Column("actual_balance", sa.Numeric(14, 2), nullable=True))
    op.create_index(
        "ix_account_balance_adjustments_balance_date",
        "account_balance_adjustments",
        ["balance_date"],
        unique=False,
    )
    op.create_unique_constraint(
        "uq_account_balance_adjustments_account_balance_date",
        "account_balance_adjustments",
        ["account_id", "balance_date"],
    )
    op.execute(
        """
        UPDATE account_balance_adjustments
        SET
            balance_date = (date_trunc('month', period_month)::date + INTERVAL '1 month - 1 day')::date,
            actual_balance = actual_closing_balance
        WHERE actual_closing_balance IS NOT NULL
          AND balance_date IS NULL
          AND actual_balance IS NULL
        """
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_account_balance_adjustments_account_balance_date",
        "account_balance_adjustments",
        type_="unique",
    )
    op.drop_index("ix_account_balance_adjustments_balance_date", table_name="account_balance_adjustments")
    op.drop_column("account_balance_adjustments", "actual_balance")
    op.drop_column("account_balance_adjustments", "balance_date")

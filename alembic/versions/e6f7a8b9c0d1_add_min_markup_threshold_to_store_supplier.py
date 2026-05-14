"""add min markup threshold to store supplier settings

Revision ID: e6f7a8b9c0d1
Revises: d4e5f6a7b8c9
Create Date: 2026-04-29 13:10:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision: str = "e6f7a8b9c0d1"
down_revision: Union[str, None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_column(table_name: str, column_name: str) -> bool:
    inspector = inspect(op.get_bind())
    return any(item.get("name") == column_name for item in inspector.get_columns(table_name))


def _has_check_constraint(table_name: str, constraint_name: str) -> bool:
    inspector = inspect(op.get_bind())
    return any(item.get("name") == constraint_name for item in inspector.get_check_constraints(table_name))


def upgrade() -> None:
    if not _has_column("business_store_supplier_settings", "min_markup_threshold"):
        op.add_column(
            "business_store_supplier_settings",
            sa.Column("min_markup_threshold", sa.Numeric(12, 4), nullable=True),
        )
    if not _has_check_constraint("business_store_supplier_settings", "ck_bsss_min_thr_nn"):
        op.create_check_constraint(
            "ck_bsss_min_thr_nn",
            "business_store_supplier_settings",
            "(min_markup_threshold IS NULL) OR (min_markup_threshold >= 0)",
        )


def downgrade() -> None:
    if _has_check_constraint("business_store_supplier_settings", "ck_bsss_min_thr_nn"):
        op.drop_constraint("ck_bsss_min_thr_nn", "business_store_supplier_settings", type_="check")
    if _has_column("business_store_supplier_settings", "min_markup_threshold"):
        op.drop_column("business_store_supplier_settings", "min_markup_threshold")

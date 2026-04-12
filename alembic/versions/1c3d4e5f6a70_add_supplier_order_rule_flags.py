"""add supplier order rule flags

Revision ID: 1c3d4e5f6a70
Revises: 8f1a6b7c9d20
Create Date: 2026-04-08 17:20:00.000000

"""

from typing import Sequence, Union
import os

from alembic import op
import sqlalchemy as sa


revision: str = "1c3d4e5f6a70"
down_revision: Union[str, None] = "8f1a6b7c9d20"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _parse_supplier_ids(raw: str | None, *, fallback: list[int] | None = None) -> list[int]:
    if raw is None or str(raw).strip() == "":
        return list(fallback or [])

    parts = str(raw).replace(",", ";").split(";")
    parsed: list[int] = []
    for part in parts:
        item = part.strip()
        if not item:
            continue
        try:
            parsed.append(int(item))
        except (TypeError, ValueError):
            continue
    return parsed or list(fallback or [])


def upgrade() -> None:
    op.add_column(
        "dropship_enterprises",
        sa.Column("biotus_orders_enabled", sa.Boolean(), nullable=True, server_default=sa.text("false")),
    )
    op.add_column(
        "dropship_enterprises",
        sa.Column("np_fulfillment_enabled", sa.Boolean(), nullable=True, server_default=sa.text("false")),
    )

    bind = op.get_bind()

    allowed_supplier_ids = _parse_supplier_ids(os.getenv("ALLOWED_SUPPLIERS"), fallback=[38, 41])
    fulfillment_supplier_ids = _parse_supplier_ids(os.getenv("NP_FULFILLMENT_SUPPLIER_IDS"), fallback=[])

    for supplier_id in allowed_supplier_ids:
        bind.execute(
            sa.text(
                """
                UPDATE dropship_enterprises
                SET biotus_orders_enabled = TRUE
                WHERE salesdrive_supplier_id = :supplier_id
                """
            ),
            {"supplier_id": supplier_id},
        )

    for supplier_id in fulfillment_supplier_ids:
        bind.execute(
            sa.text(
                """
                UPDATE dropship_enterprises
                SET np_fulfillment_enabled = TRUE
                WHERE salesdrive_supplier_id = :supplier_id
                """
            ),
            {"supplier_id": supplier_id},
        )

    bind.execute(sa.text("UPDATE dropship_enterprises SET biotus_orders_enabled = FALSE WHERE biotus_orders_enabled IS NULL"))
    bind.execute(sa.text("UPDATE dropship_enterprises SET np_fulfillment_enabled = FALSE WHERE np_fulfillment_enabled IS NULL"))

    op.alter_column(
        "dropship_enterprises",
        "biotus_orders_enabled",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("false"),
    )
    op.alter_column(
        "dropship_enterprises",
        "np_fulfillment_enabled",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("false"),
    )


def downgrade() -> None:
    op.drop_column("dropship_enterprises", "np_fulfillment_enabled")
    op.drop_column("dropship_enterprises", "biotus_orders_enabled")

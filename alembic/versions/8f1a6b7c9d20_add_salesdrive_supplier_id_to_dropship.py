"""add salesdrive_supplier_id to dropship_enterprises

Revision ID: 8f1a6b7c9d20
Revises: 5b7f1c2d9e10
Create Date: 2026-04-08 15:40:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "8f1a6b7c9d20"
down_revision: Union[str, None] = "5b7f1c2d9e10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


LEGACY_SUPPLIER_CODE_TO_ID = {
    "D1": 38,
    "D2": 39,
    "D3": 40,
    "D4": 41,
    "D5": 42,
    "D6": 43,
    "D7": 44,
    "D8": 45,
    "D9": 46,
    "D10": 47,
    "D11": 48,
    "D12": 49,
    "D13": 51,
    "D14": 52,
}


def upgrade() -> None:
    op.add_column("dropship_enterprises", sa.Column("salesdrive_supplier_id", sa.Integer(), nullable=True))

    bind = op.get_bind()
    stmt = sa.text(
        """
        UPDATE dropship_enterprises
        SET salesdrive_supplier_id = :supplier_id
        WHERE upper(trim(code)) = :supplier_code
          AND salesdrive_supplier_id IS NULL
        """
    )
    for supplier_code, supplier_id in LEGACY_SUPPLIER_CODE_TO_ID.items():
        bind.execute(stmt, {"supplier_code": supplier_code, "supplier_id": supplier_id})


def downgrade() -> None:
    op.drop_column("dropship_enterprises", "salesdrive_supplier_id")

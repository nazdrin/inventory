"""Merge multiple heads

Revision ID: 97e3c532bd4f
Revises: 409a451f2a4c, b686a2966958
Create Date: 2025-03-07 07:24:52.781937

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '97e3c532bd4f'
down_revision: Union[str, None] = ('409a451f2a4c', 'b686a2966958')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

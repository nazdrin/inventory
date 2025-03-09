"""Fix missing ID_telegram

Revision ID: a8a69dc301fd
Revises: ececdc85fc1e
Create Date: 2025-03-08 21:04:50.008494

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a8a69dc301fd'
down_revision: Union[str, None] = 'ececdc85fc1e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Добавляем колонку, если её нет
    # op.add_column('mapping_branch', sa.Column('ID_telegram', sa.Integer(), nullable=True))
    pass

def downgrade():
    # op.drop_column('mapping_branch', 'ID_telegram')
    pass


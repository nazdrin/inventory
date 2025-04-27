"""Merge heads 146fa1f41622 and c846e7ac972c

Revision ID: ed1552a81731
Revises: 146fa1f41622, c846e7ac972c
Create Date: 2025-04-25 14:02:07.737025

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ed1552a81731'
down_revision: Union[str, None] = ('146fa1f41622', 'c846e7ac972c')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

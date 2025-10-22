"""merge 51dc89364365 + b2185162aa32

Revision ID: a0f60eea3d8a
Revises: 51dc89364365, b2185162aa32
Create Date: 2025-10-21 20:23:40.237306

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a0f60eea3d8a'
down_revision: Union[str, None] = ('51dc89364365', 'b2185162aa32')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass

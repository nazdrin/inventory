"""balancer: best_rules json -> jsonb

Revision ID: bfdcbfc70e58
Revises: 3a1d28c948c1
Create Date: 2026-01-03 20:42:26.363379

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'bfdcbfc70e58'
down_revision: Union[str, None] = '3a1d28c948c1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.alter_column(
        "balancer_live_state",
        "best_rules",
        type_=postgresql.JSONB(),
        existing_type=sa.JSON(),
        postgresql_using="best_rules::jsonb",
        existing_nullable=True,
    )


def downgrade():
    op.alter_column(
        "balancer_live_state",
        "best_rules",
        type_=sa.JSON(),
        existing_type=postgresql.JSONB(),
        postgresql_using="best_rules::json",
        existing_nullable=True,
    )
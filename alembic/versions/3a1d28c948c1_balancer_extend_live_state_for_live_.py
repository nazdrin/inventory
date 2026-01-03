"""balancer: extend live_state for live logic

Revision ID: 3a1d28c948c1
Revises: 0e54e9d93e75
Create Date: 2026-01-03 09:10:11.800146
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from alembic.runtime import migration
from alembic import context


# revision identifiers, used by Alembic.
revision: str = "3a1d28c948c1"
down_revision: Union[str, None] = "0e54e9d93e75"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _is_offline() -> bool:
    return context.is_offline_mode()


def _has_column(table: str, column: str) -> bool:
    """
    Online-only check. In offline mode return False so SQL can be generated.
    """
    if _is_offline():
        return False

    conn = op.get_bind()
    res = conn.execute(
        sa.text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = :table
              AND column_name = :column
            """
        ),
        {"table": table, "column": column},
    )
    row = res.first()
    return row is not None


def upgrade():
    table = "balancer_live_state"

    def add(col):
        if not _has_column(table, col.name):
            op.add_column(table, col)

    # В этой ревизии добавляем только то, чего не было в предыдущих миграциях
    add(sa.Column("best_rules", sa.JSON(), nullable=True))
    add(sa.Column("last_run_key", sa.String(), nullable=True))


def downgrade():
    # downgrade intentionally left minimal – live_state is forward-only
    pass
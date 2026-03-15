"""add tabletki_guid to master_catalog

Revision ID: 7d3c2b1a9f4e
Revises: c1d2e3f4a5b6
Create Date: 2026-03-15 20:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7d3c2b1a9f4e"
down_revision: Union[str, None] = "c1d2e3f4a5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("master_catalog", sa.Column("tabletki_guid", sa.String(length=500), nullable=True))
    op.create_index("ix_master_catalog_tabletki_guid", "master_catalog", ["tabletki_guid"], unique=False)

    op.execute(
        """
        WITH latest_raw AS (
            SELECT DISTINCT ON (rtc.sku)
                rtc.sku,
                rtc.tabletki_guid
            FROM raw_tabletki_catalog AS rtc
            WHERE rtc.tabletki_guid IS NOT NULL
              AND NULLIF(trim(rtc.tabletki_guid), '') IS NOT NULL
            ORDER BY rtc.sku, rtc.id DESC
        )
        UPDATE master_catalog AS mc
        SET tabletki_guid = latest_raw.tabletki_guid
        FROM latest_raw
        WHERE mc.sku = latest_raw.sku
          AND (
              mc.tabletki_guid IS NULL
              OR NULLIF(trim(mc.tabletki_guid), '') IS NULL
          )
        """
    )


def downgrade() -> None:
    op.drop_index("ix_master_catalog_tabletki_guid", table_name="master_catalog")
    op.drop_column("master_catalog", "tabletki_guid")

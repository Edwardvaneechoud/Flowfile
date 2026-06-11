"""Add partition_columns to catalog_tables.

Stores the Delta partition columns (JSON array of column names) for a catalog
table, set on write and used by the catalog UI. NULL means unpartitioned.

Revision ID: 020
Revises: 019
Create Date: 2026-06-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "020"
down_revision: str | None = "019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("catalog_tables", sa.Column("partition_columns", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("catalog_tables", "partition_columns")

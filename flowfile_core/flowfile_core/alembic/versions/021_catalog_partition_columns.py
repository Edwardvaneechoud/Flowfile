"""Add partition_columns to catalog_tables.

Stores the Delta partition columns (JSON array of column names) for a catalog
table, set on write and used by the catalog UI. NULL means unpartitioned.

Revision ID: 021
Revises: 020
Create Date: 2026-06-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "021"
down_revision: str | None = "020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    # Guarded: dev DBs that ran this migration under its pre-merge revision id (020)
    # already have the column.
    if not _has_column("catalog_tables", "partition_columns"):
        op.add_column("catalog_tables", sa.Column("partition_columns", sa.Text, nullable=True))


def downgrade() -> None:
    if _has_column("catalog_tables", "partition_columns"):
        op.drop_column("catalog_tables", "partition_columns")

"""Add sql_query column for query-based virtual tables.

Revision ID: 003
Revises: 002
Create Date: 2026-04-14
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "003"
down_revision: str | None = "002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("catalog_tables", sa.Column("sql_query", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("catalog_tables", "sql_query")

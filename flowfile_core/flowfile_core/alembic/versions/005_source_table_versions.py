"""Add source_table_versions column for staleness detection on virtual tables.

Revision ID: 005
Revises: 004
Create Date: 2026-04-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "005"
down_revision: str | None = "004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("catalog_tables", sa.Column("source_table_versions", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("catalog_tables", "source_table_versions")

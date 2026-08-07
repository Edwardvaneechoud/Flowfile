"""Add prediction_table_id to catalog_tables.

Points a labelled catalog table at a sibling catalog table holding model predictions for it;
NULL means no association. Deliberately no DB-level foreign key — SQLite cannot add one via
ALTER TABLE ADD COLUMN, and dangling references are nulled out in application code on delete.

Revision ID: 030
Revises: 029
Create Date: 2026-08-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "030"
down_revision: str | None = "029"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("catalog_tables", "prediction_table_id"):
        op.add_column("catalog_tables", sa.Column("prediction_table_id", sa.Integer, nullable=True))


def downgrade() -> None:
    if _has_column("catalog_tables", "prediction_table_id"):
        op.drop_column("catalog_tables", "prediction_table_id")

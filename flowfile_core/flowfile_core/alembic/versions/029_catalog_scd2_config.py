"""Add scd2_config to catalog_tables.

Stores the SCD2 shape (business keys + the four generated column names + the resolved
compare-column set) of a catalog table maintained by an SCD2 write. NULL means the table is
not SCD2-tracked. Written on every SCD2 write and cleared by any non-SCD2 physical write.

Revision ID: 029
Revises: 028
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "029"
down_revision: str | None = "028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("catalog_tables", "scd2_config"):
        op.add_column("catalog_tables", sa.Column("scd2_config", sa.Text, nullable=True))


def downgrade() -> None:
    if _has_column("catalog_tables", "scd2_config"):
        op.drop_column("catalog_tables", "scd2_config")

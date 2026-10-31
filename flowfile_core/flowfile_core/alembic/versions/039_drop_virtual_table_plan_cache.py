"""Drop the plan an optimized virtual table cached, and clear its explain() text.

A virtual table now always reads through its producer flow, so ``serialized_lazy_frame`` has no reader. Its
contents were a Polars plan serialised by the version that wrote it, which no other Polars version can load.
``polars_plan`` now holds the print_tree drawing of the producer's branch, so its old ``explain()`` text is
cleared; the next producer run or read writes the drawing.

Revision ID: 039
Revises: 038
Create Date: 2026-10-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "039"
down_revision: str | None = "038"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if _has_column("catalog_tables", "serialized_lazy_frame"):
        with op.batch_alter_table("catalog_tables") as batch:
            batch.drop_column("serialized_lazy_frame")
    if _has_column("catalog_tables", "polars_plan"):
        op.execute(sa.text("UPDATE catalog_tables SET polars_plan = NULL"))


def downgrade() -> None:
    if not _has_column("catalog_tables", "serialized_lazy_frame"):
        op.add_column("catalog_tables", sa.Column("serialized_lazy_frame", sa.LargeBinary(), nullable=True))

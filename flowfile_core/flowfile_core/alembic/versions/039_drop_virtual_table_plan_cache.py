"""Drop the plans an optimized virtual table stored.

Both ``serialized_lazy_frame`` (a Polars plan no other Polars version can load) and ``polars_plan`` (its
``explain()`` text) lost their readers: reads run the producer flow, and the table's page describes the
saved producer.

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

_DROPPED = (("serialized_lazy_frame", sa.LargeBinary), ("polars_plan", sa.Text))


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    present = [name for name, _ in _DROPPED if _has_column("catalog_tables", name)]
    if present:
        with op.batch_alter_table("catalog_tables") as batch:
            for name in present:
                batch.drop_column(name)


def downgrade() -> None:
    for name, type_ in _DROPPED:
        if not _has_column("catalog_tables", name):
            op.add_column("catalog_tables", sa.Column(name, type_(), nullable=True))

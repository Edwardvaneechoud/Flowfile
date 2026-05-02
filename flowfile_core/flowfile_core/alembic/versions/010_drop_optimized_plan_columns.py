"""Drop optimized virtual-table plan storage columns.

Removes ``serialized_lazy_frame``, ``polars_plan``, and ``source_table_versions``
from ``catalog_tables``. These backed the optimized virtual-table read path,
which has been collapsed into the unified ``resolve_virtual_flow_table`` flow.
``is_optimized`` is kept as a derived "producer flow is fully lazy" indicator
that drives the laziness-blocker propagation.

Revision ID: 010
Revises: 009
Create Date: 2026-05-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "010"
down_revision: str | None = "009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("catalog_tables", recreate="auto") as batch_op:
        batch_op.drop_column("source_table_versions")
        batch_op.drop_column("polars_plan")
        batch_op.drop_column("serialized_lazy_frame")


def downgrade() -> None:
    op.add_column("catalog_tables", sa.Column("serialized_lazy_frame", sa.LargeBinary, nullable=True))
    op.add_column("catalog_tables", sa.Column("polars_plan", sa.Text, nullable=True))
    op.add_column("catalog_tables", sa.Column("source_table_versions", sa.Text, nullable=True))

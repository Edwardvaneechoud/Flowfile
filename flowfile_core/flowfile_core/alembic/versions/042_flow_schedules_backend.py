"""A schedule names the compute backend its runs go to.

``flow_schedules.backend`` holds a backend id (``worker``, ``worker:<name>``, an entry point's), passed to the run as
``--backend``. NULL, which every existing row gets, leaves the choice to the flow, as before.

Revision ID: 042
Revises: 041
Create Date: 2026-11-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "042"
down_revision: str | None = "041"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("flow_schedules", "backend"):
        op.add_column("flow_schedules", sa.Column("backend", sa.String(), nullable=True))


def downgrade() -> None:
    if _has_column("flow_schedules", "backend"):
        with op.batch_alter_table("flow_schedules") as batch:
            batch.drop_column("backend")

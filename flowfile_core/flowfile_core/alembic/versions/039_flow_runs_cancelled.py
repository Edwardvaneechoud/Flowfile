"""Record that a run was stopped on purpose.

``flow_runs.cancelled`` is true for a run a person cancelled before it finished. Such a run keeps ``success`` false,
so every reader that only knows the boolean still sees an ended, unsuccessful run; a reader that knows the column
tells a cancel from a failure. Existing rows stay false, so a run recorded as failed before this revision reads as
failed.

Revision ID: 039
Revises: 038
Create Date: 2026-11-10
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
    if not _has_column("flow_runs", "cancelled"):
        op.add_column(
            "flow_runs",
            sa.Column("cancelled", sa.Boolean(), nullable=False, server_default=sa.false()),
        )


def downgrade() -> None:
    if _has_column("flow_runs", "cancelled"):
        with op.batch_alter_table("flow_runs") as batch:
            batch.drop_column("cancelled")

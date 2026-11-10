"""A schedule the scheduler disabled itself says why.

``flow_schedules.disabled_reason`` holds the reason a launch was refused for, written when the scheduler sets
``enabled`` to false. NULL on every existing row and on a schedule a person disabled; cleared when one is enabled
again.

Revision ID: 043
Revises: 042
Create Date: 2026-11-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "043"
down_revision: str | None = "042"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("flow_schedules", "disabled_reason"):
        op.add_column("flow_schedules", sa.Column("disabled_reason", sa.Text(), nullable=True))


def downgrade() -> None:
    if _has_column("flow_schedules", "disabled_reason"):
        with op.batch_alter_table("flow_schedules") as batch:
            batch.drop_column("disabled_reason")

"""Add last_cron_slot to flow_schedules.

Adds a nullable ``last_cron_slot`` column: the naive *local* wall-clock cursor
used to compute a cron schedule's next fire. Evaluating cron in local wall-clock
time (rather than as UTC-aware instants) keeps daily/weekly/monthly schedules
firing exactly once across DST transitions — a repeated fall-back hour is a
single naive instant, so "02:30" fires once instead of twice. Nullable: only
``schedule_type == 'cron'`` rows populate it, on first fire, and existing rows
are unaffected.

Revision ID: 016
Revises: 015
Create Date: 2026-05-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "016"
down_revision: str | None = "015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("flow_schedules", "last_cron_slot"):
        op.add_column("flow_schedules", sa.Column("last_cron_slot", sa.DateTime(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("flow_schedules") as batch:
        batch.drop_column("last_cron_slot")

"""Add cron_expression + cron_timezone + last_cron_slot to flow_schedules.

Introduces the ``cron`` schedule type: time-of-day / day-of-week recurring
runs (e.g. "every day at 2 AM"). ``cron_expression`` holds a 5-field cron
string; ``cron_timezone`` holds the IANA zone it is evaluated in (so a
schedule runs at local wall-clock time, DST-aware). ``last_cron_slot`` is the
naive *local* wall-clock cursor used to compute the next fire — evaluating cron
in local wall-clock time (rather than as UTC-aware instants) keeps daily/weekly/
monthly schedules firing exactly once across DST transitions (a repeated
fall-back hour is a single naive instant, so "02:30" fires once, not twice).
All three are nullable: only ``schedule_type == 'cron'`` rows populate them, and
existing rows are unaffected.

Revision ID: 015
Revises: 014
Create Date: 2026-05-25
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "015"
down_revision: str | None = "014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("flow_schedules", "cron_expression"):
        op.add_column("flow_schedules", sa.Column("cron_expression", sa.String(), nullable=True))
    if not _has_column("flow_schedules", "cron_timezone"):
        op.add_column("flow_schedules", sa.Column("cron_timezone", sa.String(), nullable=True))
    if not _has_column("flow_schedules", "last_cron_slot"):
        op.add_column("flow_schedules", sa.Column("last_cron_slot", sa.DateTime(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("flow_schedules") as batch:
        batch.drop_column("last_cron_slot")
        batch.drop_column("cron_timezone")
        batch.drop_column("cron_expression")

"""A run's record says where it ran and where its log and version are.

``flow_runs`` gains ``executed_on`` (the backend's name), ``remote_run_id`` (the run id the executor reported under),
``log_ref`` (``backend:<name>:<id>``, the executor's run row whose log route holds the log; NULL when the log is this
core's own file) and ``flow_version`` (the published version that ran, in place of a snapshot). A run this core
executed itself leaves the first three NULL. The run type ``hub`` is renamed ``dispatched``: stored rows are converted
on the way up and back on the way down, since ``FlowRunOut`` refuses a run type it does not know.

Revision ID: 041
Revises: 040
Create Date: 2026-11-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "041"
down_revision: str | None = "040"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

COLUMNS = (
    ("executed_on", sa.String()),
    ("remote_run_id", sa.String(64)),
    ("log_ref", sa.String()),
    ("flow_version", sa.Integer()),
)


def _present() -> set[str]:
    return {c["name"] for c in inspect(op.get_bind()).get_columns("flow_runs")}


def _rename_run_type(old: str, new: str) -> None:
    op.execute(sa.text("UPDATE flow_runs SET run_type = :new WHERE run_type = :old").bindparams(old=old, new=new))


def upgrade() -> None:
    present = _present()
    with op.batch_alter_table("flow_runs") as batch:
        for name, kind in COLUMNS:
            if name not in present:
                batch.add_column(sa.Column(name, kind, nullable=True))
    _rename_run_type("hub", "dispatched")


def downgrade() -> None:
    _rename_run_type("dispatched", "hub")
    present = _present()
    with op.batch_alter_table("flow_runs") as batch:
        for name, _ in reversed(COLUMNS):
            if name in present:
                batch.drop_column(name)

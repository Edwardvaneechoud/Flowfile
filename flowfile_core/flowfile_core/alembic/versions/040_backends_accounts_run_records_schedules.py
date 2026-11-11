"""Backend rows a controller fills, accounts of people from an issuer, where a run ran, and a schedule's backend.

Four changes that arrived together, each applied only where it is missing, so a database that already has some of
them is brought to the same shape:

- ``compute_backends`` rows may be filled by a controller instead of added by hand. A filled row names the backend's
  id at the controller (``remote_id``) and the local user whose account the row is for (``account_user_id``); it
  stores no token of its own, so ``token`` becomes nullable. Rows added by hand keep their token and leave both NULL.
- ``issuer_accounts`` says which local account a person from an issuer signs in as: one row per person, keyed by the
  issuer and that person's id there. Accounts that sign in with a password have no row.
- ``flow_runs`` says where a run ran and where its log and version are: ``executed_on`` (the backend's name),
  ``remote_run_id`` (the run id the executor reported under), ``log_ref`` (the executor's run row whose log route
  holds the log; NULL when the log is this core's own file) and ``flow_version`` (the published version that ran, in
  place of a snapshot). The run type ``hub`` is renamed ``dispatched``, converted on the way up and back on the way
  down, since ``FlowRunOut`` refuses a run type it does not know.
- ``flow_schedules`` names the compute backend its runs go to (``backend``; NULL leaves the choice to the flow, as
  before) and why the scheduler disabled it itself (``disabled_reason``; NULL when a person did).

Revision ID: 040
Revises: 039
Create Date: 2026-11-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "040"
down_revision: str | None = "039"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RUN_COLUMNS = (
    ("executed_on", sa.String()),
    ("remote_run_id", sa.String(64)),
    ("log_ref", sa.String()),
    ("flow_version", sa.Integer()),
)
SCHEDULE_COLUMNS = (("backend", sa.String()), ("disabled_reason", sa.Text()))


def _columns(table: str) -> dict[str, dict]:
    return {c["name"]: c for c in inspect(op.get_bind()).get_columns(table)}


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def _add_missing(table: str, wanted: Sequence[tuple[str, sa.types.TypeEngine]]) -> None:
    missing = [(name, kind) for name, kind in wanted if name not in _columns(table)]
    if not missing:
        return
    with op.batch_alter_table(table) as batch:
        for name, kind in missing:
            batch.add_column(sa.Column(name, kind, nullable=True))


def _drop_present(table: str, wanted: Sequence[tuple[str, sa.types.TypeEngine]]) -> None:
    present = [name for name, _ in reversed(wanted) if name in _columns(table)]
    if not present:
        return
    with op.batch_alter_table(table) as batch:
        for name in present:
            batch.drop_column(name)


def _rename_run_type(old: str, new: str) -> None:
    op.execute(sa.text("UPDATE flow_runs SET run_type = :new WHERE run_type = :old").bindparams(old=old, new=new))


def upgrade() -> None:
    backends = _columns("compute_backends")
    if not backends["token"]["nullable"]:
        with op.batch_alter_table("compute_backends") as batch:
            batch.alter_column("token", existing_type=sa.Text(), nullable=True)
    _add_missing("compute_backends", (("remote_id", sa.Integer()), ("account_user_id", sa.Integer())))
    if not _has_table("issuer_accounts"):
        op.create_table(
            "issuer_accounts",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("issuer", sa.String(), nullable=False),
            sa.Column("subject_id", sa.Integer(), nullable=False),
            sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False, unique=True),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("issuer", "subject_id", name="uq_issuer_account_subject"),
        )
    _add_missing("flow_runs", RUN_COLUMNS)
    _rename_run_type("hub", "dispatched")
    _add_missing("flow_schedules", SCHEDULE_COLUMNS)


def downgrade() -> None:
    _drop_present("flow_schedules", SCHEDULE_COLUMNS)
    _rename_run_type("dispatched", "hub")
    _drop_present("flow_runs", RUN_COLUMNS)
    if _has_table("issuer_accounts"):
        op.drop_table("issuer_accounts")
    # A filled row has no token, which the column cannot hold again once it is NOT NULL.
    op.execute(sa.text("DELETE FROM compute_backends WHERE token IS NULL"))
    _drop_present("compute_backends", (("remote_id", sa.Integer()), ("account_user_id", sa.Integer())))
    if _columns("compute_backends")["token"]["nullable"]:
        with op.batch_alter_table("compute_backends") as batch:
            batch.alter_column("token", existing_type=sa.Text(), nullable=False)

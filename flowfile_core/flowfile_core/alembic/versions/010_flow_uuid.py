"""Add flow_uuid to flow_registrations and flow_runs.

Stable identity for flow registrations so that SQLite reusing a deleted
``FlowRegistration.id`` can never pull another flow's runs into the new flow's
history. ``FlowRun`` keeps its own ``flow_uuid`` snapshot so deleting a
registration (which nulls ``registration_id``) preserves run attribution in the
global view.

Revision ID: 010
Revises: 009
Create Date: 2026-04-30
"""

from collections.abc import Sequence
from uuid import uuid4

import sqlalchemy as sa
from alembic import op

revision: str = "010"
down_revision: str | None = "009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Add as nullable first so we can backfill before enforcing NOT NULL.
    with op.batch_alter_table("flow_registrations") as batch:
        batch.add_column(sa.Column("flow_uuid", sa.String(length=36), nullable=True))

    with op.batch_alter_table("flow_runs") as batch:
        batch.add_column(sa.Column("flow_uuid", sa.String(length=36), nullable=True))
        batch.create_index("ix_flow_runs_flow_uuid", ["flow_uuid"])

    bind = op.get_bind()

    # 1. Backfill flow_registrations.flow_uuid (one uuid per registration).
    rows = bind.execute(sa.text("SELECT id FROM flow_registrations")).fetchall()
    for (reg_id,) in rows:
        bind.execute(
            sa.text("UPDATE flow_registrations SET flow_uuid = :u WHERE id = :i"),
            {"u": str(uuid4()), "i": reg_id},
        )

    # 2. Backfill flow_runs.flow_uuid by copying from the registration the run points at.
    bind.execute(
        sa.text(
            "UPDATE flow_runs SET flow_uuid = ("
            "SELECT flow_uuid FROM flow_registrations "
            "WHERE flow_registrations.id = flow_runs.registration_id"
            ") WHERE registration_id IS NOT NULL"
        )
    )

    # 3. Now enforce NOT NULL + UNIQUE on flow_registrations.flow_uuid.
    with op.batch_alter_table("flow_registrations") as batch:
        batch.alter_column("flow_uuid", existing_type=sa.String(length=36), nullable=False)
        batch.create_unique_constraint("uq_flow_registrations_flow_uuid", ["flow_uuid"])


def downgrade() -> None:
    with op.batch_alter_table("flow_registrations") as batch:
        batch.drop_constraint("uq_flow_registrations_flow_uuid", type_="unique")
        batch.drop_column("flow_uuid")
    with op.batch_alter_table("flow_runs") as batch:
        batch.drop_index("ix_flow_runs_flow_uuid")
        batch.drop_column("flow_uuid")

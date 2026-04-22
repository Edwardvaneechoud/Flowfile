"""Normalize legacy flow_runs.run_type values.

Early versions persisted the flow-engine's run kind (``fetch_one``,
``full_run``, ``init``) into the catalog ``flow_runs.run_type`` column,
which is meant for provenance (``in_designer_run``, ``scheduled``,
``manual``, ``on_demand``). Those stale rows now fail Pydantic validation
on ``GET /catalog/runs``. Remap them to ``in_designer_run``, which is the
only context in which the leak occurred.

Revision ID: 006
Revises: 005
Create Date: 2026-04-21
"""

from collections.abc import Sequence

from alembic import op

revision: str = "006"
down_revision: str | None = "005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "UPDATE flow_runs SET run_type = 'in_designer_run' "
        "WHERE run_type IN ('full_run', 'fetch_one', 'init')"
    )


def downgrade() -> None:
    pass

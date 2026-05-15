"""Add scratch_flow_registration_id FK column to kernels.

Each kernel auto-creates a "scratch" FlowRegistration so that artifacts
published from interactive cells have a valid producer to point at. The
column is nullable: existing kernels lazily acquire one on first publish.

Revision ID: 014
Revises: 013
Create Date: 2026-05-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "014"
down_revision: str | None = "013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if _has_column("kernels", "scratch_flow_registration_id"):
        return
    with op.batch_alter_table("kernels") as batch:
        # SQLite's batch_alter_table requires every constraint to be named, so
        # we wrap the FK in a named ``ForeignKeyConstraint`` rather than using
        # the shorthand ``sa.ForeignKey(...)`` arg on the column.
        batch.add_column(
            sa.Column("scratch_flow_registration_id", sa.Integer(), nullable=True)
        )
        batch.create_foreign_key(
            "fk_kernels_scratch_flow_registration_id",
            "flow_registrations",
            ["scratch_flow_registration_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    with op.batch_alter_table("kernels") as batch:
        batch.drop_constraint("fk_kernels_scratch_flow_registration_id", type_="foreignkey")
        batch.drop_column("scratch_flow_registration_id")

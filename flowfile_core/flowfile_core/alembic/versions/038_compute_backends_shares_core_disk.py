"""Mark a configured worker that sees core's files.

``shares_core_disk`` is true for a worker on core's host or on a mount core shares with it; core then offers it
``file`` among the schemes it reads and writes, whatever the worker declares. Existing rows stay false.

Revision ID: 038
Revises: 037
Create Date: 2026-10-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "038"
down_revision: str | None = "037"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("compute_backends", "shares_core_disk"):
        op.add_column(
            "compute_backends",
            sa.Column("shares_core_disk", sa.Boolean(), nullable=False, server_default=sa.false()),
        )


def downgrade() -> None:
    if _has_column("compute_backends", "shares_core_disk"):
        with op.batch_alter_table("compute_backends") as batch:
            batch.drop_column("shares_core_disk")

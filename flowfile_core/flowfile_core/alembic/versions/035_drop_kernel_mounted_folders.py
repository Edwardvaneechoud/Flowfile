"""Drop the folders a kernel may read from kernels.

A kernel mounts no host folder any more: a notebook kernel reads files, catalog tables, flow files and
custom node sources through core, so ``mounted_folders`` (034) has nothing to describe.

Revision ID: 035
Revises: 034
Create Date: 2026-10-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "035"
down_revision: str | None = "034"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if _has_column("kernels", "mounted_folders"):
        with op.batch_alter_table("kernels") as batch:
            batch.drop_column("mounted_folders")


def downgrade() -> None:
    if not _has_column("kernels", "mounted_folders"):
        op.add_column("kernels", sa.Column("mounted_folders", sa.Text(), nullable=True, server_default="[]"))

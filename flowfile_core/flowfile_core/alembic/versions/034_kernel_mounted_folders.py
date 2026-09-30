"""Add the folders a kernel may read to kernels.

``mounted_folders`` is a JSON list of host directories bind-mounted read-only into the kernel's
container (electron mode only). Existing kernels get ``[]``.

Revision ID: 034
Revises: 033
Create Date: 2026-09-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "034"
down_revision: str | None = "033"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("kernels", "mounted_folders"):
        op.add_column("kernels", sa.Column("mounted_folders", sa.Text(), nullable=True, server_default="[]"))


def downgrade() -> None:
    if _has_column("kernels", "mounted_folders"):
        with op.batch_alter_table("kernels") as batch:
            batch.drop_column("mounted_folders")

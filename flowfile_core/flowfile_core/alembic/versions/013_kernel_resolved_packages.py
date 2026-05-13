"""Add resolved_packages JSON column to kernels.

Stores the actual versions pip resolved for each user-requested package after
the derived image is built, so the UI can show ``pandas 2.3.3`` instead of
just the user's spec ``pandas``.

Revision ID: 013
Revises: 012
Create Date: 2026-05-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "013"
down_revision: str | None = "012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if _has_column("kernels", "resolved_packages"):
        return
    with op.batch_alter_table("kernels") as batch:
        batch.add_column(
            sa.Column("resolved_packages", sa.Text(), nullable=False, server_default="[]")
        )


def downgrade() -> None:
    with op.batch_alter_table("kernels") as batch:
        batch.drop_column("resolved_packages")

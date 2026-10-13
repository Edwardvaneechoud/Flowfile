"""Add the compute backends table: the workers core reaches by id, beside its built-in ones.

One row per configured worker, ``worker:<name>``, with its base URL and its bearer token stored as a ``$ffsec$``
secret-manager token, never plaintext. ``local`` and ``worker`` are built in and never rows.

Revision ID: 036
Revises: 035
Create Date: 2026-10-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "036"
down_revision: str | None = "035"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_table("compute_backends"):
        op.create_table(
            "compute_backends",
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("url", sa.String(), nullable=False),
            sa.Column("token", sa.Text(), nullable=False),
        )


def downgrade() -> None:
    if _has_table("compute_backends"):
        op.drop_table("compute_backends")

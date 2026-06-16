"""Add workspace_projects table.

Maps a local install to a git-friendly project folder that mirrors its flows /
connections / schedules as deterministic, secret-free YAML. The DB stays the
runtime source of truth; this table is the export/import + history layer.

Revision ID: 022
Revises: 021
Create Date: 2026-06-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "022"
down_revision: str | None = "021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if _has_table("workspace_projects"):
        return
    op.create_table(
        "workspace_projects",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("folder_path", sa.String, nullable=False, unique=True),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("last_synced_head_sha", sa.String(length=40), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    if _has_table("workspace_projects"):
        op.drop_table("workspace_projects")

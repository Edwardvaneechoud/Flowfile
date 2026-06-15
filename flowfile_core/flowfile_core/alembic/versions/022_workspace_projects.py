"""Add workspace_projects table.

Maps a local catalog DB to its git-enabled project tree(s) on disk: where the
deterministic, secret-free projection lives (``root_path``), its stable
``project_id`` (mirrored in the tree manifest), the owning user, and the last
export's git sha (Phase 2). Git is the history store by design, so there is no
companion history table.

Revision ID: 022
Revises: 021
Create Date: 2026-06-15
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
        sa.Column("project_id", sa.String(36), nullable=False),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("root_path", sa.String, nullable=False),
        sa.Column("namespace_roots", sa.Text, nullable=True),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("git_enabled", sa.Boolean, nullable=False, server_default="0"),
        sa.Column("last_export_sha", sa.String, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("project_id", name="uq_workspace_project_id"),
        sa.UniqueConstraint("owner_id", "root_path", name="uq_workspace_owner_root"),
    )


def downgrade() -> None:
    if _has_table("workspace_projects"):
        op.drop_table("workspace_projects")

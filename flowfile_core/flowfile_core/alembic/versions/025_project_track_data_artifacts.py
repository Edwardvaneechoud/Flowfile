"""Add per-project track_data_artifacts toggle.

A git-backed project can opt out of versioning catalog tables (tables.yaml) and global
artifacts (models.yaml). The flag lives in project.yaml; this column mirrors it on the
WorkspaceProject row so the runtime can read it without touching the filesystem. Existing
projects default to True, preserving the current always-track behavior.

Revision ID: 025
Revises: 024
Create Date: 2026-06-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "025"
down_revision: str | None = "024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if _has_column("workspace_projects", "track_data_artifacts"):
        return
    with op.batch_alter_table("workspace_projects") as batch:
        batch.add_column(
            sa.Column("track_data_artifacts", sa.Boolean(), nullable=False, server_default=sa.true())
        )


def downgrade() -> None:
    if _has_column("workspace_projects", "track_data_artifacts"):
        with op.batch_alter_table("workspace_projects") as batch:
            batch.drop_column("track_data_artifacts")

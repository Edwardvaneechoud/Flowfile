"""Add catalog_dashboards table.

A dashboard is a saved 2D canvas of visualization tiles. Tile placement,
grid metadata, and dashboard-level filters all live in ``layout_json``;
no FK to ``catalog_visualizations`` because tiles are decoupled
references — deleted viz surface a placeholder at view time rather
than cascading.

Revision ID: 009
Revises: 008
Create Date: 2026-04-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "009"
down_revision: str | None = "008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "catalog_dashboards",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("layout_json", sa.Text, nullable=False),
        sa.Column("layout_version", sa.Integer, nullable=False, server_default="1"),
        sa.Column(
            "namespace_id",
            sa.Integer,
            sa.ForeignKey("catalog_namespaces.id"),
            nullable=True,
            index=True,
        ),
        sa.Column("created_by", sa.Integer, sa.ForeignKey("users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("catalog_dashboards")

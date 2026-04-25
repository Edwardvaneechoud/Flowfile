"""Add catalog_visualizations table for saved Graphic Walker chart specs.

Revision ID: 008
Revises: 007
Create Date: 2026-04-25
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "008"
down_revision: str | None = "007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "catalog_visualizations",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column("catalog_table_id", sa.Integer, sa.ForeignKey("catalog_tables.id"), nullable=False, index=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("chart_type", sa.String, nullable=True),
        sa.Column("spec_json", sa.Text, nullable=False),
        sa.Column("spec_gw_version", sa.String, nullable=True),
        sa.Column("created_by", sa.Integer, sa.ForeignKey("users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("catalog_table_id", "name", name="uq_viz_table_name"),
    )


def downgrade() -> None:
    op.drop_table("catalog_visualizations")

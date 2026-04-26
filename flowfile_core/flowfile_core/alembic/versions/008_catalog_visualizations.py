"""Add catalog_visualizations table.

Stores saved Graphic Walker chart specs as first-class catalog entities.
A visualization may reference a catalog table (``source_type="table"``,
``catalog_table_id`` set), or embed a SQL query that runs against the
catalog (``source_type="sql"``, ``sql_query`` set). ``namespace_id``
locates the viz in the catalog hierarchy independently of any source
table; ``thumbnail_data_url`` carries a base64 PNG snapshot for grid
previews.

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
        sa.Column("name", sa.String, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("chart_type", sa.String, nullable=True),
        sa.Column("spec_json", sa.Text, nullable=False),
        sa.Column("spec_gw_version", sa.String, nullable=True),
        sa.Column("source_type", sa.String, nullable=False, server_default="table"),
        sa.Column(
            "catalog_table_id",
            sa.Integer,
            sa.ForeignKey("catalog_tables.id"),
            nullable=True,
            index=True,
        ),
        sa.Column("sql_query", sa.Text, nullable=True),
        sa.Column("thumbnail_data_url", sa.Text, nullable=True),
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
    op.drop_table("catalog_visualizations")

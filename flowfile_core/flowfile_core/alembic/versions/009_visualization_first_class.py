"""Decouple catalog_visualizations from catalog_tables.

Visualizations become a first-class entity that may reference a catalog
table OR an inline SQL query. The parent table relationship is now
optional; viz also carries its own ``namespace_id`` so it can live on its
own in the catalog tree.

Revision ID: 009
Revises: 008
Create Date: 2026-04-26
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "009"
down_revision: str | None = "008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # SQLite requires batch mode for column / constraint changes.
    with op.batch_alter_table("catalog_visualizations", recreate="auto") as batch_op:
        batch_op.add_column(
            sa.Column("source_type", sa.String, nullable=False, server_default="table")
        )
        batch_op.add_column(sa.Column("sql_query", sa.Text, nullable=True))
        batch_op.add_column(sa.Column("description", sa.Text, nullable=True))
        batch_op.add_column(sa.Column("namespace_id", sa.Integer, nullable=True))
        batch_op.alter_column("catalog_table_id", existing_type=sa.Integer, nullable=True)
        # Drop the old (catalog_table_id, name) uniqueness; the new model lets
        # a viz live without a table parent. Names are scoped to the namespace
        # instead, with NULL namespace allowed so v1 doesn't force migrations.
        batch_op.drop_constraint("uq_viz_table_name", type_="unique")

    # Backfill namespace_id from each viz's parent table.
    op.execute(
        """
        UPDATE catalog_visualizations
        SET namespace_id = (
            SELECT namespace_id FROM catalog_tables
            WHERE catalog_tables.id = catalog_visualizations.catalog_table_id
        )
        WHERE catalog_table_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("UPDATE catalog_visualizations SET catalog_table_id = -1 WHERE catalog_table_id IS NULL")
    with op.batch_alter_table("catalog_visualizations", recreate="auto") as batch_op:
        batch_op.alter_column("catalog_table_id", existing_type=sa.Integer, nullable=False)
        batch_op.create_unique_constraint("uq_viz_table_name", ["catalog_table_id", "name"])
        batch_op.drop_column("namespace_id")
        batch_op.drop_column("description")
        batch_op.drop_column("sql_query")
        batch_op.drop_column("source_type")

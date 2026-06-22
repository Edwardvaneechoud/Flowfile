"""Move notebook content to disk: recreate catalog_notebooks.

Notebook cells now live on disk as deterministic YAML files keyed by
``notebook_uuid`` (see ``catalog/services/notebook_store.py``); the row keeps
only metadata. There is no notebook data to preserve, so this recreates the
table in the new shape: drop ``cells_json`` and add ``notebook_uuid``.

Revision ID: 027
Revises: 026
Create Date: 2026-06-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "027"
down_revision: str | None = "026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def _create_notebooks() -> None:
    op.create_table(
        "catalog_notebooks",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column("notebook_uuid", sa.String(36), nullable=False),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("namespace_id", sa.Integer, sa.ForeignKey("catalog_namespaces.id"), nullable=True, index=True),
        sa.Column("default_kernel_id", sa.String, nullable=True),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name", "namespace_id", name="uq_catalog_notebook_name_ns"),
    )
    op.create_index(
        "ix_catalog_notebooks_notebook_uuid", "catalog_notebooks", ["notebook_uuid"], unique=True
    )


def upgrade() -> None:
    # No notebook data to preserve — drop the cells_json-era table and recreate.
    if _has_table("catalog_notebooks"):
        op.drop_table("catalog_notebooks")
    _create_notebooks()


def downgrade() -> None:
    if _has_table("catalog_notebooks"):
        op.drop_table("catalog_notebooks")

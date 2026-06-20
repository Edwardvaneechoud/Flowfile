"""Add catalog_notebooks table.

Stores saved notebooks — the catalog's exploration console. Each notebook owns
an ordered list of mixed cells (Python / SQL / Markdown) serialised into the
``cells_json`` Text column, lives in the catalog hierarchy via ``namespace_id``,
and is owned by ``owner_id``. Unique on ``(name, namespace_id)``.

Revision ID: 022
Revises: 021
Create Date: 2026-06-19
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
    if _has_table("catalog_notebooks"):
        return
    op.create_table(
        "catalog_notebooks",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("namespace_id", sa.Integer, sa.ForeignKey("catalog_namespaces.id"), nullable=True, index=True),
        sa.Column("cells_json", sa.Text, nullable=False, server_default="[]"),
        sa.Column("default_kernel_id", sa.String, nullable=True),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name", "namespace_id", name="uq_catalog_notebook_name_ns"),
    )


def downgrade() -> None:
    if _has_table("catalog_notebooks"):
        op.drop_table("catalog_notebooks")

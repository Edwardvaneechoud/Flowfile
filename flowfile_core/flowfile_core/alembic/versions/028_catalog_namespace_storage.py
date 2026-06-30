"""Add per-catalog object storage columns to catalog_namespaces.

Per-catalog (level-0 namespace) object storage: ``storage_uri`` +
``storage_connection_name`` let a catalog point its table data at object storage
(S3, etc.); schemas/tables inherit from their root catalog. Both nullable; existing
catalogs read back NULL ⇒ local filesystem (zero regression). Forward-only, no backfill.

Revision ID: 028
Revises: 027
Create Date: 2026-06-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "028"
down_revision: str | None = "027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("catalog_namespaces", "storage_uri"):
        with op.batch_alter_table("catalog_namespaces") as batch:
            batch.add_column(sa.Column("storage_uri", sa.String(), nullable=True))
    if not _has_column("catalog_namespaces", "storage_connection_name"):
        with op.batch_alter_table("catalog_namespaces") as batch:
            batch.add_column(sa.Column("storage_connection_name", sa.String(), nullable=True))


def downgrade() -> None:
    if _has_column("catalog_namespaces", "storage_connection_name"):
        with op.batch_alter_table("catalog_namespaces") as batch:
            batch.drop_column("storage_connection_name")
    if _has_column("catalog_namespaces", "storage_uri"):
        with op.batch_alter_table("catalog_namespaces") as batch:
            batch.drop_column("storage_uri")

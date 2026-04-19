"""Add virtual flow table support.

Revision ID: 002
Revises: 001
Create Date: 2026-04-12
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: str | None = "001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Plain ADD COLUMN works in SQLite without constraints.
    # We omit the ForeignKey in the migration; the ORM model defines it for
    # relationship resolution, and SQLite doesn't enforce FK by default anyway.
    op.add_column("catalog_tables", sa.Column("table_type", sa.String, nullable=False, server_default="physical"))
    op.add_column("catalog_tables", sa.Column("producer_registration_id", sa.Integer, nullable=True))
    op.add_column("catalog_tables", sa.Column("serialized_lazy_frame", sa.LargeBinary, nullable=True))
    op.add_column("catalog_tables", sa.Column("is_optimized", sa.Boolean, nullable=True, server_default=sa.text("0")))

    # Allow NULL file_path for optimized virtual tables (no physical storage).
    # SQLite requires batch mode for ALTER COLUMN.
    with op.batch_alter_table("catalog_tables", recreate="auto") as batch_op:
        batch_op.alter_column("file_path", existing_type=sa.String, nullable=True)


def downgrade() -> None:
    with op.batch_alter_table("catalog_tables", recreate="auto") as batch_op:
        batch_op.alter_column("file_path", existing_type=sa.String, nullable=False)
    op.drop_column("catalog_tables", "is_optimized")
    op.drop_column("catalog_tables", "serialized_lazy_frame")
    op.drop_column("catalog_tables", "producer_registration_id")
    op.drop_column("catalog_tables", "table_type")

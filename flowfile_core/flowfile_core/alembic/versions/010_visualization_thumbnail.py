"""Add a stored thumbnail (base64 PNG data URL) to catalog_visualizations.

The frontend captures GraphicWalker's ``exportChart('data-url')`` output on
save, so the catalog can render a static preview without re-mounting the
chart on every list/grid view. The column is nullable: legacy rows and
older clients simply skip thumbnail rendering.

Revision ID: 010
Revises: 009
Create Date: 2026-04-26
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "010"
down_revision: str | None = "009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("catalog_visualizations", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("thumbnail_data_url", sa.Text, nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("catalog_visualizations", recreate="auto") as batch_op:
        batch_op.drop_column("thumbnail_data_url")

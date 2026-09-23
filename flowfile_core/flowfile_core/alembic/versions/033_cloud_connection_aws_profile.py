"""Add an explicit AWS profile to cloud storage connections.

``cloud_storage_connections.aws_profile`` names the local AWS profile an
``aws-cli`` connection resolves its credentials from; NULL means boto3's
default credential chain. Before this revision the connection's display name
was passed to boto3 as the profile name, implicitly.

Existing rows are deliberately not backfilled: the connection name is a label,
never a profile, so every existing ``aws-cli`` connection moves to the default
credential chain until a profile is set explicitly.

Revision ID: 033
Revises: 032
Create Date: 2026-09-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "033"
down_revision: str | None = "032"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("cloud_storage_connections", "aws_profile"):
        op.add_column("cloud_storage_connections", sa.Column("aws_profile", sa.String(), nullable=True))


def downgrade() -> None:
    if _has_column("cloud_storage_connections", "aws_profile"):
        with op.batch_alter_table("cloud_storage_connections") as batch:
            batch.drop_column("aws_profile")

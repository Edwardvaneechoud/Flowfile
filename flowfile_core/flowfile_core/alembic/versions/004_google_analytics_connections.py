"""Add google_analytics_connections table for GA4 reader credentials.

Revision ID: 004
Revises: 003
Create Date: 2026-04-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "004"
down_revision: str | None = "003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "google_analytics_connections",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column("connection_name", sa.String, nullable=False, index=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("default_property_id", sa.String, nullable=True),
        sa.Column(
            "service_account_key_id",
            sa.Integer,
            sa.ForeignKey("secrets.id"),
            nullable=False,
        ),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("google_analytics_connections")

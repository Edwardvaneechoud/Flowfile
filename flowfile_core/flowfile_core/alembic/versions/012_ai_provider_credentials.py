"""Add ai_provider_credentials table.

Per plan §6.5 / §8 — BYOK API keys live in a typed connection row that
references the existing ``secrets`` table for the encrypted blob, mirroring
``cloud_storage_connections``. Per-user uniqueness on ``(user_id, provider)``
so re-saving a credential is idempotent on the natural key.

Revision ID: 012
Revises: 011
Create Date: 2026-05-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "012"
down_revision: str | None = "011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ai_provider_credentials",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column(
            "user_id",
            sa.Integer,
            sa.ForeignKey("users.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("provider", sa.String, nullable=False, index=True),
        sa.Column(
            "api_key_secret_id",
            sa.Integer,
            sa.ForeignKey("secrets.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("api_base", sa.String, nullable=True),
        sa.Column("default_model", sa.String, nullable=True),
        sa.Column("last_tested_at", sa.DateTime, nullable=True),
        sa.Column("last_test_status", sa.String, nullable=True),
        sa.Column("last_test_error", sa.Text, nullable=True),
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
        sa.UniqueConstraint("user_id", "provider", name="uq_ai_provider_per_user"),
    )


def downgrade() -> None:
    op.drop_table("ai_provider_credentials")

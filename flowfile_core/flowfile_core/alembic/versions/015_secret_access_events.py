"""Add secret_access_events table for secret CRUD audit logging.

This table captures every attempt to create, list, read, or delete a secret via
the public secrets API. The goal is operator-visible forensics — "who touched
what, when, and from where" — independent of the per-decrypt activity that
happens deep inside flow execution. Decrypt-on-flow events are intentionally
not logged here to keep the audit table at a useful signal-to-noise ratio.

``secret_id`` is FK-with-SET-NULL on the existing ``secrets`` table so a row
survives a secret deletion (otherwise we'd lose the very record we need to
investigate the deletion). ``secret_name`` is denormalized into the audit row
for the same reason.

Revision ID: 015
Revises: 014
Create Date: 2026-05-12
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "015"
down_revision: str | None = "014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "secret_access_events",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column(
            "user_id",
            sa.Integer,
            sa.ForeignKey("users.id"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "secret_id",
            sa.Integer,
            sa.ForeignKey("secrets.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("secret_name", sa.String, nullable=True, index=True),
        sa.Column("action", sa.String, nullable=False, index=True),
        sa.Column("result_status", sa.String, nullable=False),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("source", sa.String, nullable=False, server_default="api"),
        sa.Column("ip_address", sa.String, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
            index=True,
        ),
    )


def downgrade() -> None:
    op.drop_table("secret_access_events")

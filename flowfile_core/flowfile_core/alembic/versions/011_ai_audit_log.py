"""Add ai_audit_events table.

Per plan §9.4 ("Every AI action recorded"). Source for the §13 success metrics
— tool-call validation pass rate, diff accept rate, token / cost roll-ups —
plus the future ``GET /ai/audit/{flow_id}`` route. ``flow_id`` is a plain
integer (not an FK to ``flow_registrations``) because draft flows aren't
registered yet but still produce auditable AI actions.

Revision ID: 011
Revises: 010
Create Date: 2026-05-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "011"
down_revision: str | None = "010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ai_audit_events",
        sa.Column("id", sa.Integer, primary_key=True, index=True),
        sa.Column("session_id", sa.String, nullable=False, index=True),
        sa.Column("flow_id", sa.Integer, nullable=True, index=True),
        sa.Column(
            "user_id",
            sa.Integer,
            sa.ForeignKey("users.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("tool_name", sa.String, nullable=False, index=True),
        sa.Column("tool_args", sa.Text, nullable=True),
        sa.Column("result_status", sa.String, nullable=False),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("provider", sa.String, nullable=True),
        sa.Column("model", sa.String, nullable=True),
        sa.Column("prompt_tokens", sa.Integer, nullable=False, server_default="0"),
        sa.Column("completion_tokens", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_tokens", sa.Integer, nullable=False, server_default="0"),
        sa.Column("diff_action", sa.String, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.now(),
            nullable=False,
            index=True,
        ),
    )


def downgrade() -> None:
    op.drop_table("ai_audit_events")

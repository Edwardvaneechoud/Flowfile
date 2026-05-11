"""Add AI tables: ai_audit_events and ai_provider_credentials.

Two tables that together back the BYOK + audit infrastructure for the
LLM-integration feature.

``ai_audit_events`` — per plan §9.4 ("Every AI action recorded"). Source for
the §13 success metrics — tool-call validation pass rate, diff accept rate,
token / cost roll-ups — plus the future ``GET /ai/audit/{flow_id}`` route.
``flow_id`` is a plain integer (not an FK to ``flow_registrations``) because
draft flows aren't registered yet but still produce auditable AI actions.

``ai_provider_credentials`` — per plan §6.5 / §8. BYOK API keys live in a
typed connection row that references the existing ``secrets`` table for the
encrypted blob, mirroring ``cloud_storage_connections``. Per-user uniqueness
on ``(user_id, provider)`` so re-saving a credential is idempotent on the
natural key. The ``models`` column (per W29) holds a JSON-encoded list so a
single API key (OpenRouter, Groq, …) can advertise its curated model set
without a re-typing loop in the chat-drawer picker; stored as nullable
``Text`` to match the project's existing pattern for JSON-shaped data
(``flow_runs.node_results_json``, ``configs.tags`` …) and avoid the
SQLite-vs-PG ``JSON`` type discrepancy. Encode/decode lives in
:mod:`flowfile_core.ai.credentials`.

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
        sa.Column("models", sa.Text, nullable=True),
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
    op.drop_table("ai_audit_events")

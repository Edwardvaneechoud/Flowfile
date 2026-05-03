"""Add models JSON column to ai_provider_credentials.

Per W29 — a single API key (OpenRouter, Groq, …) gives access to many
models. Storing one curated list per credential lets the chat-drawer
model picker offer those models without a re-typing loop or per-request
free-text input. The list is stored as a JSON-encoded string in a
nullable ``Text`` column to match the project's existing pattern for
JSON-shaped data (``flow_runs.node_results_json``,
``configs.tags`` …) and avoid the SQLite-vs-PG ``JSON`` type
discrepancy. Encode/decode lives in
:mod:`flowfile_core.ai.credentials`.

Revision ID: 013
Revises: 012
Create Date: 2026-05-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "013"
down_revision: str | None = "012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "ai_provider_credentials",
        sa.Column("models", sa.Text, nullable=True),
    )


def downgrade() -> None:
    with op.batch_alter_table("ai_provider_credentials") as batch_op:
        batch_op.drop_column("models")

"""Add flow_api_endpoints and flow_api_keys tables.

Backs the "host flows as HTTP APIs" feature. A ``FlowApiEndpoint`` publishes a
registered flow under a URL slug; ``FlowApiKey`` rows authenticate callers and
are stored hashed (never recoverable). See ``routes/flow_api.py``.

Revision ID: 015
Revises: 014
Create Date: 2026-05-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "015"
down_revision: str | None = "014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_table("flow_api_endpoints"):
        op.create_table(
            "flow_api_endpoints",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column(
                "registration_id",
                sa.Integer,
                sa.ForeignKey("flow_registrations.id"),
                nullable=False,
            ),
            sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
            sa.Column("slug", sa.String, nullable=False, index=True),
            sa.Column("enabled", sa.Boolean, nullable=False, server_default=sa.text("1")),
            sa.Column("response_node_id", sa.Integer, nullable=True),
            sa.Column("param_schema_json", sa.Text, nullable=True),
            sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("slug", name="uq_api_endpoint_slug"),
            sa.UniqueConstraint("registration_id", name="uq_api_endpoint_registration"),
        )
    if not _has_table("flow_api_keys"):
        op.create_table(
            "flow_api_keys",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column(
                "endpoint_id",
                sa.Integer,
                sa.ForeignKey("flow_api_endpoints.id"),
                nullable=False,
                index=True,
            ),
            sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
            sa.Column("name", sa.String, nullable=False),
            sa.Column("key_hash", sa.String, nullable=False, index=True),
            sa.Column("key_prefix", sa.String, nullable=False),
            sa.Column("enabled", sa.Boolean, nullable=False, server_default=sa.text("1")),
            sa.Column("last_used_at", sa.DateTime, nullable=True),
            sa.Column("expires_at", sa.DateTime, nullable=True),
            sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("key_hash", name="uq_api_key_hash"),
        )


def downgrade() -> None:
    if _has_table("flow_api_keys"):
        op.drop_table("flow_api_keys")
    if _has_table("flow_api_endpoints"):
        op.drop_table("flow_api_endpoints")

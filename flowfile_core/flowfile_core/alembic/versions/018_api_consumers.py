"""Add api_consumers + api_consumer_endpoints; add flow_api_keys.consumer_id.

Introduces reusable API consumers (service accounts) that hold rotatable keys and
can be granted access to multiple published flow endpoints. Backfills one implicit
consumer + grant per existing key so keys created before this change keep working.

Revision ID: 018
Revises: 017
Create Date: 2026-05-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "018"
down_revision: str | None = "017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def _backfill_consumers() -> None:
    """Create one implicit consumer + grant per existing key and link the key to it.

    Keys on the same endpoint share a single implicit consumer. Idempotent: skips
    keys that already have a consumer_id, and reuses existing consumers/grants. Uses
    a SELECT-after-INSERT lookup (consumers are unique on (owner_id, name)) so it
    works on both SQLite and PostgreSQL without relying on lastrowid/RETURNING.
    """
    bind = op.get_bind()
    keys = bind.execute(
        sa.text("SELECT id, endpoint_id, owner_id FROM flow_api_keys WHERE consumer_id IS NULL")
    ).fetchall()
    for key_id, endpoint_id, owner_id in keys:
        if endpoint_id is None:
            continue
        slug_row = bind.execute(
            sa.text("SELECT slug FROM flow_api_endpoints WHERE id = :e"), {"e": endpoint_id}
        ).fetchone()
        slug = slug_row[0] if slug_row else str(endpoint_id)
        consumer_name = f"endpoint:{slug}"

        existing = bind.execute(
            sa.text("SELECT id FROM api_consumers WHERE owner_id = :o AND name = :n"),
            {"o": owner_id, "n": consumer_name},
        ).fetchone()
        if existing is None:
            bind.execute(
                sa.text(
                    "INSERT INTO api_consumers "
                    "(name, description, owner_id, enabled, is_implicit, created_at, updated_at) "
                    "VALUES (:n, :d, :o, 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                ),
                {"n": consumer_name, "d": f"Auto-created for endpoint {slug}", "o": owner_id},
            )
            existing = bind.execute(
                sa.text("SELECT id FROM api_consumers WHERE owner_id = :o AND name = :n"),
                {"o": owner_id, "n": consumer_name},
            ).fetchone()
        consumer_id = existing[0]

        has_grant = bind.execute(
            sa.text("SELECT 1 FROM api_consumer_endpoints WHERE consumer_id = :c AND endpoint_id = :e"),
            {"c": consumer_id, "e": endpoint_id},
        ).fetchone()
        if has_grant is None:
            bind.execute(
                sa.text(
                    "INSERT INTO api_consumer_endpoints (consumer_id, endpoint_id, created_at) "
                    "VALUES (:c, :e, CURRENT_TIMESTAMP)"
                ),
                {"c": consumer_id, "e": endpoint_id},
            )

        bind.execute(
            sa.text("UPDATE flow_api_keys SET consumer_id = :c WHERE id = :k"),
            {"c": consumer_id, "k": key_id},
        )


def upgrade() -> None:
    if not _has_table("api_consumers"):
        op.create_table(
            "api_consumers",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("name", sa.String, nullable=False),
            sa.Column("description", sa.Text, nullable=True),
            sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False, index=True),
            sa.Column("enabled", sa.Boolean, nullable=False, server_default="1"),
            sa.Column("is_implicit", sa.Boolean, nullable=False, server_default="0"),
            sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("owner_id", "name", name="uq_api_consumer_owner_name"),
        )
    if not _has_table("api_consumer_endpoints"):
        op.create_table(
            "api_consumer_endpoints",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column(
                "consumer_id",
                sa.Integer,
                sa.ForeignKey("api_consumers.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.Column(
                "endpoint_id",
                sa.Integer,
                sa.ForeignKey("flow_api_endpoints.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("consumer_id", "endpoint_id", name="uq_consumer_endpoint"),
        )

    # Add consumer_id (nullable) and relax endpoint_id to nullable in one table rebuild.
    # batch_alter_table is the SQLite-safe way to ALTER (used by 010/013/014/017).
    with op.batch_alter_table("flow_api_keys") as batch:
        if not _has_column("flow_api_keys", "consumer_id"):
            batch.add_column(
                sa.Column(
                    "consumer_id",
                    sa.Integer,
                    sa.ForeignKey("api_consumers.id", ondelete="CASCADE"),
                    nullable=True,
                )
            )
        batch.alter_column("endpoint_id", existing_type=sa.Integer(), nullable=True)

    _backfill_consumers()

    # Fail loud rather than ship keys that can never authenticate under the new path.
    orphaned = op.get_bind().execute(
        sa.text("SELECT COUNT(*) FROM flow_api_keys WHERE consumer_id IS NULL AND endpoint_id IS NOT NULL")
    ).scalar()
    if orphaned:
        raise RuntimeError(f"Migration 018 left {orphaned} API key(s) without a consumer; aborting.")


def downgrade() -> None:
    if _has_column("flow_api_keys", "consumer_id"):
        with op.batch_alter_table("flow_api_keys") as batch:
            batch.drop_column("consumer_id")
    if _has_table("api_consumer_endpoints"):
        op.drop_table("api_consumer_endpoints")
    if _has_table("api_consumers"):
        op.drop_table("api_consumers")

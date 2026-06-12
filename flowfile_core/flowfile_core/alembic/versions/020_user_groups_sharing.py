"""Add user_groups + user_group_memberships + resource_grants; add catalog_namespaces.is_public.

Introduces group-based sharing of secrets, connections, and catalog resources
(multi-user mode). Backfills is_public=1 for the seeded system namespaces
(General catalog and its default/Unnamed Flows/Local Flows schemas) so they stay
visible to every user once the catalog becomes private-by-default in docker mode.

Revision ID: 020
Revises: 019
Create Date: 2026-06-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "020"
down_revision: str | None = "019"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def _backfill_public_namespaces() -> None:
    """Mark the init_db-seeded namespaces public. Idempotent; no-op when the seeds are absent."""
    bind = op.get_bind()
    local_user = bind.execute(sa.text("SELECT id FROM users WHERE username = 'local_user'")).fetchone()
    if local_user is None:
        return
    general = bind.execute(
        sa.text("SELECT id FROM catalog_namespaces WHERE parent_id IS NULL AND name = 'General' AND owner_id = :o"),
        {"o": local_user[0]},
    ).fetchone()
    if general is None:
        return
    bind.execute(sa.text("UPDATE catalog_namespaces SET is_public = 1 WHERE id = :g"), {"g": general[0]})
    bind.execute(
        sa.text(
            "UPDATE catalog_namespaces SET is_public = 1 "
            "WHERE parent_id = :g AND name IN ('default', 'Unnamed Flows', 'Local Flows')"
        ),
        {"g": general[0]},
    )


def upgrade() -> None:
    if not _has_table("user_groups"):
        op.create_table(
            "user_groups",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("name", sa.String, nullable=False, unique=True, index=True),
            sa.Column("description", sa.Text, nullable=True),
            sa.Column("created_by", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
            sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
        )
    if not _has_table("user_group_memberships"):
        op.create_table(
            "user_group_memberships",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("group_id", sa.Integer, sa.ForeignKey("user_groups.id"), nullable=False, index=True),
            sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False, index=True),
            sa.Column("role", sa.String, nullable=False, server_default="member"),
            sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("group_id", "user_id", name="uq_group_member"),
        )
    if not _has_table("resource_grants"):
        op.create_table(
            "resource_grants",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("resource_type", sa.String, nullable=False, index=True),
            sa.Column("resource_id", sa.Integer, nullable=False),
            sa.Column("group_id", sa.Integer, sa.ForeignKey("user_groups.id"), nullable=False, index=True),
            sa.Column("permission", sa.String, nullable=False, server_default="use"),
            sa.Column("granted_by", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
            sa.Column("created_at", sa.DateTime, server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("resource_type", "resource_id", "group_id", name="uq_resource_group_grant"),
        )
        op.create_index("ix_resource_grants_resource", "resource_grants", ["resource_type", "resource_id"])

    if not _has_column("catalog_namespaces", "is_public"):
        with op.batch_alter_table("catalog_namespaces") as batch:
            batch.add_column(sa.Column("is_public", sa.Boolean(), nullable=False, server_default="0"))
        _backfill_public_namespaces()


def downgrade() -> None:
    if _has_column("catalog_namespaces", "is_public"):
        with op.batch_alter_table("catalog_namespaces") as batch:
            batch.drop_column("is_public")
    if _has_table("resource_grants"):
        op.drop_table("resource_grants")
    if _has_table("user_group_memberships"):
        op.drop_table("user_group_memberships")
    if _has_table("user_groups"):
        op.drop_table("user_groups")

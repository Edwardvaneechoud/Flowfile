"""Catalogs kept by another provider: how each is reached, and its schemas marked as kept elsewhere.

A catalog kept elsewhere (an Iceberg REST catalog) is an ordinary level-0 ``catalog_namespaces`` row marked
``kept_elsewhere``, and its schemas are level-1 rows marked the same way. ``gone_since`` is set on a schema the other
catalog no longer lists. ``external_catalogs`` holds one row per such catalog: its address, its warehouse, the kind of
sign-in and the secret that holds it. Every existing namespace is Flowfile's own (``kept_elsewhere`` false).

Revision ID: 047
Revises: 046
Create Date: 2026-11-22
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "047"
down_revision: str | None = "046"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NAMESPACES = "catalog_namespaces"
_TABLE = "external_catalogs"


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def _has_table(table: str) -> bool:
    return inspect(op.get_bind()).has_table(table)


def upgrade() -> None:
    if not _has_column(_NAMESPACES, "kept_elsewhere"):
        op.add_column(_NAMESPACES, sa.Column("kept_elsewhere", sa.Boolean(), nullable=False, server_default="0"))
    if not _has_column(_NAMESPACES, "gone_since"):
        op.add_column(_NAMESPACES, sa.Column("gone_since", sa.DateTime(), nullable=True))
    if not _has_table(_TABLE):
        op.create_table(
            _TABLE,
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("namespace_id", sa.Integer(), sa.ForeignKey("catalog_namespaces.id"), nullable=False),
            sa.Column("kind", sa.String(), nullable=False),
            sa.Column("uri", sa.String(), nullable=False),
            sa.Column("warehouse", sa.String(), nullable=True),
            sa.Column("sign_in", sa.String(), nullable=False),
            sa.Column("client_id", sa.String(), nullable=True),
            sa.Column("scope", sa.String(), nullable=True),
            sa.Column("token_endpoint", sa.String(), nullable=True),
            sa.Column("secret_id", sa.Integer(), sa.ForeignKey("secrets.id"), nullable=True),
            sa.Column("synced_at", sa.DateTime(), nullable=True),
            sa.Column("sync_error", sa.Text(), nullable=True),
            sa.Column("not_shown", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("namespace_id", name="uq_external_catalog_namespace"),
        )


def downgrade() -> None:
    if _has_table(_TABLE):
        op.drop_table(_TABLE)
    with op.batch_alter_table(_NAMESPACES) as batch:
        if _has_column(_NAMESPACES, "gone_since"):
            batch.drop_column("gone_since")
        if _has_column(_NAMESPACES, "kept_elsewhere"):
            batch.drop_column("kept_elsewhere")

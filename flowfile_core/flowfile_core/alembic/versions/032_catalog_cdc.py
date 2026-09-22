"""Add catalog change tracking (Delta change data feed) state.

``catalog_tables.cdc_enabled`` mirrors the Delta ``delta.enableChangeDataFeed``
property and ``cdc_enabled_version`` records the commit version it was enabled
at — the floor every cursor is clamped to, because reads below it are
inconsistent.

``catalog_cdc_cursors`` holds one row per ``(table_id, consumer_key)``: the last
Delta commit version a consumer fully processed. ``table_path`` is stamped at
mint time so a cursor that outlived its table (SQLite reuses rowids) is treated
as absent rather than resumed against unrelated data.

Revision ID: 032
Revises: 031
Create Date: 2026-09-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "032"
down_revision: str | None = "031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_column("catalog_tables", "cdc_enabled"):
        op.add_column(
            "catalog_tables",
            sa.Column("cdc_enabled", sa.Boolean(), nullable=False, server_default="0"),
        )
    if not _has_column("catalog_tables", "cdc_enabled_version"):
        op.add_column("catalog_tables", sa.Column("cdc_enabled_version", sa.Integer(), nullable=True))

    if not _has_table("catalog_cdc_cursors"):
        op.create_table(
            "catalog_cdc_cursors",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("table_id", sa.Integer(), sa.ForeignKey("catalog_tables.id"), nullable=False),
            sa.Column("consumer_key", sa.String(), nullable=False),
            sa.Column("consumer_label", sa.String(), nullable=True),
            sa.Column("owner_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
            sa.Column("last_version", sa.Integer(), nullable=False),
            sa.Column("last_commit_timestamp", sa.DateTime(), nullable=True),
            sa.Column("table_path", sa.Text(), nullable=True),
            sa.Column("last_run_id", sa.Integer(), sa.ForeignKey("flow_runs.id"), nullable=True),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("table_id", "consumer_key", name="uq_cdc_cursor_table_consumer"),
        )
        op.create_index("ix_catalog_cdc_cursors_table_id", "catalog_cdc_cursors", ["table_id"])


def downgrade() -> None:
    if _has_table("catalog_cdc_cursors"):
        op.drop_index("ix_catalog_cdc_cursors_table_id", table_name="catalog_cdc_cursors")
        op.drop_table("catalog_cdc_cursors")
    with op.batch_alter_table("catalog_tables") as batch:
        if _has_column("catalog_tables", "cdc_enabled_version"):
            batch.drop_column("cdc_enabled_version")
        if _has_column("catalog_tables", "cdc_enabled"):
            batch.drop_column("cdc_enabled")

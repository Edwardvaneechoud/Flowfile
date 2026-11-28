"""Delta Sharing: shares of catalog tables, and which API consumers may read them.

``delta_shares`` holds one row per share, ``delta_share_tables`` the catalog tables in it under the schema and
table names a recipient sees, and ``api_consumer_delta_shares`` the consumers granted a share.

Revision ID: 048
Revises: 047
Create Date: 2026-11-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "048"
down_revision: str | None = "047"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_table(table: str) -> bool:
    return inspect(op.get_bind()).has_table(table)


def upgrade() -> None:
    if not _has_table("delta_shares"):
        op.create_table(
            "delta_shares",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("owner_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
            sa.Column("uuid", sa.String(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        )
        op.create_index("ix_delta_shares_id", "delta_shares", ["id"])
        op.create_index("ix_delta_shares_name", "delta_shares", ["name"], unique=True)
        op.create_index("ix_delta_shares_owner_id", "delta_shares", ["owner_id"])
    if not _has_table("delta_share_tables"):
        op.create_table(
            "delta_share_tables",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("share_id", sa.Integer(), sa.ForeignKey("delta_shares.id", ondelete="CASCADE"), nullable=False),
            sa.Column("table_id", sa.Integer(), sa.ForeignKey("catalog_tables.id"), nullable=False),
            sa.Column("schema_name", sa.String(), nullable=False),
            sa.Column("table_name", sa.String(), nullable=False),
            sa.Column("share_history", sa.Boolean(), nullable=False, server_default="1"),
            sa.Column("table_path", sa.Text(), nullable=True),
            sa.Column("uuid", sa.String(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("share_id", "schema_name", "table_name", name="uq_delta_share_table_name"),
            sa.UniqueConstraint("share_id", "table_id", name="uq_delta_share_table_id"),
        )
        op.create_index("ix_delta_share_tables_id", "delta_share_tables", ["id"])
        op.create_index("ix_delta_share_tables_share_id", "delta_share_tables", ["share_id"])
        op.create_index("ix_delta_share_tables_table_id", "delta_share_tables", ["table_id"])
    if not _has_table("api_consumer_delta_shares"):
        op.create_table(
            "api_consumer_delta_shares",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column(
                "consumer_id", sa.Integer(), sa.ForeignKey("api_consumers.id", ondelete="CASCADE"), nullable=False
            ),
            sa.Column("share_id", sa.Integer(), sa.ForeignKey("delta_shares.id", ondelete="CASCADE"), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.UniqueConstraint("consumer_id", "share_id", name="uq_consumer_delta_share"),
        )
        op.create_index("ix_api_consumer_delta_shares_id", "api_consumer_delta_shares", ["id"])
        op.create_index("ix_api_consumer_delta_shares_consumer_id", "api_consumer_delta_shares", ["consumer_id"])
        op.create_index("ix_api_consumer_delta_shares_share_id", "api_consumer_delta_shares", ["share_id"])


def downgrade() -> None:
    for table in ("api_consumer_delta_shares", "delta_share_tables", "delta_shares"):
        if _has_table(table):
            op.drop_table(table)

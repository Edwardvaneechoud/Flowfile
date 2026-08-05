"""Add extra_params to database_connections.

JSON dict of dialect-specific connection parameters that don't fit the
host/port shape (e.g. Snowflake's account/warehouse/role). Keys that could
override credentials are rejected at the API boundary and dropped again at
URI-build time (shared.db_dialects.base.is_blocked_extra_param). NULL means
the connection has no dialect-specific parameters.

Revision ID: 030
Revises: 029
Create Date: 2026-08-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "030"
down_revision: str | None = "029"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("database_connections", "extra_params"):
        op.add_column("database_connections", sa.Column("extra_params", sa.Text, nullable=True))


def downgrade() -> None:
    if _has_column("database_connections", "extra_params"):
        op.drop_column("database_connections", "extra_params")

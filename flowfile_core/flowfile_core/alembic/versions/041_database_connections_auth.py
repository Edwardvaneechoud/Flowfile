"""Add how a database connection signs in, and its session role and warehouse.

Snowflake connections sign in with an access token, a key-pair or a browser sign-in (OAuth refresh token), always
stored as the connection's one secret; ``auth_method`` says which, and ``role`` and ``warehouse`` set up the session.
All three are NULL for every other dialect and on every existing row.

Revision ID: 041
Revises: 040
Create Date: 2026-11-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "041"
down_revision: str | None = "040"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = ("auth_method", "role", "warehouse")


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    for column in _COLUMNS:
        if not _has_column("database_connections", column):
            op.add_column("database_connections", sa.Column(column, sa.String(), nullable=True))


def downgrade() -> None:
    present = [column for column in _COLUMNS if _has_column("database_connections", column)]
    if present:
        with op.batch_alter_table("database_connections") as batch:
            for column in present:
                batch.drop_column(column)

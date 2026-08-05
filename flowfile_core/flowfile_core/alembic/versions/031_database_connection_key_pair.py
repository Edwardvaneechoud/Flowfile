"""Add key-pair auth columns to database_connections.

auth_method selects the connection's authentication method (NULL means
password, the historical default). private_key_id / private_key_passphrase_id
reference encrypted secrets holding the PEM text and its optional passphrase.
The id columns are plain integers here — the ForeignKey lives in the ORM only,
because SQLite cannot drop a column that carries a FK constraint on downgrade.

Revision ID: 031
Revises: 030
Create Date: 2026-08-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "031"
down_revision: str | None = "030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = (
    ("auth_method", sa.String),
    ("private_key_id", sa.Integer),
    ("private_key_passphrase_id", sa.Integer),
)


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    for name, column_type in _COLUMNS:
        if not _has_column("database_connections", name):
            op.add_column("database_connections", sa.Column(name, column_type, nullable=True))


def downgrade() -> None:
    for name, _ in _COLUMNS:
        if _has_column("database_connections", name):
            op.drop_column("database_connections", name)

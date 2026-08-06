"""Add OAuth (SSO) columns to database_connections.

auth_method="oauth" connections store a per-connection OAuth client config
(client id, optional authorize/token endpoint overrides for External OAuth,
optional redirect-uri override) plus two secret FKs: the encrypted client
secret and the encrypted refresh token minted by the interactive sign-in.
The id columns are plain integers here — the ForeignKey lives in the ORM
only, because SQLite cannot drop a column that carries a FK constraint on
downgrade (the 031 precedent).

Revision ID: 032
Revises: 031
Create Date: 2026-08-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "032"
down_revision: str | None = "031"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = (
    ("oauth_client_id", sa.String),
    ("oauth_authorize_endpoint", sa.String),
    ("oauth_token_endpoint", sa.String),
    ("oauth_redirect_uri", sa.String),
    ("oauth_client_secret_id", sa.Integer),
    ("oauth_refresh_token_id", sa.Integer),
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

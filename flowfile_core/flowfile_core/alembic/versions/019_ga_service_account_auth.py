"""Add auth_method to google_analytics_connections.

Selects how a GA4 connection authenticates: ``"oauth"`` (refresh token, the
existing flow) or ``"service_account"`` (encrypted service-account JSON key).
Existing rows backfill to ``"oauth"`` so current connections keep working.

Revision ID: 019
Revises: 018
Create Date: 2026-06-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "019"
down_revision: str | None = "018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if _has_column("google_analytics_connections", "auth_method"):
        return
    with op.batch_alter_table("google_analytics_connections") as batch:
        batch.add_column(sa.Column("auth_method", sa.String(), nullable=False, server_default="oauth"))


def downgrade() -> None:
    if _has_column("google_analytics_connections", "auth_method"):
        with op.batch_alter_table("google_analytics_connections") as batch:
            batch.drop_column("auth_method")

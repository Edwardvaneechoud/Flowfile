"""Add is_api_compatible flag to flow_registrations.

True when a flow contains exactly one ``api_response`` node and can be published
as an HTTP data API. Recomputed from the flow graph on save.

Revision ID: 017
Revises: 016
Create Date: 2026-05-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "017"
down_revision: str | None = "016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if _has_column("flow_registrations", "is_api_compatible"):
        return
    with op.batch_alter_table("flow_registrations") as batch:
        batch.add_column(sa.Column("is_api_compatible", sa.Boolean(), nullable=False, server_default="0"))


def downgrade() -> None:
    if _has_column("flow_registrations", "is_api_compatible"):
        with op.batch_alter_table("flow_registrations") as batch:
            batch.drop_column("is_api_compatible")

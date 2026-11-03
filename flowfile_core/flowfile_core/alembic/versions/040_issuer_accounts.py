"""Add the issuer accounts table: which local account a person from a hub signs in as on this server.

One row per person, keyed by the issuer the hub signs with and that person's user id on the hub, and naming the local
user it signs in as. A local account belongs to at most one such person. Accounts that sign in with a password have no
row.

Revision ID: 040
Revises: 039
Create Date: 2026-11-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "040"
down_revision: str | None = "039"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_table(table: str) -> bool:
    return table in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_table("issuer_accounts"):
        op.create_table(
            "issuer_accounts",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("issuer", sa.String(), nullable=False),
            sa.Column("subject_id", sa.Integer(), nullable=False),
            sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False, unique=True),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),
            sa.UniqueConstraint("issuer", "subject_id", name="uq_issuer_account_subject"),
        )


def downgrade() -> None:
    if _has_table("issuer_accounts"):
        op.drop_table("issuer_accounts")

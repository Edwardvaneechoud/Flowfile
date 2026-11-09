"""Let a compute backends row be filled by a controller instead of added by hand.

A filled row names the backend's id at the controller that filled it (``remote_id``; a hub's compute node id) and
the local user whose account the row is for (``account_user_id``; their session at the controller mints the tokens
the backend signs with); it stores no token of its own, so ``token`` becomes nullable. Rows added by hand keep their
token and leave both new columns NULL.

Revision ID: 039
Revises: 038
Create Date: 2026-11-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "039"
down_revision: str | None = "038"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _columns() -> dict[str, dict]:
    return {c["name"]: c for c in inspect(op.get_bind()).get_columns("compute_backends")}


def upgrade() -> None:
    columns = _columns()
    if columns["token"]["nullable"] and "remote_id" in columns and "account_user_id" in columns:
        return
    with op.batch_alter_table("compute_backends") as batch:
        if not columns["token"]["nullable"]:
            batch.alter_column("token", existing_type=sa.Text(), nullable=True)
        if "remote_id" not in columns:
            batch.add_column(sa.Column("remote_id", sa.Integer(), nullable=True))
        if "account_user_id" not in columns:
            batch.add_column(sa.Column("account_user_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    columns = _columns()
    # A hub-filled row has no token, which the column cannot hold again once it is NOT NULL.
    op.execute(sa.text("DELETE FROM compute_backends WHERE token IS NULL"))
    with op.batch_alter_table("compute_backends") as batch:
        if "account_user_id" in columns:
            batch.drop_column("account_user_id")
        if "remote_id" in columns:
            batch.drop_column("remote_id")
        if columns["token"]["nullable"]:
            batch.alter_column("token", existing_type=sa.Text(), nullable=False)

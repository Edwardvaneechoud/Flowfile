"""Let a compute backends row be filled from a hub instead of added by hand.

A hub-filled row names the hub's node (``hub_node_id``) and the local user whose hub session mints the tokens it
signs with (``hub_user_id``); it stores no token of its own, so ``token`` becomes nullable. Rows added by hand keep
their token and leave both new columns NULL.

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
    if columns["token"]["nullable"] and "hub_node_id" in columns and "hub_user_id" in columns:
        return
    with op.batch_alter_table("compute_backends") as batch:
        if not columns["token"]["nullable"]:
            batch.alter_column("token", existing_type=sa.Text(), nullable=True)
        if "hub_node_id" not in columns:
            batch.add_column(sa.Column("hub_node_id", sa.Integer(), nullable=True))
        if "hub_user_id" not in columns:
            batch.add_column(sa.Column("hub_user_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    columns = _columns()
    # A hub-filled row has no token, which the column cannot hold again once it is NOT NULL.
    op.execute(sa.text("DELETE FROM compute_backends WHERE token IS NULL"))
    with op.batch_alter_table("compute_backends") as batch:
        if "hub_user_id" in columns:
            batch.drop_column("hub_user_id")
        if "hub_node_id" in columns:
            batch.drop_column("hub_node_id")
        if columns["token"]["nullable"]:
            batch.alter_column("token", existing_type=sa.Text(), nullable=False)

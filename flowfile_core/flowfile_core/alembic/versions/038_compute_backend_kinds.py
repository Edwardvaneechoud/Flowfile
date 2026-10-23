"""Give each configured compute backend a kind, so a row can be a Polars cluster beside a worker.

``kind`` is ``worker`` for every existing row and the default; ``polars-cloud`` names a Polars cluster's scheduler in
``url`` and has no token, so ``token`` becomes nullable. ``settings`` holds how a kind reaches its compute as JSON text
and ``secret`` a secret the kind needs beyond a worker's token, as a ``$ffsec$`` secret-manager token; both NULL for a
worker. A downgrade deletes every row that is not a worker's, since the older schema cannot hold one.

Revision ID: 038
Revises: 037
Create Date: 2026-10-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "038"
down_revision: str | None = "037"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _columns(table: str) -> dict[str, dict]:
    return {c["name"]: c for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    columns = _columns("compute_backends")
    with op.batch_alter_table("compute_backends") as batch:
        if "kind" not in columns:
            batch.add_column(sa.Column("kind", sa.String(), nullable=False, server_default="worker"))
        if "settings" not in columns:
            batch.add_column(sa.Column("settings", sa.Text(), nullable=True))
        if "secret" not in columns:
            batch.add_column(sa.Column("secret", sa.Text(), nullable=True))
        if not columns["token"]["nullable"]:
            batch.alter_column("token", existing_type=sa.Text(), nullable=True)


def downgrade() -> None:
    columns = _columns("compute_backends")
    if "kind" in columns:
        op.execute(sa.text("DELETE FROM compute_backends WHERE kind != 'worker' OR token IS NULL"))
    with op.batch_alter_table("compute_backends") as batch:
        for name in ("secret", "settings", "kind"):
            if name in columns:
                batch.drop_column(name)
        batch.alter_column("token", existing_type=sa.Text(), nullable=False)

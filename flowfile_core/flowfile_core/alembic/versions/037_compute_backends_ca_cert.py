"""Add the CA certificate core trusts for a configured worker's TLS certificate.

``ca_cert`` is the PEM of the certificate authority that signed the worker's certificate, or a self-signed
certificate itself, for a worker whose certificate is not public; NULL for a public certificate or a plain
``http://`` worker.

Revision ID: 037
Revises: 036
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "037"
down_revision: str | None = "036"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if not _has_column("compute_backends", "ca_cert"):
        op.add_column("compute_backends", sa.Column("ca_cert", sa.Text(), nullable=True))


def downgrade() -> None:
    if _has_column("compute_backends", "ca_cert"):
        with op.batch_alter_table("compute_backends") as batch:
            batch.drop_column("ca_cert")

"""Give each kernel a token of its own.

A kernel signs its calls to core with a token minted when it starts. ``token_id`` names the token of the kernel's
current run; a stopped kernel keeps it null, which is what every existing kernel gets, so a kernel that was running
across the upgrade gets its token at its next cell or start.

Revision ID: 045
Revises: 044
Create Date: 2026-11-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "045"
down_revision: str | None = "044"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "kernels"
_COLUMN = "token_id"


def _has_column() -> bool:
    return _COLUMN in {c["name"] for c in inspect(op.get_bind()).get_columns(_TABLE)}


def upgrade() -> None:
    if not _has_column():
        with op.batch_alter_table(_TABLE) as batch:
            batch.add_column(sa.Column(_COLUMN, sa.String(), nullable=True))


def downgrade() -> None:
    if _has_column():
        with op.batch_alter_table(_TABLE) as batch:
            batch.drop_column(_COLUMN)

"""Add a stable uuid to flow schedules.

A schedule had only its integer primary key, so a project's schedule file could not name one and an
import replaced every schedule of a flow, losing its notification rules and run state. With
``schedule_uuid`` written as each entry's ``id``, an import updates the row it names in place.

Revision ID: 041
Revises: 040
Create Date: 2026-11-12
"""

import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "041"
down_revision: str | None = "040"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "flow_schedules"
_COLUMN = "schedule_uuid"
_INDEX = f"ix_{_TABLE}_{_COLUMN}"


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def upgrade() -> None:
    if _has_column(_TABLE, _COLUMN):
        return
    with op.batch_alter_table(_TABLE) as batch:
        batch.add_column(sa.Column(_COLUMN, sa.String(length=36), nullable=True))
    bind = op.get_bind()
    rows = bind.execute(sa.text(f"SELECT id FROM {_TABLE} WHERE {_COLUMN} IS NULL")).fetchall()
    for (row_id,) in rows:
        bind.execute(
            sa.text(f"UPDATE {_TABLE} SET {_COLUMN} = :value WHERE id = :id"),
            {"value": str(uuid.uuid4()), "id": row_id},
        )
    op.create_index(_INDEX, _TABLE, [_COLUMN], unique=True)


def downgrade() -> None:
    if _has_column(_TABLE, _COLUMN):
        op.drop_index(_INDEX, table_name=_TABLE)
        with op.batch_alter_table(_TABLE) as batch:
            batch.drop_column(_COLUMN)

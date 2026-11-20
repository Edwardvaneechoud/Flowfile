"""Let a chart belong to a dashboard.

A chart made from inside a dashboard is that dashboard's: it stays out of the chart library unless asked
for and goes when its dashboard goes or stops using it. ``dashboard_id`` names the dashboard; a chart of
its own keeps it null, which is what every existing chart gets.

Revision ID: 043
Revises: 042
Create Date: 2026-11-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "043"
down_revision: str | None = "042"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "catalog_visualizations"
_COLUMN = "dashboard_id"
_INDEX = f"ix_{_TABLE}_{_COLUMN}"
_FOREIGN_KEY = f"fk_{_TABLE}_{_COLUMN}"


def _has_column() -> bool:
    return _COLUMN in {c["name"] for c in inspect(op.get_bind()).get_columns(_TABLE)}


def _has_index() -> bool:
    return _INDEX in {i["name"] for i in inspect(op.get_bind()).get_indexes(_TABLE)}


def upgrade() -> None:
    if not _has_column():
        with op.batch_alter_table(_TABLE) as batch:
            batch.add_column(sa.Column(_COLUMN, sa.Integer(), nullable=True))
            # SQLite's batch mode takes only a named constraint.
            batch.create_foreign_key(_FOREIGN_KEY, "catalog_dashboards", [_COLUMN], ["id"], ondelete="SET NULL")
    if not _has_index():
        op.create_index(_INDEX, _TABLE, [_COLUMN])


def downgrade() -> None:
    if not _has_column():
        return
    if _has_index():
        op.drop_index(_INDEX, table_name=_TABLE)
    with op.batch_alter_table(_TABLE) as batch:
        batch.drop_constraint(_FOREIGN_KEY, type_="foreignkey")
        batch.drop_column(_COLUMN)

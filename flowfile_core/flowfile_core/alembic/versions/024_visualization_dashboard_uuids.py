"""Add stable uuids to catalog visualizations and dashboards.

Visualizations and dashboards have only machine-local integer primary keys, which don't
round-trip to another machine. A stable per-row uuid (mirroring ``FlowRegistration.flow_uuid``)
gives the git-backed project projection a portable identity to key them by, and lets a dashboard
tile reference its visualization portably (``viz_uuid`` instead of the local ``viz_id``).

Revision ID: 024
Revises: 023
Create Date: 2026-06-16
"""

import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "024"
down_revision: str | None = "023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _has_column(table: str, column: str) -> bool:
    return column in {c["name"] for c in inspect(op.get_bind()).get_columns(table)}


def _add_uuid_column(table: str, column: str) -> None:
    if _has_column(table, column):
        return
    with op.batch_alter_table(table) as batch:
        batch.add_column(sa.Column(column, sa.String(length=36), nullable=True))
    bind = op.get_bind()
    rows = bind.execute(sa.text(f"SELECT id FROM {table} WHERE {column} IS NULL")).fetchall()
    for (row_id,) in rows:
        bind.execute(
            sa.text(f"UPDATE {table} SET {column} = :value WHERE id = :id"),
            {"value": str(uuid.uuid4()), "id": row_id},
        )
    op.create_index(f"ix_{table}_{column}", table, [column], unique=True)


def upgrade() -> None:
    _add_uuid_column("catalog_visualizations", "viz_uuid")
    _add_uuid_column("catalog_dashboards", "dashboard_uuid")


def downgrade() -> None:
    for table, column in (("catalog_visualizations", "viz_uuid"), ("catalog_dashboards", "dashboard_uuid")):
        if _has_column(table, column):
            op.drop_index(f"ix_{table}_{column}", table_name=table)
            with op.batch_alter_table(table) as batch:
                batch.drop_column(column)

"""Add output_schema JSON column to global_artifacts.

Used by the data_science_predict node to look up an artefact's output schema
without loading the pickle, so downstream nodes can resolve their schema
lazily.

Revision ID: 006
Revises: 005
Create Date: 2026-04-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "006"
down_revision: str | None = "005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("global_artifacts", sa.Column("output_schema", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("global_artifacts", "output_schema")

"""Persist SharePoint incremental read checkpoints outside saved flows."""

import sqlalchemy as sa
from alembic import op

revision = "050"
down_revision = "049"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "sharepoint_delta_cursors",
        sa.Column("scope", sa.String(64), primary_key=True),
        sa.Column("state", sa.Text(), nullable=False),
    )


def downgrade():
    op.drop_table("sharepoint_delta_cursors")

"""Record explicit delegated Excel preview consent."""

import sqlalchemy as sa
from alembic import op

revision = "048"
down_revision = "047"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "sharepoint_connections",
        sa.Column("excel_column_preview", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade():
    op.drop_column("sharepoint_connections", "excel_column_preview")

"""Add saved SharePoint Online connections."""

import sqlalchemy as sa
from alembic import op

revision = "046"
down_revision = "045"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sharepoint_connections",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("connection_name", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("client_id", sa.String(), nullable=False),
        sa.Column("default_site_url", sa.String()),
        sa.Column("auth_method", sa.String(), nullable=False),
        sa.Column("credential_secret_id", sa.Integer(), sa.ForeignKey("secrets.id"), nullable=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("sharepoint_connections")

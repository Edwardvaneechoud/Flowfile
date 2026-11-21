"""Prevent SQLite from reassigning deleted SharePoint connection IDs."""

from alembic import op

revision = "047"
down_revision = "046"
branch_labels = None
depends_on = None


def _set_autoincrement(enabled: bool) -> None:
    if op.get_bind().dialect.name == "sqlite":
        with op.batch_alter_table(
            "sharepoint_connections", recreate="always", table_kwargs={"sqlite_autoincrement": enabled}
        ):
            pass


def upgrade() -> None:
    _set_autoincrement(True)


def downgrade() -> None:
    _set_autoincrement(False)

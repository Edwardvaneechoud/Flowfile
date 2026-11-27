"""Add the semantic ledger (glosslayer's ``gloss_*`` tables).

glosslayer keeps its own schema version in ``gloss_meta`` and a ladder of DDL steps, so this revision only hands
the connection to ``glosslayer.schema.upgrade``, which creates the tables in an empty database and otherwise brings
them to the installed version. It is idempotent, as 022 is. A later glosslayer whose schema moved needs a new
revision that calls ``upgrade`` again.

Revision ID: 046
Revises: 045
Create Date: 2026-11-24
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import inspect

revision: str = "046"
down_revision: str | None = "045"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SQLITE_VIRTUAL_TABLES = ("gloss_search_vocab", "gloss_search_fts")


def upgrade() -> None:
    import glosslayer.schema

    glosslayer.schema.upgrade(op.get_bind())


def downgrade() -> None:
    """Drop every ``gloss_`` table; SQLite's FTS5 virtual tables first, since their shadow tables go with them."""
    import glosslayer.schema

    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        for name in _SQLITE_VIRTUAL_TABLES:
            bind.exec_driver_sql(f"DROP TABLE IF EXISTS {name}")
    glosslayer.schema.metadata().drop_all(bind, checkfirst=True)
    for name in inspect(bind).get_table_names():
        if glosslayer.schema.is_gloss_table(name):
            op.drop_table(name)

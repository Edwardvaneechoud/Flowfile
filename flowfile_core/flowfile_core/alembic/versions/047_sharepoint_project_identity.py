"""Give SharePoint connections a portable, owner-scoped project identity."""

from uuid import uuid4

import sqlalchemy as sa
from alembic import op

revision = "047"
down_revision = "046"
branch_labels = None
depends_on = None


def _sequence():
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return bind.scalar(sa.text("SELECT seq FROM sqlite_sequence WHERE name = 'sharepoint_connections'"))
    return None


def _restore_sequence(seq):
    if seq is not None:
        bind = op.get_bind()
        bind.execute(sa.text("DELETE FROM sqlite_sequence WHERE name = 'sharepoint_connections'"))
        bind.execute(
            sa.text("INSERT INTO sqlite_sequence (name, seq) VALUES ('sharepoint_connections', :seq)"), {"seq": seq}
        )


def upgrade():
    seq = _sequence()
    op.add_column("sharepoint_connections", sa.Column("connection_uuid", sa.String(36), nullable=True))
    table = sa.table("sharepoint_connections", sa.column("id"), sa.column("connection_uuid"))
    bind = op.get_bind()
    for row_id in bind.scalars(sa.select(table.c.id)).all():
        bind.execute(table.update().where(table.c.id == row_id).values(connection_uuid=str(uuid4())))
    with op.batch_alter_table("sharepoint_connections", table_kwargs={"sqlite_autoincrement": True}) as batch:
        batch.alter_column("connection_uuid", existing_type=sa.String(36), nullable=False)
        batch.create_unique_constraint("uq_sharepoint_owner_uuid", ["user_id", "connection_uuid"])
    _restore_sequence(seq)


def downgrade():
    seq = _sequence()
    with op.batch_alter_table("sharepoint_connections", table_kwargs={"sqlite_autoincrement": True}) as batch:
        batch.drop_constraint("uq_sharepoint_owner_uuid", type_="unique")
        batch.drop_column("connection_uuid")
    _restore_sequence(seq)

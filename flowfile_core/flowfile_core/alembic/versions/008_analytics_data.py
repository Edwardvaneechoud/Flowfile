"""Analytics data: add google_analytics_connections + schema drift cleanup.

Revision ID: 008
Revises: 007
Create Date: 2026-04-22 17:49:55.533772+00:00
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "008"
down_revision: str | None = "007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Tables whose PK id columns were marked index=True in the models but never
# had matching CREATE INDEX emitted. Adding the missing indexes so migrations
# and models agree. (Note: PKs are already indexed implicitly; these are
# redundant but harmless, and keeping them aligns with the model definitions.)
_TABLES_NEEDING_ID_INDEX: tuple[str, ...] = (
    "catalog_namespaces",
    "catalog_table_read_links",
    "catalog_tables",
    "cloud_storage_connections",
    "cloud_storage_permissions",
    "database_connections",
    "flow_favorites",
    "flow_follows",
    "flow_registrations",
    "flow_runs",
    "flow_schedules",
    "global_artifacts",
    "kafka_connections",
    "schedule_trigger_tables",
    "secrets",
    "table_favorites",
    "users",
)

# Named explicitly because batch mode (SQLite) requires a concrete constraint
# name — autogenerate left it as None, which fails at apply time.
_CATALOG_TABLES_PRODUCER_FK = (
    "fk_catalog_tables_producer_registration_id_flow_registrations"
)


def upgrade() -> None:
    # --- Primary change: new table for GA4 OAuth connections -------------
    op.create_table(
        "google_analytics_connections",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("connection_name", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("default_property_id", sa.String(), nullable=True),
        sa.Column("oauth_user_email", sa.String(), nullable=True),
        sa.Column("credential_secret_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["credential_secret_id"], ["secrets.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("google_analytics_connections") as batch_op:
        batch_op.create_index(
            "ix_google_analytics_connections_connection_name",
            ["connection_name"],
            unique=False,
        )
        batch_op.create_index(
            "ix_google_analytics_connections_id", ["id"], unique=False
        )

    # --- Drift fix: add missing ix_<table>_id indexes --------------------
    for table_name in _TABLES_NEEDING_ID_INDEX:
        with op.batch_alter_table(table_name) as batch_op:
            batch_op.create_index(f"ix_{table_name}_id", ["id"], unique=False)

    # --- Drift fix: FK from catalog_tables.producer_registration_id to
    # flow_registrations.id. Model declares it; DB was missing it.
    with op.batch_alter_table("catalog_tables") as batch_op:
        batch_op.create_foreign_key(
            _CATALOG_TABLES_PRODUCER_FK,
            "flow_registrations",
            ["producer_registration_id"],
            ["id"],
        )

    # --- Drift fix: drop obsolete global_artifacts.output_schema column --
    # Removed from the model; dropping here so schema matches. Conditional
    # because the column was never created by an earlier migration — only
    # legacy DBs that pre-date the Alembic baseline carry it.
    inspector = sa.inspect(op.get_bind())
    if "output_schema" in {c["name"] for c in inspector.get_columns("global_artifacts")}:
        with op.batch_alter_table("global_artifacts") as batch_op:
            batch_op.drop_column("output_schema")


def downgrade() -> None:
    # Reverse in opposite order.
    with op.batch_alter_table("global_artifacts") as batch_op:
        batch_op.add_column(sa.Column("output_schema", sa.TEXT(), nullable=True))

    with op.batch_alter_table("catalog_tables") as batch_op:
        batch_op.drop_constraint(_CATALOG_TABLES_PRODUCER_FK, type_="foreignkey")

    for table_name in reversed(_TABLES_NEEDING_ID_INDEX):
        with op.batch_alter_table(table_name) as batch_op:
            batch_op.drop_index(f"ix_{table_name}_id")

    with op.batch_alter_table("google_analytics_connections") as batch_op:
        batch_op.drop_index("ix_google_analytics_connections_id")
        batch_op.drop_index("ix_google_analytics_connections_connection_name")
    op.drop_table("google_analytics_connections")

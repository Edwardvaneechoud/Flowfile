"""Initial schema baseline.

Revision ID: 001
Revises: None
Create Date: 2026-04-07
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- users (no FK dependencies) ---
    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("username", sa.String, unique=True, index=True),
        sa.Column("email", sa.String, unique=True, index=True),
        sa.Column("full_name", sa.String),
        sa.Column("hashed_password", sa.String),
        sa.Column("disabled", sa.Boolean, default=False),
        sa.Column("is_admin", sa.Boolean, default=False),
        sa.Column("must_change_password", sa.Boolean, default=True),
    )

    # --- secrets ---
    op.create_table(
        "secrets",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String, index=True),
        sa.Column("encrypted_value", sa.Text),
        sa.Column("iv", sa.String),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
    )

    # --- database_connections ---
    op.create_table(
        "database_connections",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("connection_name", sa.String, index=True),
        sa.Column("database_type", sa.String),
        sa.Column("username", sa.String),
        sa.Column("host", sa.String),
        sa.Column("port", sa.Integer),
        sa.Column("database", sa.String, default=None),
        sa.Column("ssl_enabled", sa.Boolean, default=False),
        sa.Column("password_id", sa.Integer, sa.ForeignKey("secrets.id")),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id")),
    )

    # --- cloud_storage_connections ---
    op.create_table(
        "cloud_storage_connections",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("connection_name", sa.String, index=True, nullable=False),
        sa.Column("storage_type", sa.String, nullable=False),
        sa.Column("auth_method", sa.String, nullable=False),
        # AWS S3
        sa.Column("aws_region", sa.String, nullable=True),
        sa.Column("aws_access_key_id", sa.String, nullable=True),
        sa.Column("aws_secret_access_key_id", sa.Integer, sa.ForeignKey("secrets.id"), nullable=True),
        sa.Column("aws_session_token_id", sa.Integer, sa.ForeignKey("secrets.id"), nullable=True),
        sa.Column("aws_role_arn", sa.String, nullable=True),
        sa.Column("aws_allow_unsafe_html", sa.Boolean, nullable=True),
        # Azure ADLS
        sa.Column("azure_account_name", sa.String, nullable=True),
        sa.Column("azure_account_key_id", sa.Integer, sa.ForeignKey("secrets.id"), nullable=True),
        sa.Column("azure_tenant_id", sa.String, nullable=True),
        sa.Column("azure_client_id", sa.String, nullable=True),
        sa.Column("azure_client_secret_id", sa.Integer, sa.ForeignKey("secrets.id"), nullable=True),
        sa.Column("azure_sas_token_id", sa.Integer, sa.ForeignKey("secrets.id"), nullable=True),
        # Google Cloud Storage
        sa.Column("gcs_service_account_key_id", sa.Integer, sa.ForeignKey("secrets.id"), nullable=True),
        sa.Column("gcs_project_id", sa.String, nullable=True),
        # Common
        sa.Column("endpoint_url", sa.String, nullable=True),
        sa.Column("extra_config", sa.Text, nullable=True),
        sa.Column("verify_ssl", sa.Boolean, default=True),
        # Metadata
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )

    # --- cloud_storage_permissions ---
    op.create_table(
        "cloud_storage_permissions",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("connection_id", sa.Integer, sa.ForeignKey("cloud_storage_connections.id"), nullable=False),
        sa.Column("resource_path", sa.String, nullable=False),
        sa.Column("can_read", sa.Boolean, default=True),
        sa.Column("can_write", sa.Boolean, default=False),
        sa.Column("can_delete", sa.Boolean, default=False),
        sa.Column("can_list", sa.Boolean, default=True),
    )

    # --- kernels ---
    op.create_table(
        "kernels",
        sa.Column("id", sa.String, primary_key=True, index=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("packages", sa.Text, default="[]"),
        sa.Column("cpu_cores", sa.Float, default=2.0),
        sa.Column("memory_gb", sa.Float, default=4.0),
        sa.Column("gpu", sa.Boolean, default=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )

    # --- catalog_namespaces (self-referential FK) ---
    op.create_table(
        "catalog_namespaces",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String, nullable=False, index=True),
        sa.Column("parent_id", sa.Integer, sa.ForeignKey("catalog_namespaces.id"), nullable=True),
        sa.Column("level", sa.Integer, nullable=False, default=0),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("name", "parent_id", name="uq_namespace_name_parent"),
    )

    # --- flow_registrations ---
    op.create_table(
        "flow_registrations",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String, nullable=False, index=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("flow_path", sa.String, nullable=False),
        sa.Column("namespace_id", sa.Integer, sa.ForeignKey("catalog_namespaces.id"), nullable=True),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )

    # --- flow_schedules (trigger_table_id FK to catalog_tables is nullable; catalog_tables created later) ---
    op.create_table(
        "flow_schedules",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("registration_id", sa.Integer, sa.ForeignKey("flow_registrations.id"), nullable=False),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("enabled", sa.Boolean, default=True, nullable=False),
        sa.Column("name", sa.String, nullable=True),
        sa.Column("description", sa.String, nullable=True),
        sa.Column("schedule_type", sa.String, nullable=False),
        sa.Column("interval_seconds", sa.Integer, nullable=True),
        sa.Column("trigger_table_id", sa.Integer, nullable=True),  # FK added after catalog_tables
        sa.Column("last_triggered_at", sa.DateTime, nullable=True),
        sa.Column("last_trigger_table_updated_at", sa.DateTime, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )

    # --- flow_runs ---
    op.create_table(
        "flow_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("registration_id", sa.Integer, sa.ForeignKey("flow_registrations.id"), nullable=True),
        sa.Column("flow_name", sa.String, nullable=False),
        sa.Column("flow_path", sa.String, nullable=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("started_at", sa.DateTime, nullable=False),
        sa.Column("ended_at", sa.DateTime, nullable=True),
        sa.Column("success", sa.Boolean, nullable=True),
        sa.Column("nodes_completed", sa.Integer, default=0),
        sa.Column("number_of_nodes", sa.Integer, default=0),
        sa.Column("duration_seconds", sa.Float, nullable=True),
        sa.Column("run_type", sa.String, nullable=False, server_default="in_designer_run"),
        sa.Column("pid", sa.Integer, nullable=True),
        sa.Column("schedule_id", sa.Integer, sa.ForeignKey("flow_schedules.id"), nullable=True),
        sa.Column("flow_snapshot", sa.Text, nullable=True),
        sa.Column("node_results_json", sa.Text, nullable=True),
    )

    # --- flow_favorites ---
    op.create_table(
        "flow_favorites",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("registration_id", sa.Integer, sa.ForeignKey("flow_registrations.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("user_id", "registration_id", name="uq_user_favorite"),
    )

    # --- flow_follows ---
    op.create_table(
        "flow_follows",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("registration_id", sa.Integer, sa.ForeignKey("flow_registrations.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("user_id", "registration_id", name="uq_user_follow"),
    )

    # --- catalog_tables ---
    op.create_table(
        "catalog_tables",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String, nullable=False, index=True),
        sa.Column("namespace_id", sa.Integer, sa.ForeignKey("catalog_namespaces.id"), nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("file_path", sa.String, nullable=False),
        sa.Column("storage_format", sa.String, nullable=False, server_default="delta"),
        sa.Column("schema_json", sa.Text, nullable=True),
        sa.Column("row_count", sa.Integer, nullable=True),
        sa.Column("column_count", sa.Integer, nullable=True),
        sa.Column("size_bytes", sa.Integer, nullable=True),
        sa.Column("source_registration_id", sa.Integer, sa.ForeignKey("flow_registrations.id"), nullable=True),
        sa.Column("source_run_id", sa.Integer, sa.ForeignKey("flow_runs.id"), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("name", "namespace_id", name="uq_catalog_table_name_ns"),
    )

    # Now add the FK from flow_schedules.trigger_table_id → catalog_tables.id
    with op.batch_alter_table("flow_schedules") as batch_op:
        batch_op.create_foreign_key(
            "fk_flow_schedules_trigger_table_id",
            "catalog_tables",
            ["trigger_table_id"],
            ["id"],
        )

    # --- schedule_trigger_tables ---
    op.create_table(
        "schedule_trigger_tables",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("schedule_id", sa.Integer, sa.ForeignKey("flow_schedules.id"), nullable=False),
        sa.Column("table_id", sa.Integer, sa.ForeignKey("catalog_tables.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("schedule_id", "table_id", name="uq_schedule_trigger_table"),
    )

    # --- table_favorites ---
    op.create_table(
        "table_favorites",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("table_id", sa.Integer, sa.ForeignKey("catalog_tables.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("user_id", "table_id", name="uq_user_table_favorite"),
    )

    # --- global_artifacts ---
    op.create_table(
        "global_artifacts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String, nullable=False, index=True),
        sa.Column("namespace_id", sa.Integer, sa.ForeignKey("catalog_namespaces.id"), nullable=True),
        sa.Column("version", sa.Integer, nullable=False, server_default="1"),
        sa.Column("status", sa.String, nullable=False, server_default="pending"),
        sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("source_registration_id", sa.Integer, sa.ForeignKey("flow_registrations.id"), nullable=False),
        sa.Column("source_flow_id", sa.Integer, nullable=True),
        sa.Column("source_node_id", sa.Integer, nullable=True),
        sa.Column("source_kernel_id", sa.String, nullable=True),
        sa.Column("python_type", sa.String, nullable=True),
        sa.Column("python_module", sa.String, nullable=True),
        sa.Column("serialization_format", sa.String, nullable=False),
        sa.Column("storage_key", sa.String, nullable=True),
        sa.Column("size_bytes", sa.Integer, nullable=True),
        sa.Column("sha256", sa.String, nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("tags", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("name", "namespace_id", "version", name="uq_artifact_name_ns_version"),
    )

    # --- catalog_table_read_links ---
    op.create_table(
        "catalog_table_read_links",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("table_id", sa.Integer, sa.ForeignKey("catalog_tables.id"), nullable=False),
        sa.Column("registration_id", sa.Integer, sa.ForeignKey("flow_registrations.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("table_id", "registration_id", name="uq_table_read_link"),
    )

    # --- scheduler_lock ---
    op.create_table(
        "scheduler_lock",
        sa.Column("id", sa.Integer, primary_key=True, default=1),
        sa.Column("holder_id", sa.String, nullable=False),
        sa.Column("started_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("heartbeat_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )

    # --- kafka_connections ---
    op.create_table(
        "kafka_connections",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("connection_name", sa.String, index=True, nullable=False),
        sa.Column("bootstrap_servers", sa.String, nullable=False),
        sa.Column("security_protocol", sa.String, nullable=False, server_default="PLAINTEXT"),
        sa.Column("sasl_mechanism", sa.String, nullable=True),
        sa.Column("sasl_username", sa.String, nullable=True),
        sa.Column("sasl_password_id", sa.Integer, sa.ForeignKey("secrets.id"), nullable=True),
        sa.Column("ssl_ca_location", sa.String, nullable=True),
        sa.Column("ssl_cert_location", sa.String, nullable=True),
        sa.Column("ssl_key_id", sa.Integer, sa.ForeignKey("secrets.id"), nullable=True),
        sa.Column("schema_registry_url", sa.String, nullable=True),
        sa.Column("extra_config", sa.Text, nullable=True),
        sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )

    # --- db_info ---
    op.create_table(
        "db_info",
        sa.Column("id", sa.Integer, primary_key=True, default=1),
        sa.Column("app_version", sa.String, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("db_info")
    op.drop_table("kafka_connections")
    op.drop_table("scheduler_lock")
    op.drop_table("catalog_table_read_links")
    op.drop_table("global_artifacts")
    op.drop_table("table_favorites")
    op.drop_table("schedule_trigger_tables")
    # Remove FK before dropping catalog_tables
    with op.batch_alter_table("flow_schedules") as batch_op:
        batch_op.drop_constraint("fk_flow_schedules_trigger_table_id", type_="foreignkey")
    op.drop_table("catalog_tables")
    op.drop_table("flow_follows")
    op.drop_table("flow_favorites")
    op.drop_table("flow_runs")
    op.drop_table("flow_schedules")
    op.drop_table("flow_registrations")
    op.drop_table("catalog_namespaces")
    op.drop_table("kernels")
    op.drop_table("cloud_storage_permissions")
    op.drop_table("cloud_storage_connections")
    op.drop_table("database_connections")
    op.drop_table("secrets")
    op.drop_table("users")

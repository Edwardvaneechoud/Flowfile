from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    full_name = Column(String)
    hashed_password = Column(String)
    disabled = Column(Boolean, default=False)
    is_admin = Column(Boolean, default=False)
    must_change_password = Column(Boolean, default=True)


class Secret(Base):
    __tablename__ = "secrets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    encrypted_value = Column(Text)
    iv = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))


class DatabaseConnection(Base):
    __tablename__ = "database_connections"

    id = Column(Integer, primary_key=True, index=True)
    connection_name = Column(String, index=True)
    database_type = Column(String)
    username = Column(String)
    host = Column(String)
    port = Column(Integer)
    database = Column(String, default=None)
    ssl_enabled = Column(Boolean, default=False)
    password_id = Column(Integer, ForeignKey("secrets.id"))
    user_id = Column(Integer, ForeignKey("users.id"))


class CloudStorageConnection(Base):
    __tablename__ = "cloud_storage_connections"

    id = Column(Integer, primary_key=True, index=True)
    connection_name = Column(String, index=True, nullable=False)
    storage_type = Column(String, nullable=False)  # 's3', 'adls', 'gcs'
    auth_method = Column(String, nullable=False)  # 'access_key', 'iam_role', etc.

    # AWS S3 fields
    aws_region = Column(String, nullable=True)
    aws_access_key_id = Column(String, nullable=True)
    aws_secret_access_key_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    aws_session_token_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    aws_role_arn = Column(String, nullable=True)
    aws_allow_unsafe_html = Column(Boolean, nullable=True)

    # Azure ADLS fields
    azure_account_name = Column(String, nullable=True)
    azure_account_key_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    azure_tenant_id = Column(String, nullable=True)
    azure_client_id = Column(String, nullable=True)
    azure_client_secret_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    azure_sas_token_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)

    # Common fields
    endpoint_url = Column(String, nullable=True)
    extra_config = Column(Text, nullable=True)  # JSON field for additional config
    verify_ssl = Column(Boolean, default=True)

    # Metadata
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


class CloudStoragePermission(Base):
    __tablename__ = "cloud_storage_permissions"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(Integer, ForeignKey("cloud_storage_connections.id"), nullable=False)
    resource_path = Column(String, nullable=False)  # e.g., "s3://bucket-name"
    can_read = Column(Boolean, default=True)
    can_write = Column(Boolean, default=False)
    can_delete = Column(Boolean, default=False)
    can_list = Column(Boolean, default=True)


class Kernel(Base):
    __tablename__ = "kernels"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    packages = Column(Text, default="[]")  # JSON-serialized list of package names
    cpu_cores = Column(Float, default=2.0)
    memory_gb = Column(Float, default=4.0)
    gpu = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)


# ==================== Catalog Models ====================


class CatalogNamespace(Base):
    """Unity Catalog-style hierarchical namespace: catalog -> schema -> (flows live here).

    level 0 = catalog, level 1 = schema. Flows are registered under a schema
    via FlowRegistration.namespace_id.
    """

    __tablename__ = "catalog_namespaces"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    parent_id = Column(Integer, ForeignKey("catalog_namespaces.id"), nullable=True)
    level = Column(Integer, nullable=False, default=0)  # 0=catalog, 1=schema
    description = Column(Text, nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("name", "parent_id", name="uq_namespace_name_parent"),)


class FlowRegistration(Base):
    """Persistent registry entry for a flow. Links a flow file path to catalog metadata."""

    __tablename__ = "flow_registrations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    description = Column(Text, nullable=True)
    flow_path = Column(String, nullable=False)
    namespace_id = Column(Integer, ForeignKey("catalog_namespaces.id"), nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


class FlowRun(Base):
    """Persistent record of every flow execution, with a snapshot of the flow version."""

    __tablename__ = "flow_runs"

    id = Column(Integer, primary_key=True, index=True)
    registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=True)
    flow_name = Column(String, nullable=False)
    flow_path = Column(String, nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    success = Column(Boolean, nullable=True)
    nodes_completed = Column(Integer, default=0)
    number_of_nodes = Column(Integer, default=0)
    duration_seconds = Column(Float, nullable=True)
    run_type = Column(String, nullable=False, default="full_run")
    pid = Column(Integer, nullable=True)
    schedule_id = Column(Integer, ForeignKey("flow_schedules.id"), nullable=True)
    # YAML snapshot of the flow definition at run time
    flow_snapshot = Column(Text, nullable=True)
    # JSON-serialised node step results
    node_results_json = Column(Text, nullable=True)


class FlowFavorite(Base):
    """Allows a user to bookmark/favorite a registered flow."""

    __tablename__ = "flow_favorites"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("user_id", "registration_id", name="uq_user_favorite"),)


class FlowFollow(Base):
    """Allows a user to follow/subscribe to a registered flow for updates."""

    __tablename__ = "flow_follows"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("user_id", "registration_id", name="uq_user_follow"),)


class FlowSchedule(Base):
    """Defines a schedule for automatic flow execution."""

    __tablename__ = "flow_schedules"

    id = Column(Integer, primary_key=True, index=True)
    registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    description = Column(String, nullable=True)
    schedule_type = Column(String, nullable=False)  # "interval" | "table_trigger" | "table_set_trigger"
    interval_seconds = Column(Integer, nullable=True)
    trigger_table_id = Column(Integer, ForeignKey("catalog_tables.id"), nullable=True)
    last_triggered_at = Column(DateTime, nullable=True)
    last_trigger_table_updated_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


class ScheduleTriggerTable(Base):
    """Join table linking a table_set_trigger schedule to multiple catalog tables."""

    __tablename__ = "schedule_trigger_tables"

    id = Column(Integer, primary_key=True, index=True)
    schedule_id = Column(Integer, ForeignKey("flow_schedules.id"), nullable=False)
    table_id = Column(Integer, ForeignKey("catalog_tables.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("schedule_id", "table_id", name="uq_schedule_trigger_table"),)


class TableFavorite(Base):
    """Allows a user to bookmark/favorite a catalog table."""

    __tablename__ = "table_favorites"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    table_id = Column(Integer, ForeignKey("catalog_tables.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("user_id", "table_id", name="uq_user_table_favorite"),)


# ==================== Global Artifacts ====================


class GlobalArtifact(Base):
    """Persisted Python object with versioning and lineage tracking.

    Global artifacts allow users to persist Python objects (ML models, DataFrames,
    configuration objects) from kernel code and retrieve them later—either in the
    same flow, a different flow, or a different session.
    """

    __tablename__ = "global_artifacts"

    id = Column(Integer, primary_key=True, index=True)

    # Identity
    name = Column(String, nullable=False, index=True)
    namespace_id = Column(Integer, ForeignKey("catalog_namespaces.id"), nullable=True)
    version = Column(Integer, nullable=False, default=1)

    # Status: pending (upload in progress), active (ready to use), deleted (soft delete)
    status = Column(String, nullable=False, default="pending")

    # Ownership & Lineage
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    source_registration_id = Column(
        Integer,
        ForeignKey("flow_registrations.id"),
        nullable=False,
    )
    source_flow_id = Column(Integer, nullable=True)
    source_node_id = Column(Integer, nullable=True)
    source_kernel_id = Column(String, nullable=True)

    source_registration = relationship(
        "FlowRegistration",
        backref="artifacts",
        passive_deletes=True,
    )

    # Serialization
    python_type = Column(String, nullable=True)  # e.g., "sklearn.ensemble.RandomForestClassifier"
    python_module = Column(String, nullable=True)  # e.g., "sklearn.ensemble"
    serialization_format = Column(String, nullable=False)  # parquet, joblib, pickle

    # Storage
    storage_key = Column(String, nullable=True)  # e.g., "42/model.joblib"
    size_bytes = Column(Integer, nullable=True)
    sha256 = Column(String, nullable=True)

    # Metadata
    description = Column(Text, nullable=True)
    tags = Column(Text, nullable=True)  # JSON array: ["ml", "classification"]

    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("name", "namespace_id", "version", name="uq_artifact_name_ns_version"),)


# ==================== Catalog Tables ====================


class CatalogTable(Base):
    """A materialized data table registered in the catalog.

    When a user registers a table, its data is materialized as a Parquet file
    in the catalog tables storage directory for fast, consistent reads and
    previews.
    """

    __tablename__ = "catalog_tables"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    namespace_id = Column(Integer, ForeignKey("catalog_namespaces.id"), nullable=True)
    description = Column(Text, nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Storage: path to the materialized Parquet file
    file_path = Column(String, nullable=False)

    # Schema metadata (JSON array of {name, dtype} objects)
    schema_json = Column(Text, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)
    size_bytes = Column(Integer, nullable=True)

    # Lineage: which flow produced this table
    source_registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=True)
    source_run_id = Column(Integer, ForeignKey("flow_runs.id"), nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("name", "namespace_id", name="uq_catalog_table_name_ns"),)


class CatalogTableReadLink(Base):
    """Tracks which registered flows read from which catalog tables."""

    __tablename__ = "catalog_table_read_links"

    id = Column(Integer, primary_key=True, index=True)
    table_id = Column(Integer, ForeignKey("catalog_tables.id"), nullable=False)
    registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (UniqueConstraint("table_id", "registration_id", name="uq_table_read_link"),)


class SchedulerLock(Base):
    """Advisory lock row to ensure only one scheduler instance is active.

    The table always contains at most one row (id=1). The active scheduler
    updates ``heartbeat_at`` every poll cycle. A new scheduler can only
    acquire the lock if no heartbeat has been seen for ``STALE_THRESHOLD``
    seconds, or if the row doesn't exist yet.
    """

    __tablename__ = "scheduler_lock"

    id = Column(Integer, primary_key=True, default=1)
    holder_id = Column(String, nullable=False)
    started_at = Column(DateTime, default=func.now(), nullable=False)
    heartbeat_at = Column(DateTime, default=func.now(), nullable=False)

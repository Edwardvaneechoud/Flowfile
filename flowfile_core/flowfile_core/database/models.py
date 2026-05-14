import uuid
from typing import Literal

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

RunType = Literal["in_designer_run", "scheduled", "manual", "on_demand"]

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

    # Google Cloud Storage fields
    gcs_service_account_key_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    gcs_project_id = Column(String, nullable=True)

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
    # JSON-serialized list of {name, version} for packages actually installed in
    # the derived image. Populated after bake; empty for legacy kernels until they
    # are next edited.
    resolved_packages = Column(Text, default="[]")
    cpu_cores = Column(Float, default=2.0)
    memory_gb = Column(Float, default=4.0)
    gpu = Column(Boolean, default=False)
    image_flavour = Column(String, nullable=False, default="base")
    custom_image = Column(String, nullable=True)
    # Auto-created FlowRegistration that artifacts published from this kernel's
    # interactive cells are attributed to. Lives and dies with the kernel; see
    # ``KernelManager._provision_scratch_flow`` in
    # ``flowfile_core/kernel/manager.py``. ``ON DELETE SET NULL`` so a
    # manually-removed registration leaves the kernel record intact (the
    # manager will lazily re-create on the next publish).
    scratch_flow_registration_id = Column(
        Integer,
        ForeignKey("flow_registrations.id", ondelete="SET NULL"),
        nullable=True,
    )
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
    # Stable identity that survives delete+recreate. Run history is keyed against this so
    # SQLite reusing a deleted FlowRegistration.id can never pull another flow's runs in.
    flow_uuid = Column(String(36), nullable=False, unique=True, default=lambda: str(uuid.uuid4()))
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
    # Copied from FlowRegistration.flow_uuid at run-record creation. Survives the
    # registration being deleted (registration_id is nulled) so historical runs stay
    # attributable to the original flow without leaking under a new registration.
    flow_uuid = Column(String(36), nullable=True, index=True)
    flow_name = Column(String, nullable=False)
    flow_path = Column(String, nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    success = Column(Boolean, nullable=True)
    nodes_completed = Column(Integer, default=0)
    number_of_nodes = Column(Integer, default=0)
    duration_seconds = Column(Float, nullable=True)
    run_type: RunType = Column(String, nullable=False, default="in_designer_run")
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
    name = Column(String, nullable=True)
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


class CatalogTable(Base):  # Pydantic schemas: schemas/catalog_schema.py; interface: catalog/repository.py
    """A materialized data table registered in the catalog.

    When a user registers a table, its data is materialized as a Delta table
    (or legacy Parquet file) in the catalog tables storage directory for fast,
    consistent reads and previews.
    """

    __tablename__ = "catalog_tables"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    namespace_id = Column(Integer, ForeignKey("catalog_namespaces.id"), nullable=True)
    description = Column(Text, nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Storage: path to the materialized table (Delta directory or legacy Parquet file).
    # Nullable for optimized virtual tables that have no physical storage.
    file_path = Column(String, nullable=True)
    storage_format = Column(String, nullable=False, default="delta")  # "delta" or "parquet"

    # Schema metadata (JSON array of {name, dtype} objects)
    schema_json = Column(Text, nullable=True)
    row_count = Column(Integer, nullable=True)
    column_count = Column(Integer, nullable=True)
    size_bytes = Column(Integer, nullable=True)

    # Lineage: which flow produced this table
    source_registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=True)
    source_run_id = Column(Integer, ForeignKey("flow_runs.id"), nullable=True)

    # Virtual Table fields (NULL for physical tables)
    table_type = Column(String, nullable=False, default="physical", server_default="physical")
    producer_registration_id = Column(Integer, ForeignKey("flow_registrations.id"), nullable=True)
    serialized_lazy_frame = Column(LargeBinary, nullable=True)  #  TODO Should be hashed
    is_optimized = Column(Boolean, nullable=True, default=False)
    sql_query = Column(Text, nullable=True)  # SQL definition for query-based virtual tables
    polars_plan = Column(Text, nullable=True)  # Polars explain() plan for optimized virtual tables
    source_table_versions = Column(Text, nullable=True)  # JSON list of SourceTableVersion for staleness detection

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


class CatalogVisualization(Base):
    """A saved GraphicWalker chart spec.

    Visualizations are first-class catalog entities. A viz may reference a
    catalog table (``source_type="table"``, ``catalog_table_id`` set), or
    embed a SQL query that runs against the catalog (``source_type="sql"``,
    ``sql_query`` set). ``namespace_id`` controls where the viz lives in
    the catalog hierarchy independently of any source table.
    """

    __tablename__ = "catalog_visualizations"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    chart_type = Column(String, nullable=True)
    spec_json = Column(Text, nullable=False)
    spec_gw_version = Column(String, nullable=True)

    source_type = Column(String, nullable=False, default="table")
    catalog_table_id = Column(Integer, ForeignKey("catalog_tables.id"), nullable=True, index=True)
    sql_query = Column(Text, nullable=True)

    # Base64 PNG data URL captured client-side via GraphicWalker's
    # exportChart('data-url'). Used as a static thumbnail in catalog grids
    # so we don't have to re-mount the chart per card.
    thumbnail_data_url = Column(Text, nullable=True)

    namespace_id = Column(Integer, ForeignKey("catalog_namespaces.id"), nullable=True, index=True)

    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


class CatalogDashboard(Base):
    """A saved canvas of catalog visualizations.

    A dashboard is a 2D layout of tiles, each referencing an existing
    ``CatalogVisualization`` by id. The full layout (tiles, grid metadata,
    optional dashboard-level filters) is serialised into ``layout_json``;
    no FK to visualizations because tiles are decoupled (deleted-viz tiles
    surface a placeholder at view time rather than cascading).
    """

    __tablename__ = "catalog_dashboards"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    layout_json = Column(Text, nullable=False)
    layout_version = Column(Integer, nullable=False, default=1)

    namespace_id = Column(Integer, ForeignKey("catalog_namespaces.id"), nullable=True, index=True)

    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


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


class GoogleAnalyticsConnection(Base):
    """A Google Analytics 4 connection. The OAuth refresh token is stored as
    a single encrypted Secret, referenced by ``credential_secret_id``.
    """

    __tablename__ = "google_analytics_connections"

    id = Column(Integer, primary_key=True, index=True)
    connection_name = Column(String, index=True, nullable=False)
    description = Column(Text, nullable=True)
    default_property_id = Column(String, nullable=True)
    oauth_user_email = Column(String, nullable=True)
    credential_secret_id = Column(Integer, ForeignKey("secrets.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    credential_secret = relationship("Secret", foreign_keys=[credential_secret_id], lazy="joined")


class KafkaConnection(Base):
    __tablename__ = "kafka_connections"

    id = Column(Integer, primary_key=True, index=True)
    connection_name = Column(String, index=True, nullable=False)
    bootstrap_servers = Column(String, nullable=False)
    security_protocol = Column(String, nullable=False, default="PLAINTEXT")
    sasl_mechanism = Column(String, nullable=True)
    sasl_username = Column(String, nullable=True)
    sasl_password_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    ssl_ca_location = Column(String, nullable=True)
    ssl_cert_location = Column(String, nullable=True)
    ssl_key_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    schema_registry_url = Column(String, nullable=True)
    extra_config = Column(Text, nullable=True)  # JSON string
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    sasl_password = relationship("Secret", foreign_keys=[sasl_password_id], lazy="joined")
    ssl_key = relationship("Secret", foreign_keys=[ssl_key_id], lazy="joined")


class DbInfo(Base):
    """Single-row table tracking the application version that last touched this database."""

    __tablename__ = "db_info"

    id = Column(Integer, primary_key=True, default=1)
    app_version = Column(String, nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


# ==================== AI Audit Log ====================


class AiAuditEvent(Base):
    """One row per AI-driven action (plan §9.4).

    Records what the agent did on the user's behalf so the user can inspect
    after the fact. Source for the §13 success metrics (tool-call validation
    pass rate, diff accept rate, cost-per-flow). ``flow_id`` is the in-memory
    runtime id — kept as a plain integer rather than an FK because draft flows
    don't always have a ``FlowRegistration``. ``tool_args`` is a JSON blob
    truncated to ``ai.audit.MAX_ARGS_BYTES`` before persistence.
    """

    __tablename__ = "ai_audit_events"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, nullable=False, index=True)
    flow_id = Column(Integer, nullable=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    tool_name = Column(String, nullable=False, index=True)
    tool_args = Column(Text, nullable=True)
    result_status = Column(String, nullable=False)  # "success" | "error" | "rejected"
    error = Column(Text, nullable=True)
    provider = Column(String, nullable=True)
    model = Column(String, nullable=True)
    prompt_tokens = Column(Integer, nullable=False, default=0, server_default="0")
    completion_tokens = Column(Integer, nullable=False, default=0, server_default="0")
    total_tokens = Column(Integer, nullable=False, default=0, server_default="0")
    diff_action = Column(String, nullable=True)  # "accepted" | "rejected" | None
    created_at = Column(DateTime, default=func.now(), nullable=False, index=True)


# ==================== AI BYOK Credentials ====================


class AiProviderCredential(Base):
    """One row per (user, provider) BYOK credential (plan §6.5, §8).

    Mirrors ``cloud_storage_connections``: plaintext metadata in the row,
    encrypted ``api_key`` blob via FK to the ``secrets`` table. Deletion of a
    referenced ``Secret`` row sets ``api_key_secret_id`` to NULL rather than
    cascading — a safety net so an accidental secret-row delete doesn't lose
    the user's BYOK metadata. ``delete_provider_credential`` deletes both
    rows explicitly inside a transaction.
    """

    __tablename__ = "ai_provider_credentials"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    provider = Column(String, nullable=False, index=True)  # 'anthropic', 'openai', ...
    api_key_secret_id = Column(
        Integer,
        ForeignKey("secrets.id", ondelete="SET NULL"),
        nullable=True,
    )
    api_base = Column(String, nullable=True)
    default_model = Column(String, nullable=True)
    # JSON-encoded list[str] of models the user has curated for this credential
    #. Decoded at the schema layer in flowfile_core.ai.credentials. Null
    # or an empty list both mean "no curated list — fall through to the
    # resolution order".
    models = Column(Text, nullable=True)
    last_tested_at = Column(DateTime, nullable=True)
    last_test_status = Column(String, nullable=True)  # "ok" | "error"
    last_test_error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    api_key_secret = relationship("Secret", foreign_keys=[api_key_secret_id], lazy="joined")

    __table_args__ = (UniqueConstraint("user_id", "provider", name="uq_ai_provider_per_user"),)

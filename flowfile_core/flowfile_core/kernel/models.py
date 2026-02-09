from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class KernelState(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RESTARTING = "restarting"
    IDLE = "idle"
    EXECUTING = "executing"
    ERROR = "error"


class RecoveryMode(str, Enum):
    LAZY = "lazy"
    EAGER = "eager"
    CLEAR = "clear"  # Clears all persisted artifacts on startup (destructive)


class KernelConfig(BaseModel):
    id: str
    name: str
    packages: list[str] = Field(default_factory=list)
    cpu_cores: float = 2.0
    memory_gb: float = 4.0
    gpu: bool = False
    health_timeout: int = 120
    # Persistence configuration
    persistence_enabled: bool = True
    recovery_mode: RecoveryMode = RecoveryMode.LAZY


class KernelInfo(BaseModel):
    id: str
    name: str
    state: KernelState = KernelState.STOPPED
    container_id: str | None = None
    port: int | None = None
    packages: list[str] = Field(default_factory=list)
    memory_gb: float = 4.0
    cpu_cores: float = 2.0
    gpu: bool = False
    health_timeout: int = 120
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: str | None = None
    kernel_version: str | None = None
    # Persistence configuration
    persistence_enabled: bool = True
    recovery_mode: RecoveryMode = RecoveryMode.LAZY


class DockerStatus(BaseModel):
    available: bool
    image_available: bool
    error: str | None = None


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    input_paths: dict[str, list[str]] = Field(default_factory=dict)
    output_dir: str = ""
    flow_id: int = 0
    source_registration_id: int | None = None
    log_callback_url: str = ""
    interactive: bool = False  # When True, auto-display last expression
    internal_token: str | None = None  # Core→kernel auth token for artifact API calls


class ClearNodeArtifactsRequest(BaseModel):
    """Request to selectively clear artifacts owned by specific node IDs."""

    node_ids: list[int]
    flow_id: int | None = None


class ClearNodeArtifactsResult(BaseModel):
    """Result of a selective artifact clear operation."""

    status: str = "cleared"
    removed: list[str] = Field(default_factory=list)


class DisplayOutput(BaseModel):
    """A single display output from code execution."""

    mime_type: str  # "image/png", "text/html", "text/plain"
    data: str  # base64 for images, raw HTML for text/html, plain text otherwise
    title: str = ""


class ExecuteResult(BaseModel):
    success: bool
    output_paths: list[str] = Field(default_factory=list)
    artifacts_published: list[str] = Field(default_factory=list)
    artifacts_deleted: list[str] = Field(default_factory=list)
    display_outputs: list[DisplayOutput] = Field(default_factory=list)
    stdout: str = ""
    stderr: str = ""
    error: str | None = None
    execution_time_ms: float = 0.0


# ---------------------------------------------------------------------------
# Artifact Persistence & Recovery models
# ---------------------------------------------------------------------------


class RecoveryStatus(BaseModel):
    status: str  # "pending", "recovering", "completed", "error", "disabled"
    mode: str | None = None
    recovered: list[str] = Field(default_factory=list)
    indexed: int | None = None
    errors: list[str] = Field(default_factory=list)


class ArtifactIdentifier(BaseModel):
    """Identifies a specific artifact by flow_id and name."""

    flow_id: int
    name: str


class CleanupRequest(BaseModel):
    """Request to clean up old persisted artifacts."""

    max_age_hours: float | None = None
    artifact_names: list[ArtifactIdentifier] | None = Field(
        default=None,
        description="List of specific artifacts to delete",
    )


class CleanupResult(BaseModel):
    status: str
    removed_count: int = 0


class ArtifactPersistenceInfo(BaseModel):
    """Persistence configuration and stats for a kernel."""

    enabled: bool
    recovery_mode: str = "lazy"
    kernel_id: str | None = None
    persistence_path: str | None = None
    persisted_count: int = 0
    in_memory_count: int = 0
    disk_usage_bytes: int = 0
    artifacts: dict = Field(default_factory=dict)

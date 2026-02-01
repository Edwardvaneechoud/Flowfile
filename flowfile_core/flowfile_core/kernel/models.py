from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class KernelState(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    IDLE = "idle"
    EXECUTING = "executing"
    ERROR = "error"


class RecoveryMode(str, Enum):
    LAZY = "lazy"
    EAGER = "eager"
    NONE = "none"


class KernelConfig(BaseModel):
    id: str
    name: str
    packages: list[str] = Field(default_factory=list)
    cpu_cores: float = 2.0
    memory_gb: float = 4.0
    gpu: bool = False
    health_timeout: int = 120
    persistence_enabled: bool = True
    persistence_mode: RecoveryMode = RecoveryMode.LAZY


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
    persistence_enabled: bool = True
    persistence_mode: RecoveryMode = RecoveryMode.LAZY
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: str | None = None


class DockerStatus(BaseModel):
    available: bool
    image_available: bool
    error: str | None = None


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    input_paths: dict[str, list[str]] = Field(default_factory=dict)
    output_dir: str = ""


class ExecuteResult(BaseModel):
    success: bool
    output_paths: list[str] = Field(default_factory=list)
    artifacts_published: list[str] = Field(default_factory=list)
    artifacts_deleted: list[str] = Field(default_factory=list)
    stdout: str = ""
    stderr: str = ""
    error: str | None = None
    execution_time_ms: float = 0.0


class RecoveryStatus(BaseModel):
    mode: str
    status: str
    recovered_artifacts: list[str] = Field(default_factory=list)


class CleanupRequest(BaseModel):
    max_age_hours: int = 24


class CleanupResult(BaseModel):
    removed_artifacts: list[str] = Field(default_factory=list)
    remaining_count: int = 0


class PersistenceInfo(BaseModel):
    enabled: bool
    kernel_id: str = ""
    persistence_path: str = ""
    mode: str = "lazy"
    artifact_count: int = 0
    total_disk_bytes: int = 0
    artifacts: dict[str, Any] = Field(default_factory=dict)

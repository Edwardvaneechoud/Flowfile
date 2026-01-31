from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class KernelState(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    IDLE = "idle"
    EXECUTING = "executing"
    ERROR = "error"


class KernelConfig(BaseModel):
    id: str
    name: str
    packages: list[str] = Field(default_factory=list)
    cpu_cores: float = 2.0
    memory_gb: float = 4.0
    gpu: bool = False


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
    created_at: datetime = Field(default_factory=datetime.now)
    error_message: str | None = None


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    input_paths: dict[str, str] = Field(default_factory=dict)
    output_dir: str = ""


class ExecuteResult(BaseModel):
    success: bool
    output_paths: list[str] = Field(default_factory=list)
    artifacts_published: list[str] = Field(default_factory=list)
    stdout: str = ""
    stderr: str = ""
    error: str | None = None
    execution_time_ms: float = 0.0

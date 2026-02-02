import contextlib
import io
import os
import time
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel

from kernel_runtime import __version__, flowfile_client
from kernel_runtime.artifact_store import ArtifactStore, RecoveryMode

app = FastAPI(title="FlowFile Kernel Runtime", version=__version__)

# ---------------------------------------------------------------------------
# Persistence configuration (from container environment variables)
# ---------------------------------------------------------------------------
KERNEL_ID = os.environ.get("KERNEL_ID", "")
PERSISTENCE_ENABLED = os.environ.get("PERSISTENCE_ENABLED", "false").lower() == "true"
PERSISTENCE_PATH = os.environ.get("PERSISTENCE_PATH", "/shared/artifacts")
RECOVERY_MODE_STR = os.environ.get("RECOVERY_MODE", "lazy")

_persistence_manager = None
_recovery_mode = RecoveryMode.LAZY

if PERSISTENCE_ENABLED and KERNEL_ID:
    from kernel_runtime.persistence import PersistenceManager

    _recovery_mode = RecoveryMode(RECOVERY_MODE_STR)
    _persistence_manager = PersistenceManager(PERSISTENCE_PATH, KERNEL_ID)

artifact_store = ArtifactStore(
    persistence=_persistence_manager,
    recovery_mode=_recovery_mode,
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    input_paths: dict[str, list[str]] = {}
    output_dir: str = ""
    flow_id: int = 0
    log_callback_url: str = ""


class ExecuteResponse(BaseModel):
    success: bool
    output_paths: list[str] = []
    artifacts_published: list[str] = []
    artifacts_deleted: list[str] = []
    stdout: str = ""
    stderr: str = ""
    error: str | None = None
    execution_time_ms: float = 0.0


class CleanupRequest(BaseModel):
    max_age_hours: float | None = None
    artifact_names: list[str] | None = None


# ---------------------------------------------------------------------------
# Execution endpoint
# ---------------------------------------------------------------------------


@app.post("/execute", response_model=ExecuteResponse)
async def execute(request: ExecuteRequest):
    start = time.perf_counter()
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    output_dir = request.output_dir
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    artifacts_before = set(artifact_store.list_all().keys())

    try:
        flowfile_client._set_context(
            node_id=request.node_id,
            input_paths=request.input_paths,
            output_dir=output_dir,
            artifact_store=artifact_store,
            flow_id=request.flow_id,
            log_callback_url=request.log_callback_url,
        )

        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            exec(request.code, {"flowfile": flowfile_client})  # noqa: S102

        # Collect output parquet files
        output_paths: list[str] = []
        if output_dir and Path(output_dir).exists():
            output_paths = [
                str(p) for p in sorted(Path(output_dir).glob("*.parquet"))
            ]

        artifacts_after = set(artifact_store.list_all().keys())
        new_artifacts = sorted(artifacts_after - artifacts_before)
        deleted_artifacts = sorted(artifacts_before - artifacts_after)

        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=True,
            output_paths=output_paths,
            artifacts_published=new_artifacts,
            artifacts_deleted=deleted_artifacts,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            execution_time_ms=elapsed,
        )
    except Exception as exc:
        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=False,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            error=f"{type(exc).__name__}: {exc}",
            execution_time_ms=elapsed,
        )
    finally:
        flowfile_client._clear_context()


# ---------------------------------------------------------------------------
# Artifact endpoints
# ---------------------------------------------------------------------------


@app.post("/clear")
async def clear_artifacts():
    artifact_store.clear()
    return {"status": "cleared"}


@app.get("/artifacts")
async def list_artifacts():
    return artifact_store.list_all()


# ---------------------------------------------------------------------------
# Recovery & persistence endpoints
# ---------------------------------------------------------------------------


@app.post("/recover")
async def recover_artifacts():
    """Trigger manual recovery of all persisted artifacts into memory."""
    results = artifact_store.recover_all()
    return {"status": "recovery_complete", "artifacts": results}


@app.get("/recovery-status")
async def recovery_status():
    """Get recovery status information."""
    return artifact_store.recovery_status()


@app.post("/cleanup")
async def cleanup_artifacts(request: CleanupRequest):
    """Clean up persisted artifacts from disk."""
    deleted = artifact_store.cleanup(
        max_age_hours=request.max_age_hours,
        names=request.artifact_names,
    )
    return {"status": "cleanup_complete", "deleted": deleted}


@app.get("/persistence")
async def persistence_info():
    """Get persistence information."""
    return artifact_store.persistence_info()


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    info: dict = {
        "status": "healthy",
        "version": __version__,
        "artifact_count": len(artifact_store.list_all()),
    }
    if _persistence_manager:
        info["persistence"] = {
            "enabled": True,
            "kernel_id": KERNEL_ID,
            "recovery_mode": _recovery_mode.value,
            "persisted_count": len(_persistence_manager.list_persisted()),
            "disk_usage_bytes": _persistence_manager.disk_usage(),
        }
    return info

import contextlib
import io
import logging
import os
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel

from kernel_runtime import flowfile_client
from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime.persistence import ArtifactPersistence, RecoveryMode

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Persistence configuration from environment
# ---------------------------------------------------------------------------
_kernel_id = os.environ.get("KERNEL_ID", "")
_persistence_enabled = os.environ.get("PERSISTENCE_ENABLED", "true").lower() == "true"
_persistence_path = os.environ.get("PERSISTENCE_PATH", "/shared/artifacts")
_recovery_mode_str = os.environ.get("PERSISTENCE_MODE", "lazy")

persistence: ArtifactPersistence | None = None
if _kernel_id and _persistence_enabled:
    persistence = ArtifactPersistence(_persistence_path, _kernel_id)

artifact_store = ArtifactStore(persistence=persistence)

# Parse recovery mode
try:
    _recovery_mode = RecoveryMode(_recovery_mode_str)
except ValueError:
    _recovery_mode = RecoveryMode.LAZY

_recovery_status: dict[str, Any] = {
    "mode": _recovery_mode.value,
    "recovered_artifacts": [],
    "status": "pending",
}


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    input_paths: dict[str, list[str]] = {}
    output_dir: str = ""


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
    max_age_hours: int = 24


# ---------------------------------------------------------------------------
# Startup: automatic recovery via lifespan
# ---------------------------------------------------------------------------


def _run_recovery() -> None:
    global _recovery_status

    if persistence is None:
        _recovery_status["status"] = "disabled"
        return

    if _recovery_mode == RecoveryMode.EAGER:
        recovered = artifact_store.recover_all()
        _recovery_status["recovered_artifacts"] = recovered
        _recovery_status["status"] = "completed"
        logger.info("Eager recovery loaded %d artifact(s)", len(recovered))
    elif _recovery_mode == RecoveryMode.LAZY:
        _recovery_status["status"] = "ready"
        logger.info("Lazy recovery mode: artifacts will be loaded on first access")
    elif _recovery_mode == RecoveryMode.NONE:
        persistence.clear()
        _recovery_status["status"] = "cleared"
        logger.info("Recovery mode NONE: cleared all persisted artifacts")


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    _run_recovery()
    yield


app = FastAPI(title="FlowFile Kernel Runtime", lifespan=_lifespan)


# ---------------------------------------------------------------------------
# Existing endpoints
# ---------------------------------------------------------------------------


@app.post("/execute", response_model=ExecuteResponse)
async def execute(request: ExecuteRequest):
    start = time.perf_counter()
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    output_dir = request.output_dir
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Clear artifacts previously published by this node so it starts fresh.
    # Artifacts from other nodes remain untouched — critical for cached nodes
    # whose artifacts must survive across re-runs.
    cleared = artifact_store.clear_for_node(request.node_id)
    if cleared:
        logger.info(
            "Cleared %d artifact(s) for node %d before execution: %s",
            len(cleared), request.node_id, cleared,
        )

    artifacts_before = set(artifact_store.list_all().keys())

    try:
        flowfile_client._set_context(
            node_id=request.node_id,
            input_paths=request.input_paths,
            output_dir=output_dir,
            artifact_store=artifact_store,
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


@app.post("/clear")
async def clear_artifacts():
    artifact_store.clear()
    return {"status": "cleared"}


@app.get("/artifacts")
async def list_artifacts():
    return artifact_store.list_available()


@app.get("/health")
async def health():
    available = artifact_store.list_available()
    persistence_info = {}
    if persistence is not None:
        persistence_info = {
            "persistence_enabled": True,
            "persisted_count": sum(
                1 for v in available.values() if v.get("persisted")
            ),
        }
    else:
        persistence_info = {"persistence_enabled": False}

    return {
        "status": "healthy",
        "artifact_count": len(available),
        **persistence_info,
    }


# ---------------------------------------------------------------------------
# Recovery & persistence endpoints
# ---------------------------------------------------------------------------


@app.post("/recover")
async def recover():
    """Manually trigger artifact recovery from disk."""
    if persistence is None:
        return {"status": "disabled", "recovered_artifacts": []}

    recovered = artifact_store.recover_all()
    _recovery_status["recovered_artifacts"] = list(
        set(_recovery_status["recovered_artifacts"]) | set(recovered)
    )
    _recovery_status["status"] = "completed"
    return {"status": "completed", "recovered_artifacts": recovered}


@app.get("/recovery-status")
async def recovery_status():
    """Get recovery status information."""
    return _recovery_status


@app.post("/cleanup")
async def cleanup(request: CleanupRequest):
    """Clean up old persisted artifacts."""
    if persistence is None:
        return {"removed_artifacts": [], "remaining_count": 0}

    removed = persistence.cleanup(max_age_hours=request.max_age_hours)

    # Also remove cleaned-up artifacts from memory if they were loaded
    for name in removed:
        try:
            artifact_store.delete(name)
        except KeyError:
            pass

    remaining = persistence.list_persisted()
    return {"removed_artifacts": removed, "remaining_count": len(remaining)}


@app.get("/persistence")
async def persistence_info():
    """Get persistence statistics."""
    if persistence is None:
        return {
            "enabled": False,
            "kernel_id": _kernel_id,
            "persistence_path": _persistence_path,
            "mode": _recovery_mode.value,
            "artifact_count": 0,
            "total_disk_bytes": 0,
            "artifacts": {},
        }

    stats = persistence.get_stats()
    return {
        "enabled": True,
        "mode": _recovery_mode.value,
        **stats,
    }

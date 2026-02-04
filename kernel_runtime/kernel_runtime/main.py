import ast
import contextlib
import io
import logging
import os
import time
from collections.abc import AsyncIterator
from pathlib import Path

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

from kernel_runtime import __version__, flowfile_client
from kernel_runtime.artifact_persistence import ArtifactPersistence, RecoveryMode
from kernel_runtime.artifact_store import ArtifactStore

logger = logging.getLogger(__name__)

artifact_store = ArtifactStore()

# ---------------------------------------------------------------------------
# Persistence setup (driven by environment variables)
# ---------------------------------------------------------------------------
_persistence: ArtifactPersistence | None = None
_recovery_mode = RecoveryMode.LAZY
_recovery_status: dict = {"status": "pending", "recovered": [], "errors": []}
_kernel_id: str = "default"
_persistence_path: str = "/shared/artifacts"


def _setup_persistence() -> None:
    """Initialize persistence from environment variables.

    Environment variables are read at call time (not import time) so tests
    can set them before creating the TestClient.
    """
    global _persistence, _recovery_mode, _recovery_status, _kernel_id, _persistence_path

    persistence_enabled = os.environ.get("PERSISTENCE_ENABLED", "true").lower() in ("1", "true", "yes")
    _persistence_path = os.environ.get("PERSISTENCE_PATH", "/shared/artifacts")
    _kernel_id = os.environ.get("KERNEL_ID", "default")
    recovery_mode_env = os.environ.get("RECOVERY_MODE", "lazy").lower()
    # Cleanup artifacts older than this many hours on startup (0 = disabled)
    cleanup_age_hours = float(os.environ.get("PERSISTENCE_CLEANUP_HOURS", "24"))

    if not persistence_enabled:
        _recovery_status = {"status": "disabled", "recovered": [], "errors": []}
        logger.info("Artifact persistence is disabled")
        return

    base_path = Path(_persistence_path) / _kernel_id
    _persistence = ArtifactPersistence(base_path)
    artifact_store.enable_persistence(_persistence)

    # Cleanup stale artifacts before recovery
    if cleanup_age_hours > 0:
        try:
            removed = _persistence.cleanup(max_age_hours=cleanup_age_hours)
            if removed > 0:
                logger.info(
                    "Startup cleanup: removed %d artifacts older than %.1f hours",
                    removed, cleanup_age_hours
                )
        except Exception as exc:
            logger.warning("Startup cleanup failed (continuing anyway): %s", exc)

    try:
        _recovery_mode = RecoveryMode(recovery_mode_env)
    except ValueError:
        _recovery_mode = RecoveryMode.LAZY

    if _recovery_mode == RecoveryMode.EAGER:
        _recovery_status = {"status": "recovering", "recovered": [], "errors": []}
        try:
            recovered = artifact_store.recover_all()
            _recovery_status = {
                "status": "completed",
                "mode": "eager",
                "recovered": recovered,
                "errors": [],
            }
            logger.info("Eager recovery complete: %d artifacts restored", len(recovered))
        except Exception as exc:
            _recovery_status = {
                "status": "error",
                "mode": "eager",
                "recovered": [],
                "errors": [str(exc)],
            }
            logger.error("Eager recovery failed: %s", exc)

    elif _recovery_mode == RecoveryMode.LAZY:
        count = artifact_store.build_lazy_index()
        _recovery_status = {
            "status": "completed",
            "mode": "lazy",
            "indexed": count,
            "recovered": [],
            "errors": [],
        }
        logger.info("Lazy recovery index built: %d artifacts available on disk", count)

    elif _recovery_mode == RecoveryMode.CLEAR:
        logger.warning(
            "RECOVERY_MODE=clear: Deleting ALL persisted artifacts. "
            "This is destructive and cannot be undone."
        )
        _persistence.clear()
        _recovery_status = {
            "status": "completed",
            "mode": "clear",
            "recovered": [],
            "errors": [],
        }
        logger.info("Recovery mode=clear: cleared all persisted artifacts")


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    _setup_persistence()
    yield


app = FastAPI(title="FlowFile Kernel Runtime", version=__version__, lifespan=_lifespan)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

# Matplotlib setup code to auto-capture plt.show() calls
_MATPLOTLIB_SETUP = """\
try:
    import matplotlib as _mpl
    _mpl.use('Agg')
    import matplotlib.pyplot as _plt
    _original_show = _plt.show
    def _flowfile_show(*args, **kwargs):
        import matplotlib.pyplot as __plt
        for _fig_num in __plt.get_fignums():
            flowfile.display(__plt.figure(_fig_num))
        __plt.close('all')
    _plt.show = _flowfile_show
except ImportError:
    pass
"""


def _maybe_wrap_last_expression(code: str) -> str:
    """If the last statement is a bare expression, wrap it in flowfile.display().

    This provides Jupyter-like behavior where the result of the last expression
    is automatically displayed.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return code
    if not tree.body:
        return code
    last = tree.body[-1]
    if not isinstance(last, ast.Expr):
        return code

    # Don't wrap if the expression is None, a string literal, or already a call to display/print
    if isinstance(last.value, ast.Constant) and last.value.value is None:
        return code
    if isinstance(last.value, ast.Call):
        # Check if it's already a print or display call
        func = last.value.func
        if isinstance(func, ast.Name) and func.id in ("print", "display"):
            return code
        if isinstance(func, ast.Attribute) and func.attr in ("print", "display"):
            return code

    # Use ast.get_source_segment for robust source extraction (Python 3.8+)
    last_expr_text = ast.get_source_segment(code, last)
    if last_expr_text is None:
        # Fallback if get_source_segment fails
        return code

    # Build the new code with the last expression wrapped
    lines = code.split('\n')
    prefix = '\n'.join(lines[:last.lineno - 1])
    if prefix:
        prefix += '\n'
    return prefix + f'flowfile.display({last_expr_text})\n'


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    input_paths: dict[str, list[str]] = {}
    output_dir: str = ""
    flow_id: int = 0
    log_callback_url: str = ""
    interactive: bool = False  # When True, auto-display last expression


class ClearNodeArtifactsRequest(BaseModel):
    node_ids: list[int]
    flow_id: int | None = None


class DisplayOutput(BaseModel):
    """A single display output from code execution."""
    mime_type: str  # "image/png", "text/html", "text/plain"
    data: str       # base64 for images, raw HTML for text/html, plain text otherwise
    title: str = ""


class ExecuteResponse(BaseModel):
    success: bool
    output_paths: list[str] = []
    artifacts_published: list[str] = []
    artifacts_deleted: list[str] = []
    display_outputs: list[DisplayOutput] = []
    stdout: str = ""
    stderr: str = ""
    error: str | None = None
    execution_time_ms: float = 0.0


class ArtifactIdentifier(BaseModel):
    """Identifies a specific artifact by flow_id and name."""
    flow_id: int
    name: str


class CleanupRequest(BaseModel):
    max_age_hours: float | None = None
    artifact_names: list[ArtifactIdentifier] | None = Field(
        default=None,
        description="List of specific artifacts to delete",
    )


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

    # Clear any artifacts this node previously published so re-execution
    # doesn't fail with "already exists".
    artifact_store.clear_by_node_ids({request.node_id}, flow_id=request.flow_id)

    artifacts_before = set(artifact_store.list_all(flow_id=request.flow_id).keys())

    try:
        flowfile_client._set_context(
            node_id=request.node_id,
            input_paths=request.input_paths,
            output_dir=output_dir,
            artifact_store=artifact_store,
            flow_id=request.flow_id,
            log_callback_url=request.log_callback_url,
        )

        # Reset display outputs for this execution
        flowfile_client._reset_displays()

        # Prepare execution namespace with flowfile module
        exec_globals = {"flowfile": flowfile_client}

        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            # Execute matplotlib setup to patch plt.show()
            exec(_MATPLOTLIB_SETUP, exec_globals)  # noqa: S102

            # Prepare user code - optionally wrap last expression for interactive mode
            user_code = request.code
            if request.interactive:
                user_code = _maybe_wrap_last_expression(user_code)

            # Execute user code
            exec(user_code, exec_globals)  # noqa: S102

        # Collect display outputs
        display_outputs = [
            DisplayOutput(**d) for d in flowfile_client._get_displays()
        ]

        # Collect output parquet files
        output_paths: list[str] = []
        if output_dir and Path(output_dir).exists():
            output_paths = [
                str(p) for p in sorted(Path(output_dir).glob("*.parquet"))
            ]

        artifacts_after = set(artifact_store.list_all(flow_id=request.flow_id).keys())
        new_artifacts = sorted(artifacts_after - artifacts_before)
        deleted_artifacts = sorted(artifacts_before - artifacts_after)

        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=True,
            output_paths=output_paths,
            artifacts_published=new_artifacts,
            artifacts_deleted=deleted_artifacts,
            display_outputs=display_outputs,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            execution_time_ms=elapsed,
        )
    except Exception as exc:
        # Still collect any display outputs that were generated before the error
        display_outputs = [
            DisplayOutput(**d) for d in flowfile_client._get_displays()
        ]
        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=False,
            display_outputs=display_outputs,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            error=f"{type(exc).__name__}: {exc}",
            execution_time_ms=elapsed,
        )
    finally:
        flowfile_client._clear_context()


@app.post("/clear")
async def clear_artifacts(flow_id: int | None = Query(default=None)):
    """Clear all artifacts, or only those belonging to a specific flow."""
    artifact_store.clear(flow_id=flow_id)
    return {"status": "cleared"}


@app.post("/clear_node_artifacts")
async def clear_node_artifacts(request: ClearNodeArtifactsRequest):
    """Clear only artifacts published by the specified node IDs."""
    removed = artifact_store.clear_by_node_ids(
        set(request.node_ids), flow_id=request.flow_id,
    )
    return {"status": "cleared", "removed": removed}


@app.get("/artifacts")
async def list_artifacts(flow_id: int | None = Query(default=None)):
    """List all artifacts, optionally filtered by flow_id."""
    return artifact_store.list_all(flow_id=flow_id)


@app.get("/artifacts/node/{node_id}")
async def list_node_artifacts(
    node_id: int, flow_id: int | None = Query(default=None),
):
    """List artifacts published by a specific node."""
    return artifact_store.list_by_node_id(node_id, flow_id=flow_id)


# ---------------------------------------------------------------------------
# Persistence & Recovery endpoints
# ---------------------------------------------------------------------------

@app.post("/recover")
async def recover_artifacts():
    """Trigger manual artifact recovery from disk."""
    global _recovery_status

    if _persistence is None:
        return {"status": "disabled", "message": "Persistence is not enabled"}

    _recovery_status = {"status": "recovering", "recovered": [], "errors": []}
    try:
        recovered = artifact_store.recover_all()
        _recovery_status = {
            "status": "completed",
            "mode": "manual",
            "recovered": recovered,
            "errors": [],
        }
        return _recovery_status
    except Exception as exc:
        _recovery_status = {
            "status": "error",
            "mode": "manual",
            "recovered": [],
            "errors": [str(exc)],
        }
        return _recovery_status


@app.get("/recovery-status")
async def recovery_status():
    """Return the current recovery status."""
    return _recovery_status


@app.post("/cleanup")
async def cleanup_artifacts(request: CleanupRequest):
    """Clean up old or specific persisted artifacts."""
    if _persistence is None:
        return {"status": "disabled", "removed_count": 0}

    names = None
    if request.artifact_names:
        names = [(item.flow_id, item.name) for item in request.artifact_names]

    removed_count = _persistence.cleanup(
        max_age_hours=request.max_age_hours,
        names=names,
    )
    # Rebuild lazy index after cleanup
    artifact_store.build_lazy_index()
    return {"status": "cleaned", "removed_count": removed_count}


@app.get("/persistence")
async def persistence_info():
    """Return persistence configuration and stats."""
    if _persistence is None:
        return {
            "enabled": False,
            "recovery_mode": _recovery_mode.value,
            "persisted_count": 0,
            "disk_usage_bytes": 0,
        }

    persisted = _persistence.list_persisted()
    in_memory = artifact_store.list_all()

    # Build per-artifact status
    artifact_status = {}
    for (fid, name), meta in persisted.items():
        artifact_status[name] = {
            "flow_id": fid,
            "persisted": True,
            "in_memory": name in in_memory and in_memory[name].get("in_memory", True) is not False,
        }
    for name, meta in in_memory.items():
        if name not in artifact_status:
            artifact_status[name] = {
                "flow_id": meta.get("flow_id", 0),
                "persisted": meta.get("persisted", False),
                "in_memory": True,
            }

    return {
        "enabled": True,
        "recovery_mode": _recovery_mode.value,
        "kernel_id": _kernel_id,
        "persistence_path": str(Path(_persistence_path) / _kernel_id),
        "persisted_count": len(persisted),
        "in_memory_count": len([a for a in in_memory.values() if a.get("in_memory", True) is not False]),
        "disk_usage_bytes": _persistence.disk_usage_bytes(),
        "artifacts": artifact_status,
    }


@app.get("/health")
async def health():
    persistence_status = "enabled" if _persistence is not None else "disabled"
    return {
        "status": "healthy",
        "version": __version__,
        "artifact_count": len(artifact_store.list_all()),
        "persistence": persistence_status,
        "recovery_mode": _recovery_mode.value,
    }

import ast
import asyncio
import contextlib
import ctypes
import io
import logging
import os
import signal
import threading
import time
import warnings
from collections import OrderedDict
from collections.abc import AsyncIterator
from pathlib import Path

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

from kernel_runtime import __version__, flowfile_client
from kernel_runtime.artifact_persistence import ArtifactPersistence, RecoveryMode
from kernel_runtime.artifact_store import ArtifactStore
from kernel_runtime.lsp import analysis as lsp_analysis
from kernel_runtime.lsp.models import (
    CompleteResponse,
    DiagnosticsResponse,
    HoverResponse,
    LspCapabilities,
    LspRequest,
    SignatureResponse,
)


class _DeprecatedFlowfileAlias:
    """Backwards-compat alias for the renamed ``flowfile_ctx`` kernel global.

    Forwards attribute access to the real ``flowfile_client`` module and emits
    a one-shot ``DeprecationWarning`` per execution. The kernel injects an
    instance under the legacy name ``flowfile`` so existing user code, saved
    flows, and tutorials keep working while users migrate to ``flowfile_ctx``.
    """

    __slots__ = ("_target", "_warned")

    def __init__(self, target):
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_warned", False)

    def __getattr__(self, name):
        if not self._warned:
            warnings.warn(
                "The kernel global `flowfile` is deprecated; use `flowfile_ctx` "
                "instead (e.g. `flowfile_ctx.read_input()`). The old name will "
                "be removed in a future release.",
                DeprecationWarning,
                stacklevel=3,
            )
            object.__setattr__(self, "_warned", True)
        return getattr(self._target, name)

    def __dir__(self):
        return dir(self._target)

    def __repr__(self):
        return f"<DeprecatedFlowfileAlias for {self._target!r}>"


logger = logging.getLogger(__name__)

artifact_store = ArtifactStore()

# Persistent namespace store for notebook-style execution.
# Per flow_id so variables defined in one cell are available in later cells;
# LRU eviction prevents unbounded memory growth.
_namespace_store: dict[int, dict] = {}
_namespace_access: dict[int, float] = {}  # flow_id -> last access timestamp
_MAX_NAMESPACES = int(os.environ.get("MAX_NAMESPACES", "20"))

# Display outputs from the most recent execution of each node, retrievable by
# the frontend after a flow run completes. Bounded (LRU) so base64-image / 10k-row
# table payloads can't accumulate across the kernel's lifetime.
_display_output_store: OrderedDict[tuple[int, int], list[dict]] = OrderedDict()
_MAX_DISPLAY_OUTPUTS = int(os.environ.get("MAX_DISPLAY_OUTPUTS", "200"))


def _store_display_outputs(flow_id: int, node_id: int, payload: list[dict]) -> None:
    """Store a node's display outputs, evicting the oldest entries past the cap."""
    key = (flow_id, node_id)
    _display_output_store.pop(key, None)
    _display_output_store[key] = payload
    while len(_display_output_store) > _MAX_DISPLAY_OUTPUTS:
        _display_output_store.popitem(last=False)


def _purge_display_outputs(flow_id: int) -> None:
    """Drop all stored display outputs belonging to a flow."""
    for key in [k for k in _display_output_store if k[0] == flow_id]:
        del _display_output_store[key]


def _evict_oldest_namespace() -> None:
    """Evict the least recently used namespace if at capacity."""
    if len(_namespace_store) < _MAX_NAMESPACES:
        return
    if not _namespace_access:
        return
    oldest_flow_id = min(_namespace_access, key=lambda k: _namespace_access[k])
    _namespace_store.pop(oldest_flow_id, None)
    _namespace_access.pop(oldest_flow_id, None)
    logger.debug("Evicted namespace for flow_id=%d (LRU)", oldest_flow_id)


def _get_namespace(flow_id: int) -> dict:
    """Get or create a persistent namespace for the given flow_id."""
    if flow_id not in _namespace_store:
        _evict_oldest_namespace()
        _namespace_store[flow_id] = {}
    _namespace_access[flow_id] = time.time()
    return _namespace_store[flow_id]


def _clear_namespace(flow_id: int) -> None:
    """Clear the namespace for a flow (e.g., on kernel restart)."""
    _namespace_store.pop(flow_id, None)
    _namespace_access.pop(flow_id, None)
    _purge_display_outputs(flow_id)


def _peek_namespace(flow_id: int) -> dict:
    """Read-only snapshot of a flow's namespace — does NOT create it or bump the LRU.

    The LSP endpoints read this so completion traffic on a never-run notebook can't
    allocate a namespace slot (and evict a real one). The ``dict()`` copy is a single
    GIL-atomic op, so it's safe against a concurrent ``/execute`` writing the same dict.
    """
    return dict(_namespace_store.get(flow_id, {}))


# Execution cancellation.
# When user code runs via asyncio.to_thread(), we track its thread ident so
# that /interrupt (or SIGUSR1) can inject a KeyboardInterrupt into it.
_exec_thread_id: int | None = None
_exec_lock = threading.Lock()


def _raise_in_exec_thread() -> bool:
    """Inject ``KeyboardInterrupt`` into the executing thread (if any).

    Uses ``PyThreadState_SetAsyncExc`` to set a pending async exception.
    Also sends ``SIGUSR1`` to the thread to interrupt blocking C calls
    (e.g. ``time.sleep``) so the exception is checked sooner.

    Returns ``True`` if an exec thread was found and interrupted.
    """
    with _exec_lock:
        tid = _exec_thread_id
    if tid is None:
        return False

    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(tid),
        ctypes.py_object(KeyboardInterrupt),
    )
    # Send SIGUSR1 to the thread to kick it out of any blocking syscall.
    # The signal itself is harmless (handler below ignores it when not
    # targeting the main thread), but the EINTR it causes lets the
    # thread re-enter the bytecode eval loop where the async exception fires.
    try:
        signal.pthread_kill(tid, signal.SIGUSR1)
    except (OSError, ValueError):
        pass  # thread may have already exited
    return True


def _cancel_signal_handler(signum, frame):
    """Handle SIGUSR1: interrupt the exec thread if one is running."""
    if _raise_in_exec_thread():
        logger.warning("SIGUSR1 received – interrupting execution thread")
    else:
        logger.debug("SIGUSR1 received outside execution, ignoring")


# Persistence setup (driven by environment variables)
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

    if cleanup_age_hours > 0:
        try:
            removed = _persistence.cleanup(max_age_hours=cleanup_age_hours)
            if removed > 0:
                logger.info("Startup cleanup: removed %d artifacts older than %.1f hours", removed, cleanup_age_hours)
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
            "RECOVERY_MODE=clear: Deleting ALL persisted artifacts. " "This is destructive and cannot be undone."
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
    try:
        signal.signal(signal.SIGUSR1, _cancel_signal_handler)
    except ValueError:
        pass  # not in main thread (e.g. TestClient)
    yield


app = FastAPI(title="FlowFile Kernel Runtime", version=__version__, lifespan=_lifespan)


# Request / Response models

# Matplotlib setup code to auto-capture plt.show() calls.
# Captures ``flowfile_ctx`` into a dunder-prefixed name so the hook keeps
# working even if user code later rebinds ``flowfile_ctx``.
_MATPLOTLIB_SETUP = """\
__flowfile_ctx = flowfile_ctx
try:
    import matplotlib as _mpl
    _mpl.use('Agg')
    import matplotlib.pyplot as _plt
    _original_show = _plt.show
    def _flowfile_show(*args, **kwargs):
        import matplotlib.pyplot as __plt
        for _fig_num in __plt.get_fignums():
            __flowfile_ctx.display(__plt.figure(_fig_num))
        __plt.close('all')
    _plt.show = _flowfile_show
except ImportError:
    pass
"""


def _maybe_wrap_last_expression(code: str) -> str:
    """Wrap a bare last expression in flowfile_ctx._auto_display() (Jupyter-like).

    DataFrames show their repr there; use display()/explore() for the rich table.
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

    # Don't wrap a None literal or an explicit print/display/explore call.
    if isinstance(last.value, ast.Constant) and last.value.value is None:
        return code
    if isinstance(last.value, ast.Call):
        func = last.value.func
        if isinstance(func, ast.Name) and func.id in ("print", "display", "explore"):
            return code
        if isinstance(func, ast.Attribute) and func.attr in ("print", "display", "explore"):
            return code

    last_expr_text = ast.get_source_segment(code, last)
    if last_expr_text is None:
        return code

    lines = code.split("\n")
    prefix = "\n".join(lines[: last.lineno - 1])
    if prefix:
        prefix += "\n"
    return prefix + f"flowfile_ctx._auto_display({last_expr_text})\n"


class ExecuteRequest(BaseModel):
    node_id: int
    code: str
    flow_id: int  # Required - namespaces are keyed by flow_id
    input_paths: dict[str, list[str]] = {}
    output_dir: str = ""
    flow_id: int = 0
    source_registration_id: int | None = None
    log_callback_url: str = ""
    interactive: bool = False  # When True, auto-display last expression
    internal_token: str | None = None  # Core→kernel auth token for artifact API calls


class ClearNodeArtifactsRequest(BaseModel):
    node_ids: list[int]
    flow_id: int | None = None


class DisplayOutput(BaseModel):
    """A single display output from code execution."""

    mime_type: str  # "image/png", "text/html", "text/plain"
    data: str  # base64 for images, raw HTML for text/html, plain text otherwise
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


# Existing endpoints


def _execute_sync(request: ExecuteRequest) -> ExecuteResponse:
    """Run user code synchronously (called via ``asyncio.to_thread``).

    Executing in a worker thread keeps the event loop free so that the
    ``/interrupt`` endpoint can be served while user code is running.
    """
    global _exec_thread_id

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
            source_registration_id=request.source_registration_id,
            log_callback_url=request.log_callback_url,
            internal_token=request.internal_token,
            interactive=request.interactive,
        )

        flowfile_client._reset_displays()

        exec_globals = _get_namespace(request.flow_id)

        # Always update the kernel-context reference (context changes between
        # executions). ``flowfile_ctx`` is the canonical name; ``flowfile``
        # remains as a deprecation-warning alias so legacy user code keeps
        # running. Include ``__name__`` and ``__builtins__`` so classes
        # defined in user code get ``__module__ = "__main__"`` instead of
        # ``builtins``, enabling cloudpickle to serialize them correctly.
        exec_globals["flowfile_ctx"] = flowfile_client
        exec_globals["flowfile"] = _DeprecatedFlowfileAlias(flowfile_client)
        exec_globals["__builtins__"] = __builtins__
        exec_globals["__name__"] = "__main__"

        with (
            warnings.catch_warnings(),
            contextlib.redirect_stdout(stdout_buf),
            contextlib.redirect_stderr(stderr_buf),
        ):
            # Force the default warning filter so the ``flowfile`` deprecation
            # warning is actually shown — Python's default config suppresses
            # ``DeprecationWarning`` for non-``__main__`` callers, and ``exec``'s
            # frame attribution is fragile. Scoped to user-code execution so the
            # process-wide filter state is not mutated.
            warnings.simplefilter("default", DeprecationWarning)

            exec(_MATPLOTLIB_SETUP, exec_globals)  # noqa: S102

            user_code = request.code
            if request.interactive:
                user_code = _maybe_wrap_last_expression(user_code)

            # Execute user code — track the thread so /interrupt can target it
            with _exec_lock:
                _exec_thread_id = threading.get_ident()
            try:
                exec(user_code, exec_globals)  # noqa: S102
            finally:
                with _exec_lock:
                    _exec_thread_id = None

        display_outputs = [DisplayOutput(**d) for d in flowfile_client._get_displays()]

        _store_display_outputs(request.flow_id, request.node_id, [d.model_dump() for d in display_outputs])

        output_paths: list[str] = []
        if output_dir and Path(output_dir).exists():
            output_paths = [str(p) for p in sorted(Path(output_dir).glob("*.parquet"))]

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
    except KeyboardInterrupt:
        with _exec_lock:
            _exec_thread_id = None
        display_outputs = [DisplayOutput(**d) for d in flowfile_client._get_displays()]
        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=False,
            display_outputs=display_outputs,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            error="Execution cancelled by user",
            execution_time_ms=elapsed,
        )
    except Exception as exc:
        display_outputs = [DisplayOutput(**d) for d in flowfile_client._get_displays()]
        _store_display_outputs(request.flow_id, request.node_id, [d.model_dump() for d in display_outputs])
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
        with _exec_lock:
            _exec_thread_id = None
        flowfile_client._clear_context()


@app.post("/execute", response_model=ExecuteResponse)
async def execute(request: ExecuteRequest):
    return await asyncio.to_thread(_execute_sync, request)


@app.post("/interrupt")
async def interrupt():
    """Interrupt running user code by injecting ``KeyboardInterrupt``."""
    if _raise_in_exec_thread():
        return {"status": "interrupted"}
    return {"status": "no_execution_running"}


# Code intelligence (Jedi). Runs in a worker thread so it can't stall /execute, with
# a hard per-request timeout; any failure/timeout degrades to an empty result.
_LSP_TIMEOUT_S = 2.0


@app.get("/lsp/capabilities", response_model=LspCapabilities)
async def lsp_capabilities():
    return lsp_analysis.capabilities(version=__version__)


@app.post("/lsp/complete", response_model=CompleteResponse)
async def lsp_complete(request: LspRequest):
    live = _peek_namespace(request.flow_id)
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(lsp_analysis.complete, request.code, request.line, request.column, live),
            timeout=_LSP_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        return CompleteResponse(items=[])


@app.post("/lsp/hover", response_model=HoverResponse)
async def lsp_hover(request: LspRequest):
    live = _peek_namespace(request.flow_id)
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(lsp_analysis.hover, request.code, request.line, request.column, live),
            timeout=_LSP_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        return HoverResponse(contents=None)


@app.post("/lsp/signature", response_model=SignatureResponse)
async def lsp_signature(request: LspRequest):
    live = _peek_namespace(request.flow_id)
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(lsp_analysis.signature, request.code, request.line, request.column, live),
            timeout=_LSP_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        return SignatureResponse(signatures=[], active_signature=0)


@app.post("/lsp/diagnostics", response_model=DiagnosticsResponse)
async def lsp_diagnostics(request: LspRequest):
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(lsp_analysis.diagnostics, request.code),
            timeout=_LSP_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        return DiagnosticsResponse(diagnostics=[])


@app.post("/clear")
async def clear_artifacts(flow_id: int | None = Query(default=None)):
    """Clear all artifacts, or only those belonging to a specific flow."""
    artifact_store.clear(flow_id=flow_id)
    if flow_id is not None:
        _clear_namespace(flow_id)
    else:
        _namespace_store.clear()
        _namespace_access.clear()
        _display_output_store.clear()
    return {"status": "cleared"}


@app.post("/clear_namespace")
async def clear_namespace(flow_id: int = Query(...)):
    """Clear the execution namespace for a flow (variables, imports, etc.)."""
    _clear_namespace(flow_id)
    return {"status": "cleared", "flow_id": flow_id}


@app.get("/display_outputs", response_model=list[DisplayOutput])
async def get_display_outputs(flow_id: int = Query(...), node_id: int = Query(...)):
    """Retrieve stored display outputs from the last execution of a node."""
    key = (flow_id, node_id)
    stored = _display_output_store.get(key, [])
    return [DisplayOutput(**d) for d in stored]


@app.post("/clear_node_artifacts")
async def clear_node_artifacts(request: ClearNodeArtifactsRequest):
    """Clear only artifacts published by the specified node IDs."""
    removed = artifact_store.clear_by_node_ids(
        set(request.node_ids),
        flow_id=request.flow_id,
    )
    return {"status": "cleared", "removed": removed}


@app.get("/artifacts")
async def list_artifacts(flow_id: int | None = Query(default=None)):
    """List all artifacts, optionally filtered by flow_id."""
    return artifact_store.list_all(flow_id=flow_id)


@app.get("/artifacts/node/{node_id}")
async def list_node_artifacts(
    node_id: int,
    flow_id: int | None = Query(default=None),
):
    """List artifacts published by a specific node."""
    return artifact_store.list_by_node_id(node_id, flow_id=flow_id)


# Persistence & Recovery endpoints


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

    artifact_status = {}
    for (fid, name), _meta in persisted.items():
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


class MemoryInfo(BaseModel):
    """Container memory usage information read from cgroup fs."""

    used_bytes: int = 0
    limit_bytes: int = 0
    usage_percent: float = 0.0


def _read_cgroup_memory() -> MemoryInfo:
    """Read memory usage from the Linux cgroup filesystem.

    Supports both cgroup v2 (``/sys/fs/cgroup/memory.current``) and
    cgroup v1 (``/sys/fs/cgroup/memory/memory.usage_in_bytes``).
    """
    used: int = 0
    limit: int = 0

    # cgroup v2 paths
    v2_current = Path("/sys/fs/cgroup/memory.current")
    v2_max = Path("/sys/fs/cgroup/memory.max")
    # cgroup v1 paths
    v1_current = Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    v1_max = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

    try:
        if v2_current.exists():
            used = int(v2_current.read_text().strip())
            max_text = v2_max.read_text().strip()
            limit = 0 if max_text == "max" else int(max_text)
        elif v1_current.exists():
            used = int(v1_current.read_text().strip())
            limit_text = v1_max.read_text().strip()
            limit_val = int(limit_text)
            # v1 uses a very large sentinel (PAGE_COUNTER_MAX) for "no limit"
            limit = 0 if limit_val >= (1 << 62) else limit_val
    except (OSError, ValueError) as exc:
        logger.debug("Could not read cgroup memory stats: %s", exc)

    percent = (used / limit * 100.0) if limit > 0 else 0.0
    return MemoryInfo(used_bytes=used, limit_bytes=limit, usage_percent=round(percent, 1))


@app.get("/memory", response_model=MemoryInfo)
async def memory_stats():
    """Return current container memory usage from cgroup filesystem."""
    return _read_cgroup_memory()


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

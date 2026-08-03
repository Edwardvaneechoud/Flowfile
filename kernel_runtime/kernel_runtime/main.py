import ast
import asyncio
import contextlib
import ctypes
import io
import logging
import os
import signal
import sys
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


# Rendered artifact previews (base64 PNG / HTML) from the most recent run,
# fetched on demand by Core. Kept only in-kernel; only two tiny flags ride in
# ExecuteResponse. Bounded (LRU) so blobs can't accumulate.
_artifact_preview_store: OrderedDict[tuple[int, str], dict] = OrderedDict()
_MAX_ARTIFACT_PREVIEWS = int(os.environ.get("MAX_ARTIFACT_PREVIEWS", "100"))


def _store_artifact_preview(flow_id: int, name: str, payload: dict) -> None:
    """Store an artifact preview, evicting the oldest entries past the cap."""
    key = (flow_id, name)
    _artifact_preview_store.pop(key, None)
    _artifact_preview_store[key] = payload
    while len(_artifact_preview_store) > _MAX_ARTIFACT_PREVIEWS:
        _artifact_preview_store.popitem(last=False)


def _purge_artifact_previews(flow_id: int) -> None:
    """Drop all stored artifact previews belonging to a flow."""
    for key in [k for k in _artifact_preview_store if k[0] == flow_id]:
        del _artifact_preview_store[key]


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
# More than one cell can be in flight at once: a cell blocked in an
# uninterruptible C call (e.g. time.sleep, which retries across EINTR per PEP 475)
# keeps running after it is abandoned while the next cell starts. We track every
# running execution by a monotonic generation and bind an interrupt to the exact
# generation it targeted, so a stale interrupt can never land on a later cell.
# RLock: the SIGUSR1 handler runs on the main thread and may re-enter a lock a
# caller on that thread already holds.
_exec_lock = threading.RLock()
_exec_generation = 0
_running_execs: dict[int, int] = {}  # generation -> thread ident
_interrupt_generation: int | None = None  # generation the last interrupt targeted


def _raise_in_thread(tid: int) -> None:
    """Set a pending ``KeyboardInterrupt`` in thread *tid* (no signal sent)."""
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(tid),
        ctypes.py_object(KeyboardInterrupt),
    )


def _request_interrupt() -> bool:
    """Interrupt the most recently started running cell, if any.

    Injects ``KeyboardInterrupt`` and sends a single ``SIGUSR1`` to nudge the
    thread out of a blocking syscall. The interrupt is bound to that cell's
    generation, so a later cell can never receive it. One-shot: never re-arms.
    """
    global _interrupt_generation
    with _exec_lock:
        if not _running_execs:
            return False
        _interrupt_generation = max(_running_execs)
        tid = _running_execs[_interrupt_generation]
    _raise_in_thread(tid)
    try:
        signal.pthread_kill(tid, signal.SIGUSR1)
    except (OSError, ValueError):
        pass  # thread may have already exited
    return True


def _cancel_signal_handler(signum, frame):
    """Handle SIGUSR1: re-assert the pending interrupt on the cell it was bound to.

    Only the generation an interrupt was bound to (via ``_request_interrupt``) is
    targeted, and only while that cell is still running. A stale, coalesced, or
    external signal whose target has already finished is ignored rather than
    misdirected onto a later cell. Never re-sends SIGUSR1, so no signal storm.
    """
    with _exec_lock:
        gen = _interrupt_generation
        if gen is None or gen not in _running_execs:
            return  # stale/external: bound target gone — don't misdirect onto a later cell
        tid = _running_execs[gen]
    _raise_in_thread(tid)


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


def _capture_open_figures() -> None:
    """Send any figures the cell left open to the UI, like Jupyter's inline backend.

    Looks matplotlib up in ``sys.modules`` rather than importing it, so a cell
    that never plots never pays for the import and a broken matplotlib install
    cannot surface as an error inside unrelated user code. The container sets
    ``MPLBACKEND=Agg``, so no ``matplotlib.use()`` call is needed here.
    """
    plt = sys.modules.get("matplotlib.pyplot")
    if plt is None:
        return
    try:
        rendered = flowfile_client._get_rendered_figures()
        for fig_num in plt.get_fignums():
            fig = plt.figure(fig_num)
            if fig not in rendered:
                flowfile_client.display(fig)
        plt.close("all")
    except Exception:
        logger.debug("Could not capture matplotlib figures", exc_info=True)


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
    input_paths: dict[str, list[str]] = {}
    output_dir: str = ""
    flow_id: int = 0
    source_registration_id: int | None = None
    log_callback_url: str = ""
    interactive: bool = False  # When True, auto-display last expression
    internal_token: str | None = None  # Core→kernel auth token for artifact API calls
    # artifact name -> source_node_id. None means no lineage context (no enforcement);
    # {} means lineage is known but nothing is in this node's input lineage.
    available_artifacts: dict[str, int] | None = None


class ClearNodeArtifactsRequest(BaseModel):
    node_ids: list[int]
    flow_id: int | None = None


class DisplayOutput(BaseModel):
    """A single display output from code execution."""

    mime_type: str  # "image/png", "text/html", "text/plain"
    data: str  # base64 for images, raw HTML for text/html, plain text otherwise
    title: str = ""


class PublishedArtifact(BaseModel):
    """Metadata for an in-memory artifact published during a run."""

    name: str
    type_name: str = ""
    module: str = ""
    size_bytes: int = 0
    has_preview: bool = False
    preview_mime: str | None = None


class ExecuteResponse(BaseModel):
    success: bool
    output_paths: list[str] = []
    artifacts_published: list[PublishedArtifact] = []
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
    """Register this execution and guard its whole body.

    Each cell gets a fresh generation so /interrupt targets exactly this cell and
    never a later one; any escaping ``BaseException`` (e.g. a late async
    ``KeyboardInterrupt``) becomes a clean 200 response instead of an unhandled 500.
    Runs via ``asyncio.to_thread`` so the event loop stays free.
    """
    global _exec_generation, _interrupt_generation

    start = time.perf_counter()
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    with _exec_lock:
        _exec_generation += 1
        my_gen = _exec_generation
        _running_execs[my_gen] = threading.get_ident()
    try:
        return _run_user_code(request, start, stdout_buf, stderr_buf)
    except BaseException as exc:  # noqa: BLE001 - never surface a stray interrupt as a 500
        elapsed = (time.perf_counter() - start) * 1000
        return ExecuteResponse(
            success=False,
            stdout=stdout_buf.getvalue(),
            stderr=stderr_buf.getvalue(),
            error="Execution cancelled by user"
            if isinstance(exc, KeyboardInterrupt)
            else f"{type(exc).__name__}: {exc}",
            execution_time_ms=elapsed,
        )
    finally:
        with _exec_lock:
            _running_execs.pop(my_gen, None)
            if _interrupt_generation == my_gen:
                _interrupt_generation = None


def _run_user_code(
    request: ExecuteRequest,
    start: float,
    stdout_buf: io.StringIO,
    stderr_buf: io.StringIO,
) -> ExecuteResponse:
    """Execute one cell's user code. Wrapped by ``_execute_sync``."""
    output_dir = request.output_dir
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Clear any artifacts this node previously published so re-execution
    # doesn't fail with "already exists".
    removed = artifact_store.clear_by_node_ids({request.node_id}, flow_id=request.flow_id)
    for _name in removed:
        _artifact_preview_store.pop((request.flow_id, _name), None)

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
            available_artifacts=request.available_artifacts,
        )

        flowfile_client._reset_displays()
        flowfile_client._reset_artifact_previews()
        flowfile_client._reset_deleted_artifacts()
        flowfile_client._reset_rendered_figures()

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
            # plt.show() is a harmless no-op under Agg; hide its warning.
            warnings.filterwarnings("ignore", message="FigureCanvasAgg is non-interactive", category=UserWarning)

            user_code = request.code
            if request.interactive:
                user_code = _maybe_wrap_last_expression(user_code)

            try:
                exec(user_code, exec_globals)  # noqa: S102
            finally:
                _capture_open_figures()

        display_outputs = [DisplayOutput(**d) for d in flowfile_client._get_displays()]

        _store_display_outputs(request.flow_id, request.node_id, [d.model_dump() for d in display_outputs])

        output_paths: list[str] = []
        if output_dir and Path(output_dir).exists():
            output_paths = [str(p) for p in sorted(Path(output_dir).glob("*.parquet"))]

        # Attribute artifacts by the owning node_id (stamped in store metadata),
        # never by a store-wide name diff — the store is shared across nodes on
        # the same kernel and concurrent flow execution would otherwise misattribute.
        owned = artifact_store.list_by_node_id(request.node_id, flow_id=request.flow_id)
        previews = flowfile_client._get_artifact_previews()
        for name, payload in previews.items():
            if name in owned:
                _store_artifact_preview(request.flow_id, name, payload)
        new_artifacts = [
            PublishedArtifact(
                name=name,
                type_name=meta.get("type_name", ""),
                module=meta.get("module", ""),
                size_bytes=meta.get("size_bytes", 0),
                has_preview=name in previews,
                preview_mime=previews[name]["mime_type"] if name in previews else None,
            )
            for name, meta in sorted(owned.items())
        ]
        deleted_names = flowfile_client._get_deleted_artifacts()
        deleted_artifacts = sorted(set(deleted_names) - set(owned))

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
        flowfile_client._clear_context()


@app.post("/execute", response_model=ExecuteResponse)
async def execute(request: ExecuteRequest):
    return await asyncio.to_thread(_execute_sync, request)


@app.post("/interrupt")
async def interrupt():
    """Interrupt running user code by injecting ``KeyboardInterrupt``."""
    if _request_interrupt():
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
        _purge_artifact_previews(flow_id)
    else:
        _namespace_store.clear()
        _namespace_access.clear()
        _display_output_store.clear()
        _artifact_preview_store.clear()
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


@app.get("/artifact_preview", response_model=DisplayOutput | None)
async def get_artifact_preview(flow_id: int = Query(...), name: str = Query(...)):
    """Retrieve a stored artifact preview (base64 PNG / HTML), if any."""
    stored = _artifact_preview_store.get((flow_id, name))
    return DisplayOutput(**stored) if stored else None


@app.post("/clear_node_artifacts")
async def clear_node_artifacts(request: ClearNodeArtifactsRequest):
    """Clear only artifacts published by the specified node IDs."""
    removed = artifact_store.clear_by_node_ids(
        set(request.node_ids),
        flow_id=request.flow_id,
    )
    for name in removed:
        if request.flow_id is not None:
            _artifact_preview_store.pop((request.flow_id, name), None)
        else:
            for key in [k for k in _artifact_preview_store if k[1] == name]:
                del _artifact_preview_store[key]
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

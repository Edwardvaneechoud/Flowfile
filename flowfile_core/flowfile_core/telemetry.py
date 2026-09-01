"""The one place in ``flowfile_core`` that knows telemetry exists.

Product code publishes neutral domain events on :mod:`flowfile_core.events` and
HTTP handlers just answer requests; this module is the observer that turns both
into events on :mod:`shared.telemetry`. It is wired up once, from the
composition root — :func:`install` for the FastAPI app, :func:`install_headless`
for the CLI run paths. Nothing else imports it.

Two seams, no in-handler emits:

* domain events → the ``_on_*`` subscribers below, which own every guard
  (subflow and system runs are ignored, cancels stay silent, error classes come
  from the nodes that failed in *this* run).
* HTTP requests → :data:`ROUTE_EVENTS` plus one pure-ASGI middleware, which
  emits for a mapped route only when the response succeeded.

Nothing derived from user content ever reaches an event. The run snapshot keeps
node *counts* and built-in node *type keys* (anything not shipped with Flowfile
collapses to ``"custom"``); node names, settings, file paths and error messages
are never read. Error classes are mapped through a frozen allowlist so a
user-defined exception name cannot leak either.

Import stays side-effect free: ``node_store`` is imported lazily and
``flow_graph`` never at all.
"""

from __future__ import annotations

import weakref
from typing import Any

from flowfile_core import events
from shared import telemetry as _client

SAMPLE_SOURCE_TYPES = frozenset({"manual_input", "flow_input", "external_source", "polars_lazy_frame"})
CATALOG_NODE_TYPES = frozenset({"catalog_reader", "catalog_writer"})
CUSTOM_NODE_TYPE = "custom"
OTHER_ERROR = "OtherError"
ACTIVATION_MIN_NODES = 3

ERROR_CLASS_ALLOWLIST = frozenset(
    {
        "AttributeError",
        "ConnectionError",
        "FileNotFoundError",
        "KeyError",
        "MemoryError",
        "NotImplementedError",
        "OSError",
        "PermissionError",
        "RuntimeError",
        "TimeoutError",
        "TypeError",
        "ValueError",
        "ZeroDivisionError",
        "ColumnNotFoundError",
        "ComputeError",
        "InvalidOperationError",
        "NoDataError",
        "PanicException",
        "SchemaError",
        "ShapeError",
        "ConnectError",
        "ConnectTimeout",
        "HTTPStatusError",
        "ReadTimeout",
        "IntegrityError",
        "OperationalError",
        "ProgrammingError",
        "ApiConfigError",
        "ApiExecutionError",
        "ArtifactError",
        "CatalogError",
        "CodegenError",
        "CustomNodeExecError",
        "DatabaseReadCancelledError",
        "DirectoryScanUnsupportedError",
        "KernelCodegenError",
        "KernelDependencyError",
        "KernelRequiredError",
        "NamespaceNotFoundError",
        "NoFilesMatchedError",
        "NodeNotRunError",
        "NotAuthorizedError",
        "SecretResolutionError",
        "StaleWriteError",
        "SubflowResolutionError",
        "TableNotFoundError",
        "UnknownDialectError",
        "UnsafeSQLError",
        "UnsupportedNodeError",
        "WorkerUnavailableError",
    }
)

NODE_ERROR_ATTR = "_last_exception_class"

ROUTE_EVENTS: dict[tuple[str, str], tuple[str, dict[str, Any] | None]] = {
    ("POST", "/editor/create_flow/"): ("flow_created", None),
    # The code panel's GET fires on every render, so export is confirmed by its own POST.
    ("POST", "/editor/code_to_polars/exported"): ("export_code_used", {"target": "polars"}),
    ("POST", "/editor/code_to_flowframe/exported"): ("export_code_used", {"target": "flowframe"}),
    ("GET", "/editor/code_to_project/zip"): ("export_code_used", {"target": "project_zip"}),
    ("POST", "/editor/code_to_project/save"): ("export_code_used", {"target": "project_save"}),
    ("POST", "/ai/diff/{diff_id}/accept"): ("ai_diff_accepted", None),
    ("POST", "/ai/diff/{diff_id}/reject"): ("ai_diff_rejected", None),
    ("POST", "/catalog/schedules"): ("schedule_created", None),
}

_builtin_node_types: frozenset[str] | None = None
_snapshots: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
_middleware_installed = False
_subscribed = False


def emit(event: str, props: dict[str, Any] | None = None) -> None:
    """Queue one event. Never blocks, never raises."""
    _client.emit(event, props)


def emit_once(event: str, props: dict[str, Any] | None = None) -> None:
    """Like :func:`emit`, but at most once per process."""
    _client.emit_once(event, props)


def classify_error(name: str | None) -> str:
    """Map an exception class name onto the allowlist, or ``"OtherError"``."""
    return name if name in ERROR_CLASS_ALLOWLIST else OTHER_ERROR


def _builtin_types() -> frozenset[str]:
    global _builtin_node_types
    if _builtin_node_types is None:
        from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

        _, node_dict, _ = get_all_standard_nodes()
        _builtin_node_types = frozenset(node_dict)
    return _builtin_node_types


def run_snapshot(graph: Any) -> dict[str, Any] | None:
    """Shape-only description of *graph*, taken before it runs.

    Returns ``None`` if anything at all goes wrong — telemetry may never be the
    reason a run behaves differently.
    """
    try:
        builtin = _builtin_types()
        nodes = list(graph.nodes)
        node_types: set[str] = set()
        source_types: set[str] = set()
        for node in nodes:
            node_type = getattr(node, "node_type", None)
            if not isinstance(node_type, str):
                continue
            node_types.add(node_type if node_type in builtin else CUSTOM_NODE_TYPE)
            template = getattr(node, "node_template", None)
            if template is not None and getattr(template, "input", None) == 0:
                source_types.add(node_type)
        return {
            "node_count": len(nodes),
            "node_types": sorted(node_types),
            "used_sample_data": not (source_types - SAMPLE_SOURCE_TYPES),
            "uses_catalog": bool(node_types & CATALOG_NODE_TYPES),
        }
    except Exception:
        return None


def emit_run_events(
    snapshot: dict[str, Any] | None,
    *,
    outcome: str,
    duration_seconds: float | None = None,
    error_class: str | None = None,
) -> None:
    """Emit the terminal events for one run.

    Pure and self-contained: it takes an already-made decision and turns it into
    events. Canceled runs never reach it (``flow_run_started`` was already sent).
    """
    try:
        if outcome == "failed":
            emit("flow_run_failed", {"error_class": error_class or OTHER_ERROR})
            return
        if outcome != "succeeded" or not snapshot:
            return
        node_count = int(snapshot.get("node_count", 0))
        used_sample_data = bool(snapshot.get("used_sample_data", True))
        emit(
            "flow_run_succeeded",
            {
                "node_count_bucket": _client.bucket_node_count(node_count),
                "node_types": list(snapshot.get("node_types") or []),
                "duration_bucket": _client.bucket_duration_seconds(float(duration_seconds or 0.0)),
                "used_sample_data": used_sample_data,
            },
        )
        if node_count >= ACTIVATION_MIN_NODES and not used_sample_data:
            emit_once("activation")
    except Exception:
        return


def _skip(graph: Any) -> bool:
    """Runs that are not the user's own: nested subflows and app-initiated runs."""
    return getattr(graph, "_subflow_depth", 0) > 0 or getattr(graph, "_system_run", False)


def _failed_error_class(graph: Any, run_info: Any) -> str:
    """Classified exception name of a node that failed in *this* run."""
    try:
        by_id = {getattr(node, "node_id", None): node for node in graph.nodes}
        for result in getattr(run_info, "node_step_result", None) or ():
            if result.success is not False or getattr(result, "skipped", False):
                continue
            recorded = getattr(by_id.get(result.node_id), NODE_ERROR_ATTR, None)
            if recorded:
                return classify_error(recorded)
    except Exception:
        return OTHER_ERROR
    return OTHER_ERROR


def _on_flow_run_started(graph: Any) -> None:
    if _skip(graph):
        return
    snapshot = run_snapshot(graph)
    if snapshot is not None:
        _snapshots[graph] = snapshot
    emit("flow_run_started")
    if snapshot and snapshot["uses_catalog"]:
        emit_once("catalog_used")


def _on_flow_run_finished(graph: Any, run_info: Any) -> None:
    if _skip(graph):
        return
    snapshot = _snapshots.pop(graph, None)
    if getattr(graph.flow_settings, "is_canceled", False):
        return
    start = getattr(run_info, "start_time", None)
    end = getattr(run_info, "end_time", None)
    duration = (end - start).total_seconds() if start is not None and end is not None else 0.0
    if getattr(run_info, "success", False):
        emit_run_events(snapshot, outcome="succeeded", duration_seconds=duration)
    else:
        emit_run_events(snapshot, outcome="failed", error_class=_failed_error_class(graph, run_info))


def _on_flow_run_crashed(graph: Any, error: BaseException) -> None:
    if _skip(graph):
        return
    _snapshots.pop(graph, None)
    emit_run_events(None, outcome="failed", error_class=classify_error(type(error).__name__))


def _on_kernel_exec() -> None:
    emit_once("kernel_used")


def _on_app_started() -> None:
    emit("app_started")


def _emit_for_route(scope: dict[str, Any], status: int) -> None:
    if status >= 300:
        return
    path = getattr(scope.get("route"), "path", None)
    if path is None:
        return
    mapped = ROUTE_EVENTS.get((scope.get("method", ""), path))
    if mapped is None:
        return
    event, props = mapped
    emit(event, dict(props) if props else None)


class TelemetryMiddleware:
    """Emit one event per successful request on a route in :data:`ROUTE_EVENTS`.

    Pure ASGI on purpose: ``BaseHTTPMiddleware`` wraps and buffers the response
    body, which would break the streaming ``/ai`` endpoints.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def _send(message) -> None:
            if message["type"] == "http.response.start":
                # The router stamps scope["route"] while handling the request.
                try:
                    _emit_for_route(scope, message.get("status", 500))
                except Exception:
                    pass
            await send(message)

        await self.app(scope, receive, _send)


def _subscribe() -> None:
    """Register the domain-event subscribers. Idempotent."""
    global _subscribed
    if _subscribed:
        return
    events.subscribe("flow_run_started", _on_flow_run_started)
    events.subscribe("flow_run_finished", _on_flow_run_finished)
    events.subscribe("flow_run_crashed", _on_flow_run_crashed)
    events.subscribe("kernel_exec", _on_kernel_exec)
    events.subscribe("app_started", _on_app_started)
    _subscribed = True


def install(app) -> None:
    """Wire telemetry onto the FastAPI app: route middleware + subscribers. Idempotent."""
    global _middleware_installed
    _subscribe()
    if _middleware_installed:
        return
    app.add_middleware(TelemetryMiddleware)
    _middleware_installed = True


def install_headless() -> None:
    """Subscribe without an HTTP app, for the CLI run paths. Idempotent."""
    _subscribe()


def flush(timeout: float = 2.0) -> None:
    """Best-effort synchronous drain, for processes that exit right after a run."""
    _client.flush(timeout)

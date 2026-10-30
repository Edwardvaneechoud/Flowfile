"""Synchronous in-process execution of a published flow for the HTTP data API.

Loads a registered flow fresh per request, injects its validated query parameters, runs it in performance mode and
serializes the data flowing into its single ``api_response`` node. A remote run's final collect runs on the worker,
and a worker that cannot run it fails the request with a typed compute error. With ``FLOWFILE_COMPUTE_PROTOCOL``
on, a remote response is a sample of the node's result, bounded by the worker's sample cap. The flow is closed
when the call ends, releasing every result it minted.

A freshly opened flow keeps the ``flow_id`` in its file, and that id keys process-wide scratch state
(``FlowLogger``, kernel I/O dirs, the cache). Runs are therefore serialized per ``flow_id`` (:func:`_flow_run_lock`).

Untrusted query values reach the flow only as ``${param}`` substitutions into node settings, a ``polars_code``
node's ``exec()``'d source included. String parameters are therefore validated (:func:`_reject_unsafe_string`) so
a key holder cannot break out of a string literal and inject Polars. Editor flows never pass through this seam,
so they keep full expressivity.
"""

from __future__ import annotations

import io
import threading
from pathlib import Path
from typing import Any

import polars as pl
import requests

from flowfile_core.configs.settings import FLOWFILE_COMPUTE_PROTOCOL
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.errors import ComputeError, ComputeUnavailable, worker_failure
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import ExternalDfFetcher
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.param_types import coerce_param_value, stringify_param_value
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.flow_api_schema import ApiParamSpec
from flowfile_core.schemas.output_model import RunInformation
from flowfile_core.schemas.schemas import get_global_execution_location


class ApiParamError(ValueError):
    """A query parameter was missing or failed type validation (-> HTTP 400)."""


class ApiConfigError(Exception):
    """The published flow is misconfigured, e.g. not exactly one api_response node (-> HTTP 500)."""


class ApiExecutionError(Exception):
    """The flow ran but failed or produced no data (-> HTTP 500)."""


# The most rows a worker answers for one sample; a published response is never larger.
_SAMPLE_ROWS_CAP = 10_000

# Compute error subclasses by name, as a report row names them (the base's name clashes with Polars').
_COMPUTE_ERRORS = {error.__name__: error for error in ComputeError.__subclasses__()}

_run_locks_guard = threading.Lock()
_run_locks: dict[int, threading.Lock] = {}


def _flow_run_lock(flow_id: int) -> threading.Lock:
    """Return the process-wide lock that serializes runs of *flow_id*.

    ``run_flow_as_api`` is synchronous and executes in a worker thread (the public
    route's ``anyio.to_thread.run_sync`` and FastAPI's sync-route threadpool), so a
    ``threading.Lock`` — not the event-loop-bound ``asyncio.Lock`` returned by
    ``routes.get_flow_run_lock`` — is the primitive that can serialize it without a
    blocking portal (which would also break direct/synchronous callers). Two
    concurrent requests for the same published flow would otherwise race on the
    ``flow_id``-keyed FlowLogger, kernel I/O dirs, and cache dir.
    """
    with _run_locks_guard:
        lock = _run_locks.get(flow_id)
        if lock is None:
            lock = threading.Lock()
            _run_locks[flow_id] = lock
        return lock


_UNSAFE_STRING_CHARS = frozenset("'\"()\\;\r\n")
"""Characters that let a query value escape a string literal once substituted into settings or exec'd code.

Quotes and parentheses escape ``'${param}'``, a backslash starts an escape, and ``;`` or a newline splices a
statement; ``_reject_unsafe_string`` refuses ``${`` separately.
"""


def _reject_unsafe_string(spec: ApiParamSpec, value: str) -> str:
    """Reject string param values that could inject code; return *value* if safe.

    Applied only to free-form (string-typed) API parameters at this seam, so a key
    holder cannot turn a ``${param}`` reference into arbitrary Polars. Enum values
    are constrained to an owner-defined allow-list and numeric/boolean values are
    normalized, so neither needs this gate. Editor/designer flows never reach here.
    """
    if "${" in value or any(ch in _UNSAFE_STRING_CHARS for ch in value):
        raise ApiParamError(
            f"parameter '{spec.name}' contains disallowed characters; string values "
            "may not contain quotes, parentheses, backslashes, semicolons, newlines, or '${'"
        )
    return value


def _coerce(spec: ApiParamSpec, raw: str) -> str:
    """Validate ``raw`` against ``spec`` and return the string value to substitute."""
    if spec.type == "string":
        return _reject_unsafe_string(spec, raw)
    try:
        return stringify_param_value(coerce_param_value(spec.type, raw, spec.enum_values))
    except ValueError as exc:
        raise ApiParamError(f"parameter '{spec.name}': {exc}") from exc


def resolve_params(specs: list[ApiParamSpec], query: dict[str, str]) -> dict[str, str]:
    """Validate query values against the endpoint's param specs.

    Returns a name->value dict for params that were supplied (or have an endpoint
    default). Params left out fall back to the flow's own ``FlowParameter`` default.
    """
    resolved: dict[str, str] = {}
    for spec in specs:
        raw = query.get(spec.name)
        if raw is not None and raw != "":
            resolved[spec.name] = _coerce(spec, raw)
        elif spec.required:
            raise ApiParamError(f"missing required parameter '{spec.name}'")
        elif spec.default is not None:
            # Endpoint defaults obey the same type/enum/safety rules as request values.
            resolved[spec.name] = _coerce(spec, spec.default)
    return resolved


def _first_error(run_info: RunInformation | None) -> str | None:
    if run_info is None:
        return None
    for node_result in run_info.node_step_result:
        if node_result.success is False and node_result.error:
            return f"node {node_result.node_id}: {node_result.error}"
    return None


def _first_compute_error(run_info: RunInformation | None) -> ComputeError | None:
    """The typed compute error of the run's first failed node, rebuilt from its run report row, else None.

    The row's text already carries ``ComputeUnavailable``'s hint, which the class does not add twice.
    """
    if run_info is None:
        return None
    for node_result in run_info.node_step_result:
        if node_result.success is False and node_result.error:
            error_class = _COMPUTE_ERRORS.get(node_result.compute_error)
            if error_class is None:
                return None
            return error_class(f"node {node_result.node_id}: {node_result.error}")
    return None


def _materialize(data: Any, max_rows: int | None, flow: Any, api_node: Any):
    """Collect the api_response input: in core for a local run, on the worker for a remote one.

    The flow runs in performance mode, so this is the one collect that executes the whole plan. On the remote
    path the row cap is pushed in first and nothing is collected in core. With ``FLOWFILE_COMPUTE_PROTOCOL`` on,
    a remote response is ``sample(ref, n)`` of the node's reference on the backend that holds it, ``n`` being
    *max_rows* clamped to the worker's sample cap (the cap when unset).

    Raises:
        ComputeUnavailable: The worker could not be reached to submit the plan.
        ComputeError: The worker could not run the plan (error code -1, typed by
            :func:`~flowfile_core.flowfile.execution.errors.worker_failure`); with the flag
            on, any typed failure of the sample.
        ApiExecutionError: The plan could not be submitted (not serialisable, or refused),
            or it failed on its data on the worker.
    """
    if flow.flow_settings.execution_location == "local":
        return data.collect(n_records=max_rows)

    if FLOWFILE_COMPUTE_PROTOCOL:
        ref = api_node.results.result_ref
        n = _SAMPLE_ROWS_CAP if max_rows is None else min(max_rows, _SAMPLE_ROWS_CAP)
        return pl.read_ipc_stream(io.BytesIO(registry.backend(ref.backend_id).sample(ref, n)))

    lf = data.data_frame
    if max_rows is not None:
        lf = lf.head(max_rows)
    try:
        fetcher = ExternalDfFetcher(
            lf=lf,
            file_ref=f"__api_{api_node.hash}",
            flow_id=flow.flow_id,
            node_id=api_node.node_id,
            wait_on_completion=False,
            worker_url=registry.worker_url(),
        )
    except ComputeError:
        # The handshake or the transport already typed it (unreachable, version mismatch).
        raise
    except requests.RequestException as exc:
        raise ComputeUnavailable(str(exc)) from exc
    except Exception as exc:
        raise ApiExecutionError(str(exc) or "could not submit the plan to the worker") from exc

    try:
        result = fetcher.get_result()
    except Exception as exc:
        if fetcher.error_code == -1:
            raise worker_failure(fetcher.error_kind, fetcher.error_description) from exc
        raise ApiExecutionError(fetcher.error_description or str(exc) or "flow execution failed on worker") from exc
    return result.collect()


def _truncated(df: pl.DataFrame, max_rows: int | None, flow: Any, api_node: Any) -> bool:
    """Whether the worker's sample cap, not the node's *max_rows*, left rows out of a response.

    Only a remote read with ``FLOWFILE_COMPUTE_PROTOCOL`` on is capped. The total is the reference's row count,
    else a ``count`` on the backend that holds it.
    """
    if flow.flow_settings.execution_location == "local" or not FLOWFILE_COMPUTE_PROTOCOL:
        return False
    if df.height < _SAMPLE_ROWS_CAP or (max_rows is not None and max_rows <= _SAMPLE_ROWS_CAP):
        return False
    ref = api_node.results.result_ref
    total = ref.row_count if ref.row_count is not None else registry.backend(ref.backend_id).count(ref)
    return total > df.height


def _serialize(data: Any, settings: Any, flow: Any, api_node: Any) -> dict[str, Any]:
    """The response body: the rows, how many there are, the orientation, and whether the sample cap cut them."""
    orientation = getattr(settings, "orientation", "records")
    max_rows = getattr(settings, "max_rows", None)
    df = _materialize(data, max_rows, flow, api_node)
    truncated = _truncated(df, max_rows, flow, api_node)
    if orientation == "columns":
        return {
            "data": df.to_dict(as_series=False),
            "row_count": df.height,
            "orientation": "columns",
            "truncated": truncated,
        }
    records = df.to_dicts()
    return {"data": records, "row_count": len(records), "orientation": "records", "truncated": truncated}


def _effective_specs(flow, overrides: list[ApiParamSpec]) -> list[ApiParamSpec]:
    """Derive the endpoint's parameter set from the flow's own ${name} parameters.

    Parameters are *inherited* from the flow: every flow parameter is accepted (as a
    string by default). The stored ``overrides`` only refine type/required/enum per
    name; stale overrides for params no longer in the flow are ignored.
    """
    by_name = {spec.name: spec for spec in overrides}
    specs: list[ApiParamSpec] = []
    for param in flow.flow_settings.parameters:
        override = by_name.get(param.name)
        if override is not None:
            specs.append(override)
        else:
            # No stored override: inherit the flow parameter's own declared type.
            specs.append(
                ApiParamSpec(name=param.name, type=param.type, enum_values=param.enum_values, required=False)
            )
    return specs


def run_flow_as_api(
    flow_path: str,
    owner_id: int,
    param_specs: list[ApiParamSpec],
    query: dict[str, str],
    execution_location: str | None = None,
) -> dict[str, Any]:
    """Run a published flow synchronously and return its serialized API response.

    Args:
        flow_path: Filesystem path to the registered flow.
        owner_id: User id the flow runs as (the endpoint owner).
        param_specs: Stored type overrides for the endpoint (parameters themselves
            are inherited from the flow's ${name} references).
        query: Raw query parameters from the request.
        execution_location: Override for where compute runs ("local"/"remote"). Defaults
            to the global, worker-aware setting; tests pass "local" to stay hermetic.

    Raises:
        ApiParamError: A parameter is missing or fails type validation.
        ApiConfigError: The flow does not have exactly one api_response node.
        ApiExecutionError: The flow failed or produced no data.
        ComputeError: The worker could not be reached (``ComputeUnavailable``) or could
            not run the final collect. With ``FLOWFILE_COMPUTE_PROTOCOL`` on, also the typed
            error of the run's first failed node, rebuilt from its run report row. Nothing
            is collected in core instead.
    """
    flow = open_flow(Path(flow_path), user_id=owner_id)
    try:
        # Machine traffic, not a user pressing Run.
        flow._system_run = True

        api_nodes = [n for n in flow.nodes if n.node_type == "api_response"]
        if len(api_nodes) == 0:
            raise ApiConfigError("flow has no API response node")
        if len(api_nodes) > 1:
            raise ApiConfigError("flow has more than one API response node")
        api_node = api_nodes[0]
        if not api_node.is_setup:
            # Every setting has a default, so a never-configured node means the defaults.
            flow.add_api_response(input_schema.NodeApiResponse(flow_id=flow.flow_id, node_id=api_node.node_id))

        resolved = resolve_params(_effective_specs(flow, param_specs), query)
        for param in flow.flow_settings.parameters:
            if param.name in resolved:
                param.default_value = resolved[param.name]

        # Performance mode: one lazy plan, whose final collect _materialize sends to the worker when remote.
        flow.flow_settings.execution_location = execution_location or get_global_execution_location()
        flow.flow_settings.execution_mode = "Performance"

        # One run per flow_id at a time (module docstring); held while serializing, which reads the flow_id cache.
        with _flow_run_lock(flow.flow_id):
            run_info = flow.run_graph()
            if run_info is None or not run_info.success:
                compute_error = _first_compute_error(run_info) if FLOWFILE_COMPUTE_PROTOCOL else None
                if compute_error is not None:
                    raise compute_error
                raise ApiExecutionError(_first_error(run_info) or "flow execution failed")

            deliberately_skipped = {nr.node_id for nr in run_info.node_step_result if nr.skipped}
            if api_node.node_id in deliberately_skipped:
                # Gated off: pulling it would run the closed branch and serve it as if the gate were open.
                raise ApiExecutionError(
                    "API response node was gated off for these parameter values (a gate "
                    "condition upstream evaluated false)"
                )

            data = api_node.get_resulting_data()
            if data is None:
                raise ApiExecutionError("API response node produced no data")
            return _serialize(data, api_node.setting_input, flow, api_node)
    finally:
        flow.close_flow()

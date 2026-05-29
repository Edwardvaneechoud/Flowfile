"""Synchronous in-process execution of a published flow for the HTTP data API.

Loads a registered flow fresh per request, validates and injects typed query
parameters, runs the graph locally, and serializes the data flowing into the
flow's single ``api_response`` node.

A freshly opened flow keeps the ``flow_id`` stored in its file, and that id keys
*process-wide* scratch state: the singleton ``FlowLogger``, the kernel I/O dirs
``shared_volume/{flow_id}/{node_id}`` and the on-disk cache. Concurrent runs of
the *same* published flow would therefore corrupt each other, so runs are
serialized per ``flow_id`` (see :func:`_flow_run_lock`); distinct flows still run
concurrently.

Untrusted query values reach the flow only as ``${param}`` substitutions, which
are interpolated into node settings (including a ``polars_code`` node's source,
later ``exec()``'d with ``pl`` exposed). String-typed parameters are therefore
validated here (see :func:`_reject_unsafe_string`) so a key holder cannot break
out of a string literal and inject Polars. Editor/designer flows never pass
through this seam and keep full expressivity.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas.flow_api_schema import ApiParamSpec
from flowfile_core.schemas.output_model import RunInformation


class ApiParamError(ValueError):
    """A query parameter was missing or failed type validation (-> HTTP 400)."""


class ApiConfigError(Exception):
    """The published flow is misconfigured, e.g. not exactly one api_response node (-> HTTP 500)."""


class ApiExecutionError(Exception):
    """The flow ran but failed or produced no data (-> HTTP 500)."""


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


# Characters and tokens that let a raw query value break out of a Polars string
# literal or splice in new code once it is regex-substituted into node settings
# and, for a ``polars_code`` node, ``exec()``'d. Quotes/parentheses are what an
# attacker needs to escape ``'${param}'`` and reach ``pl.read_csv``/DB calls or
# rewrite a filter predicate; backslash enables escapes, ``;``/newlines splice new
# statements, and ``${`` would introduce a fresh substitution token.
_UNSAFE_STRING_CHARS = frozenset("'\"()\\;\r\n")


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
    t = spec.type
    if t == "string":
        return _reject_unsafe_string(spec, raw)
    if t == "integer":
        try:
            return str(int(raw))
        except ValueError as exc:
            raise ApiParamError(f"parameter '{spec.name}' must be an integer") from exc
    if t == "float":
        try:
            return str(float(raw))
        except ValueError as exc:
            raise ApiParamError(f"parameter '{spec.name}' must be a number") from exc
    if t == "boolean":
        low = raw.strip().lower()
        if low in ("true", "1", "yes"):
            return "true"
        if low in ("false", "0", "no"):
            return "false"
        raise ApiParamError(f"parameter '{spec.name}' must be a boolean (true/false)")
    if t == "enum":
        if raw not in (spec.enum_values or []):
            raise ApiParamError(f"parameter '{spec.name}' must be one of {spec.enum_values}")
        return raw
    return _reject_unsafe_string(spec, raw)


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


def _serialize(data: Any, settings: Any) -> dict[str, Any]:
    orientation = getattr(settings, "orientation", "records")
    max_rows = getattr(settings, "max_rows", None)
    if orientation == "columns":
        # Push the row limit into collect() instead of collecting everything and
        # slicing, so a large result isn't fully materialized just to be truncated.
        df = data.collect(n_records=max_rows)
        columns = df.to_dict(as_series=False)
        return {"data": columns, "row_count": df.height, "orientation": "columns"}
    df = data.collect(n_records=max_rows)
    records = df.to_dicts()
    return {"data": records, "row_count": len(records), "orientation": "records"}


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
            specs.append(ApiParamSpec(name=param.name, type="string", required=False))
    return specs


def run_flow_as_api(
    flow_path: str,
    owner_id: int,
    param_specs: list[ApiParamSpec],
    query: dict[str, str],
) -> dict[str, Any]:
    """Run a published flow synchronously and return its serialized API response.

    Args:
        flow_path: Filesystem path to the registered flow.
        owner_id: User id the flow runs as (the endpoint owner).
        param_specs: Stored type overrides for the endpoint (parameters themselves
            are inherited from the flow's ${name} references).
        query: Raw query parameters from the request.

    Raises:
        ApiParamError: A parameter is missing or fails type validation.
        ApiConfigError: The flow does not have exactly one api_response node.
        ApiExecutionError: The flow failed or produced no data.
    """
    flow = open_flow(Path(flow_path), user_id=owner_id)

    api_nodes = [n for n in flow.nodes if n.node_type == "api_response"]
    if len(api_nodes) == 0:
        raise ApiConfigError("flow has no API response node")
    if len(api_nodes) > 1:
        raise ApiConfigError("flow has more than one API response node")
    api_node = api_nodes[0]

    resolved = resolve_params(_effective_specs(flow, param_specs), query)
    for param in flow.flow_settings.parameters:
        if param.name in resolved:
            param.default_value = resolved[param.name]

    # Force local execution so the response data is materialized in-process.
    flow.flow_settings.execution_location = "local"

    # Serialize concurrent runs of the same published flow: the freshly opened flow
    # keeps its saved flow_id, which keys process-wide scratch state (FlowLogger,
    # kernel I/O dirs, cache dir) that overlapping runs would corrupt. Held through
    # serialization too, since collecting the result reads the flow_id-keyed cache.
    with _flow_run_lock(flow.flow_id):
        run_info = flow.run_graph()
        if run_info is None or not run_info.success:
            raise ApiExecutionError(_first_error(run_info) or "flow execution failed")

        data = api_node.get_resulting_data()
        if data is None:
            raise ApiExecutionError("API response node produced no data")
        return _serialize(data, api_node.setting_input)

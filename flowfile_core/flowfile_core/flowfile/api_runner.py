"""Synchronous in-process execution of a published flow for the HTTP data API.

Loads a registered flow fresh per request (no shared mutable state), validates and
injects typed query parameters, runs the graph locally, and serializes the data
flowing into the flow's single ``api_response`` node.
"""

from __future__ import annotations

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


def _coerce(spec: ApiParamSpec, raw: str) -> str:
    """Validate ``raw`` against ``spec`` and return the string value to substitute."""
    t = spec.type
    if t == "string":
        return raw
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
    return raw


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
            resolved[spec.name] = spec.default
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
        columns = data.to_dict()
        if max_rows is not None:
            columns = {k: v[:max_rows] for k, v in columns.items()}
        row_count = len(next(iter(columns.values()))) if columns else 0
        return {"data": columns, "row_count": row_count, "orientation": "columns"}
    df = data.collect(n_records=max_rows)
    records = df.to_dicts()
    return {"data": records, "row_count": len(records), "orientation": "records"}


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
        param_specs: Typed parameter specs declared on the endpoint.
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

    resolved = resolve_params(param_specs, query)
    for param in flow.flow_settings.parameters:
        if param.name in resolved:
            param.default_value = resolved[param.name]

    # Force local execution so the response data is materialized in-process.
    flow.flow_settings.execution_location = "local"

    run_info = flow.run_graph()
    if run_info is None or not run_info.success:
        raise ApiExecutionError(_first_error(run_info) or "flow execution failed")

    data = api_node.get_resulting_data()
    if data is None:
        raise ApiExecutionError("API response node produced no data")
    return _serialize(data, api_node.setting_input)

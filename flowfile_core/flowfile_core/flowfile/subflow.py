"""Runtime for the run_flow node: resolve, inspect, and execute catalog flows as subflows.

The child graph always runs in-process and local (fresh flow_id, never registered
in the FlowfileHandler, per-graph execution settings — the global worker-offload
flag is untouched). Outputs stay lazy; only the small parameter frame is
bounded-collected in core.
"""

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import BaseModel

from flowfile_core.auth import sharing
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_node.multi_output import NamedOutputs, output_handle
from flowfile_core.flowfile.param_types import (
    FlowParameter,
    ParamValue,
    coerce_param_value,
    stringify_param_value,
)
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.schemas import input_schema

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

MAX_SUBFLOW_DEPTH = 5
MAX_SUBFLOW_RUNS = 1000
RUN_INDEX_COLUMN = "run_index"
PARAM_COLUMN_PREFIX = "param_"

_PARAM_TYPE_TO_POLARS = {
    "string": "String",
    "enum": "String",
    "integer": "Int64",
    "float": "Float64",
    "boolean": "Boolean",
}


class SubflowResolutionError(Exception):
    """The referenced flow registration or its file cannot be resolved."""


class SubflowPortSpec(BaseModel):
    name: str
    node_id: int


class SubflowInterface(BaseModel):
    """The callable surface of a flow: its flow_input/flow_output ports and parameters."""

    inputs: list[SubflowPortSpec]
    outputs: list[SubflowPortSpec]
    parameters: list[FlowParameter]


@dataclass(frozen=True)
class ResolvedSubflow:
    path: Path
    registration_id: int
    name: str
    flow_uuid: str | None


_cache_lock = threading.Lock()
_interface_cache: dict[str, tuple[float, SubflowInterface]] = {}
_output_schema_cache: dict[str, tuple[float, dict[int, list[FlowfileColumn]]]] = {}


def resolve_subflow_path(ref: input_schema.SubflowReference, user_id: int | None) -> ResolvedSubflow:
    """Resolve a SubflowReference to an on-disk flow file, enforcing access.

    ``registration_id`` first; a stamped ``flow_uuid`` repairs a dangling id.
    ``user_id`` None means an internal/CLI run and is unrestricted.
    """
    from flowfile_core.database import models as db_models

    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        registration = repo.get_flow(ref.registration_id)
        if registration is None and ref.flow_uuid:
            registration = (
                db.query(db_models.FlowRegistration)
                .filter(db_models.FlowRegistration.flow_uuid == ref.flow_uuid)
                .first()
            )
        if registration is None:
            raise SubflowResolutionError(
                f"Referenced flow (registration {ref.registration_id}) no longer exists in the catalog"
            )
        if user_id is not None and not sharing.user_id_can_use(db, user_id, "flow", registration.id):
            raise SubflowResolutionError(f"No access to the referenced flow '{registration.name}'")
        resolved = ResolvedSubflow(
            path=Path(registration.flow_path),
            registration_id=registration.id,
            name=registration.name,
            flow_uuid=registration.flow_uuid,
        )
    if not resolved.path.is_file():
        raise SubflowResolutionError(f"Flow file for '{resolved.name}' not found: {resolved.path}")
    return resolved


def stamp_flow_reference(settings: input_schema.NodeRunFlow) -> None:
    """Best-effort: fill flow_uuid/flow_path on the reference from the registry."""
    try:
        with get_db_context() as db:
            registration = SQLAlchemyCatalogRepository(db).get_flow(settings.flow_reference.registration_id)
            if registration is not None:
                settings.flow_reference.flow_uuid = registration.flow_uuid
                settings.flow_reference.flow_path = registration.flow_path
    except Exception:  # noqa: BLE001 - purely informational stamping
        logger.warning("Could not stamp flow reference for run_flow node %s", settings.node_id, exc_info=True)


def get_subflow_interface(path: Path) -> SubflowInterface:
    """Parse a flow file's interface (mtime-cached; no FlowGraph construction)."""
    from flowfile_core.flowfile.manage.io_flowfile import _load_flow_storage

    key = str(path.resolve())
    mtime = path.stat().st_mtime
    with _cache_lock:
        cached = _interface_cache.get(key)
        if cached is not None and cached[0] == mtime:
            return cached[1]
    flow_info = _load_flow_storage(path)
    inputs = []
    outputs = []
    for node_id in sorted(flow_info.data):
        node_info = flow_info.data[node_id]
        if node_info.type == "flow_input" and node_info.setting_input is not None:
            inputs.append(SubflowPortSpec(name=node_info.setting_input.input_name, node_id=node_id))
        elif node_info.type == "flow_output" and node_info.setting_input is not None:
            outputs.append(SubflowPortSpec(name=node_info.setting_input.output_name, node_id=node_id))
    interface = SubflowInterface(
        inputs=inputs,
        outputs=outputs,
        parameters=list(flow_info.flow_settings.parameters),
    )
    with _cache_lock:
        _interface_cache[key] = (mtime, interface)
    return interface


def get_subflow_output_schemas(path: Path, user_id: int | None) -> dict[int, list[FlowfileColumn]]:
    """Predicted schema per flow_output node id (mtime-cached; opens the graph)."""
    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    key = str(path.resolve())
    mtime = path.stat().st_mtime
    with _cache_lock:
        cached = _output_schema_cache.get(key)
        if cached is not None and cached[0] == mtime:
            return cached[1]
    graph = open_flow(path, user_id=user_id)
    schemas: dict[int, list[FlowfileColumn]] = {}
    for node in graph.nodes:
        if node.node_type != "flow_output":
            continue
        try:
            schemas[node.node_id] = node.get_predicted_schema() or []
        except Exception:  # noqa: BLE001 - prediction is best-effort
            schemas[node.node_id] = []
    with _cache_lock:
        _output_schema_cache[key] = (mtime, schemas)
    return schemas


def _metadata_column_names(settings: input_schema.NodeRunFlow, existing: list[str]) -> dict[str, str]:
    """Desired metadata column -> collision-free name (prefix '_' until unique)."""

    def uniquify(name: str) -> str:
        while name in existing:
            name = f"_{name}"
        return name

    names = {}
    for binding in settings.parameter_bindings:
        if binding.source == "default":
            continue
        desired = f"{PARAM_COLUMN_PREFIX}{binding.parameter_name}"
        names[binding.parameter_name] = uniquify(desired)
    names[RUN_INDEX_COLUMN] = uniquify(RUN_INDEX_COLUMN)
    return names


def _appends_metadata(settings: input_schema.NodeRunFlow) -> bool:
    return settings.iteration_mode == "iterate" and settings.append_run_metadata


def predict_run_flow_named_schemas(settings: input_schema.NodeRunFlow) -> dict[str, list[FlowfileColumn]]:
    """Predicted schema per output handle; {} when the subflow can't be inspected."""
    try:
        resolved = resolve_subflow_path(settings.flow_reference, settings.user_id)
        interface = get_subflow_interface(resolved.path)
        output_schemas = get_subflow_output_schemas(resolved.path, settings.user_id)
        ports_by_name = {port.name: port for port in interface.outputs}
        specs_by_name = {p.name: p for p in interface.parameters}
        named: dict[str, list[FlowfileColumn]] = {}
        for i, slot_name in enumerate(settings.output_slots):
            port = ports_by_name.get(slot_name)
            columns = list(output_schemas.get(port.node_id, [])) if port is not None else []
            if _appends_metadata(settings):
                metadata_names = _metadata_column_names(settings, [c.column_name for c in columns])
                for binding in settings.parameter_bindings:
                    if binding.source == "default":
                        continue
                    spec = specs_by_name.get(binding.parameter_name)
                    dtype = _PARAM_TYPE_TO_POLARS.get(spec.type if spec else "string", "String")
                    columns.append(FlowfileColumn.from_input(metadata_names[binding.parameter_name], dtype))
                columns.append(FlowfileColumn.from_input(metadata_names[RUN_INDEX_COLUMN], "UInt32"))
            named[output_handle(i)] = columns
        return named
    except Exception:  # noqa: BLE001 - schema prediction must never raise
        logger.warning("Could not predict run_flow schemas for node %s", settings.node_id, exc_info=True)
        return {}


def _stringify_cell(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def _build_run_params(
    settings: input_schema.NodeRunFlow,
    interface: SubflowInterface,
    param_input: FlowDataEngine | None,
    node_logger,
) -> list[dict[str, ParamValue]]:
    """Resolve the per-run parameter values. One dict per subflow run."""
    specs_by_name = {p.name: p for p in interface.parameters}
    for binding in settings.parameter_bindings:
        if binding.parameter_name not in specs_by_name:
            raise ValueError(
                f"Parameter '{binding.parameter_name}' does not exist in the subflow — "
                "refresh the Run Flow settings to re-sync its interface"
            )

    base: dict[str, ParamValue] = {}
    for binding in settings.parameter_bindings:
        if binding.source != "constant":
            continue
        spec = specs_by_name[binding.parameter_name]
        try:
            base[binding.parameter_name] = coerce_param_value(spec.type, binding.constant_value, spec.enum_values)
        except ValueError as exc:
            raise ValueError(f"Parameter '{binding.parameter_name}': {exc}") from exc

    column_bindings = [b for b in settings.parameter_bindings if b.source == "column"]
    if param_input is None or not column_bindings:
        if column_bindings:
            node_logger.warning(
                "Column-mapped parameters configured but no data is connected to the parameter input; "
                "using defaults/constants"
            )
        return [base]

    n_records = 1 if settings.iteration_mode == "first_value" else MAX_SUBFLOW_RUNS + 1
    param_df = param_input.collect(n_records=n_records)
    missing = sorted({b.column_name for b in column_bindings} - set(param_df.columns))
    if missing:
        raise ValueError(f"Parameter input is missing mapped column(s): {', '.join(missing)}")
    if settings.iteration_mode == "iterate" and param_df.height > MAX_SUBFLOW_RUNS:
        raise ValueError(f"Parameter input has more than {MAX_SUBFLOW_RUNS} rows; refusing to iterate")
    if param_df.height == 0:
        if settings.iteration_mode == "first_value":
            node_logger.warning("Parameter input has no rows; running once with defaults/constants")
            return [base]
        return []

    runs: list[dict[str, ParamValue]] = []
    for row_index in range(param_df.height):
        run_params = dict(base)
        for binding in column_bindings:
            spec = specs_by_name[binding.parameter_name]
            cell = param_df[binding.column_name][row_index]
            try:
                run_params[binding.parameter_name] = coerce_param_value(
                    spec.type, _stringify_cell(cell), spec.enum_values
                )
            except ValueError as exc:
                raise ValueError(
                    f"Parameter '{binding.parameter_name}' (row {row_index + 1}): {exc}"
                ) from exc
        runs.append(run_params)
    return runs


def _first_child_error(run_info) -> str:
    for node_result in run_info.node_step_result:
        if node_result.success is False and node_result.error:
            return f"node {node_result.node_id}: {node_result.error}"
    return "unknown error"


_PARAM_TYPE_TO_PL = {
    "string": pl.String,
    "enum": pl.String,
    "integer": pl.Int64,
    "float": pl.Float64,
    "boolean": pl.Boolean,
}


def _metadata_literals(
    settings: input_schema.NodeRunFlow,
    interface: SubflowInterface,
    run_params: dict[str, ParamValue],
    run_index: int,
    existing_columns: list[str],
) -> list[pl.Expr]:
    specs_by_name = {p.name: p for p in interface.parameters}
    names = _metadata_column_names(settings, existing_columns)
    literals: list[pl.Expr] = []
    for binding in settings.parameter_bindings:
        if binding.source == "default":
            continue
        spec = specs_by_name.get(binding.parameter_name)
        value = run_params.get(binding.parameter_name)
        if value is None:
            value = spec.typed_default() if spec else ""
        # Cast so the materialized dtype matches the predicted schema
        # (pl.lit(int) alone yields Int32, prediction promises Int64).
        dtype = _PARAM_TYPE_TO_PL.get(spec.type if spec else "string", pl.String)
        literals.append(pl.lit(value).cast(dtype).alias(names[binding.parameter_name]))
    literals.append(pl.lit(run_index).cast(pl.UInt32).alias(names[RUN_INDEX_COLUMN]))
    return literals


def _empty_outputs(settings: input_schema.NodeRunFlow) -> "NamedOutputs | FlowDataEngine":
    predicted = predict_run_flow_named_schemas(settings)
    if not settings.output_slots:
        return FlowDataEngine()
    engines: dict[str, FlowDataEngine] = {}
    for i, slot_name in enumerate(settings.output_slots):
        columns = predicted.get(output_handle(i), [])
        engines[slot_name] = FlowDataEngine.create_from_schema(columns) if columns else FlowDataEngine()
    return NamedOutputs(engines)


def _run_summary(settings: input_schema.NodeRunFlow, runs: list[dict[str, ParamValue]]) -> FlowDataEngine:
    rows = []
    for i, run_params in enumerate(runs, start=1):
        row: dict[str, Any] = {RUN_INDEX_COLUMN: i, "success": True}
        for name, value in run_params.items():
            row[f"{PARAM_COLUMN_PREFIX}{name}"] = value
        rows.append(row)
    if not rows:
        return FlowDataEngine(pl.DataFrame({RUN_INDEX_COLUMN: [], "success": []}))
    return FlowDataEngine(pl.DataFrame(rows))


def execute_run_flow_node(
    parent_graph: "FlowGraph",
    settings: input_schema.NodeRunFlow,
    param_input: FlowDataEngine | None,
    data_inputs: tuple,
) -> "NamedOutputs | FlowDataEngine":
    """Execute the referenced subflow once (or once per parameter row) and collect outputs."""
    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    node = parent_graph.get_node(settings.node_id)
    node_logger = parent_graph.flow_logger.get_node_logger(settings.node_id)

    resolved = resolve_subflow_path(settings.flow_reference, settings.user_id)
    path_key = str(resolved.path.resolve())

    parent_path = parent_graph.flow_settings.path
    parent_key = str(Path(parent_path).resolve()) if parent_path else None
    ancestry = parent_graph._subflow_ancestry | ({parent_key} if parent_key else set())
    if path_key in ancestry:
        chain = " -> ".join([*sorted(ancestry), path_key])
        raise ValueError(f"Subflow recursion detected: '{resolved.name}' is already running in this chain ({chain})")
    depth = parent_graph._subflow_depth + 1
    if depth > MAX_SUBFLOW_DEPTH:
        raise ValueError(f"Subflow nesting exceeds the maximum depth of {MAX_SUBFLOW_DEPTH}")

    interface = get_subflow_interface(resolved.path)
    input_ports = {port.name: port for port in interface.inputs}
    output_ports = {port.name: port for port in interface.outputs}
    stale_inputs = [name for name in settings.input_slots if name not in input_ports]
    stale_outputs = [name for name in settings.output_slots if name not in output_ports]
    if stale_inputs or stale_outputs:
        stale = ", ".join(stale_inputs + stale_outputs)
        raise ValueError(
            f"Subflow '{resolved.name}' no longer has port(s): {stale} — "
            "refresh the Run Flow settings to re-sync its interface"
        )

    runs = _build_run_params(settings, interface, param_input, node_logger)
    if not runs:
        node_logger.info("Parameter input has 0 rows in iterate mode; producing empty outputs")
        return _empty_outputs(settings)

    unbound = [
        slot
        for i, slot in enumerate(settings.input_slots)
        if i >= len(data_inputs) or data_inputs[i] is None
    ]
    if unbound:
        node_logger.warning(f"Subflow input(s) not connected: {', '.join(unbound)}; using their sample data")

    # Must match predict_run_flow_named_schemas (_appends_metadata): appending is
    # driven by the settings alone, even for a single run without parameter data.
    append_metadata = _appends_metadata(settings)
    collected: dict[str, list[pl.LazyFrame]] = {name: [] for name in settings.output_slots}

    for run_index, run_params in enumerate(runs, start=1):
        if parent_graph.flow_settings.is_canceled:
            raise Exception("Flow run was canceled")
        node_logger.info(f"Subflow '{resolved.name}' run {run_index}/{len(runs)} starting")

        child = open_flow(resolved.path, user_id=settings.user_id)
        child.flow_id = create_unique_id()
        # The setter does not rebuild the per-flow_id logger; without this, child
        # runs log into the subflow file's own editor log (and collide when the
        # same subflow runs from two parents at once).
        child.flow_logger = FlowLogger(child.flow_id)
        child.flow_settings.execution_location = "local"
        child.flow_settings.execution_mode = "Performance"
        child.flow_settings.auto_save = False
        child._subflow_ancestry = ancestry | {path_key}
        child._subflow_depth = depth

        child_params = {p.name: p for p in child.flow_settings.parameters}
        for name, value in run_params.items():
            if name in child_params:
                child_params[name].default_value = stringify_param_value(value)

        for slot_index, slot_name in enumerate(settings.input_slots):
            engine = data_inputs[slot_index] if slot_index < len(data_inputs) else None
            if engine is None:
                continue
            child_node = child.get_node(input_ports[slot_name].node_id)
            if child_node is None:
                raise ValueError(f"Subflow input node for '{slot_name}' could not be loaded")
            child_node.function = FlowDataEngine(engine.data_frame)

        if node is not None:
            node._subflow_cancel_context = child
        try:
            run_info = child.run_graph()
        finally:
            if node is not None:
                node._subflow_cancel_context = None

        # Cancel first: a mid-node cancel leaves node results unsuccessful, which
        # would otherwise surface as a bogus "run failed" error.
        if parent_graph.flow_settings.is_canceled or child.flow_settings.is_canceled:
            raise Exception("Flow run was canceled")
        if run_info is None or run_info.success is False:
            error = _first_child_error(run_info) if run_info is not None else "run did not start"
            raise Exception(f"Subflow '{resolved.name}' run {run_index}/{len(runs)} failed: {error}")

        for slot_name in settings.output_slots:
            out_node = child.get_node(output_ports[slot_name].node_id)
            if out_node is None or not out_node.has_input:
                raise ValueError(
                    f"Subflow output '{slot_name}' has no input connected inside '{resolved.name}' — "
                    "connect it or remove the flow_output node"
                )
            result = out_node.get_resulting_data()
            lf = result.data_frame if result is not None else pl.DataFrame().lazy()
            if not isinstance(lf, pl.LazyFrame):
                lf = lf.lazy()
            if append_metadata:
                existing = list(lf.collect_schema().names())
                lf = lf.with_columns(_metadata_literals(settings, interface, run_params, run_index, existing))
            collected[slot_name].append(lf)
        node_logger.info(f"Subflow '{resolved.name}' run {run_index}/{len(runs)} completed")

    if not settings.output_slots:
        return _run_summary(settings, runs)

    engines: dict[str, FlowDataEngine] = {}
    for slot_name in settings.output_slots:
        frames = collected[slot_name]
        if len(frames) == 1:
            engines[slot_name] = FlowDataEngine(frames[0])
        else:
            # Iterated runs may legitimately drift schema (parameter-driven selects);
            # relaxed diagonal concat fills gaps with nulls and unifies supertypes.
            engines[slot_name] = FlowDataEngine(pl.concat(frames, how="diagonal_relaxed"))
    return NamedOutputs(engines)

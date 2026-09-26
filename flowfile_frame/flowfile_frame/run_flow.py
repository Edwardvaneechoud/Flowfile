"""Subflows from Python: declare a flow's ports, register it in the catalog, and run it from another flow.

``FlowInput`` and ``FlowFrame.to_flow_output`` place the ``flow_input``/``flow_output`` nodes that
make up a flow's callable surface; the run_flow node orders its slots by node id, so the order
the ports are created in is the order of ``RunFlow``'s inputs and outputs. ``register_flow``
saves the graph as a YAML file and files it in the catalog, ``flow_ref`` (and
``SchemaReference.get_flow``) finds a registered flow, and ``RunFlow`` places a run_flow node
whose outputs only exist once the parent flow runs.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import ValidationError

from flowfile_core.auth import sharing
from flowfile_core.auth.utils import get_local_user_id
from flowfile_core.catalog import (
    AmbiguousFlowError,
    CatalogService,
    FlowExistsError,
    FlowNotFoundError,
    NamespaceNotFoundError,
    NotAuthorizedError,
    SQLAlchemyCatalogRepository,
)
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile import subflow
from flowfile_core.flowfile.catalog_helpers import _safe_filename_stem, register_python_editor_flow
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.input_handles import input_handle
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_core.flowfile.param_types import coerce_param_value
from flowfile_core.flowfile.parameter_resolver import find_unresolved_in_model
from flowfile_core.schemas import input_schema
from flowfile_frame.catalog_reference import CatalogReference, SchemaReference
from flowfile_frame.config import logger
from flowfile_frame.custom_node import _warn_session_only_custom_nodes
from flowfile_frame.expr import Expr
from flowfile_frame.native import NativeNode, NativeNodeError, Node
from flowfile_frame.parameters import (
    Parameter,
    _as_parameter_string,
    _graph_of,
    _param_name,
    refuse_parameter_as_column,
)
from shared.storage_config import storage

if TYPE_CHECKING:
    from polars._typing import PolarsDataType
    from sqlalchemy.orm import Session

    from flowfile_core.database.models import FlowRegistration
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode
    from flowfile_core.flowfile.param_types import FlowParameter
    from flowfile_frame.flow_frame import FlowFrame


class FlowRef:
    """Handle to a catalog-registered flow: what ``RunFlow`` runs.

    Get one from :func:`flow_ref`, ``SchemaReference.get_flow`` or :func:`register_flow`, which
    check the registration, access and the flow file; the constructor only holds the values.
    ``flow_uuid`` survives the registration being deleted and recreated (the run_flow node uses
    it to repair a dangling id). ``schema`` is the schema the flow is filed under (``None``
    outside a schema), so ``ref.schema.read_table(...)`` works from the same handle.
    """

    __slots__ = ("registration_id", "flow_uuid", "flow_path", "name", "schema")

    registration_id: int
    flow_uuid: str
    flow_path: str
    name: str
    schema: SchemaReference | None

    def __init__(
        self,
        registration_id: int,
        flow_uuid: str,
        flow_path: str,
        name: str,
        schema: SchemaReference | None = None,
    ) -> None:
        object.__setattr__(self, "registration_id", registration_id)
        object.__setattr__(self, "flow_uuid", flow_uuid)
        object.__setattr__(self, "flow_path", flow_path)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "schema", schema)

    def __setattr__(self, key: str, value: object) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __delattr__(self, key: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __repr__(self) -> str:
        return (
            f"FlowRef(name={self.name!r}, namespace={self.namespace_full_name!r}, "
            f"registration_id={self.registration_id}, flow_uuid={self.flow_uuid!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FlowRef):
            return NotImplemented
        return self.registration_id == other.registration_id and self.flow_uuid == other.flow_uuid

    def __hash__(self) -> int:
        return hash(("FlowRef", self.flow_uuid))

    def __getstate__(self) -> tuple[int, str, str, str, SchemaReference | None]:
        return (self.registration_id, self.flow_uuid, self.flow_path, self.name, self.schema)

    def __setstate__(self, state: tuple[int, str, str, str, SchemaReference | None]) -> None:
        for key, value in zip(self.__slots__, state, strict=True):
            object.__setattr__(self, key, value)

    @property
    def namespace_full_name(self) -> str | None:
        """``"catalog.schema"`` of the schema the flow is filed under, or ``None``."""
        if self.schema is None:
            return None
        return f"{self.schema.catalog.name}.{self.schema.name}"

    def to_subflow_reference(self) -> input_schema.SubflowReference:
        """The reference a run_flow node stores."""
        return input_schema.SubflowReference(
            registration_id=self.registration_id, flow_uuid=self.flow_uuid, flow_path=self.flow_path
        )


def _schema_reference(repo: SQLAlchemyCatalogRepository, namespace_id: int | None) -> SchemaReference | None:
    """The ``SchemaReference`` of a level-1 namespace; ``None`` for no namespace or a catalog."""
    namespace = repo.get_namespace(namespace_id) if namespace_id is not None else None
    if namespace is None or namespace.parent_id is None:
        return None
    parent = repo.get_namespace(namespace.parent_id)
    if parent is None:
        return None
    return SchemaReference._from_namespace(CatalogReference._from_namespace(parent), namespace)


def _flow_ref_from_registration(repo: SQLAlchemyCatalogRepository, registration: FlowRegistration) -> FlowRef:
    return FlowRef(
        registration.id,
        registration.flow_uuid,
        registration.flow_path,
        registration.name,
        _schema_reference(repo, registration.namespace_id),
    )


def _resolve_namespace(
    service: CatalogService, namespace: str | SchemaReference | CatalogReference | None
) -> tuple[int | None, str | None]:
    """``(namespace id, "catalog.schema")`` of ``namespace``; ``(None, None)`` means every namespace.

    A string is a dotted ``"catalog.schema"`` name (a bare catalog name is the catalog itself);
    a reference's id is re-verified, since the namespace can have been deleted since.
    """
    if namespace is None:
        return None, None
    if isinstance(namespace, str):
        namespace_id = service.resolve_namespace_id_by_full_name(namespace)
        if namespace_id is None:
            raise NamespaceNotFoundError(name=namespace)
        return namespace_id, namespace
    if isinstance(namespace, SchemaReference | CatalogReference):
        stored = service.repo.get_namespace(namespace.id)
        if stored is None or stored.name != namespace.name:
            raise NamespaceNotFoundError(namespace_id=namespace.id)
        return stored.id, service.resolve_namespace_full_name(stored.id)
    raise NativeNodeError(
        f"namespace must be a 'catalog.schema' string, a SchemaReference or a CatalogReference, "
        f"got {type(namespace).__name__}"
    )


def _ambiguous(service: CatalogService, name: str, matches: list[FlowRegistration]) -> AmbiguousFlowError:
    candidates = [
        {
            "id": r.id,
            "name": r.name,
            "namespace_id": r.namespace_id,
            "namespace_name": service.resolve_namespace_full_name(r.namespace_id),
        }
        for r in matches
    ]
    return AmbiguousFlowError(name, candidates)


def _find_registration(
    db: Session,
    service: CatalogService,
    user_id: int,
    namespace: tuple[int | None, str | None],
    name: str | None,
    uuid: str | None,
    registration_id: int | None,
) -> FlowRegistration:
    """The one registration the arguments select; a uuid or id is cross-checked against name and namespace."""
    repo = service.repo
    namespace_id, namespace_name = namespace
    if uuid is None and registration_id is None:
        matches = [
            r for r in repo.list_flows_by_name(name, namespace_id) if sharing.user_id_can_use(db, user_id, "flow", r.id)
        ]
        if not matches:
            raise FlowNotFoundError(name=f"{namespace_name}.{name}" if namespace_name else name)
        if len(matches) > 1:
            raise _ambiguous(service, name, matches)
        return matches[0]
    if uuid is not None:
        registration = repo.get_flow_by_uuid(uuid)
        if registration is None:
            raise FlowNotFoundError(name=f"uuid={uuid}")
        if registration_id is not None and registration.id != registration_id:
            raise NativeNodeError(f"uuid {uuid} belongs to registration {registration.id}, not {registration_id}")
    else:
        registration = repo.get_flow(registration_id)
        if registration is None:
            raise FlowNotFoundError(registration_id=registration_id)
    if name is not None and registration.name != name:
        raise NativeNodeError(f"Flow registration {registration.id} is named {registration.name!r}, not {name!r}")
    if namespace_id is not None and registration.namespace_id != namespace_id:
        raise NativeNodeError(f"Flow {registration.name!r} ({registration.id}) is not in {namespace_name!r}")
    if not sharing.user_id_can_use(db, user_id, "flow", registration.id):
        raise NotAuthorizedError(user_id, f"use flow '{registration.name}'")
    return registration


def flow_ref(
    namespace: str | SchemaReference | CatalogReference | None = None,
    name: str | None = None,
    *,
    uuid: str | None = None,
    registration_id: int | None = None,
) -> FlowRef:
    """Find a registered flow by ``uuid``, by ``registration_id``, or by ``name`` within ``namespace``.

    ``namespace`` is a ``"catalog.schema"`` string (a bare catalog name selects the catalog
    itself), a ``SchemaReference``/``CatalogReference``, or ``None`` for every namespace. A uuid
    or id is exact and is cross-checked against ``name`` and ``namespace`` when those are given
    too. A flow name is unique neither across namespaces nor within one, so more than one
    match raises ``AmbiguousFlowError`` rather than picking one; flows the local user may not
    use are left out. The registration must store an absolute path to an existing file: the
    run_flow node opens it without a base directory. The result always carries the uuid.
    """
    if uuid is None and registration_id is None and not name:
        raise NativeNodeError("flow_ref needs a flow name, uuid= or registration_id=")
    user_id = get_local_user_id()
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        resolved_namespace = _resolve_namespace(service, namespace)
        registration = _find_registration(db, service, user_id, resolved_namespace, name, uuid, registration_id)
        ref = _flow_ref_from_registration(service.repo, registration)
    path = Path(ref.flow_path)
    if not path.is_absolute() or not path.is_file():
        raise NativeNodeError(
            f"Flow {ref.name!r} (registration {ref.registration_id}) has no flow file at {ref.flow_path!r}; "
            "a run_flow node needs an absolute path to an existing file"
        )
    return ref


def _list_flow_refs(schema: SchemaReference) -> list[FlowRef]:
    """Every flow filed under ``schema`` that the local user may use, by name."""
    user_id = get_local_user_id()
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        namespace_id, _ = _resolve_namespace(service, schema)
        return [
            FlowRef(r.id, r.flow_uuid, r.flow_path, r.name, schema)
            for r in service.repo.list_flows(namespace_id=namespace_id)
            if sharing.user_id_can_use(db, user_id, "flow", r.id)
        ]


def _registration_path(
    service: CatalogService, namespace_id: int, namespace_name: str | None, name: str, overwrite: bool
) -> Path:
    """Where the flow file goes: the file of the flow already registered under ``name``, else a new one.

    Re-registering keeps the file (and so the registration id and uuid) when it lives in the
    Python-editor flows directory, which only holds flows written from code; any other file
    is only replaced with ``overwrite``. A new file is named after the schema and the flow,
    suffixed when a registration already uses that path.
    """
    matches = service.repo.list_flows_by_name(name, namespace_id)
    if len(matches) > 1:
        raise _ambiguous(service, name, matches)
    editor_dir = storage.python_editor_flows_directory.resolve()
    if matches:
        existing = Path(matches[0].flow_path)
        if overwrite or existing.resolve().is_relative_to(editor_dir):
            return existing
        raise FlowExistsError(name, namespace_id)
    stem = _safe_filename_stem(f"{namespace_name}__{name}")
    path, suffix = editor_dir / f"{stem}.yaml", 1
    while service.repo.get_flow_by_path(str(path)) is not None:
        suffix += 1
        path = editor_dir / f"{stem}_{suffix}.yaml"
    return path


def register_flow(
    flow_or_frame: FlowGraph | FlowFrame,
    *,
    name: str,
    schema: SchemaReference | None = None,
    overwrite: bool = False,
) -> FlowRef:
    """Save a flow as a YAML file and register it in the catalog under ``name``; return its ``FlowRef``.

    ``schema`` defaults to ``General.Python Editor``. Registering the same name in the same
    schema again rewrites the same file, so a re-run script keeps the registration id and uuid;
    a same-name flow whose file lives elsewhere (one saved from the designer) raises
    ``FlowExistsError`` unless ``overwrite=True``, which replaces that file. This writes the
    file and the catalog row when it is called. The graph is laid out first so it opens cleanly
    on the canvas, and afterwards lives at the registered file (``flow_settings.path``). Warns
    about custom node classes that are not installed.
    """
    graph = _graph_of(flow_or_frame)
    if not name or not name.strip():
        raise NativeNodeError("register_flow needs a non-empty name")
    _warn_session_only_custom_nodes(graph, f"register_flow({name!r})")
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        if schema is None:
            default = service.ensure_python_editor_flows_namespace()
            if default is None:
                raise NativeNodeError("There is no 'General' catalog to hold the default schema; pass schema=")
            namespace_id = default.id
        else:
            namespace_id, _ = _resolve_namespace(service, schema)
        namespace_name = service.resolve_namespace_full_name(namespace_id)
        path = _registration_path(service, namespace_id, namespace_name, name, overwrite)
    graph.apply_layout()
    registration_id = register_python_editor_flow(
        graph, name=name, namespace_id=namespace_id, flow_path=str(path), user_id=get_local_user_id()
    )
    return flow_ref(registration_id=registration_id)


def _raw_data_from_schema(
    schema: Mapping[str, PolarsDataType] | Sequence[tuple[str, PolarsDataType]],
) -> input_schema.RawData:
    """Zero-row ``RawData`` typed by ``schema``; dtypes are written in full, nested ones included."""
    items = list(schema.items()) if isinstance(schema, Mapping) else list(schema)
    try:
        columns = [input_schema.MinimalFieldInfo(name=column, data_type=str(dtype)) for column, dtype in items]
    except (TypeError, ValueError) as exc:
        raise NativeNodeError(f"schema must map column names to dtypes: {exc}") from exc
    return input_schema.RawData(columns=columns, data=[[] for _ in columns])


def _raw_data_from_sample(sample: Mapping[str, Sequence[Any]] | pl.DataFrame) -> input_schema.RawData:
    try:
        frame = sample if isinstance(sample, pl.DataFrame) else pl.DataFrame(sample)
    except Exception as exc:
        raise NativeNodeError(f"sample must be a DataFrame or a dict of columns: {exc}") from exc
    return FlowDataEngine(frame).to_raw_data()


def FlowInput(
    name: str,
    *,
    schema: Mapping[str, PolarsDataType] | Sequence[tuple[str, PolarsDataType]] | None = None,
    sample: Mapping[str, Sequence[Any]] | pl.DataFrame | None = None,
    flow_graph: FlowGraph | None = None,
    description: str | None = None,
) -> FlowFrame:
    """Place a named ``flow_input`` node, an input a parent flow's ``RunFlow`` feeds, and return its frame.

    The frame is typed by ``schema`` (column name to dtype, as a dict, ``pl.Schema`` or list of
    pairs) with zero rows, or holds ``sample`` (a dict of columns or a DataFrame), which is what
    the flow reads when it runs on its own; with neither it is empty. The name must be unique
    among the graph's inputs. ``RunFlow`` orders its inputs by the order they were created in.
    """
    if schema is not None and sample is not None:
        raise NativeNodeError("FlowInput takes schema= or sample=, not both")
    refuse_parameter_as_column(schema, "FlowInput(schema=)")
    refuse_parameter_as_column(list(sample) if isinstance(sample, Mapping) else None, "FlowInput(sample=)")
    if schema is not None:
        raw_data = _raw_data_from_schema(schema)
    elif sample is not None:
        raw_data = _raw_data_from_sample(sample)
    else:
        raw_data = input_schema.RawData(columns=[], data=[])
    settings = {"input_name": name, "raw_data_format": raw_data}
    return Node("flow_input", settings=settings, description=description, flow_graph=flow_graph).output


class FlowOutput:
    """A flow output name, declared once and used by ``to_flow_output`` and ``RunFlow``.

    ``frame.to_flow_output(output)`` places the ``flow_output`` node named ``output.name``, with
    ``description`` when the call gives none; ``run[output]`` and ``run.get_output(output)`` read
    that output of a ``RunFlow``. A plain string works in both places. Equal and hashable by name.
    """

    name: str
    description: str | None

    def __init__(self, name: str, *, description: str | None = None) -> None:
        if not isinstance(name, str) or not name.strip():
            raise NativeNodeError("FlowOutput needs a non-empty name")
        self.name = name
        self.description = description

    def __repr__(self) -> str:
        return f"FlowOutput({self.name!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FlowOutput):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)


def _to_flow_output(frame: FlowFrame, name: str | FlowOutput, description: str | None = None) -> FlowFrame:
    """Place a ``flow_output`` node named ``name`` below ``frame``; ``frame`` itself is returned.

    The sink has no output handle on the canvas, so nothing may chain from it.
    """
    if isinstance(name, FlowOutput):
        description = name.description if description is None else description
        name = name.name
    refuse_parameter_as_column(name, "to_flow_output")
    Node("flow_output", frame, settings={"output_name": name}, description=description)
    return frame


def _as_flow_ref(flow: FlowRef | int | FlowGraph | FlowFrame, name: str | None) -> FlowRef:
    """The registered flow ``RunFlow`` runs; a graph or frame is registered under ``name`` first."""
    from flowfile_frame.flow_frame import FlowFrame

    if isinstance(flow, FlowGraph | FlowFrame):
        if not name:
            raise NativeNodeError(
                "RunFlow(<graph>) needs name=: the flow is registered under that name, and a re-run reuses it"
            )
        return register_flow(flow, name=name)
    if name is not None:
        raise NativeNodeError("name= only applies when flow is a FlowGraph or a FlowFrame to register")
    if isinstance(flow, FlowRef):
        return flow
    if isinstance(flow, int) and not isinstance(flow, bool):
        return flow_ref(registration_id=flow)
    raise NativeNodeError(
        f"flow must be a FlowRef, a registration id, a FlowGraph or a FlowFrame, got {type(flow).__name__}"
    )


def _interface(ref: FlowRef) -> subflow.SubflowInterface:
    try:
        return subflow.get_subflow_interface(Path(ref.flow_path))
    except Exception as exc:
        raise NativeNodeError(f"Could not read the inputs and outputs of flow {ref.name!r}: {exc}") from exc


def _bound_column(value: Any) -> str | None:
    """The column a parameter is bound to (a plain ``fl.col(...)``), or ``None`` for a constant."""
    if isinstance(value, Expr):
        value = value.expr
    if not isinstance(value, pl.Expr):
        return None
    if not value.meta.is_column():
        raise NativeNodeError(f"A parameter is bound to a plain column such as fl.col('region'), not {value}")
    return value.meta.output_name()


def _parameter_bindings(
    flow_name: str,
    specs: list[FlowParameter],
    params: Mapping[str, Any],
    param_frame: FlowFrame | None,
) -> list[input_schema.RunFlowParameterBinding]:
    """One binding per child parameter, in the child's order, as the designer writes them.

    An expression binds the parameter to a column of ``param_frame``; any other value is a
    constant, checked against the parameter's type now rather than when the flow runs. A
    ``Parameter`` or ``"${name}"`` value forwards a parameter of this flow, substituted by the
    run. An unset parameter keeps the child's default.
    """
    by_name = {spec.name: spec for spec in specs}
    unknown = sorted(set(params) - set(by_name))
    if unknown:
        raise NativeNodeError(f"Flow {flow_name!r} has no parameter(s) {unknown}; its parameters are {list(by_name)}")
    columns = param_frame.data.collect_schema().names() if param_frame is not None else []
    bindings = []
    for spec in specs:
        if spec.name not in params:
            bindings.append(input_schema.RunFlowParameterBinding(parameter_name=spec.name))
            continue
        column = _bound_column(params[spec.name])
        if column is not None:
            if param_frame is None:
                raise NativeNodeError(
                    f"Parameter {spec.name!r} is bound to column {column!r}; pass the frame holding it as param_frame="
                )
            if column not in columns:
                raise NativeNodeError(f"param_frame has no column {column!r} (parameter {spec.name!r}): {columns}")
            bindings.append(
                input_schema.RunFlowParameterBinding(parameter_name=spec.name, source="column", column_name=column)
            )
            continue
        constant = _as_parameter_string(params[spec.name])
        try:
            # A ${name} reference forwards a parent parameter; the run substitutes and checks it.
            if not find_unresolved_in_model(constant):
                coerce_param_value(spec.type, constant, spec.enum_values)
        except ValueError as exc:
            raise NativeNodeError(f"Parameter {spec.name!r} of flow {flow_name!r}: {exc}") from exc
        bindings.append(
            input_schema.RunFlowParameterBinding(parameter_name=spec.name, source="constant", constant_value=constant)
        )
    if param_frame is not None and not any(b.source == "column" for b in bindings):
        raise NativeNodeError(
            "param_frame is only read by column bindings; bind a parameter to one of its columns, "
            "e.g. params={'region': fl.col('region')}"
        )
    return bindings


class RunFlow(NativeNode):
    """A run_flow node: runs a registered flow inside this one, feeding its inputs and parameters.

    ``flow`` is a ``FlowRef``, a registration id, or a ``FlowGraph``/``FlowFrame`` that is
    registered under ``name=`` first (a re-run reuses that registration). Each keyword frame
    feeds the child input of that name. ``params`` sets child parameters: a constant, or
    ``fl.col(name)`` to read the value from ``param_frame`` (the first row, or one child run
    per row with ``iterate=True``, which appends ``param_*`` and ``run_index`` columns unless
    ``append_metadata=False``). Outputs are named after the child's outputs, in creation
    order, and read with ``run[name]`` or ``run.get_output(name)`` (a name or a ``FlowOutput``);
    a child without outputs has one, a summary row per run. Outputs are deferred: the
    child only runs when the parent flow does, or on ``collect()``, which runs it.
    """

    flow: FlowRef

    def __init__(
        self,
        flow: FlowRef | int | FlowGraph | FlowFrame,
        *,
        name: str | None = None,
        params: Mapping[str | Parameter, Any] | None = None,
        param_frame: FlowFrame | None = None,
        iterate: bool = False,
        append_metadata: bool = True,
        description: str | None = None,
        flow_graph: FlowGraph | None = None,
        **input_frames: FlowFrame,
    ) -> None:
        from flowfile_frame.flow_frame import FlowFrame

        refuse_parameter_as_column(list(input_frames), "RunFlow input slots")
        params = {_param_name(key): value for key, value in (params or {}).items()}
        self.flow = _as_flow_ref(flow, name)
        interface = _interface(self.flow)
        input_slots = [port.name for port in interface.inputs]
        output_slots = [port.name for port in interface.outputs]
        unknown = sorted(set(input_frames) - set(input_slots))
        if unknown:
            raise NativeNodeError(f"Flow {self.flow.name!r} has no input(s) {unknown}; its inputs are {input_slots}")
        not_frames = [key for key, value in input_frames.items() if not isinstance(value, FlowFrame)]
        if param_frame is not None and not isinstance(param_frame, FlowFrame):
            not_frames.append("param_frame")
        if not_frames:
            raise NativeNodeError(f"{not_frames} must be FlowFrames; wrap a Polars frame with fl.FlowFrame(...)")
        bindings = _parameter_bindings(self.flow.name, interface.parameters, params, param_frame)
        frames: list[FlowFrame] = [param_frame] if param_frame is not None else []
        handles = [input_handle(0)] if param_frame is not None else []
        for index, slot in enumerate(input_slots):
            if slot in input_frames:
                frames.append(input_frames[slot])
                handles.append(input_handle(index + 1))

        def make_settings(base: dict[str, Any]) -> input_schema.NodeRunFlow:
            try:
                return input_schema.NodeRunFlow(
                    **base,
                    flow_reference=self.flow.to_subflow_reference(),
                    input_slots=input_slots,
                    output_slots=output_slots,
                    parameter_specs=list(interface.parameters),
                    parameter_bindings=bindings,
                    iteration_mode="iterate" if iterate else "first_value",
                    append_run_metadata=append_metadata,
                )
            except ValidationError as exc:
                raise NativeNodeError(f"Invalid run_flow settings for flow {self.flow.name!r}: {exc}") from exc

        self._build(
            "run_flow",
            input_schema.NodeRunFlow,
            frames,
            make_settings,
            deferred=None,
            description=description,
            flow_graph=flow_graph,
            handles=handles,
        )

    def _seed_schemas(
        self, node: FlowNode, frames: Sequence[FlowFrame], handles: list[str]
    ) -> dict[str, list[FlowfileColumn]]:
        """Predicted schema per output handle, read from the child's file without running it.

        A child without outputs returns one summary row per run, so its single handle carries
        the run-summary columns.
        """
        settings: input_schema.NodeRunFlow = node.setting_input
        if not settings.output_slots:
            return {output_handle(0): subflow.predict_run_summary_schema(settings)}
        predicted = subflow.predict_run_flow_named_schemas(settings)
        if not predicted:
            logger.warning(
                "Could not predict the outputs of flow registration %s; they start without columns until the flow runs",
                settings.flow_reference.registration_id,
            )
        return {handle: list(predicted.get(handle, [])) for handle in handles}

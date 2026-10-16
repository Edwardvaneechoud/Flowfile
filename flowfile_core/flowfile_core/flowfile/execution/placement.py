"""Which backend runs each node of a run, and the nodes a backend cannot run.

``place`` compares what each node needs from its compute (``flow_node.requirements.requirements_for``) with what the
backend the policy assigns it declares (``Capabilities``): the operation the node runs there, the schemes it reads and
writes, the plugins its work loads and, for a node on a notebook kernel, ``file`` among the backend's write schemes,
because the kernel shared volume is a local-disk location on core's host. A Train Model node that publishes its model
and an Apply Model node that takes one from the catalog also need the scheme the artifact store's bytes travel by:
``file`` for the filesystem store, ``https`` for the S3 store's presigned URLs. A node whose backend falls short is a
``Refusal`` with the reasons in words. Stored connections are not compared: every backend resolves one with the master
key it holds. A Run Flow node is placed with the flow it runs: that flow is built for the call, the nodes its run would
run are placed on the node's backend by the same policy, and each of them refused there refuses the Run Flow node.

``SingleBackendPolicy`` puts a whole run on one backend, except the node types whose builders always run on a backend
of their own. ``MixedPlacementPolicy`` puts each node on the backend its own ``execution_location`` names, and ``place``
lists the edges between two nodes on different backends, whose data crosses through the exchange store.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import CatalogStorageTarget, locate_for_namespace
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_node.requirements import NodeRequirements, requirements_for
from flowfile_core.flowfile.param_types import coerce_param_value, stringify_param_value
from flowfile_core.schemas import input_schema
from shared.compute_protocol import Capabilities

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

_PINNED_BACKENDS: dict[str, str] = {
    "google_analytics_reader": registry.WORKER_BACKEND_ID,
}
"""Node type -> the backend its builder runs it on whatever the flow's: the Google Analytics reader
(``builders/connectors.py``) runs on the worker, since core's backend reads no Google Analytics."""


@dataclass(frozen=True)
class Refusal:
    """A node the backend assigned to it cannot run.

    Attributes:
        node_id: The node.
        backend_id: The backend it was assigned.
        reason: What the node needs that the backend does not offer, in words.
    """

    node_id: int
    backend_id: str
    reason: str


@dataclass(frozen=True)
class Placement:
    """Which backend runs each node, and the nodes refused there.

    Attributes:
        assignment: Node id -> the id of the backend that runs it.
        boundaries: The edges whose data crosses from one backend to another, as (source node id, target node id).
        refusals: The nodes their backend cannot run.
    """

    assignment: dict[int, str]
    boundaries: list[tuple[int, int]]
    refusals: list[Refusal]


@dataclass(frozen=True)
class SingleBackendPolicy:
    """Every node on ``backend_id``, except the node types whose builders pin them to a backend (``_PINNED_BACKENDS``).

    A pinned node is placed, and checked, where its builder runs it.
    """

    backend_id: str

    def backend_for(self, node: FlowNode) -> str:
        """The id of the backend ``node`` runs on."""
        return _PINNED_BACKENDS.get(node.node_type, self.backend_id)


@dataclass(frozen=True)
class MixedPlacementPolicy:
    """Each node on the backend its own ``execution_location`` names, ``backend_id`` (the run's) for ``"auto"``.

    A node type its builder pins to a backend (``_PINNED_BACKENDS``) is placed there whatever its setting says. A saved
    id is taken as it is, ``local`` included; one the registry does not know is refused when the run is placed.
    """

    backend_id: str

    def backend_for(self, node: FlowNode) -> str:
        """The id of the backend ``node`` runs on."""
        pinned = _PINNED_BACKENDS.get(node.node_type)
        if pinned is not None:
            return pinned
        location = node.setting_input.execution_location
        return self.backend_id if location == "auto" else location


PlacementPolicy = SingleBackendPolicy | MixedPlacementPolicy


def place(
    flow: FlowGraph,
    backends: Mapping[str, Capabilities],
    policy: PlacementPolicy,
    *,
    nodes: list[FlowNode] | None = None,
) -> Placement:
    """Assign each of ``nodes`` (default all of the flow's) its backend by ``policy``, refusing each it cannot run.

    ``backends`` holds the capabilities in hand of the backends the policy names for them. A node whose backend is not
    among them (one that could not be asked) is neither assigned nor refused: reaching a backend is the handshake's
    concern, and the node meets that backend's typed error when it runs. Nothing is asked of a backend; the three
    lookups are where a catalog reader's or writer's namespace keeps its tables (``locate_for_namespace``, which reads
    the namespace row and no credential), for a Train or Apply Model node, the kind of the artifact store
    (``artifacts.get_storage_backend``, whose only side effect is making the store the first time: the filesystem store
    makes its two folders, the S3 store a client) and, for a Run Flow node, the flow it runs (``_child_refusals``: its
    registration and its file, built for the call).

    ``boundaries`` lists, in the order of ``nodes``, each pair of a node and an input of it assigned to another backend,
    once; an edge with an end that is not assigned is none.
    """
    placed = flow.nodes if nodes is None else nodes
    assignment: dict[int, str] = {}
    refusals: list[Refusal] = []
    for node in placed:
        backend_id = policy.backend_for(node)
        capabilities = backends.get(backend_id)
        if capabilities is None:
            continue
        assignment[node.node_id] = backend_id
        target, namespace = _catalog_target(node)
        requirements = requirements_for(
            node.node_template, node.setting_input, catalog_target=target, artifact_scheme=_artifact_scheme(node)
        )
        reasons = _unmet(requirements, capabilities, namespace)
        if isinstance(node.setting_input, input_schema.NodeRunFlow):
            reasons += _child_refusals(flow, node, backend_id, backends, policy)
        if reasons:
            refusals.append(Refusal(node_id=node.node_id, backend_id=backend_id, reason="; ".join(reasons)))
    boundaries: list[tuple[int, int]] = []
    for node in placed:
        backend_id = assignment.get(node.node_id)
        for source, _ in node._slot_input_pairs():
            if backend_id is None or source is None:
                continue
            source_backend_id = assignment.get(source.node_id)
            edge = (source.node_id, node.node_id)
            if source_backend_id is not None and source_backend_id != backend_id and edge not in boundaries:
                boundaries.append(edge)
    return Placement(assignment=assignment, boundaries=boundaries, refusals=refusals)


def _unmet(requirements: NodeRequirements, capabilities: Capabilities, namespace: str | None) -> list[str]:
    """What ``requirements`` ask that ``capabilities`` do not offer, one phrase each.

    A catalog node (``namespace`` named) on a backend without ``file`` is refused for its namespace's local disk.
    """
    reasons = []
    operation = requirements.operation
    if operation is not None and operation.value not in capabilities.operations:
        reasons.append(f"it runs the {operation.value} operation, which this backend does not offer")
    for verb, needed, offered in (
        ("read", requirements.read_schemes, capabilities.read_schemes),
        ("write", requirements.write_schemes, capabilities.write_schemes),
    ):
        missing = needed - offered
        if namespace is not None and "file" in missing:
            reasons.append(
                f'namespace "{namespace}" is on local disk; remote execution needs an object-storage namespace'
            )
        elif missing:
            reasons.append(f"it {verb}s storage this backend does not {verb}: {', '.join(sorted(missing))}")
    missing_plugins = requirements.plugins - capabilities.plugins.keys()
    if missing_plugins:
        reasons.append(f"it needs plugins this backend does not have: {', '.join(sorted(missing_plugins))}")
    if requirements.needs_kernel and "file" not in capabilities.write_schemes:
        reasons.append(
            "it runs on a notebook kernel, which reads its inputs from the kernel shared volume on core's host, "
            "where this backend does not write"
        )
    return reasons


def _child_refusals(
    flow: FlowGraph, node: FlowNode, backend_id: str, backends: Mapping[str, Capabilities], policy: PlacementPolicy
) -> list[str]:
    """One reason per node the flow a Run Flow node runs would run that its placement on ``backend_id`` refuses.

    The flow is built for the call (``ephemeral_flow_graph``) at the depth its run would have, and the nodes
    ``_child_run_nodes`` names are placed by ``policy`` with ``backend_id`` as the run's backend, as the child's own run
    places them, so its Run Flow nodes are walked in turn. Past ``subflow.MAX_SUBFLOW_DEPTH``, and for any error while
    the flow is resolved, built or placed (logged with the node and the flow), it claims nothing, so the node fails when
    it runs with the run's own message, as it would unplaced.
    """
    # Function-local: flow_graph imports this module, and ephemeral_graph imports flow_graph.
    from flowfile_core.flowfile import subflow
    from flowfile_core.flowfile.manage.ephemeral_graph import ephemeral_flow_graph

    depth = flow._subflow_depth + 1
    if depth > subflow.MAX_SUBFLOW_DEPTH:
        return []
    settings = node.setting_input
    try:
        resolved = subflow.resolve_subflow_path(settings.flow_reference, settings.user_id)
        with ephemeral_flow_graph(resolved.path, settings.user_id) as child:
            child._subflow_depth = depth
            child_policy = dataclasses.replace(policy, backend_id=backend_id)
            placement = place(child, backends, child_policy, nodes=_child_run_nodes(child, settings))
    except Exception as e:
        logger.warning(
            "Run Flow node %s of flow %s is placed without the flow it runs (registration %s), which could not be "
            "resolved, built or placed: %s: %s",
            node.node_id,
            flow.flow_id,
            settings.flow_reference.registration_id,
            type(e).__name__,
            e,
        )
        return []
    return [
        f'node {refusal.node_id} of the flow it runs ("{resolved.name}") cannot run on backend '
        f'"{refusal.backend_id}": {refusal.reason}'
        for refusal in placement.refusals
    ]


def _child_run_nodes(child: FlowGraph, settings: input_schema.NodeRunFlow) -> list[FlowNode]:
    """The nodes of ``child`` that its run by a Run Flow node with ``settings`` runs, as ``run_graph`` plans a run.

    The child's parameters take the node's constant bindings, as ``subflow.execute_run_flow_node`` sets them, its
    parameter gates are decided on them (``_evaluate_gate_conditions``), and the execution plan leaves out the nodes
    behind a closed gate and the ones the classifier error-skips (``compute_execution_plan``). A gate on a parameter
    the node binds from a column, or whose value refers to one, is decided by each run from its row, which is not read
    before the run: it counts here as closed on both handles, so nothing behind it is placed and the child's own run
    places those nodes. A formula gate is decided when it runs, so the nodes behind one are placed, as the run places
    them. A binding to a parameter the child lacks or a constant its type refuses raises, as the run does.
    """
    # Function-local: these import flow_node, which this module names only for type checking.
    from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
    from flowfile_core.flowfile.util.skip_rules import ELSE_HANDLE, THEN_HANDLE

    parameters = {parameter.name: parameter for parameter in child.flow_settings.parameters}
    from_columns: set[str] = set()
    for binding in settings.parameter_bindings:
        parameter = parameters[binding.parameter_name]
        if binding.source == "constant":
            value = coerce_param_value(parameter.type, binding.constant_value, parameter.enum_values)
            parameter.default_value = stringify_param_value(value)
        elif binding.source == "column":
            from_columns.add(parameter.name)
    closed = child._evaluate_gate_conditions()
    for gate in child.nodes:
        condition = getattr(gate.setting_input, "gate_input", None)
        if gate.node_type != "gate" or condition is None or condition.condition_source != "parameter":
            continue
        if condition.parameter in from_columns or any(f"${{{name}}}" in condition.value for name in from_columns):
            closed[gate.node_id] = frozenset({THEN_HANDLE, ELSE_HANDLE})
    starts = child._flow_starts + child.get_implicit_starter_nodes()
    return compute_execution_plan(nodes=child.nodes, flow_starts=starts, closed_gate_handles=closed).all_nodes


def _artifact_scheme(node: FlowNode) -> str | None:
    """The scheme a Train or Apply Model node moves the artifact store's bytes by; ``None`` for every other node.

    ``https`` for the S3 store, whose upload targets and download sources are presigned URLs, else ``file`` for the
    filesystem store. A store that cannot be made (an S3 store without a bucket) answers ``None``, so the node fails
    when it runs, as it would unplaced.
    """
    if node.node_type not in ("train_model", "apply_model"):
        return None
    # Function-local, as the model builders import it: only a model node needs the artifact store.
    from flowfile_core.artifacts import get_storage_backend
    from shared.artifact_storage import S3Storage

    try:
        store = get_storage_backend()
    except ValueError:
        return None
    return "https" if isinstance(store, S3Storage) else "file"


def _catalog_target(node: FlowNode) -> tuple[CatalogStorageTarget | None, str | None]:
    """Where a catalog writer's or reader's namespace keeps its tables, and the namespace's name; else ``(None, None)``.

    A writer's namespace is the one its ``namespace_full_name`` names, else its ``namespace_id``, as the writer resolves
    it; with neither, its tables go to the local catalog tables directory of the default namespace. A reader's is its
    ``catalog_namespace_id``; a reader without one (a SQL reader, or a table picked with no namespace) claims no
    storage, its table's namespace being unknown here. A namespace whose storage cannot be located (an object-storage
    root without a connection) answers no target either, so the node fails when it runs, as it would unplaced.
    """
    settings = node.setting_input
    if isinstance(settings, input_schema.NodeCatalogWriter):
        write_settings = settings.catalog_write_settings
        with get_db_context() as db:
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            namespace_id = service.resolve_namespace_id_by_full_name(write_settings.namespace_full_name)
            if namespace_id is None:
                namespace_id = write_settings.namespace_id
            name = service.resolve_namespace_full_name(namespace_id)
    elif isinstance(settings, input_schema.NodeCatalogReader) and settings.catalog_namespace_id is not None:
        namespace_id = settings.catalog_namespace_id
        with get_db_context() as db:
            name = CatalogService(SQLAlchemyCatalogRepository(db)).resolve_namespace_full_name(namespace_id)
    else:
        return None, None
    try:
        target = locate_for_namespace(namespace_id)
    except ValueError:
        return None, None
    return target, name or ("default" if namespace_id is None else str(namespace_id))

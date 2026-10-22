"""Which backend runs each node of a run, and the nodes a backend cannot run.

``place`` compares what each node needs from its compute (``flow_node.requirements.requirements_for``) with what the
backend the policy assigns it declares (``Capabilities``): the operation the node runs there, the schemes it reads and
writes, the plugins its work loads, whether core's Python functions run there for a node whose plan may carry one and,
for a node on a notebook kernel, ``file`` among the backend's write schemes, because the kernel shared volume is a
local-disk location on core's host. What a backend's handshake found it does not offer comes with the reason
(``absent``), which the refusal names. A Train Model node that publishes its model and an Apply Model node that takes
one from the catalog also need the scheme the artifact store's bytes travel by: ``file`` for the filesystem store,
``https`` for the S3 store's presigned URLs. A node whose backend falls short is a ``Refusal`` with the reasons in
words. Stored connections are compared only on a backend whose
``credential_resolution`` is ``ambient``, a compute that reaches storage with its own identity and holds no catalog: a
cloud reader or writer, and a catalog reader or writer over an object-storage namespace, is refused there for each
stored connection it names whose ``auth_method`` is not one a plan leaves to the executor's environment
(``_AMBIENT_AUTH_METHODS``). A backend declaring ``master_key`` or ``vended`` resolves a stored connection itself. A Run
Flow node is placed with the flow it runs: that flow is built for the call, the nodes its run would run are placed on
the node's backend by the same policy, and each of them refused there refuses the Run Flow node.
An Explore Data node's ``viz_session`` is compared only on a configured worker (``worker:<name>``), its chart's session
running on the default worker where its backend has none.
Each node's needs are read from its settings with its flow's parameters in them, as the run puts them there before the
node runs (``parameter_resolver.node_parameters_resolved``), and the settings are restored after, so a path on
``${dir}`` is placed by where ``dir`` points; a node that refers to a ``${name}`` its flow does not define claims
nothing, so the run fails it with the resolver's message, and a node whose needs cannot be read from its resolved
settings is refused with the error, logged with its traceback.

``SingleBackendPolicy`` puts a whole run on one backend, except the node types whose builders always run on a backend
of their own. ``MixedPlacementPolicy`` puts each node on the backend its own ``execution_location`` names, and ``place``
lists the edges between two nodes on different backends, whose data crosses through the exchange store.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from typing import TYPE_CHECKING

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import CatalogStorageTarget, locate_for_namespace
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager import db_connections
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_node.requirements import NodeRequirements, requirements_for
from flowfile_core.flowfile.param_types import coerce_param_value, stringify_param_value
from flowfile_core.flowfile.parameter_resolver import node_parameters_resolved
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

PYTHON_FUNCTIONS = "python"
"""The name an absence map gives core's Python functions: where its reason is, a plan that carries one does not run."""

_AMBIENT_AUTH_METHODS = frozenset({"env_vars", "managed_identity"})
"""The stored-connection methods a backend reaching storage with its own identity can serve: a plan of one carries no
credential provider, the executor's environment supplying the credentials (``CloudStorageReader.get_read_credentials``),
where every other method puts a ``CredentialRefProvider`` in the plan, which only a process holding the catalog and the
master key resolves."""


@dataclass(frozen=True)
class Refusal:
    """A node the backend assigned to it cannot run.

    Attributes:
        node_id: The node.
        backend_id: The backend it was assigned.
        reason: What the node needs that the backend does not offer, in words.
        remedy: The settings that move the node to another backend, as a sentence (``remedy_for``); ``None`` for a
            node type pinned to its backend, or before the run's placement has added it.
    """

    node_id: int
    backend_id: str
    reason: str
    remedy: str | None = None


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

_FLOW_SETTING = "the flow's Execution location under Flow Settings"
_NODE_SETTING = "its Compute backend under General Settings"


def remedy_for(node: FlowNode, policy: PlacementPolicy) -> str | None:
    """The settings that move ``node`` to another backend under ``policy``, named as the designer shows them.

    With an exchange store (``MixedPlacementPolicy``) the node's own Compute backend decides, and the flow's Execution
    location too while the node's is Auto. Without one (``SingleBackendPolicy``) only the flow's decides, so a node set
    to a backend of its own is also told that its setting needs a store. A node type pinned to its backend
    (``_PINNED_BACKENDS``) has no setting that moves it: ``None``.
    """
    if node.node_type in _PINNED_BACKENDS:
        return None
    own_backend = getattr(node.setting_input, "execution_location", "auto") != "auto"
    if isinstance(policy, MixedPlacementPolicy):
        if own_backend:
            return f"To run it elsewhere, set {_NODE_SETTING}."
        return f"To run it elsewhere, set {_NODE_SETTING}, or {_FLOW_SETTING}."
    if own_backend:
        return (
            f"To run it elsewhere, set {_FLOW_SETTING}; "
            "a node's own Compute backend applies only when an exchange store is configured."
        )
    return f"To run it elsewhere, set {_FLOW_SETTING}."


def place(
    flow: FlowGraph,
    backends: Mapping[str, Capabilities],
    policy: PlacementPolicy,
    *,
    nodes: list[FlowNode] | None = None,
    absent: Mapping[str, Mapping[str, str]] | None = None,
) -> Placement:
    """Assign each of ``nodes`` (default all of the flow's) its backend by ``policy``, refusing each it cannot run.

    ``backends`` holds the capabilities in hand of the backends the policy names for them. A node whose backend is not
    among them (one that could not be asked) is neither assigned nor refused: reaching a backend is the handshake's
    concern, and the node meets that backend's typed error when it runs. ``absent`` maps a backend id to what its
    handshake found it does not offer, a plugin's name or ``PYTHON_FUNCTIONS``, and the reason, in words that follow
    "which"; a plugin a backend lacks with no reason there is one ``this backend does not have``. Nothing is asked of a
    backend; the four lookups are where a catalog reader's or writer's namespace keeps its tables
    (``locate_for_namespace``, which reads the namespace row and no credential), for a Train or Apply Model node, the
    kind of the artifact store (``artifacts.get_storage_backend``, whose only side effect is making the store the first
    time: the filesystem store makes its two folders, the S3 store a client), for a Run Flow node, the flow it runs
    (``_child_refusals``: its registration and its file, built for the call) and, only on a backend whose
    ``credential_resolution`` is ``ambient``, the ``auth_method`` of each stored connection a cloud or catalog node
    names (``_unresolvable_connections``, which reads the connection's row and decrypts nothing). An Explore Data node's
    ``viz_session`` is compared only on a configured worker (``worker:<name>``), its chart's session running on the
    default worker where its backend has none.

    Each node's needs are read with its flow's parameters in its settings, as the run puts them there before the node
    runs (``node_parameters_resolved``), and the settings are restored once they are read. A node that refers to a
    ``${name}`` its flow does not define is assigned and claims nothing, its needs being unknown rather than unmet, so
    the run fails it with the resolver's message, as it would unplaced. A node whose needs cannot be read from its
    resolved settings, whatever ``_catalog_target`` or ``requirements_for`` raises (a cloud path that is the whole
    ``${n}`` of an integer parameter, a fault in either, a catalog database error), is refused with the error's class
    and message, and a warning with the traceback goes to core's log: its needs were not checked, so it is not sent to
    a backend that may not meet them.

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
        absent_here = (absent or {}).get(backend_id, {})
        assignment[node.node_id] = backend_id
        with ExitStack() as resolved:
            try:
                resolved.enter_context(node_parameters_resolved(node))
            except ValueError:
                continue
            artifact_scheme = _artifact_scheme(node)
            try:
                target, namespace, namespace_id = _catalog_target(node)
                requirements = requirements_for(
                    node.node_template, node.setting_input, catalog_target=target, artifact_scheme=artifact_scheme
                )
            except Exception as e:
                reason = (
                    f"its settings could not be checked with the flow's parameters filled in ({type(e).__name__}: {e})"
                )
                logger.warning(
                    'Node %s of flow %s is refused on backend "%s": %s',
                    node.node_id,
                    flow.flow_id,
                    backend_id,
                    reason,
                    exc_info=True,
                )
                refusals.append(Refusal(node_id=node.node_id, backend_id=backend_id, reason=reason))
                continue
        if requirements.operation is OperationKind.viz_session and not backend_id.startswith(
            registry.CONFIGURED_WORKER_PREFIX
        ):
            # Elsewhere an Explore Data node charts on the default worker.
            requirements = dataclasses.replace(requirements, operation=None)
        reasons = _unmet(requirements, capabilities, namespace, artifact_scheme, absent_here)
        if capabilities.credential_resolution == "ambient":
            reasons += _unresolvable_connections(node, requirements.credentials, namespace_id)
        if isinstance(node.setting_input, input_schema.NodeRunFlow):
            reasons += _child_refusals(flow, node, backend_id, backends, policy, absent)
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


def _unmet(
    requirements: NodeRequirements,
    capabilities: Capabilities,
    namespace: str | None,
    artifact_scheme: str | None,
    absent: Mapping[str, str],
) -> list[str]:
    """What ``requirements`` ask that ``capabilities`` do not offer, one phrase each.

    A catalog node (``namespace`` named) on a backend without ``file`` is refused for its namespace's local disk. A
    model node (``artifact_scheme`` named) short of the artifact store's scheme is refused for the store: the publish
    of a Train Model node, the fetch of an Apply Model node, on local disk for the filesystem store. A missing plugin
    and a plan that may carry a Python function where core's do not run are refused with the reason ``absent`` gives.
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
        elif artifact_scheme is not None and artifact_scheme in missing:
            store = f"it {'publishes its model to' if verb == 'write' else 'fetches its model from'} the artifact store"
            if artifact_scheme == "file":
                reasons.append(f"{store}, which is on local disk; remote execution needs the S3 artifact store")
            else:
                reasons.append(f"{store} over {artifact_scheme}, which this backend does not {verb}")
        elif missing:
            reasons.append(f"it {verb}s storage this backend does not {verb}: {', '.join(sorted(missing))}")
    for plugin in sorted(requirements.plugins - capabilities.plugins.keys()):
        reasons.append(f"it needs {plugin}, which {absent.get(plugin, 'this backend does not have')}")
    if requirements.python_function and PYTHON_FUNCTIONS in absent:
        reasons.append(f"its plan may carry a Python function, which {absent[PYTHON_FUNCTIONS]}")
    if requirements.needs_kernel and "file" not in capabilities.write_schemes:
        reasons.append(
            "it runs on a notebook kernel, which reads its inputs from the kernel shared volume on core's host, "
            "where this backend does not write"
        )
    return reasons


def _unresolvable_connections(node: FlowNode, credentials: frozenset[str], namespace_id: int | None) -> list[str]:
    """One phrase per stored connection of ``node`` a backend reaching storage with its own identity cannot resolve.

    A cloud reader's or writer's connection is looked up for the node's user, a catalog reader's or writer's namespace
    connection for the catalog's owner, as each resolves it when it runs; the lookup is ``get_cloud_connection``'s row
    and its ``auth_method``, nothing decrypted. A name with no row claims nothing, so the node fails when it runs with
    its own message. Any other node's connections (a database, Kafka or Google Analytics connection) are not compared.
    """
    settings = node.setting_input
    cloud = isinstance(settings, input_schema.NodeCloudStorageReader | input_schema.NodeCloudStorageWriter)
    catalog = isinstance(settings, input_schema.NodeCatalogReader | input_schema.NodeCatalogWriter)
    if not credentials or not (cloud or catalog):
        return []
    reasons = []
    with get_db_context() as db:
        if cloud:
            user_id = settings.user_id
        else:
            user_id = SQLAlchemyCatalogRepository(db).get_root_namespace(namespace_id).owner_id
        for name in sorted(credentials):
            row = db_connections.get_cloud_connection(db, name, user_id)
            if row is not None and row.auth_method not in _AMBIENT_AUTH_METHODS:
                reasons.append(
                    f'connection "{name}" uses {row.auth_method}, which this backend cannot resolve; it reaches '
                    "storage with its own identity, as env_vars and managed_identity connections do"
                )
    return reasons


def _child_refusals(
    flow: FlowGraph,
    node: FlowNode,
    backend_id: str,
    backends: Mapping[str, Capabilities],
    policy: PlacementPolicy,
    absent: Mapping[str, Mapping[str, str]] | None,
) -> list[str]:
    """One reason per node the flow a Run Flow node runs would run that its placement on ``backend_id`` refuses.

    The node's settings are read with ``flow``'s parameters in them, as the run puts them there before the node runs
    (``node_parameters_resolved``), so a constant binding ``${name}`` forwards that parameter's value, except one that
    refers to a parameter each run of ``flow`` takes from a column (its ``_column_bound_parameters``, kept when ``flow``
    is itself a child walked here), whose value is not known before the run: that binding counts as a column binding.
    The flow is built for the call (``ephemeral_flow_graph``) at the depth its run would have, and the nodes
    ``_child_run_nodes`` names are placed by ``policy`` with ``backend_id`` as the run's backend, as the child's own run
    places them, so its Run Flow nodes are walked in turn, with the parameters given to the child here. Past
    ``subflow.MAX_SUBFLOW_DEPTH``, and for any error while the settings or the flow are resolved, or the flow is built
    or placed (logged with the node and the flow), it claims nothing, so the node fails when it runs with the run's own
    message, as it would unplaced.
    """
    # Function-local: flow_graph imports this module, and ephemeral_graph imports flow_graph.
    from flowfile_core.flowfile import subflow
    from flowfile_core.flowfile.manage.ephemeral_graph import ephemeral_flow_graph

    depth = flow._subflow_depth + 1
    if depth > subflow.MAX_SUBFLOW_DEPTH:
        return []
    settings = node.setting_input
    unknown = getattr(flow, "_column_bound_parameters", frozenset())
    forwarded = {
        binding.parameter_name
        for binding in settings.parameter_bindings
        if binding.source == "constant" and any(f"${{{name}}}" in binding.constant_value for name in unknown)
    }
    try:
        with node_parameters_resolved(node):
            resolved = subflow.resolve_subflow_path(settings.flow_reference, settings.user_id)
            with ephemeral_flow_graph(resolved.path, settings.user_id) as child:
                child._subflow_depth = depth
                child_policy = dataclasses.replace(policy, backend_id=backend_id)
                nodes = _child_run_nodes(child, settings, forwarded)
                placement = place(child, backends, child_policy, nodes=nodes, absent=absent)
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


def _child_run_nodes(child: FlowGraph, settings: input_schema.NodeRunFlow, forwarded: set[str]) -> list[FlowNode]:
    """The nodes of ``child`` that its run by a Run Flow node with ``settings`` runs, as ``run_graph`` plans a run.

    The child's parameters take the node's constant bindings, as ``subflow.execute_run_flow_node`` sets them, its
    parameter gates are decided on them (``_evaluate_gate_conditions``), and the execution plan leaves out the nodes
    behind a closed gate and the ones the classifier error-skips (``compute_execution_plan``). A gate on a parameter
    the node binds from a column or by a constant in ``forwarded`` (one that refers to a parameter taken from a column
    a level up), or whose value refers to one, is decided by each run from its row, which is not read before the run:
    it counts here as closed on both handles, so nothing behind it is placed and the child's own run places those
    nodes. Those parameters are kept on ``child`` as ``_column_bound_parameters``, for the walk of its own Run Flow
    nodes. A formula gate is decided when it runs, so the nodes behind one are placed, as the run places them. A
    binding to a parameter the child lacks or a constant its type refuses raises, as the run does.
    """
    # Function-local: these import flow_node, which this module names only for type checking.
    from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
    from flowfile_core.flowfile.util.skip_rules import ELSE_HANDLE, THEN_HANDLE

    parameters = {parameter.name: parameter for parameter in child.flow_settings.parameters}
    from_columns: set[str] = set()
    for binding in settings.parameter_bindings:
        parameter = parameters[binding.parameter_name]
        if binding.source == "column" or parameter.name in forwarded:
            from_columns.add(parameter.name)
        elif binding.source == "constant":
            value = coerce_param_value(parameter.type, binding.constant_value, parameter.enum_values)
            parameter.default_value = stringify_param_value(value)
    child._column_bound_parameters = frozenset(from_columns)
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


def _catalog_target(node: FlowNode) -> tuple[CatalogStorageTarget | None, str | None, int | None]:
    """Where a catalog writer's or reader's namespace keeps its tables, the namespace's name and its id.

    ``(None, None, None)`` for any other node. A writer's namespace is the one its ``namespace_full_name`` names, else
    its ``namespace_id``, as the writer resolves it; with neither, its tables go to the local catalog tables directory
    of the default namespace. A reader's is its ``catalog_namespace_id``; a reader without one (a SQL reader, or a
    table picked with no namespace) claims no storage, its table's namespace being unknown here. A namespace whose
    storage cannot be located (an object-storage root without a connection) answers no target either, so the node
    fails when it runs, as it would unplaced.
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
        return None, None, None
    try:
        target = locate_for_namespace(namespace_id)
    except ValueError:
        return None, None, None
    return target, name or ("default" if namespace_id is None else str(namespace_id)), namespace_id

"""Which backend runs each node of a run, and the nodes a backend cannot run.

``place`` compares what each node needs (``flow_node.requirements.requirements_for``) with the ``Capabilities`` of the
backend its policy assigns: the operation, the schemes read and written, the plugins, whether core's Python functions
run there, and ``file`` among the write schemes for a notebook-kernel node (the kernel shared volume is on core's
host). Train and Apply Model nodes also need the artifact store's scheme: ``file`` for the filesystem store, ``https``
for the S3 store's presigned URLs. A node its backend falls short for is a ``Refusal`` with the reasons in words.

Stored connections are compared only on a backend whose ``credential_resolution`` is ``ambient``, one that reaches
storage with its own identity and holds no catalog. A Run Flow node is placed with the flow it runs, and each node of
that flow refused there refuses it. Needs are read from a resolved copy of the settings
(``parameter_resolver.resolved_settings``), so a path on ``${dir}`` is placed by where ``dir`` points.

``SingleBackendPolicy`` puts a whole run on one backend, except the node types pinned to a backend of their own.
``MixedPlacementPolicy`` puts each node on the backend its own ``execution_location`` names. Edges between two backends
cross through the exchange store, and a node its backend cannot run goes to the policy's ``fallback_id`` when that
backend can run it (a ``Fallback``).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import CatalogStorageTarget, locate_for_namespace
from flowfile_core.configs import logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager import db_connections
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_node.requirements import NodeRequirements, requirements_for
from flowfile_core.flowfile.param_types import coerce_param_value, stringify_param_value
from flowfile_core.flowfile.parameter_resolver import resolved_settings
from flowfile_core.schemas import input_schema
from shared.compute_protocol import Capabilities

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

_PINNED_BACKENDS: dict[str, str] = {
    "google_analytics_reader": registry.WORKER_BACKEND_ID,
}
"""Node type -> the backend its builder pins it to, whatever the flow says. The Google Analytics reader runs on
the worker (``builders/connectors.py``)."""

PYTHON_FUNCTIONS = "python"
"""The absence-map key for core's Python functions: a plan that carries one does not run on that backend."""

PLUGIN_MISSING = "not installed"
"""The absence reason for a plugin the backend lacks, the handshake's and placement's default."""

SERVING_LOCAL = "this core serves; compute runs on its worker"
"""Why a process that serves (``registry.serving()``) runs no node on ``local``."""

_AMBIENT_AUTH_METHODS = frozenset({"env_vars", "managed_identity"})
"""Connection methods an ``ambient`` backend can serve: their plans carry no credential provider. Every other method
puts a ``CredentialRefProvider`` in the plan, which only a process with the catalog and the master key resolves."""


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
class Fallback:
    """A node placed on the default backend because the backend its settings name cannot run it.

    Attributes:
        node_id: The node.
        backend_id: The backend its settings name: the run's for an ``"auto"`` node, else its own.
        fallback_id: The backend it runs on instead (``MixedPlacementPolicy.fallback_id``).
        reason: What ``backend_id`` does not offer, in words.
    """

    node_id: int
    backend_id: str
    fallback_id: str
    reason: str


@dataclass(frozen=True)
class Placement:
    """Which backend runs each node, and the nodes refused there.

    Attributes:
        assignment: Node id -> the id of the backend that runs it.
        boundaries: The edges whose data crosses between backends, as (source node id, target node id).
        refusals: The nodes their backend cannot run.
        unreachable: Backend id -> the error its handshake raised. Its nodes are assigned unchecked and fail with that
            error when they run.
        fallbacks: The nodes moved to the default backend from the one their settings name.
    """

    assignment: dict[int, str]
    boundaries: list[tuple[int, int]]
    refusals: list[Refusal]
    unreachable: dict[str, Exception] = field(default_factory=dict)
    fallbacks: list[Fallback] = field(default_factory=list)


@dataclass(frozen=True)
class SingleBackendPolicy:
    """Every node on ``backend_id``, except the node types whose builders pin them to a backend (``_PINNED_BACKENDS``).

    A pinned node is placed, and checked, where its builder runs it. Nothing falls back: one run has one backend.
    """

    backend_id: str

    def backend_for(self, node: FlowNode) -> str:
        """The id of the backend ``node`` runs on."""
        return _PINNED_BACKENDS.get(node.node_type, self.backend_id)

    def fallback_for(self, node: FlowNode) -> str | None:
        """``None``: without an exchange store no node runs elsewhere."""
        return None


@dataclass(frozen=True)
class MixedPlacementPolicy:
    """Each node on the backend its own ``execution_location`` names, ``backend_id`` (the run's) for ``"auto"``.

    A node type its builder pins to a backend (``_PINNED_BACKENDS``) is placed there whatever its setting says. A saved
    id is taken as it is, ``local`` included; one the registry does not know is refused when the run is placed. A node
    the backend it is placed on cannot run goes to ``fallback_id`` when that backend can run it, a pinned type never.
    """

    backend_id: str
    fallback_id: str | None = None

    def backend_for(self, node: FlowNode) -> str:
        """The id of the backend ``node`` runs on."""
        pinned = _PINNED_BACKENDS.get(node.node_type)
        if pinned is not None:
            return pinned
        location = node.setting_input.execution_location
        return self.backend_id if location == "auto" else location

    def fallback_for(self, node: FlowNode) -> str | None:
        """The backend ``node`` goes to when the one it is placed on cannot run it; ``None`` for a pinned type."""
        return None if node.node_type in _PINNED_BACKENDS else self.fallback_id


PlacementPolicy = SingleBackendPolicy | MixedPlacementPolicy

_FLOW_SETTING = "the flow's Execution location under Flow Settings"
_NODE_SETTING = "its Compute backend under General Settings"


def remedy_for(node: FlowNode, policy: PlacementPolicy, *, fallback_id: str | None = None) -> str | None:
    """The settings that move ``node`` to another backend under ``policy``, named as the designer shows them.

    Without an exchange store (``SingleBackendPolicy``) only the flow's setting moves a node, so a node set to a backend
    of its own is told that setting needs a store. Given ``fallback_id``, the remedy adds that a store would move the
    node there. ``None`` for a pinned node type.
    """
    if node.node_type in _PINNED_BACKENDS:
        return None
    own_backend = getattr(node.setting_input, "execution_location", "auto") != "auto"
    if isinstance(policy, MixedPlacementPolicy):
        if own_backend:
            return f"To run it elsewhere, set {_NODE_SETTING}."
        return f"To run it elsewhere, set {_NODE_SETTING}, or {_FLOW_SETTING}."
    moves = f'a node its backend cannot run to the default backend "{fallback_id}" on its own'
    if own_backend:
        store = (
            f"To run it elsewhere, set {_FLOW_SETTING}; "
            "a node's own Compute backend applies only when an exchange store is configured "
            "(Settings → Execution → Backends)"
        )
        return f"{store}, which also moves {moves}." if fallback_id else f"{store}."
    if fallback_id:
        return (
            f"To run it elsewhere, set {_FLOW_SETTING}, or configure an exchange store "
            f"(Settings → Execution → Backends), which moves {moves}."
        )
    return f"To run it elsewhere, set {_FLOW_SETTING}."


def _model_elsewhere(
    flow: FlowGraph, node: FlowNode, backend_id: str, backend_of: Callable[[FlowNode], str]
) -> list[str]:
    """An Apply Model node scoring an upstream Train Model node's model runs where that model is fitted and held."""
    apply_input = node.setting_input.apply_input
    if apply_input.source != "upstream" or apply_input.upstream_node_id is None:
        return []
    train = flow.get_node(apply_input.upstream_node_id)
    if train is None or backend_of(train) == backend_id:
        return []
    return [
        f"it scores the model Train Model node {train.node_id} fits and holds on backend "
        f'"{backend_of(train)}"; place both on one backend'
    ]


def _probe_elsewhere(
    node: FlowNode,
    backend_id: str,
    backend_of: Callable[[FlowNode], str],
    requirements: NodeRequirements,
    backends: Mapping[str, Capabilities],
    absent: Mapping[str, Mapping[str, str]] | None,
) -> list[str]:
    """A formula gate checks its formula on the backend of the input it probes, which must meet the gate's needs too."""
    gate_input = getattr(node.setting_input, "gate_input", None)
    if gate_input is None or gate_input.condition_source != "formula":
        return []
    source = node.node_inputs.right_input or next(iter(node.node_inputs.main_inputs or []), None)
    probe_backend_id = None if source is None else backend_of(source)
    if probe_backend_id in (None, backend_id) or probe_backend_id not in backends:
        return []
    unmet = _unmet(requirements, backends[probe_backend_id], None, None, (absent or {}).get(probe_backend_id, {}))
    return [
        f'its formula is checked on backend "{probe_backend_id}", where node {source.node_id} runs: {reason}'
        for reason in unmet
    ]


def place(
    flow: FlowGraph,
    backends: Mapping[str, Capabilities],
    policy: PlacementPolicy,
    *,
    nodes: list[FlowNode] | None = None,
    absent: Mapping[str, Mapping[str, str]] | None = None,
    unreachable: Mapping[str, Exception] | None = None,
    log: bool = True,
) -> Placement:
    """Assign each of ``nodes`` (default all of the flow's) its backend by ``policy``, refusing each it cannot run.

    ``backends`` holds the capabilities of the backends the policy names, the fallback's (``fallback_for``) included.
    A node its backend cannot run moves to the fallback when that backend is in ``backends`` and can run it (a
    ``Fallback``). Otherwise it is refused, with what the fallback lacks added when it was checked. ``local`` in a
    process that serves runs no node (``SERVING_LOCAL``), so its nodes fall back too. The Apply Model and formula gate
    rules (``_model_elsewhere``, ``_probe_elsewhere``) read the assignment, so nodes are checked again until none
    moves: a Train Model node that falls back takes its Apply Model node along.

    A node whose backend is in ``unreachable`` is assigned unchecked and fails with that error when it runs, never
    elsewhere. A node whose backend is in neither map is neither assigned nor refused. ``absent`` maps a backend id to
    what its handshake found missing (a plugin's name or ``PYTHON_FUNCTIONS``) and the reason, worded to follow
    "which".

    Nothing is asked of a backend. The only lookups read a catalog node's namespace, the artifact store's kind (which
    makes the store on first use), a Run Flow node's flow and, on an ``ambient`` backend, the ``auth_method`` of the
    stored connections a node names, decrypting nothing.

    A node naming a ``${name}`` its flow does not define claims nothing, so the run fails it with the resolver's
    message. A node whose needs cannot be read from its resolved settings is refused with the error, and with ``log``
    a warning with the traceback goes to core's log: unchecked, it is sent to no backend.

    ``boundaries`` lists each edge between two assigned nodes on different backends once, in the order of ``nodes``.
    """
    placed = flow.nodes if nodes is None else nodes
    assignment: dict[int, str] = {}
    unreadable: dict[int, Refusal] = {}
    needs: dict[int, _Needs | None] = {}
    for node in placed:
        backend_id = policy.backend_for(node)
        if not _serving_local(backend_id) and backend_id not in backends:
            if unreachable and backend_id in unreachable:
                assignment[node.node_id] = backend_id
            continue
        assignment[node.node_id] = backend_id
        try:
            settings = resolved_settings(node)
        except ValueError:
            needs[node.node_id] = None
            continue
        artifact_scheme = _artifact_scheme(node)
        try:
            target, namespace, namespace_id = _catalog_target(settings)
            requirements = requirements_for(
                node.node_template, settings, catalog_target=target, artifact_scheme=artifact_scheme
            )
        except Exception as e:
            reason = f"its settings could not be checked with the flow's parameters filled in ({type(e).__name__}: {e})"
            if log:
                logger.warning(
                    'Node %s of flow %s is refused on backend "%s": %s',
                    node.node_id,
                    flow.flow_id,
                    backend_id,
                    reason,
                    exc_info=True,
                )
            unreadable[node.node_id] = Refusal(node_id=node.node_id, backend_id=backend_id, reason=reason)
            continue
        needs[node.node_id] = _Needs(requirements, namespace, namespace_id, artifact_scheme)

    def backend_of(other: FlowNode) -> str:
        if other.node_id in assignment:
            return assignment[other.node_id]
        model_ref = getattr(other.results, "model_ref", None)
        return model_ref.backend_id if model_ref is not None else policy.backend_for(other)

    own: dict[tuple[int, str], list[str]] = {}

    def reasons_on(node: FlowNode, backend_id: str) -> list[str]:
        key = (node.node_id, backend_id)
        if key not in own:
            own[key] = _own_reasons(flow, node, backend_id, needs[node.node_id], backends, policy, absent, log)
        reasons = list(own[key])
        node_needs = needs[node.node_id]
        if node_needs is not None and not _serving_local(backend_id):
            if isinstance(node.setting_input, input_schema.NodeApplyModel):
                reasons += _model_elsewhere(flow, node, backend_id, backend_of)
            reasons += _probe_elsewhere(node, backend_id, backend_of, node_needs.requirements, backends, absent)
        return reasons

    def usable(node: FlowNode, backend_id: str) -> str | None:
        fallback_id = policy.fallback_for(node)
        return fallback_id if fallback_id not in (None, backend_id) and fallback_id in backends else None

    fallbacks: dict[int, Fallback] = {}
    found: dict[int, list[str]] = {}
    moved = True
    while moved:
        moved = False
        for node in placed:
            if node.node_id not in needs or node.node_id in fallbacks:
                continue
            backend_id = assignment[node.node_id]
            found[node.node_id] = reasons_on(node, backend_id)
            fallback_id = usable(node, backend_id)
            if found[node.node_id] and fallback_id is not None and not reasons_on(node, fallback_id):
                assignment[node.node_id] = fallback_id
                reason = "; ".join(found[node.node_id])
                fallbacks[node.node_id] = Fallback(node.node_id, backend_id, fallback_id, reason)
                moved = True
    refusals: list[Refusal] = []
    for node in placed:
        if node.node_id in unreadable:
            refusals.append(unreadable[node.node_id])
        elif found.get(node.node_id) and node.node_id not in fallbacks:
            backend_id = assignment[node.node_id]
            reason = "; ".join(found[node.node_id])
            fallback_id = usable(node, backend_id)
            if fallback_id is not None:
                reason += f'; on the default backend "{fallback_id}": ' + "; ".join(reasons_on(node, fallback_id))
            refusals.append(Refusal(node_id=node.node_id, backend_id=backend_id, reason=reason))
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
    return Placement(
        assignment=assignment,
        boundaries=boundaries,
        refusals=refusals,
        unreachable=dict(unreachable or {}),
        fallbacks=[fallbacks[node.node_id] for node in placed if node.node_id in fallbacks],
    )


@dataclass(frozen=True)
class _Needs:
    """What a node asks of the backend that runs it, read once from its resolved settings."""

    requirements: NodeRequirements
    namespace: str | None
    namespace_id: int | None
    artifact_scheme: str | None


def _serving_local(backend_id: str) -> bool:
    """Whether ``backend_id`` is ``local`` in a process that serves, where no node runs."""
    return backend_id == registry.LOCAL_BACKEND_ID and registry.serving()


def _own_reasons(
    flow: FlowGraph,
    node: FlowNode,
    backend_id: str,
    needs: _Needs | None,
    backends: Mapping[str, Capabilities],
    policy: PlacementPolicy,
    absent: Mapping[str, Mapping[str, str]] | None,
    log: bool,
) -> list[str]:
    """What ``node`` needs that ``backend_id`` does not offer, the rules that read other nodes' placement left out.

    ``SERVING_LOCAL`` on ``local`` in a process that serves; nothing for a node whose needs are unknown (``None``).
    """
    if _serving_local(backend_id):
        return [SERVING_LOCAL]
    if needs is None:
        return []
    capabilities = backends[backend_id]
    absent_here = (absent or {}).get(backend_id, {})
    reasons = _unmet(needs.requirements, capabilities, needs.namespace, needs.artifact_scheme, absent_here)
    if capabilities.credential_resolution == "ambient":
        reasons += _unresolvable_connections(node, needs.requirements.credentials, needs.namespace_id)
    if isinstance(node.setting_input, input_schema.NodeRunFlow):
        reasons += _child_refusals(flow, node, backend_id, backends, policy, absent, log)
    return reasons


def _unmet(
    requirements: NodeRequirements,
    capabilities: Capabilities,
    namespace: str | None,
    artifact_scheme: str | None,
    absent: Mapping[str, str],
) -> list[str]:
    """What ``requirements`` ask that ``capabilities`` do not offer, one phrase each.

    A catalog node (``namespace`` named) on a backend without ``file`` is refused for its namespace's local disk, and a
    model node (``artifact_scheme`` named) short of that scheme for the artifact store. A missing plugin takes the
    reason ``absent`` gives up to its parenthesised detail, which the Backends page shows; a Python function is refused
    where ``absent`` holds core's.
    """
    reasons = []
    operation = requirements.operation
    if operation is not None and operation.value not in capabilities.operations:
        reasons.append(f"it needs the {operation.value} operation")
    for verb, needed, offered in (
        ("read", requirements.read_schemes, capabilities.read_schemes),
        ("write", requirements.write_schemes, capabilities.write_schemes),
    ):
        missing = needed - offered
        if namespace is not None and "file" in missing:
            reasons.append(f'namespace "{namespace}" is on local disk, not object storage')
        elif artifact_scheme is not None and artifact_scheme in missing:
            store = f"it {'publishes its model to' if verb == 'write' else 'fetches its model from'} the artifact store"
            if artifact_scheme == "file":
                reasons.append(f"{store}, which is on local disk, not S3")
            else:
                reasons.append(f"{store} over {artifact_scheme}")
        elif missing:
            reasons.append(f"it {verb}s {' and '.join(_storage_words(scheme) for scheme in sorted(missing))}")
    for plugin in sorted(requirements.plugins - capabilities.plugins.keys()):
        reasons.append(f"it needs {plugin} ({absent.get(plugin, PLUGIN_MISSING).split(' (', 1)[0]})")
    if requirements.python_function and PYTHON_FUNCTIONS in absent:
        reasons.append("its plan may carry a Python function, which this backend cannot run")
    if requirements.needs_kernel and "file" not in capabilities.write_schemes:
        reasons.append("it runs on a notebook kernel, which reads its inputs from core's disk")
    return reasons


def _storage_words(scheme: str) -> str:
    """A storage scheme as a refusal names it: ``file`` as local files, a URL scheme as URLs, any other as paths."""
    if scheme == "file":
        return "local files"
    if scheme in ("http", "https"):
        return f"{scheme} URLs"
    return f"{scheme}:// paths"


def _unresolvable_connections(node: FlowNode, credentials: frozenset[str], namespace_id: int | None) -> list[str]:
    """One phrase per stored connection of ``node`` a backend reaching storage with its own identity cannot resolve.

    Only cloud and catalog readers and writers are compared. Each connection is looked up for the user that resolves
    it at run time: the node's user, or the catalog's owner for a namespace connection. Nothing is decrypted. A name
    with no row claims nothing, so the node fails with its own message when it runs.
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
                    f'connection "{name}" uses {row.auth_method}, but this backend only resolves env_vars and '
                    "managed_identity connections"
                )
    return reasons


def _child_refusals(
    flow: FlowGraph,
    node: FlowNode,
    backend_id: str,
    backends: Mapping[str, Capabilities],
    policy: PlacementPolicy,
    absent: Mapping[str, Mapping[str, str]] | None,
    log: bool = True,
) -> list[str]:
    """One reason per node of the flow a Run Flow node runs that placement on ``backend_id`` refuses.

    The flow is built for the call (``ephemeral_flow_graph``) and the nodes its run would run (``_child_run_nodes``)
    are placed by ``policy`` with ``backend_id`` as the run's backend, so its own Run Flow nodes are walked in turn. A
    constant binding of ``${name}`` forwards ``flow``'s value, unless each run of ``flow`` takes ``name`` from a column
    (its ``_column_bound_parameters``): then it counts as a column binding. Past ``subflow.MAX_SUBFLOW_DEPTH``, or on
    any error (logged when ``log``), it claims nothing, so the node fails with the run's own message.
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
        bound = resolved_settings(node)
        resolved = subflow.resolve_subflow_path(bound.flow_reference, bound.user_id)
        with ephemeral_flow_graph(resolved.path, bound.user_id) as child:
            child._subflow_depth = depth
            child_policy = dataclasses.replace(policy, backend_id=backend_id)
            nodes = _child_run_nodes(child, bound, forwarded)
            placement = place(child, backends, child_policy, nodes=nodes, absent=absent, log=log)
    except Exception as e:
        if not log:
            return []
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
    """The nodes of ``child`` a Run Flow node with ``settings`` would run, planned as ``run_graph`` plans a run.

    The child's parameters take the node's constant bindings and its parameter gates are decided on them. A gate on a
    parameter bound from a column (or by a constant in ``forwarded``), or whose value refers to one, is decided by
    each run from its row: it counts as closed on both handles, and the child's own run places what is behind it. Those
    parameters are kept on ``child`` as ``_column_bound_parameters`` for the walk of its Run Flow nodes. A binding to
    a parameter the child lacks, or a constant its type refuses, raises as the run does.
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


def _catalog_target(settings: object) -> tuple[CatalogStorageTarget | None, str | None, int | None]:
    """Where the namespace of a catalog writer's or reader's ``settings`` keeps its tables, its name and its id.

    ``(None, None, None)`` for any other node's settings. A writer's namespace is its ``namespace_full_name``, else its
    ``namespace_id``; with neither, the default namespace's local catalog tables directory. A reader's is its
    ``catalog_namespace_id``; a reader without one claims no storage. A namespace whose storage cannot be located
    answers ``(None, None, None)`` too, so the node fails when it runs.
    """
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

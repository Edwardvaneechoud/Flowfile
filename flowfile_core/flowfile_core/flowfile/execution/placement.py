"""Which backend runs each node of a run, and the nodes a backend cannot run.

``place`` compares what each node needs from its compute (``flow_node.requirements.requirements_for``) with what the
backend the policy assigns it declares (``Capabilities``): the operation the node runs there, the schemes it reads and
writes, the plugins its work loads and, for a node on a notebook kernel, ``file`` among the backend's write schemes,
because the kernel shared volume is a local-disk location on core's host. A node whose backend falls short is a
``Refusal`` with the reasons in words. Stored connections are not compared: every backend resolves one with the master
key it holds.

``SingleBackendPolicy`` puts a whole run on one backend, except the node types whose builders always run on a backend
of their own.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import CatalogStorageTarget, locate_for_namespace
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_node.requirements import NodeRequirements, requirements_for
from flowfile_core.schemas import input_schema
from shared.compute_protocol import Capabilities

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

_PINNED_BACKENDS: dict[str, str] = {
    "train_model": registry.WORKER_BACKEND_ID,
    "apply_model": registry.WORKER_BACKEND_ID,
    "google_analytics_reader": registry.WORKER_BACKEND_ID,
}
"""Node type -> the backend its builder runs it on whatever the flow's: the model nodes (``builders/ml.py``) and the
Google Analytics reader (``builders/connectors.py``) run on the worker, since core's backend fits no models and reads no
Google Analytics."""


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
        boundaries: The edges whose data crosses from one backend to another; empty while one backend runs a flow.
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


def place(
    flow: FlowGraph,
    backends: Mapping[str, Capabilities],
    policy: SingleBackendPolicy,
    *,
    nodes: list[FlowNode] | None = None,
) -> Placement:
    """Assign each of ``nodes`` (default all of the flow's) its backend by ``policy``, refusing each it cannot run.

    ``backends`` holds the capabilities of every backend the policy names for them. Nothing is asked of a backend; the
    one lookup is where a catalog reader's or writer's namespace keeps its tables (``locate_for_namespace``, which
    reads the namespace row and no credential).
    """
    assignment: dict[int, str] = {}
    refusals: list[Refusal] = []
    for node in flow.nodes if nodes is None else nodes:
        backend_id = policy.backend_for(node)
        assignment[node.node_id] = backend_id
        target, namespace = _catalog_target(node)
        requirements = requirements_for(node.node_template, node.setting_input, catalog_target=target)
        reasons = _unmet(requirements, backends[backend_id], namespace)
        if reasons:
            refusals.append(Refusal(node_id=node.node_id, backend_id=backend_id, reason="; ".join(reasons)))
    return Placement(assignment=assignment, boundaries=[], refusals=refusals)


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

"""The FlowGraph class: construction, settings, node lifecycle and observers.

The concern mixins it is composed from live beside it; the package root (``__init__``) is the
import facade and the bind surface for swappable collaborators (see ``_root.py``).
"""

import datetime
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from typing import Any, Union
from uuid import uuid1

import polars as pl
from pyarrow.parquet import ParquetFile

from flowfile_core.configs import logger
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE, register_missing_node_template
from flowfile_core.flowfile.analytics.utils import create_graphic_walker_node_from_node_promise
from flowfile_core.flowfile.artifacts import ArtifactContext
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import (
    FlowDataEngine,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.flowfile.flow_graph.builders.catalog import CatalogBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.cloud_storage import CloudStorageBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.code import CodeBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.combine import CombineBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.connectors import ConnectorBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.control import ControlBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.custom_nodes import CustomNodeBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.io import (
    FileIoBuildersMixin,
)
from flowfile_core.flowfile.flow_graph.builders.ml import MlBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.subflow import SubflowBuildersMixin
from flowfile_core.flowfile.flow_graph.builders.transforms import TransformBuildersMixin
from flowfile_core.flowfile.flow_graph.canvas import CanvasMixin
from flowfile_core.flowfile.flow_graph.execution import ExecutionMixin
from flowfile_core.flowfile.flow_graph.history import (
    GraphHistoryMixin,
)
from flowfile_core.flowfile.flow_graph.persistence import PersistenceMixin
from flowfile_core.flowfile.flow_node.flow_node import (
    FlowNode,
)
from flowfile_core.flowfile.flow_node.schema_utils import create_schema_callback_with_output_config
from flowfile_core.flowfile.param_types import ParamValue, typed_parameter_values
from flowfile_core.flowfile.parameter_resolver import (
    find_unresolved_in_model,
)
from flowfile_core.flowfile.user_defined.registry import (
    missing_custom_node_error,
)
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.kernel.execution import (
    KernelHold,
)
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
)
from flowfile_core.schemas.output_model import RunInformation

# Catalog writer/reader helpers (extracted for testability)


NodeObserver = Callable[[int | str, str, Any, bool], None]


class FlowGraph(
    GraphHistoryMixin,
    CanvasMixin,
    TransformBuildersMixin,
    CombineBuildersMixin,
    ControlBuildersMixin,
    CodeBuildersMixin,
    FileIoBuildersMixin,
    SubflowBuildersMixin,
    ConnectorBuildersMixin,
    CloudStorageBuildersMixin,
    CatalogBuildersMixin,
    MlBuildersMixin,
    CustomNodeBuildersMixin,
    ExecutionMixin,
    PersistenceMixin,
):
    """A class representing a Directed Acyclic Graph (DAG) for data processing pipelines.

    It manages nodes, connections, and the execution of the entire flow.
    """

    uuid: str
    depends_on: dict[
        int,
        Union[
            ParquetFile,
            FlowDataEngine,
            "FlowGraph",
            pl.DataFrame,
        ],
    ]
    _flow_id: int
    _input_data: Union[ParquetFile, FlowDataEngine, "FlowGraph"]
    _input_cols: list[str]
    _output_cols: list[str]
    _node_db: dict[str | int, FlowNode]
    _node_ids: list[str | int]
    _results: FlowDataEngine | None = None
    cache_results: bool = False
    schema: list[FlowfileColumn] | None = None
    has_over_row_function: bool = False
    _flow_starts: list[int | str] = None
    latest_run_info: RunInformation | None = None
    start_datetime: datetime = None
    end_datetime: datetime = None
    _flow_settings: schemas.FlowSettings = None
    flow_logger: FlowLogger

    def __init__(
        self,
        flow_settings: schemas.FlowSettings | schemas.FlowGraphConfig | None = None,
        name: str = None,
        input_cols: list[str] = None,
        output_cols: list[str] = None,
        path_ref: str = None,
        input_flow: Union[ParquetFile, FlowDataEngine, "FlowGraph"] = None,
        cache_results: bool = False,
    ):
        """Initializes a new FlowGraph instance.

        Args:
            flow_settings: The configuration settings for the flow. When omitted, a standalone
                in-process graph is created: a fresh flow id, no file path, history off and
                local execution.
            name: The name of the flow.
            input_cols: A list of input column names.
            output_cols: A list of output column names.
            path_ref: An optional path to an initial data source.
            input_flow: An optional existing data object to start the flow with.
            cache_results: A global flag to enable or disable result caching.
        """
        if flow_settings is None:
            flow_id = create_unique_id()
            name = name or f"Flow_{flow_id}"
            flow_settings = schemas.FlowSettings(
                flow_id=flow_id, name=name, path="", track_history=False, execution_location="local"
            )
        elif isinstance(flow_settings, schemas.FlowGraphConfig):
            flow_settings = schemas.FlowSettings.from_flow_settings_input(flow_settings)

        self._flow_settings = flow_settings
        self.uuid = str(uuid1())
        self.start_datetime = None
        self.end_datetime = None
        self.latest_run_info = None
        self._flow_id = flow_settings.flow_id
        self.flow_logger = FlowLogger(flow_settings.flow_id)
        self._flow_starts: list[FlowNode] = []
        self._results = None
        self.schema = None
        self.has_over_row_function = False
        self._input_cols = [] if input_cols is None else input_cols
        self._output_cols = [] if output_cols is None else output_cols
        self._node_ids = []
        self._node_db = {}
        # Highest node id that ever left the canvas; with the live ids it gives `node_id_ceiling`.
        self._node_id_seq: int = 0
        # Last run's surviving-input signature per ANY-rule node (union). A gate
        # flip changes which inputs survive without changing any hash, so a
        # signature change must invalidate the node or dev-mode serves the
        # previous run's partial concat. In-memory only: a fresh graph instance
        # rotates parent_uuid, so caches are cold anyway.
        self._any_input_liveness: dict[str | int, frozenset] = {}
        # Gate id -> dead output handles of the last run_graph (parameter and formula gates).
        self._last_closed_gate_handles: dict[str | int, frozenset[str]] = {}
        # Visual node groups: organizational only, never read by the executor.
        # Membership lives on each node's setting_input.group_id; this is the box registry.
        self._groups: dict[int, schemas.GroupInformation] = {}
        self._group_id_seq: int = 0  # monotonic group-id allocator; never reuses a freed id
        self._active_group_id: int | None = None
        # Canvas comments: free text notes, organizational only, never read by the executor.
        self._comments: dict[int, schemas.CommentInformation] = {}
        self._comment_id_seq: int = 0
        # Serializes claiming flow_settings.is_running: the bare check-then-set in the
        # run entry points raced when callers arrive from non-asyncio threads.
        self._run_claim_lock = threading.Lock()
        # Serializes graph mutations, undo/redo and saves; always taken before _run_claim_lock.
        self._edit_lock = threading.RLock()
        self._transaction_depth = 0
        # Primitive graph writes made with history on but outside any transaction.
        self._untracked_writes = 0
        # Change counter every client can compare; see `revision`.
        self._revision = 0
        self._revision_lock = threading.Lock()
        self.cache_results = cache_results
        self.__name__ = name if name else "flow_" + str(id(self))
        self.depends_on = {}
        self.artifact_context = ArtifactContext()
        # Subflow recursion guards: resolved paths of every ancestor flow file and
        # this graph's nesting depth. Attributes (not contextvars) because stages
        # execute on ThreadPoolExecutor threads.
        self._subflow_ancestry: frozenset[str] = frozenset()
        self._subflow_depth: int = 0
        # The claimed run's kernel_hold and commit_sources (run_graph); a subflow's run inherits both.
        self._kernel_hold: KernelHold | None = None
        self._commit_sources: bool = True
        # Last user_id seen on any node settings (stamped by the editor routes /
        # open_flow). Lets restore_from_snapshot re-stamp the owner even when the
        # live graph is empty at undo time (snapshots intentionally omit user_id).
        self._owner_user_id: int | None = None
        self._node_observers: list[NodeObserver] = []
        # Off only on a graph `notebook_cells.seed_session` seeded, where cells re-place the seeded ports.
        self.unique_subflow_port_names = True

        from flowfile_core.flowfile.history_manager import HistoryManager
        from flowfile_core.schemas.history_schema import HistoryConfig

        history_config = HistoryConfig(enabled=flow_settings.track_history)
        self._history_manager = HistoryManager(config=history_config)

        if path_ref is not None:
            self.add_datasource(input_schema.NodeDatasource(file_path=path_ref))
        elif input_flow is not None:
            self.add_datasource(input_file=input_flow)

        # Mark the empty initial state as the saved baseline so an unmodified
        # flow is not considered dirty.
        self._history_manager.mark_saved(self)

    @property
    def flow_settings(self) -> schemas.FlowSettings:
        return self._flow_settings

    @flow_settings.setter
    def flow_settings(self, flow_settings: schemas.FlowSettings):
        if (self._flow_settings.execution_location != flow_settings.execution_location) or (
            self._flow_settings.execution_mode != flow_settings.execution_mode
        ):
            self.reset()
        else:

            def _param_state(params: list[schemas.FlowParameter]) -> dict:
                return {p.name: (p.default_value, p.type, tuple(p.enum_values or [])) for p in params}

            old_params = _param_state(self._flow_settings.parameters)
            new_params = _param_state(flow_settings.parameters)
            if old_params != new_params:
                for node in self.nodes:
                    if node.setting_input is not None and find_unresolved_in_model(node.setting_input):
                        node.reset(deep=True)
        self._flow_settings = flow_settings

    # ==================== History Management Methods ====================

    # ==================== End History Management Methods ====================

    @property
    def node_id_ceiling(self) -> int:
        """The highest node id this canvas has held; a new node numbers above it, deleted ids are never reused."""
        return max([self._node_id_seq, *(node_id for node_id in self._node_db if isinstance(node_id, int))], default=0)

    # ==================== Group Management Methods ====================
    # Groups are purely visual containers. They never affect execution; the only
    # link to a node is that node's setting_input.group_id. The group box props
    # (name/color/bounds) live in self._groups and ride along in FlowfileData.

    # ==================== End Group Management Methods ====================

    # ==================== Comment Management Methods ====================
    # Comments are free-floating canvas notes. They are not nodes and never affect
    # execution; they only ride along in FlowfileData and the VueFlow payload.

    # ==================== End Comment Management Methods ====================

    def add_node_observer(self, observer: NodeObserver) -> None:
        """Register ``observer(node_id, node_type, settings, is_new)`` for every node add or update.

        It fires once per ``add_<type>`` call (and ``add_node_promise``), after the node is in
        the graph: ``is_new`` is True when the call created the node (a type change replaces it,
        so that counts as new) and False when it updated one in place. A canvas drop therefore
        fires twice (the ``NodePromise``, then the real settings), and undo/redo rebuilds fire
        too; dedupe by node id when that matters. Observers are per graph instance, and an
        exception from one is logged and swallowed so it can never corrupt the graph.
        """
        self._node_observers.append(observer)

    def remove_node_observer(self, observer: NodeObserver) -> None:
        """Unregister an observer added with ``add_node_observer``; unknown observers are ignored."""
        if observer in self._node_observers:
            self._node_observers.remove(observer)

    @contextmanager
    def observe_nodes(self, observer: NodeObserver) -> Iterator[None]:
        """Register ``observer`` (see ``add_node_observer``) for the duration of the block."""
        self.add_node_observer(observer)
        try:
            yield
        finally:
            self.remove_node_observer(observer)

    def _notify_node_observers(self, node: FlowNode, is_new: bool) -> None:
        if not self._node_observers:
            return
        for observer in list(self._node_observers):
            try:
                observer(node.node_id, node.node_type, node.setting_input, is_new)
            except Exception:
                logger.exception(f"Node observer {observer!r} failed for node {node.node_id}")

    def add_node_to_starting_list(self, node: FlowNode) -> None:
        """Adds a node to the list of starting nodes for the flow if not already present.

        Args:
            node: The FlowNode to add as a starting node.
        """
        if node.node_id not in {self_node.node_id for self_node in self._flow_starts}:
            self._flow_starts.append(node)

    def add_node_promise(self, node_promise: input_schema.NodePromise, track_history: bool = True):
        """Adds a placeholder node to the graph that is not yet fully configured.

        Useful for building the graph structure before all settings are available.
        Automatically captures history for undo/redo support.

        Args:
            node_promise: A promise object containing basic node information.
            track_history: Whether to track this change in history (default True).
        """

        def _do_add():
            def placeholder(n: FlowNode = None):
                if n is None:
                    return FlowDataEngine()
                return n

            if node_promise.is_user_defined and node_promise.node_type not in CUSTOM_NODE_STORE:
                root().user_defined_registry.refresh()
            self.add_node_step(
                node_id=node_promise.node_id,
                node_type=node_promise.node_type,
                function=placeholder,
                setting_input=node_promise,
            )
            if node_promise.is_user_defined:
                node_needs_settings: bool
                custom_node = CUSTOM_NODE_STORE.get(node_promise.node_type)
                if custom_node is None:
                    raise ValueError(missing_custom_node_error(node_promise.node_type))
                settings_schema = custom_node.model_fields["settings_schema"].default
                node_needs_settings = settings_schema is not None and not settings_schema.is_empty()
                if not node_needs_settings:
                    user_defined_node_settings = input_schema.UserDefinedNode(settings={}, **node_promise.model_dump())
                    initialized_model = custom_node()
                    self.add_user_defined_node(
                        custom_node=initialized_model, user_defined_node_settings=user_defined_node_settings
                    )

        if track_history:
            self._execute_with_history(
                _do_add,
                HistoryActionType.ADD_NODE,
                f"Add {node_promise.node_type} node",
                node_id=node_promise.node_id,
            )
        else:
            _do_add()

    @property
    def flow_id(self) -> int:
        """Gets the unique identifier of the flow."""
        return self._flow_id

    @flow_id.setter
    def flow_id(self, new_id: int):
        """Sets the unique identifier for the flow and updates all child nodes.

        Args:
            new_id: The new flow ID.
        """
        self._flow_id = new_id
        for node in self.nodes:
            if hasattr(node.setting_input, "flow_id"):
                node.setting_input.flow_id = new_id
        self.flow_settings.flow_id = new_id

    def __repr__(self):
        """Provides the official string representation of the FlowGraph instance."""
        settings_str = "  -" + "\n  -".join(f"{k}: {v}" for k, v in self.flow_settings)
        return f"FlowGraph(\nNodes: {self._node_db}\n\nSettings:\n{settings_str}"

    def get_nodes_overview(self):
        """Gets a list of dictionary representations for all nodes in the graph."""
        output = []
        for v in self._node_db.values():
            output.append(v.get_repr())
        return output

    def remove_from_output_cols(self, columns: list[str]):
        """Removes specified columns from the list of expected output columns.

        Args:
            columns: A list of column names to remove.
        """
        cols = set(columns)
        self._output_cols = [c for c in self._output_cols if c not in cols]

    def get_node(self, node_id: int | str = None) -> FlowNode | None:
        """Retrieves a node from the graph by its ID.

        Args:
            node_id: The ID of the node to retrieve. If None, retrieves the last added node.

        Returns:
            The FlowNode object, or None if not found.
        """
        if node_id is None:
            node_id = self._node_ids[-1]
        node = self._node_db.get(node_id)
        if node is not None:
            return node

    def add_initial_node_analysis(self, node_promise: input_schema.NodePromise, track_history: bool = True):
        """Adds a data exploration/analysis node based on a node promise.

        Automatically captures history for undo/redo support.

        Args:
            node_promise: The promise representing the node to be analyzed.
            track_history: Whether to track this change in history (default True).
        """

        def _do_add():
            node_analysis = create_graphic_walker_node_from_node_promise(node_promise)
            self.add_explore_data(node_analysis)

        if track_history:
            self._execute_with_history(
                _do_add,
                HistoryActionType.ADD_NODE,
                f"Add {node_promise.node_type} node",
                node_id=node_promise.node_id,
            )
        else:
            _do_add()

    def add_dependency_on_polars_lazy_frame(self, lazy_frame: pl.LazyFrame, node_id: int):
        """Adds a special node that directly injects a Polars LazyFrame into the graph.

        Note: This is intended for backend use and will not work in the UI editor.

        Args:
            lazy_frame: The Polars LazyFrame to inject.
            node_id: The ID for the new node.
        """

        def _func():
            return FlowDataEngine(lazy_frame)

        node_promise = input_schema.NodePromise(
            flow_id=self.flow_id, node_id=node_id, node_type="polars_lazy_frame", is_setup=True
        )
        self.add_node_step(
            node_id=node_promise.node_id, node_type=node_promise.node_type, function=_func, setting_input=node_promise
        )

    @property
    def graph_has_functions(self) -> bool:
        """Checks if the graph has any nodes."""
        return len(self._node_ids) > 0

    def delete_node(self, node_id: int | str):
        """Deletes a node from the graph and updates all its connections.

        Args:
            node_id: The ID of the node to delete.

        Raises:
            Exception: If the node with the given ID does not exist.
        """
        logger.info(f"Starting deletion of node with ID: {node_id}")

        node = self._node_db.get(node_id)
        if node:
            self._note_graph_write()
            logger.info(f"Found node: {node_id}, processing deletion")
            group_id = getattr(node.setting_input, "group_id", None)

            lead_to_steps: list[FlowNode] = node.leads_to_nodes
            logger.debug(f"Node {node_id} leads to {len(lead_to_steps)} other nodes")

            if len(lead_to_steps) > 0:
                for lead_to_step in lead_to_steps:
                    logger.debug(f"Deleting input node {node_id} from dependent node {lead_to_step}")
                    lead_to_step.delete_input_node(node_id, complete=True)

            if not node.is_start:
                depends_on: list[FlowNode] = node.node_inputs.get_all_inputs()
                logger.debug(f"Node {node_id} depends on {len(depends_on)} other nodes")

                for depend_on in depends_on:
                    logger.debug(f"Removing lead_to reference {node_id} from node {depend_on}")
                    depend_on.delete_lead_to_node(node_id)

            self._node_db.pop(node_id)
            if isinstance(node_id, int):
                self._node_id_seq = max(self._node_id_seq, node_id)
            # A later node reusing this id must not inherit its start flag.
            self._flow_starts[:] = [start for start in self._flow_starts if start.node_id != node_id]
            logger.debug(f"Successfully removed node {node_id} from node_db")
            del node
            logger.info("Node object deleted")
            # Drop a group that just lost its last member (keep it if it still holds sub-groups).
            if (
                group_id is not None
                and group_id in self._groups
                and not self._member_node_ids(group_id)
                and not self._child_group_ids(group_id)
            ):
                self._groups.pop(group_id, None)
        else:
            logger.error(f"Failed to find node with id {node_id}")
            raise Exception(f"Node with id {node_id} does not exist")

    @property
    def graph_has_input_data(self) -> bool:
        """Checks if the graph has an initial input data source."""
        return self._input_data is not None

    def add_node_step(
        self,
        node_id: int | str,
        function: Callable,
        input_columns: list[str] = None,
        output_schema: list[FlowfileColumn] = None,
        node_type: str = None,
        drop_columns: list[str] = None,
        renew_schema: bool = True,
        setting_input: Any = None,
        cache_results: bool = None,
        schema_callback: Callable = None,
        input_node_ids: list[int] = None,
    ) -> FlowNode:
        """The core method for adding or updating a node in the graph.

        Args:
            node_id: The unique ID for the node.
            function: The core processing function for the node.
            input_columns: A list of input column names required by the function.
            output_schema: A predefined schema for the node's output.
            node_type: A string identifying the type of node (e.g., 'filter', 'join').
            drop_columns: A list of columns to be dropped after the function executes.
            renew_schema: If True, the schema is recalculated after execution.
            setting_input: A configuration object containing settings for the node.
            cache_results: If True, the node's results are cached for future runs.
            schema_callback: A function that dynamically calculates the output schema.
            input_node_ids: A list of IDs for the nodes that this node depends on.

        Returns:
            The created or updated FlowNode object.
        """
        self._note_graph_write()
        output_field_config = getattr(setting_input, "output_field_config", None) if setting_input else None

        logger.info(
            f"add_node_step: node_id={node_id}, node_type={node_type}, "
            f"has_setting_input={setting_input is not None}, "
            f"has_output_field_config={output_field_config is not None}, "
            f"config_enabled={output_field_config.enabled if output_field_config else False}, "
            f"has_schema_callback={schema_callback is not None}"
        )

        # IMPORTANT: Always create wrapped callback if output_field_config exists (even if enabled=False)
        # This ensures nodes like PolarsCode get a schema callback when output_field_config is defined
        if output_field_config:
            if output_field_config.enabled:
                logger.info(
                    f"add_node_step: Creating/wrapping schema_callback for node {node_id} with output_field_config "
                    f"(validation_mode={output_field_config.validation_mode_behavior}, "
                    f"{len(output_field_config.fields)} fields, "
                    f"base_callback={'present' if schema_callback else 'None'})"
                )
            else:
                logger.debug(f"add_node_step: output_field_config present for node {node_id} but disabled")

            schema_callback = create_schema_callback_with_output_config(schema_callback, output_field_config)
            logger.info(
                f"add_node_step: schema_callback {'created' if schema_callback else 'failed'} for node {node_id}"
            )

        existing_node = self.get_node(node_id)
        if existing_node is not None:
            if existing_node.node_type != node_type:
                self.delete_node(existing_node.node_id)
                existing_node = None
        if existing_node:
            input_nodes = existing_node.all_inputs
        elif input_node_ids is not None:
            input_nodes = [self.get_node(node_id) for node_id in input_node_ids]
        else:
            input_nodes = None
        if isinstance(input_columns, str):
            input_columns = [input_columns]
        if (
            input_nodes is not None
            or function.__name__ in ("placeholder", "analysis_preparation")
            or node_type in ("cloud_storage_reader", "catalog_reader", "polars_lazy_frame", "input_data")
        ):
            if not existing_node:
                node = FlowNode(
                    node_id=node_id,
                    function=function,
                    output_schema=output_schema,
                    input_columns=input_columns,
                    drop_columns=drop_columns,
                    renew_schema=renew_schema,
                    setting_input=setting_input,
                    node_type=node_type,
                    name=function.__name__,
                    schema_callback=schema_callback,
                    parent_uuid=self.uuid,
                )
            else:
                existing_node.update_node(
                    function=function,
                    output_schema=output_schema,
                    input_columns=input_columns,
                    drop_columns=drop_columns,
                    setting_input=setting_input,
                    schema_callback=schema_callback,
                )
                node = existing_node
        else:
            raise Exception("No data initialized")
        self._node_db[node_id] = node
        self._node_ids.append(node_id)
        # Give the node a callable that returns the current flow parameters so
        # that lazy schema prediction (_predicted_data_getter) can substitute
        # ${...} refs. Using a callable (rather than a copy of the dict) means
        # the node always reads the LATEST parameters, whether they were set via
        # the flow_settings.setter or mutated directly on flow_settings.parameters.
        _graph = self

        def _get_params() -> dict[str, ParamValue]:
            return typed_parameter_values(_graph.flow_settings.parameters)

        node._params_getter = _get_params
        self._notify_node_observers(node, is_new=existing_node is None)
        return node

    def add_include_cols(self, include_columns: list[str]):
        """Adds columns to both the input and output column lists.

        Args:
            include_columns: A list of column names to include.
        """
        for column in include_columns:
            if column not in self._input_cols:
                self._input_cols.append(column)
            if column not in self._output_cols:
                self._output_cols.append(column)
        return self

    @property
    def nodes(self) -> list[FlowNode]:
        """Gets a list of all FlowNode objects in the graph."""

        return list(self._node_db.values())

    # Artifact helpers

    @property
    def node_connections(self) -> list[tuple[int, int]]:
        """Computes and returns a list of all connections in the graph.

        Returns:
            A list of tuples, where each tuple is a (source_id, target_id) pair.
        """
        connections = set()
        for node in self.nodes:
            outgoing_connections = [(node.node_id, ltn.node_id) for ltn in node.leads_to_nodes]
            incoming_connections = [(don.node_id, node.node_id) for don in node.all_inputs]
            node_connections = [
                c for c in outgoing_connections + incoming_connections if (c[0] is not None and c[1] is not None)
            ]
            for node_connection in node_connections:
                if node_connection not in connections:
                    connections.add(node_connection)
        return list(connections)

    def copy_node(
        self, new_node_settings: input_schema.NodePromise, existing_setting_input: Any, node_type: str
    ) -> None:
        """Creates a copy of an existing node.

        Args:
            new_node_settings: The promise containing new settings (like ID and position).
            existing_setting_input: The settings object from the node being copied.
            node_type: The type of the node being copied.
        """
        # A custom node whose type isn't installed needs a placeholder template before
        # the promise can be placed (mirrors the flow-restore path).
        if getattr(existing_setting_input, "is_user_defined", False) and node_type not in CUSTOM_NODE_STORE:
            register_missing_node_template(node_type)
        self.add_node_promise(new_node_settings)

        if isinstance(existing_setting_input, input_schema.NodePromise):
            return

        combined_settings = combine_existing_settings_and_new_settings(existing_setting_input, new_node_settings)
        # Subflow port names must stay unique; auto-rename the copy so it doesn't collide with the source.
        if node_type == "flow_output" and isinstance(combined_settings, input_schema.NodeFlowOutput):
            combined_settings.output_name = self._unique_subflow_port_name(
                combined_settings.output_name, node_type, combined_settings.node_id
            )
        elif node_type == "flow_input" and isinstance(combined_settings, input_schema.NodeFlowInput):
            combined_settings.input_name = self._unique_subflow_port_name(
                combined_settings.input_name, node_type, combined_settings.node_id
            )
        try:
            if getattr(existing_setting_input, "is_user_defined", False):
                self._place_user_defined_node(node_type, combined_settings)
            else:
                getattr(self, f"add_{node_type}")(combined_settings)
        except Exception:
            # A failed copy must not leave the pre-added promise dangling in the graph.
            if self.get_node(new_node_settings.node_id) is not None:
                self.delete_node(new_node_settings.node_id)
            raise

    def _unique_subflow_port_name(self, desired_name: str, node_type: str, exclude_node_id: int) -> str:
        """Return a subflow port name not already used by another flow_input/flow_output node.

        Used when copying: duplicating a 'result' output yields 'result_1', 'result_2', …
        (a trailing '_<n>' is stripped first so copies of copies keep incrementing the base).
        """
        attr = "output_name" if node_type == "flow_output" else "input_name"
        taken = {
            getattr(node.setting_input, attr, None)
            for node in self.nodes
            if node.node_type == node_type and node.node_id != exclude_node_id
        }
        taken.discard(None)
        if desired_name not in taken:
            return desired_name
        base, _, suffix = desired_name.rpartition("_")
        base = base if base and suffix.isdigit() else desired_name
        counter = 1
        while f"{base}_{counter}" in taken:
            counter += 1
        return f"{base}_{counter}"


def combine_existing_settings_and_new_settings(setting_input: Any, new_settings: input_schema.NodePromise) -> Any:
    """Merges settings from an existing object with new settings from a NodePromise.

    Typically used when copying a node to apply a new ID and position.

    Args:
        setting_input: The original settings object.
        new_settings: The NodePromise with new positional and ID data.

    Returns:
        A new settings object with the merged properties.
    """
    copied_setting_input = deepcopy(setting_input)

    fields_to_update = ("node_id", "pos_x", "pos_y", "description", "flow_id")

    for field in fields_to_update:
        if hasattr(new_settings, field) and getattr(new_settings, field) is not None:
            setattr(copied_setting_input, field, getattr(new_settings, field))

    # The paste target decides group membership (None = top level), never the source node.
    if hasattr(copied_setting_input, "group_id"):
        copied_setting_input.group_id = new_settings.group_id

    # Reset node_reference to None when copying (so it defaults to df_{node_id})
    if hasattr(copied_setting_input, "node_reference"):
        copied_setting_input.node_reference = None

    return copied_setting_input

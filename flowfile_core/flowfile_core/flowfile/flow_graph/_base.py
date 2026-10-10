"""Type-only declaration of the graph surface the concern mixins share; the real state lives on `FlowGraph`."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager
    from typing import Any

    from flowfile_core.configs.flow_logger import FlowLogger
    from flowfile_core.flowfile.artifacts import ArtifactContext
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode
    from flowfile_core.flowfile.history_manager import HistoryManager
    from flowfile_core.kernel.execution import KernelHold
    from flowfile_core.schemas import input_schema, schemas
    from flowfile_core.schemas.output_model import RunInformation


class GraphMixinBase:
    """Type-only declaration of the graph surface shared by the concern mixins."""

    if TYPE_CHECKING:
        uuid: str
        __name__: str
        flow_logger: FlowLogger
        latest_run_info: RunInformation | None
        artifact_context: ArtifactContext
        unique_subflow_port_names: bool
        _flow_settings: schemas.FlowSettings
        _flow_id: int
        _node_db: dict[str | int, FlowNode]
        _node_ids: list[str | int]
        _node_id_seq: int
        _flow_starts: list[FlowNode]
        _results: Any
        _any_input_liveness: dict[str | int, frozenset]
        _last_closed_gate_handles: dict[str | int, frozenset[str]]
        _groups: dict[int, schemas.GroupInformation]
        _group_id_seq: int
        _comments: dict[int, schemas.CommentInformation]
        _comment_id_seq: int
        _run_claim_lock: threading.Lock
        _edit_lock: threading.RLock
        _transaction_depth: int
        _untracked_writes: int
        _revision: int
        _revision_lock: threading.Lock
        _subflow_ancestry: frozenset[str]
        _subflow_depth: int
        _kernel_hold: KernelHold | None
        _commit_sources: bool
        _owner_user_id: int | None
        _node_observers: list[Callable[[int | str, str, Any, bool], None]]
        _history_manager: HistoryManager
        end_datetime: Any

        @property
        def flow_settings(self) -> schemas.FlowSettings: ...

        @property
        def flow_id(self) -> int: ...

        @property
        def nodes(self) -> list[FlowNode]: ...

        @property
        def execution_location(self) -> schemas.ExecutionLocationsLiteral: ...

        @property
        def execution_mode(self) -> schemas.ExecutionModeLiteral: ...

        @property
        def node_id_ceiling(self) -> int: ...

        def get_node(self, node_id: int | str = None) -> FlowNode | None: ...

        def add_node_step(self, node_id: int | str, function: Callable, **kwargs: Any) -> FlowNode: ...

        def delete_node(self, node_id: int | str) -> None: ...

        def add_node_to_starting_list(self, node: FlowNode) -> None: ...

        def _notify_node_observers(self, node: FlowNode, is_new: bool) -> None: ...

        def _note_graph_write(self) -> None: ...

        def _bump_revision(self, kind: str) -> int: ...

        def edit_lock(self, bounded: bool = True) -> AbstractContextManager[None]: ...

        def rebuilding(self) -> Iterator[None]: ...

        def transaction(self, *args: Any, **kwargs: Any) -> AbstractContextManager[Any]: ...

        def _execute_with_history(self, *args: Any, **kwargs: Any) -> Any: ...

        def mark_as_saved(self) -> None: ...

        def reset(self) -> None: ...

        def get_flowfile_data(self) -> schemas.FlowfileData: ...

        def get_implicit_starter_nodes(self) -> list[FlowNode]: ...

        def _get_upstream_node_ids(self, node_id: int) -> list[int]: ...

        def _resolve_input_names(self, node: FlowNode | None, table_count: int) -> list[str] | None: ...

        def _member_node_ids(self, group_id: int) -> list[int]: ...

        def _child_group_ids(self, group_id: int) -> list[int]: ...

        def _recompute_group_bounds(self, group_id: int | None = None) -> None: ...

        def _serialized_comments(self) -> list[schemas.FlowfileComment]: ...

        def _execute_on_kernel(self, *args: Any, **kwargs: Any) -> Any: ...

        def _place_user_defined_node(self, *args: Any, **kwargs: Any) -> Any: ...

        def _unique_subflow_port_name(self, desired_name: str, node_type: str, exclude_node_id: int) -> str: ...

        def add_datasource(self, input_file: input_schema.NodeDatasource | input_schema.NodeManualInput) -> Any: ...

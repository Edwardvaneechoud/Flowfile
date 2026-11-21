"""Build a flow graph from a saved file for one read, outside the editor sessions.

The catalog shows a registered flow's code without opening it: the graph lives for the call and is never
handed to ``flow_file_handler``. It gets a fresh flow id because a flow's file carries the id the editor
uses, and a graph sharing that id would share, and on release tear down, the open editor copy's logger.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path

from flowfile_core.configs.flow_logger import FlowLogger, get_flow_log_file
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import schema_prefetch_blocked
from flowfile_core.flowfile.manage.io_flowfile import (
    _flowfile_data_to_flow_information,
    _load_flow_storage,
    _resolve_flow_name,
    _validate_flow_path,
    populate_graph_from_flow_information,
)
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.schemas import schemas
from shared.flow_merge.conflict import refuse_conflicted


def free_flow_id() -> int:
    """A flow id no logger instance or log file holds."""
    flow_id = create_unique_id()
    while FlowLogger.get_instance(flow_id) is not None or get_flow_log_file(flow_id).exists():
        flow_id = create_unique_id()
    return flow_id


def release_graph(graph: FlowGraph) -> None:
    """Drop what a one-call graph left behind: node caches, its flow logger and its log file."""
    with contextlib.suppress(Exception):
        graph.close_flow()
    FlowLogger.cleanup_instance(graph.flow_id)
    with contextlib.suppress(OSError):
        get_flow_log_file(graph.flow_id).unlink(missing_ok=True)


@contextlib.contextmanager
def ephemeral_flow_graph(flow_path: Path, user_id: int) -> Iterator[FlowGraph]:
    """The flow at ``flow_path`` as a built graph under a fresh id, released when the block ends.

    Source nodes start no background schema work while the graph is built (``schema_prefetch_blocked``);
    what the caller asks of the graph afterwards, such as a code export, runs as it would on an open flow.
    """
    path = _validate_flow_path(flow_path)
    info = _load_flow_storage(path)
    info.flow_settings.path = str(path)
    name = _resolve_flow_name(path, info.flow_name)
    with _ephemeral_graph(info, name, user_id) as graph:
        yield graph


@contextlib.contextmanager
def ephemeral_document_graph(document: dict, user_id: int | None = None) -> Iterator[FlowGraph]:
    """A saved flow given as data (no file) built the way :func:`ephemeral_flow_graph` builds one from a path."""
    refuse_conflicted(document, "The flow")
    info = _flowfile_data_to_flow_information(schemas.FlowfileData.model_validate(document))
    info.flow_settings.execution_location = schemas.loaded_execution_location(info.flow_settings.execution_location)
    with _ephemeral_graph(info, info.flow_name or "flow", user_id) as graph:
        yield graph


@contextlib.contextmanager
def _ephemeral_graph(info: schemas.FlowInformation, name: str, user_id: int | None) -> Iterator[FlowGraph]:
    info.flow_settings.name = name
    info.flow_name = name
    info.flow_id = info.flow_settings.flow_id = free_flow_id()
    info.flow_settings.track_history = False
    graph = FlowGraph(name=name, flow_settings=info.flow_settings)
    if user_id is not None:
        graph._owner_user_id = user_id
    owner_of = (lambda _node_id: user_id) if user_id is not None else None
    try:
        token = schema_prefetch_blocked.set(True)
        try:
            with graph.rebuilding():
                populate_graph_from_flow_information(graph, info, owner_of=owner_of)
        finally:
            schema_prefetch_blocked.reset(token)
        yield graph
    finally:
        release_graph(graph)

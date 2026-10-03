"""Run one node a notebook session built, in core, from its settings and its inputs' rows.

The kernel computes what it can; a node it cannot run (a source that needs credentials, a subflow, a gate, a
Python Script on another kernel, a read of a file the kernel cannot see) and that has no node on the canvas
comes here, ``POST /notebook/session/node_run``. Core builds the node's inputs as ``flow_input`` nodes fed the
parquet the kernel names (a typed empty frame from columns, for ``schema_only``), the way a subflow run feeds its
child, and the node itself through the graph builder every flow opens with, on a graph that lives for the call;
runs it as the canvas would, under the session's
``KernelHold`` and without committing any source's progress; and hands the rows of every live output back as
parquet in the session's results folder. Nothing writes from a cell: only :data:`HELD_NODE_TYPES` and installed
custom nodes that are not outputs run, and the answer is never cached, so a cell run again reads again.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import threading
from typing import Any

import polars as pl
from fastapi import HTTPException
from pydantic import BaseModel, Field, ValidationError

from flowfile_core.configs import node_store
from flowfile_core.configs.flow_logger import FlowLogger, get_flow_log_file
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import schema_prefetch_blocked
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_core.flowfile.manage.io_flowfile import (
    _flowfile_data_to_flow_information,
    populate_graph_from_flow_information,
)
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.kernel.execution import KernelHold
from flowfile_core.notebook.kernel_runner import _bound_flow, _results_dir, _run_outcome, _write_result
from flowfile_core.notebook.push import node_schemas, refused_nodes
from flowfile_core.notebook.validate import MAX_PAYLOAD_BYTES, host_file_paths
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import FlowfileData, FlowfileSettings
from shared._version import get_version

HELD_NODE_TYPES: frozenset[str] = frozenset(
    {
        "read",
        "list_files",
        "database_reader",
        "rest_api_reader",
        "kafka_source",
        "google_analytics_reader",
        "external_source",
        "catalog_reader",
        "cloud_storage_reader",
        "run_flow",
        "gate",
        "python_script",
    }
)
"""The built-in types a session may have core run from a cell; installed custom nodes that are not outputs too."""

_FILE_NAME = re.compile(r"^[\w\-]+\.parquet$")
_lock = threading.Lock()
_running: dict[tuple[str, int], FlowGraph] = {}


class HeldInput(BaseModel):
    """One source the held node reads: its session node id and its rows as a parquet in the session's results
    folder (the kernel's view), or only its ``columns`` (``{"name", "data_type"}``) for ``schema_only``."""

    node_id: int
    path: str | None = None
    columns: list[dict[str, str]] | None = None


class NodeRunRequest(BaseModel):
    """Body of ``POST /notebook/session/node_run``: the node as the session graph's ``FlowfileData`` lists it (file
    paths as written in the kernel), its inputs, the session's flow parameters, and whether only its columns are
    asked."""

    flow_id: int
    node: dict[str, Any]
    inputs: list[HeldInput] = Field(default_factory=list, max_length=64)
    parameters: list[dict[str, Any]] = Field(default_factory=list, max_length=256)
    schema_only: bool = False


def _refusal(node: dict) -> str | None:
    """Why ``node`` never runs from a cell: it writes, or its type is not one core runs for a session."""
    node_id, node_type = node.get("id"), node.get("type")
    settings = node.get("setting_input")
    custom = isinstance(settings, dict) and bool(settings.get("is_user_defined"))
    template = node_store.node_dict.get(node_type)
    if template is not None and (
        template.node_group == "output" or (template.custom_node and template.node_type == "output")
    ):
        return f"Node {node_id} writes when the flow runs: Push, then run the flow on the canvas."
    if custom or node_type in HELD_NODE_TYPES:
        return None
    return f"Node {node_id} ({node_type}) cannot run from a cell: Push, then it runs on the canvas."


def _input_frames(manager, flow_id: int, inputs: list[HeldInput]) -> dict[int, pl.LazyFrame]:
    """Source node id -> frame: a scan of the named parquet, which must be a file of the session's results folder,
    or a typed empty frame from the columns."""
    results = _results_dir(manager, flow_id)
    kernel_results = manager.to_kernel_path(results).rstrip("/\\")
    frames: dict[int, pl.LazyFrame] = {}
    for entry in inputs:
        if entry.path is not None:
            folder, name = os.path.split(entry.path)
            host = os.path.join(results, name)
            if folder.rstrip("/\\") != kernel_results or not _FILE_NAME.match(name) or not os.path.isfile(host):
                raise HTTPException(422, f"Input {entry.path} of node {entry.node_id} is not a file of this session")
            frames[entry.node_id] = pl.scan_parquet(host)
        elif entry.columns is not None:
            columns = [
                FlowfileColumn.from_input(c.get("name") or c["column_name"], c["data_type"]) for c in entry.columns
            ]
            frames[entry.node_id] = FlowDataEngine.create_from_schema(columns).data_frame.lazy()
        else:
            raise HTTPException(422, f"Input {entry.node_id} names neither rows nor columns")
    return frames


def _free_flow_id() -> int:
    flow_id = create_unique_id()
    while FlowLogger.get_instance(flow_id) is not None or get_flow_log_file(flow_id).exists():
        flow_id = create_unique_id()
    return flow_id


def _release(graph: FlowGraph, manager) -> None:
    """Drop what the call's graph left: node caches, its flow logger and log file, a kernel node's exchange folder."""
    with contextlib.suppress(Exception):
        graph.close_flow()
    FlowLogger.cleanup_instance(graph.flow_id)
    with contextlib.suppress(OSError):
        get_flow_log_file(graph.flow_id).unlink(missing_ok=True)
    shutil.rmtree(os.path.join(manager.shared_volume_path, str(graph.flow_id)), ignore_errors=True)


def _handles(node) -> list[str]:
    names = getattr(node.setting_input, "output_names", None) or ["main"]
    return [output_handle(index) for index in range(len(names))]


def cancel(kernel_id: str, flow_id: int) -> bool:
    """Cancel the held node run the flow's session on ``kernel_id`` is waiting on, if any."""
    with _lock:
        graph = _running.get((kernel_id, flow_id))
    if graph is None or graph._kernel_hold is None or kernel_id not in graph._kernel_hold.kernel_ids:
        return False
    graph.cancel()
    return True


def run_held_node(kernel_id: str, user, body: NodeRunRequest) -> dict:
    """Run ``body.node`` for the flow's session on ``kernel_id``, bound as ``kernel_runner._bound_flow``.

    Answers ``{"paths": {output handle: parquet path as the kernel sees it}, "closed": [handles a gate routed
    away]}``, or ``{"schemas": {handle: columns}}`` for ``schema_only`` (the last run's, the declared or the
    predicted ones, nothing run). 422 for a payload too large, an input outside the session's folder, a node the
    canvas could not hold either (``push.refused_nodes``), a type that never runs from a cell, settings that do
    not build or a run that fails; 409 as ``kernel_runner._run_outcome``.
    """
    manager, flow = _bound_flow(kernel_id, user, body.flow_id)
    if len(json.dumps(body.node, default=str)) > MAX_PAYLOAD_BYTES:
        raise HTTPException(422, f"The node is larger than {MAX_PAYLOAD_BYTES} bytes")
    frames = _input_frames(manager, flow.flow_id, body.inputs)
    settings = FlowfileSettings(
        execution_mode="Performance",
        execution_location=flow.flow_settings.execution_location,
        auto_save=False,
        source_registration_id=flow.flow_settings.source_registration_id,
        parameters=body.parameters,
    )
    data = host_file_paths(
        {
            "flowfile_version": get_version(),
            "flowfile_id": _free_flow_id(),
            "flowfile_name": f"notebook-held-{flow.flow_id}",
            "flowfile_settings": settings.model_dump(mode="json"),
            "nodes": [body.node],
        },
        manager.host_folders(kernel_id),
    )
    refused = refused_nodes(flow.get_flowfile_data().model_dump(mode="json"), data)
    message = "\n".join(dict.fromkeys(text for text, _ in refused)) or _refusal(data["nodes"][0])
    if message:
        raise HTTPException(422, message)
    try:
        flow_info = _flowfile_data_to_flow_information(FlowfileData.model_validate(data))
    except (ValidationError, ValueError) as exc:
        raise HTTPException(422, f"The node's settings cannot be read: {exc}") from exc
    flow_info.flow_settings.track_history = False
    node_id = data["nodes"][0]["id"]
    graph = FlowGraph(flow_settings=flow_info.flow_settings)
    graph._system_run = True
    graph._owner_user_id = user.id
    try:
        for source_id, frame in frames.items():
            port = input_schema.NodeFlowInput(
                flow_id=graph.flow_id, node_id=source_id, input_name=f"in_{source_id}", is_setup=True
            )
            graph.add_flow_input(port)
            graph.get_node(source_id).function = FlowDataEngine(frame)
        token = schema_prefetch_blocked.set(not body.schema_only)
        try:
            with graph.rebuilding():
                populate_graph_from_flow_information(graph, flow_info, owner_of=lambda _node_id: user.id)
        except Exception as exc:
            raise HTTPException(422, f"Node {node_id} could not be built: {exc}") from exc
        finally:
            schema_prefetch_blocked.reset(token)
        node = graph.get_node(node_id)
        if node is None or node.results.errors:
            detail = node.results.errors if node is not None else "it was not placed"
            raise HTTPException(422, f"Node {node_id} could not be built: {detail}")
        if body.schema_only:
            return {"schemas": node_schemas(node)}
        hold = KernelHold({kernel_id})
        key = (kernel_id, flow.flow_id)
        with _lock:
            _running[key] = graph
        try:
            run_info = graph.run_graph(kernel_hold=hold, commit_sources=False)
        except Exception as exc:
            raise HTTPException(422, f"Running node {node_id} on the canvas failed: {exc}") from exc
        finally:
            with _lock:
                _running.pop(key, None)
        _run_outcome(graph, node, {n.node_id for n in graph.nodes}, kernel_id, hold, run_info)
        closed = graph.last_closed_gate_handles.get(node_id, frozenset())
        paths = {}
        for handle in _handles(node):
            if handle in closed:
                continue
            result = node.get_output(handle)
            if result is None:
                raise HTTPException(422, f"Node {node_id} has no result for output {handle}")
            path = os.path.join(_results_dir(manager, flow.flow_id), f"held_{node_id}_{handle}_{graph.flow_id}.parquet")
            _write_result(graph, node_id, result, path)
            paths[handle] = manager.to_kernel_path(path)
        return {"paths": paths, "closed": sorted(closed)}
    finally:
        _release(graph, manager)

"""Helpers shared by the native node and deferred frame tests."""

import os
import tempfile
from pathlib import Path
from typing import Any

import yaml

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.manage.io_flowfile import open_flow

ROUND_TRIP_KEYS = ("nodes", "flowfile_settings", "groups", "comments")


def core_node(placed: Any) -> FlowNode:
    """The core node behind a frame or a native node (anything with ``flow_graph`` and ``node_id``)."""
    return placed.flow_graph.get_node(placed.node_id)


def results_by_id(run_info) -> dict:
    return {result.node_id: result for result in run_info.node_step_result}


def handle_into(target: Any, source_id: int) -> str:
    """The output handle of ``source_id`` that feeds ``target``'s node."""
    return core_node(target)._input_output_handles[source_id]


def round_trip(frame: Any, file_name: str) -> tuple[FlowGraph, dict]:
    """Save ``frame``'s graph, open it, save it again, and assert both YAML documents agree.

    Returns the reopened graph and the first saved document.
    """
    with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
        first = os.path.join(first_dir, file_name)
        second = os.path.join(second_dir, file_name)
        frame.save_graph(first)
        reopened = open_flow(Path(first))
        reopened.save_flow(second)
        with open(first, encoding="utf-8") as f:
            first_doc = yaml.safe_load(f)
        with open(second, encoding="utf-8") as f:
            second_doc = yaml.safe_load(f)
    for key in ROUND_TRIP_KEYS:
        assert first_doc[key] == second_doc[key], key
    return reopened, first_doc

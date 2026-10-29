"""Helpers shared by the native node and deferred frame tests."""

import os
import tempfile
from pathlib import Path
from typing import Any

import yaml

from flowfile_core.configs import settings
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.manage.io_flowfile import open_flow

ROUND_TRIP_KEYS = ("nodes", "groups", "comments")


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

    A frame graph is saved ``local``. An opened file's ``local`` loads as ``auto`` with ``FLOWFILE_COMPUTE_PROTOCOL``
    on, so the reopened graph and the second document say ``auto`` with the flag on and ``local`` with it off; every
    other setting is equal. The reopened graph is then set ``local``, so a test that runs it runs in this process, as
    a headless run with no backend named does.

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
    reopened_on = "auto" if settings.FLOWFILE_COMPUTE_PROTOCOL else "local"
    first_settings = dict(first_doc["flowfile_settings"])
    second_settings = dict(second_doc["flowfile_settings"])
    assert first_settings.pop("execution_location") == "local"
    assert reopened.execution_location == reopened_on
    assert second_settings.pop("execution_location") == reopened_on
    assert first_settings == second_settings, "flowfile_settings"
    for key in ROUND_TRIP_KEYS:
        assert first_doc[key] == second_doc[key], key
    reopened.execution_location = "local"
    return reopened, first_doc

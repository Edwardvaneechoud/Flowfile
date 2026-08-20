"""Regression tests for the worker's per-node log shipping.

The logger cache must key on (flow_id, node_id): FlowfileLogHandler freezes
flow_id at construction, so a node_id-only cache key makes any process that
serves a second flow (e.g. a pooled worker child) ship logs to the wrong flow.
"""

import pytest

from flowfile_worker.flow_logger import FlowfileLogHandler, get_worker_logger


def _http_handler(logger) -> FlowfileLogHandler:
    handlers = [h for h in logger.handlers if isinstance(h, FlowfileLogHandler)]
    assert len(handlers) == 1
    return handlers[0]


@pytest.mark.worker
def test_same_node_in_different_flows_gets_correctly_bound_handlers():
    first = get_worker_logger(flowfile_flow_id=101, flowfile_node_id=7)
    second = get_worker_logger(flowfile_flow_id=102, flowfile_node_id=7)

    assert first is not second
    assert _http_handler(first).flowfile_flow_id == 101
    assert _http_handler(second).flowfile_flow_id == 102
    assert _http_handler(second).flowfile_node_id == 7


@pytest.mark.worker
def test_anonymous_node_id_does_not_share_handlers_across_flows():
    # funcs.calculate_number_of_records logs with node_id=-1 for every flow.
    first = get_worker_logger(flowfile_flow_id=201, flowfile_node_id=-1)
    second = get_worker_logger(flowfile_flow_id=202, flowfile_node_id=-1)

    assert _http_handler(first).flowfile_flow_id == 201
    assert _http_handler(second).flowfile_flow_id == 202


@pytest.mark.worker
def test_repeated_lookup_reuses_logger_without_duplicating_handlers():
    first = get_worker_logger(flowfile_flow_id=301, flowfile_node_id="node-a")
    second = get_worker_logger(flowfile_flow_id=301, flowfile_node_id="node-a")

    assert first is second
    assert len(second.handlers) == 2  # stream + http, added exactly once

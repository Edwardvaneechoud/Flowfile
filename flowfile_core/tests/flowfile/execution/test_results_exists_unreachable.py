"""An unreachable worker is a typed error, never a cache miss that reruns the node."""

import socket

import pytest
import requests

from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.flow_node.executor import NodeExecutor
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas


@pytest.fixture
def closed_worker_url(monkeypatch) -> str:
    """Point the worker URL at a port nothing listens on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    monkeypatch.setattr(subops, "OFFLOAD_TO_WORKER", True)
    monkeypatch.setattr(subops, "WORKER_URL", url)
    return url


def test_unreachable_worker_raises_compute_unavailable(closed_worker_url):
    with pytest.raises(ComputeUnavailable) as excinfo:
        subops.results_exists("abc")
    assert closed_worker_url in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, requests.ConnectionError)


def test_worker_answering_404_is_a_cache_miss(monkeypatch):
    not_found = requests.Response()
    not_found.status_code = 404
    monkeypatch.setattr(subops, "OFFLOAD_TO_WORKER", True)
    monkeypatch.setattr(subops._worker_session, "get", lambda *args, **kwargs: not_found)
    assert subops.results_exists("abc") is False


def test_cached_node_fails_instead_of_running_in_core(closed_worker_url, monkeypatch):
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=9101, name="unreachable", path=".", execution_location="remote"))
    graph = handler.get_flow(9101)
    graph.add_node_promise(input_schema.NodePromise(flow_id=9101, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=9101,
            node_id=1,
            cache_results=True,
            raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]),
        )
    )
    node = graph.get_node(1)

    calls = {"function": 0, "strategy": 0}
    original_function = node._function
    original_strategy = NodeExecutor._execute_with_strategy

    def counting_function(*args, **kwargs):
        calls["function"] += 1
        return original_function(*args, **kwargs)

    def counting_strategy(self, *args, **kwargs):
        calls["strategy"] += 1
        return original_strategy(self, *args, **kwargs)

    monkeypatch.setattr(node, "_function", counting_function)
    monkeypatch.setattr(NodeExecutor, "_execute_with_strategy", counting_strategy)

    run_info = graph.run_graph()

    assert graph.flow_settings.execution_location == "remote"
    assert run_info.success is False
    result = next(r for r in run_info.node_step_result if r.node_id == 1)
    assert result.success is False
    assert closed_worker_url in result.error
    assert node.results.errors == result.error
    assert node._last_exception_class == "ComputeUnavailable"
    assert calls == {"function": 0, "strategy": 0}
    assert node.results.resulting_data is None

"""The agent's post-apply observation reads a node's rows through the backend that holds its result."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from flowfile_core.ai.agents import live_observation
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from tests.flowfile.conftest import add_test_manual_input, create_test_graph

ORDERS = [
    {"order_id": 1, "region": "EU"},
    {"order_id": 2, "region": "US"},
    {"order_id": 3, "region": "EU"},
    {"order_id": 4, "region": "US"},
]


def _never_collected(*args, **kwargs):
    raise AssertionError("the observation collected the node's engine in core")


def test_a_performance_observation_reads_the_preview_the_fetch_took_on_its_backend(tmp_path, monkeypatch) -> None:
    flow = create_test_graph(31, execution_mode="Performance", path=str(tmp_path / "flow.yaml"))
    add_test_manual_input(flow, ORDERS)
    sampled: list[tuple[str, str, int]] = []
    real_sample = EmbeddedWorkerBackend.sample

    def recording_sample(self, ref, n):
        sampled.append((self.id, ref.result_id, n))
        return real_sample(self, ref, n)

    monkeypatch.setattr(EmbeddedWorkerBackend, "sample", recording_sample)
    monkeypatch.setattr(FlowDataEngine, "collect", _never_collected)

    result = asyncio.run(live_observation.observe_after_apply(flow, 1))

    assert result.success, result.error_message
    assert result.sample_rows == ORDERS[:3]
    ref = flow.get_node(1).results.result_ref
    # the fetch's preview grid is the one sample; the observation reads its rows from it
    assert sampled == [(registry.LOCAL_BACKEND_ID, ref.result_id, 100)]


def test_a_node_holding_no_preview_is_sampled_on_its_backend(tmp_path, monkeypatch) -> None:
    flow = create_test_graph(32, execution_mode="Performance", path=str(tmp_path / "flow.yaml"))
    add_test_manual_input(flow, ORDERS)
    sampled: list[tuple[str, str, int]] = []
    real_sample = EmbeddedWorkerBackend.sample

    def recording_sample(self, ref, n):
        sampled.append((self.id, ref.result_id, n))
        return real_sample(self, ref, n)

    monkeypatch.setattr(EmbeddedWorkerBackend, "sample", recording_sample)
    monkeypatch.setattr(FlowDataEngine, "collect", _never_collected)
    flow.trigger_fetch_node(1)
    node = flow.get_node(1)
    node.results.example_data_generator = None
    sampled.clear()

    assert live_observation._extract_sample_rows(node) == ORDERS[:3]
    assert sampled == [(registry.LOCAL_BACKEND_ID, node.results.result_ref.result_id, 3)]


def test_a_node_holding_no_reference_gives_no_rows() -> None:
    engine = SimpleNamespace(collect=_never_collected)
    node = SimpleNamespace(results=SimpleNamespace(result_ref=None, resulting_data=engine))

    assert live_observation._extract_sample_rows(node) == []

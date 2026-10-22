"""The agent's post-apply observation reads a node's rows through the backend that holds its result."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from flowfile_core.ai.agents import live_observation
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas


def _performance_flow(path: str) -> FlowGraph:
    flow = FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=31, execution_mode="Performance", execution_location="local", path=path
        ),
        name="live_observation_sample",
    )
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=31,
            node_id=1,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="region", data_type="String"),
                ],
                data=[[1, 2, 3, 4], ["EU", "US", "EU", "US"]],
            ),
        )
    )
    return flow


def _never_collected(*args, **kwargs):
    raise AssertionError("the observation collected the node's engine in core")


def test_a_performance_observation_samples_the_node_s_result_on_its_backend(tmp_path, monkeypatch) -> None:
    flow = _performance_flow(str(tmp_path / "flow.yaml"))
    sampled: list[tuple[str, str, int]] = []
    real_sample = EmbeddedWorkerBackend.sample

    def recording_sample(self, ref, n):
        sampled.append((self.id, ref.result_id, n))
        return real_sample(self, ref, n)

    monkeypatch.setattr(EmbeddedWorkerBackend, "sample", recording_sample)
    monkeypatch.setattr(FlowDataEngine, "collect", _never_collected)

    result = asyncio.run(live_observation.observe_after_apply(flow, 1))

    assert result.success, result.error_message
    assert result.sample_rows == [
        {"order_id": 1, "region": "EU"},
        {"order_id": 2, "region": "US"},
        {"order_id": 3, "region": "EU"},
    ]
    ref = flow.get_node(1).results.result_ref
    assert sampled == [(registry.LOCAL_BACKEND_ID, ref.result_id, 3)]


def test_a_node_holding_no_reference_gives_no_rows() -> None:
    engine = SimpleNamespace(collect=_never_collected)
    node = SimpleNamespace(results=SimpleNamespace(result_ref=None, resulting_data=engine))

    assert live_observation._extract_sample_rows(node) == []

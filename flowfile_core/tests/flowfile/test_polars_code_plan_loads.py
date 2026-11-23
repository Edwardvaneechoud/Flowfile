"""A saved Polars Code step holding a pickled plan stops with the sandbox's message on a server (GHSA-j7qm-5jvr-886f)."""

import base64

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from shared.polars_code_parser import PLAN_LOAD_REFUSED_MESSAGE

FIRED: list[str] = []


def _fire():
    FIRED.append("fired")


class _Payload:
    def __reduce__(self):
        return (_fire, ())

    def __call__(self, frame):
        return frame


def _saved_fallback_step() -> str:
    """The three lines flowfile_frame wrote for a function it could not spell as code."""
    blob = base64.b64encode(pl.LazyFrame({"a": [1]}).map_batches(_Payload()).serialize()).decode("ascii")
    return "\n".join(
        [
            f"serialized_value = base64.b64decode('{blob}')",
            "buffer = BytesIO(serialized_value)",
            "output_df = pl.LazyFrame.deserialize(buffer)",
        ]
    )


def _graph_with(code: str):
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name="saved_plan", path=".", execution_location="local"))
    graph = handler.get_flow(1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]))
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=1,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph


@pytest.fixture(autouse=True)
def _reset_fired():
    FIRED.clear()
    yield
    FIRED.clear()


@pytest.mark.parametrize("mode", ["docker", "server"])
def test_a_saved_pickled_plan_fails_with_the_sandbox_message(monkeypatch, mode):
    monkeypatch.setenv("FLOWFILE_MODE", mode)
    graph = _graph_with(_saved_fallback_step())

    run_info = graph.run_graph()

    assert not run_info.success
    failed = [step for step in run_info.node_step_result if step.node_id == 2]
    assert failed and not failed[0].success
    assert PLAN_LOAD_REFUSED_MESSAGE in (failed[0].error or "")
    assert FIRED == []

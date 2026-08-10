"""Tests for the sample node's first / random / random_fraction methods.

Run with:
    pytest flowfile_core/tests/flowfile/test_sample_node.py -v
"""
import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas

ROW_COUNT = 200


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="test_sample_flow",
            path=".",
            execution_mode="Development",
            execution_location="local",
        )
    )
    return handler.get_flow(flow_id)


def build_sample_flow(flow_id: int = 1, **sample_kwargs) -> FlowGraph:
    """A manual-input node of ROW_COUNT rows feeding one sample node (id 2)."""
    graph = create_graph(flow_id)
    data = [{"a": i, "b": f"row_{i}"} for i in range(ROW_COUNT)]
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(data),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="sample"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_sample(
        input_schema.NodeSample(flow_id=flow_id, node_id=2, depending_on_id=1, **sample_kwargs)
    )
    return graph


def sampled_values(graph: FlowGraph) -> list[int]:
    df = graph.get_node(2).get_resulting_data().data_frame
    collected = df.collect() if isinstance(df, pl.LazyFrame) else df
    return collected["a"].to_list()


def test_default_method_is_first_and_takes_the_head():
    """Settings without sample_method keep the pre-existing head behaviour."""
    settings = input_schema.NodeSample(flow_id=1, node_id=2, depending_on_id=1, sample_size=5)
    assert settings.sample_method == "first"
    assert sampled_values(build_sample_flow(sample_size=5)) == list(range(5))


def test_random_takes_exactly_n_rows_but_not_the_head():
    values = sampled_values(build_sample_flow(sample_method="random", sample_size=10, seed=42))
    assert len(values) == 10
    assert len(set(values)) == 10
    assert values != list(range(10))
    assert all(0 <= v < ROW_COUNT for v in values)


def test_random_preserves_original_row_order():
    values = sampled_values(build_sample_flow(sample_method="random", sample_size=25, seed=3))
    assert values == sorted(values)


def test_random_is_reproducible_for_a_fixed_seed():
    first = sampled_values(build_sample_flow(flow_id=1, sample_method="random", sample_size=10, seed=99))
    second = sampled_values(build_sample_flow(flow_id=2, sample_method="random", sample_size=10, seed=99))
    other = sampled_values(build_sample_flow(flow_id=3, sample_method="random", sample_size=10, seed=100))
    assert first == second
    assert first != other


def test_random_sample_larger_than_frame_returns_everything():
    values = sampled_values(build_sample_flow(sample_method="random", sample_size=ROW_COUNT * 5, seed=1))
    assert len(values) == ROW_COUNT


def test_random_fraction_keeps_that_share_of_rows():
    values = sampled_values(build_sample_flow(sample_method="random_fraction", fraction=25.0, seed=5))
    assert len(values) == ROW_COUNT // 4
    assert len(set(values)) == len(values)


def test_fraction_must_be_a_valid_percentage():
    for bad in (0, -1, 100.1):
        with pytest.raises(ValueError):
            input_schema.NodeSample(
                flow_id=1, node_id=2, depending_on_id=1, sample_method="random_fraction", fraction=bad
            )
    input_schema.NodeSample(
        flow_id=1, node_id=2, depending_on_id=1, sample_method="random_fraction", fraction=100.0
    )


def test_sample_size_zero_still_loads_for_flows_saved_before_this_feature():
    """The old UI allowed 0, so existing saved flows must keep deserializing."""
    settings = input_schema.NodeSample(flow_id=1, node_id=2, depending_on_id=1, sample_size=0)
    assert settings.sample_size == 0


def test_descriptions_name_the_method():
    def describe(**kwargs) -> str:
        return input_schema.NodeSample(flow_id=1, node_id=2, depending_on_id=1, **kwargs).get_default_description()

    assert describe(sample_size=100) == "First 100 rows"
    assert describe(sample_method="random", sample_size=100) == "Random 100 rows"
    assert describe(sample_method="random_fraction", fraction=12.5) == "Random 12.5% of rows"


@pytest.mark.parametrize(
    "sample_kwargs",
    [
        {"sample_size": 10},
        {"sample_method": "random", "sample_size": 10, "seed": 1},
        {"sample_method": "random_fraction", "fraction": 10.0, "seed": 1},
    ],
    ids=["first", "random", "random_fraction"],
)
def test_sampling_never_collects_in_core(sample_kwargs, monkeypatch):
    """Core ships plans, not frames — no sampling method may materialise one."""
    graph = build_sample_flow(**sample_kwargs)
    original = pl.LazyFrame.collect
    calls = []

    def spy(self, *args, **kwargs):
        calls.append(1)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", spy)
    result = graph.get_node(2).get_resulting_data()
    monkeypatch.undo()

    assert calls == []
    assert isinstance(result.data_frame, pl.LazyFrame)


@pytest.mark.parametrize(
    "kwargs,expected",
    [({"n": 10}, 10), ({"fraction": 0.1}, 20)],
    ids=["n", "fraction"],
)
def test_random_sample_engine_method(kwargs, expected):
    fde = FlowDataEngine(pl.LazyFrame({"a": range(ROW_COUNT)}))
    result = fde.random_sample(seed=1, **kwargs)
    assert isinstance(result.data_frame, pl.LazyFrame)
    assert result.data_frame.collect().height == expected


def test_random_sample_requires_exactly_one_of_n_or_fraction():
    fde = FlowDataEngine(pl.LazyFrame({"a": range(10)}))
    with pytest.raises(ValueError):
        fde.random_sample()
    with pytest.raises(ValueError):
        fde.random_sample(n=5, fraction=0.5)

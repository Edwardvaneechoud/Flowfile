"""Core-side tests for the Evaluate Model node.

The node computation is pure polars — no worker round-trip — so these tests
exercise the full ``_func`` path along with the schema callback and validation
errors. Metric correctness lives in :mod:`shared.tests.test_ml_metrics`.
"""

from typing import Literal

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas


def _make_graph(flow_id: int = 5151) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="evaluate_model_test_flow",
            path=".",
            execution_mode="Development",
        )
    )
    return handler.get_flow(flow_id)


def _seed_input_with_predictions(graph: FlowGraph, node_id: int = 1):
    """Manual input mimicking the output of an Apply Model node."""
    promise = input_schema.NodePromise(
        flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"
    )
    graph.add_node_promise(promise)
    payload = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(
            [
                {"y": 1.0, "prediction": 1.1},
                {"y": 2.0, "prediction": 1.9},
                {"y": 3.0, "prediction": 3.2},
                {"y": 4.0, "prediction": 3.8},
                {"y": 5.0, "prediction": 5.1},
            ]
        ),
    )
    graph.add_manual_input(payload)


def _wire(graph: FlowGraph, node_type: str, node_id: int, upstream_id: int):
    promise = input_schema.NodePromise(
        flow_id=graph.flow_id, node_id=node_id, node_type=node_type
    )
    graph.add_node_promise(promise)
    add_connection(
        graph, input_schema.NodeConnection.create_from_simple_input(upstream_id, node_id)
    )


def test_evaluate_model_schema_is_metric_value():
    graph = _make_graph()
    _seed_input_with_predictions(graph, node_id=1)
    _wire(graph, "evaluate_model", node_id=2, upstream_id=1)

    settings = input_schema.NodeEvaluateModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        evaluate_input=input_schema.EvaluateModelSettings(
            actual_column="y", predicted_column="prediction"
        ),
    )
    graph.add_evaluate_model(settings)

    schema = graph.get_node(2).schema
    assert [c.column_name for c in schema] == ["metric", "value"]
    assert {c.data_type for c in schema} == {"String", "Float64"}


def test_evaluate_model_emits_regression_metrics():
    graph = _make_graph()
    _seed_input_with_predictions(graph, node_id=1)
    _wire(graph, "evaluate_model", node_id=2, upstream_id=1)

    settings = input_schema.NodeEvaluateModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        evaluate_input=input_schema.EvaluateModelSettings(
            actual_column="y", predicted_column="prediction"
        ),
    )
    graph.add_evaluate_model(settings)

    result = graph.get_node(2).get_resulting_data()
    df: pl.DataFrame = result.data_frame.collect()
    metrics = dict(zip(df["metric"].to_list(), df["value"].to_list(), strict=True))
    assert set(metrics) == {"mae", "mse", "rmse", "r2", "mape", "n"}
    assert metrics["n"] == 5.0
    assert metrics["mae"] == pytest.approx(0.14, rel=1e-6)
    assert metrics["r2"] > 0.98


def test_evaluate_model_requires_actual_column():
    graph = _make_graph()
    _seed_input_with_predictions(graph, node_id=1)
    _wire(graph, "evaluate_model", node_id=2, upstream_id=1)

    settings = input_schema.NodeEvaluateModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        evaluate_input=input_schema.EvaluateModelSettings(
            actual_column="", predicted_column="prediction"
        ),
    )
    graph.add_evaluate_model(settings)

    with pytest.raises(Exception, match="actual_column"):
        graph.get_node(2).get_resulting_data()


def test_evaluate_model_rejects_missing_column():
    graph = _make_graph()
    _seed_input_with_predictions(graph, node_id=1)
    _wire(graph, "evaluate_model", node_id=2, upstream_id=1)

    settings = input_schema.NodeEvaluateModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        evaluate_input=input_schema.EvaluateModelSettings(
            actual_column="does_not_exist", predicted_column="prediction"
        ),
    )
    graph.add_evaluate_model(settings)

    with pytest.raises(Exception, match="does_not_exist"):
        graph.get_node(2).get_resulting_data()


def test_evaluate_model_auto_task_type_uses_upstream_trainer():
    """task_type='auto' with an upstream train node resolves via the trainer registry."""
    graph = _make_graph()
    _seed_input_with_predictions(graph, node_id=1)

    _wire(graph, "train_model", node_id=10, upstream_id=1)
    graph.add_train_model(
        input_schema.NodeTrainModel(
            flow_id=graph.flow_id,
            node_id=10,
            depending_on_id=1,
            train_input=input_schema.TrainModelSettings(
                target_column="y",
                feature_columns=["prediction"],
                model_type="linear_regression",
                params={"add_bias": True},
            ),
        )
    )

    _wire(graph, "evaluate_model", node_id=20, upstream_id=1)
    graph.add_evaluate_model(
        input_schema.NodeEvaluateModel(
            flow_id=graph.flow_id,
            node_id=20,
            depending_on_id=1,
            evaluate_input=input_schema.EvaluateModelSettings(
                actual_column="y",
                predicted_column="prediction",
                task_type="auto",
                upstream_train_node_id=10,
            ),
        )
    )

    df = graph.get_node(20).get_resulting_data().data_frame.collect()
    assert "mae" in df["metric"].to_list()


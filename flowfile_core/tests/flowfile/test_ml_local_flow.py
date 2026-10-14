"""Train Model and Apply Model on a local flow run in core, never on the worker.

A graph built with ``flowfile_frame`` (and every headless run) is local, and a script that never started a
worker must still train and score. The worker fetchers are replaced with ones that fail the test, so a local
flow that reaches for the worker fails here.
"""

import hashlib

import polars as pl
import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_graph.builders.ml import ml_flow_model_path
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas


class _WorkerRefused:
    def __init__(self, *args, **kwargs):
        raise AssertionError("a local flow called the worker")


@pytest.fixture
def no_worker(monkeypatch):
    monkeypatch.setattr("flowfile_core.flowfile.flow_graph.MLTrainFetcher", _WorkerRefused)
    monkeypatch.setattr("flowfile_core.flowfile.flow_graph.MLApplyFetcher", _WorkerRefused)


def _local_graph(flow_id: int) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="ml_local_flow", path=""))
    graph = handler.get_flow(flow_id)
    graph.execution_location = "local"
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(
                [{"x1": 1.0, "x2": 2.0, "y": 5.0}, {"x1": 3.0, "x2": 4.0, "y": 13.0}]
            ),
        )
    )
    return graph


def _wire(graph: FlowGraph, node_type: str, node_id: int, upstream_id: int) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(upstream_id, node_id))


def _train_settings(graph: FlowGraph, **train_input) -> input_schema.NodeTrainModel:
    return input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=10,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1"],
            model_type="linear_regression",
            params={"add_bias": True},
            **train_input,
        ),
    )


def _train_and_apply(graph: FlowGraph) -> None:
    """Train Model 10 on the seeded rows, and Apply Model 20 below it scoring with node 10's model into ``pred``."""
    _wire(graph, "train_model", node_id=10, upstream_id=1)
    graph.add_train_model(_train_settings(graph))
    _wire(graph, "apply_model", node_id=20, upstream_id=10)
    graph.add_apply_model(
        input_schema.NodeApplyModel(
            flow_id=graph.flow_id,
            node_id=20,
            depending_on_id=10,
            apply_input=input_schema.ApplyModelSettings(source="upstream", upstream_node_id=10, output_column="pred"),
        )
    )


def test_a_local_flow_trains_and_scores_in_core(no_worker):
    graph = _local_graph(flow_id=7101)
    _train_and_apply(graph)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    scored = graph.get_node(20).get_resulting_data().collect()
    assert scored["pred"].to_list() == pytest.approx([5.0, 13.0])
    assert ml_flow_model_path(graph.flow_id, 10).exists()


def test_a_local_flow_keeps_the_scored_rows_lazy(no_worker):
    graph = _local_graph(flow_id=7102)
    _train_and_apply(graph)

    scored = graph.get_node(20).get_resulting_data()

    assert isinstance(scored.data_frame, pl.LazyFrame)
    assert scored.data_frame.collect_schema()["pred"] == pl.Float64


def test_a_local_flow_publishes_the_model_it_fitted_in_core(no_worker, monkeypatch, tmp_path):
    from flowfile_core.artifacts import service as artifact_service_mod
    from flowfile_core.schemas.artifact_schema import PrepareUploadResponse

    staging = tmp_path / "staging" / "model.json"
    finalized: dict = {}

    def _prepare_upload(self, request, owner_id, _max_retries=3):
        return PrepareUploadResponse(artifact_id=1, version=1, method="file", path=str(staging), storage_key="1/x")

    def _finalize_upload(self, artifact_id, storage_key, sha256, size_bytes):
        finalized.update(sha256=sha256, size_bytes=size_bytes)

    monkeypatch.setattr(artifact_service_mod.ArtifactService, "prepare_upload", _prepare_upload)
    monkeypatch.setattr(artifact_service_mod.ArtifactService, "finalize_upload", _finalize_upload)
    graph = _local_graph(flow_id=7103)
    graph._flow_settings.source_registration_id = 1
    _wire(graph, "train_model", node_id=10, upstream_id=1)
    graph.add_train_model(_train_settings(graph, publish_to_catalog=True, model_name="m", namespace_id=42))

    graph.get_node(10).get_resulting_data()

    model = staging.read_bytes()
    assert finalized == {"sha256": hashlib.sha256(model).hexdigest(), "size_bytes": len(model)}
    assert ml_flow_model_path(graph.flow_id, 10).read_bytes() == model


def test_a_remote_flow_still_trains_on_the_worker(monkeypatch):
    called = []

    class _Fetcher:
        def __init__(self, *args, **kwargs):
            called.append(kwargs["model_type"])
            raise RuntimeError("worker reached")

    monkeypatch.setattr("flowfile_core.flowfile.flow_graph.MLTrainFetcher", _Fetcher)
    graph = _local_graph(flow_id=7104)
    graph.execution_location = "remote"
    _wire(graph, "train_model", node_id=10, upstream_id=1)
    graph.add_train_model(_train_settings(graph))

    with pytest.raises(RuntimeError, match="worker reached"):
        graph.get_node(10).get_resulting_data()
    assert called == ["linear_regression"]

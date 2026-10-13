"""Core-side tests for the Train Model / Apply Model nodes.

These tests focus on the orchestration logic (settings validation, schema
callbacks, artifact lifecycle) rather than the worker compute path — the
actual training runs in :mod:`flowfile_worker.tests.test_train_apply_model`.
Both nodes reach the worker only through ``WorkerBackend.run_op``, which the
tests replace with a stand-in that sees the payload each node sends; a
training's stand-in answers the model as a blob reference and writes nothing.
The two nodes on a running worker are in ``test_ml_nodes_on_the_worker``.
"""

from typing import Literal

import polars as pl
import pytest

from flowfile_core.auth.models import User
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ResultMissing
from flowfile_core.flowfile.execution.payloads import PAYLOAD_KEY, MlApplyPayload, MlTrainPayload
from flowfile_core.flowfile.execution.protocol import OperationKind, OpResult, ResultRef
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas

# Helpers


def _make_graph(
    flow_id: int = 4242,
    execution_mode: Literal["Development", "Performance"] = "Development",
    source_registration_id: int | None = None,
) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="ml_test_flow",
            path=".",
            execution_mode=execution_mode,
            source_registration_id=source_registration_id,
        )
    )
    graph: FlowGraph = handler.get_flow(flow_id)
    return graph


def _seed_manual_input(graph: FlowGraph, node_id: int = 1):
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    graph.add_node_promise(promise)
    payload = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(
            [{"x1": 1.0, "x2": 2.0, "y": 5.0}, {"x1": 3.0, "x2": 4.0, "y": 13.0}]
        ),
    )
    graph.add_manual_input(payload)


def _route_worker_ops(monkeypatch, handle) -> list[tuple[OperationKind, object]]:
    """Answer the worker backend's ``run_op`` with ``handle(op, payload dataclass)``; returns the calls it got."""
    calls: list[tuple[OperationKind, object]] = []

    def run_op(self, op, payload):
        calls.append((op, payload[PAYLOAD_KEY]))
        return handle(op, payload[PAYLOAD_KEY])

    monkeypatch.setattr(WorkerBackend, "run_op", run_op)
    return calls


def _refuse_worker_ops(monkeypatch, error: Exception) -> None:
    """Have the worker backend's ``run_op`` raise ``error``."""

    def run_op(self, op, payload):
        raise error

    monkeypatch.setattr(WorkerBackend, "run_op", run_op)


def _model(job_id: str) -> ResultRef:
    """The model a training under ``job_id`` holds on the worker backend, as a blob reference."""
    return ResultRef("worker", f"{job_id}_0", [], None, "materialised", b"held-on-the-worker", kind="blob")


def _trained(size_bytes: int = 14, sha256: str = "deadbeef" * 8):
    """A stand-in training run: answers the model as a blob the worker holds, with its digest; writes nothing."""

    def handle(op, train: MlTrainPayload) -> OpResult:
        report = {"sha256": sha256, "size_bytes": size_bytes, "model_type": train.model_type}
        return OpResult(refs={"model": _model(train.job_id)}, metadata=report)

    return handle


def _record_releases(monkeypatch) -> list[ResultRef]:
    """Have the worker backend's ``release`` keep what it is given instead of calling the worker."""
    released: list[ResultRef] = []
    monkeypatch.setattr(WorkerBackend, "release", lambda self, ref: released.append(ref))
    return released


def _scored(tmp_path, rows: pl.DataFrame):
    """A stand-in apply: holds ``rows`` in a file as the stored result a task-route apply answers."""
    scored_file = tmp_path / "scored.arrow"
    rows.write_ipc(scored_file)

    def handle(op, apply: MlApplyPayload) -> OpResult:
        ref = ResultRef("worker", apply.job_id, [], rows.height, "materialised", str(scored_file).encode())
        return OpResult(refs={"result": ref})

    return handle


def _wire_ml_node(graph: FlowGraph, node_type: str, node_id: int, upstream_id: int):
    """Register a node promise + connection so its inputs are populated.

    Mirrors how the runtime wires nodes when a flow loads from disk.
    """
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type)
    graph.add_node_promise(promise)
    connection = input_schema.NodeConnection.create_from_simple_input(upstream_id, node_id)
    add_connection(graph, connection)


# Schema-callback tests — these don't need a worker, just call the predicted
# schema getter and inspect the result.


def test_train_model_schema_passes_through_input_schema():
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)

    train_settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1", "x2"],
            model_type="linear_regression",
            params={"add_bias": True},
        ),
    )
    graph.add_train_model(train_settings)

    train_node = graph.get_node(2)
    predicted = train_node.schema
    assert {c.column_name for c in predicted} == {"x1", "x2", "y"}


def test_train_model_schema_passes_through_for_logistic_regression():
    """Logistic regression trainer integrates with the schema callback."""
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)

    train_settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1", "x2"],
            model_type="logistic_regression",
            params={
                "add_bias": True,
                "l2_reg": 0.0,
                "l1_reg": 0.0,
                "max_iter": 200,
            },
        ),
    )
    graph.add_train_model(train_settings)

    train_node = graph.get_node(2)
    predicted = train_node.schema
    assert {c.column_name for c in predicted} == {"x1", "x2", "y"}


def test_train_model_schema_passes_through_for_knn_classifier():
    """KNN classifier trainer integrates with the schema callback."""
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)

    train_settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1", "x2"],
            model_type="knn_classifier",
            params={"k": 3, "distance": "sql2"},
        ),
    )
    graph.add_train_model(train_settings)

    train_node = graph.get_node(2)
    predicted = train_node.schema
    assert {c.column_name for c in predicted} == {"x1", "x2", "y"}


def test_apply_model_schema_appends_float64_prediction_column():
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "apply_model", node_id=3, upstream_id=1)

    apply_settings = input_schema.NodeApplyModel(
        flow_id=graph.flow_id,
        node_id=3,
        depending_on_id=1,
        apply_input=input_schema.ApplyModelSettings(
            source="catalog",
            model_name="m",
            output_column="my_pred",
        ),
    )
    graph.add_apply_model(apply_settings)

    predicted = graph.get_node(3).schema
    names = [c.column_name for c in predicted]
    assert names == ["x1", "x2", "y", "my_pred"]
    pred_col = next(c for c in predicted if c.column_name == "my_pred")
    assert pred_col.data_type == "Float64"


def test_apply_model_schema_refreshes_when_output_column_changes():
    """Setting changes must reset the cached schema callback."""
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "apply_model", node_id=3, upstream_id=1)

    apply_settings = input_schema.NodeApplyModel(
        flow_id=graph.flow_id,
        node_id=3,
        depending_on_id=1,
        apply_input=input_schema.ApplyModelSettings(source="catalog", model_name="m", output_column="pred"),
    )
    graph.add_apply_model(apply_settings)
    first = {c.column_name for c in graph.get_node(3).schema}
    assert "pred" in first and "renamed" not in first

    new_settings = input_schema.NodeApplyModel(
        flow_id=graph.flow_id,
        node_id=3,
        depending_on_id=1,
        apply_input=input_schema.ApplyModelSettings(source="catalog", model_name="m", output_column="renamed"),
    )
    graph.add_apply_model(new_settings)
    second = {c.column_name for c in graph.get_node(3).schema}
    assert "renamed" in second and "pred" not in second


# _func validation tests — call the train function directly with mocks so we
# don't actually round-trip through a worker.


def _run_train_func(graph: FlowGraph, settings: input_schema.NodeTrainModel):
    """Trigger the train node's _func by running its eager step."""
    _wire_ml_node(graph, "train_model", node_id=settings.node_id, upstream_id=1)
    graph.add_train_model(settings)
    train_node = graph.get_node(settings.node_id)
    return train_node.get_resulting_data()


def test_train_model_requires_path_when_unregistered_only_for_catalog_publish():
    """An unsaved scratch flow can train (writes to flow cache); only catalog publish needs registration."""
    graph = _make_graph(source_registration_id=None)
    graph._flow_settings.path = ""
    _seed_manual_input(graph, node_id=1)

    settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1", "x2"],
            model_type="linear_regression",
            params={"add_bias": True},
            publish_to_catalog=True,
            model_name="m",
        ),
    )
    with pytest.raises(ValueError, match="flow to be registered"):
        _run_train_func(graph, settings)


def test_train_model_publish_requires_model_name():
    graph = _make_graph(source_registration_id=1)
    _seed_manual_input(graph, node_id=1)
    settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1"],
            model_type="linear_regression",
            params={},
            publish_to_catalog=True,
            model_name="",
        ),
    )
    with pytest.raises(ValueError, match="model_name.*required"):
        _run_train_func(graph, settings)


def test_train_model_requires_target_column():
    graph = _make_graph(source_registration_id=1)
    _seed_manual_input(graph, node_id=1)
    settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="",
            feature_columns=["x1"],
            model_type="linear_regression",
            params={},
        ),
    )
    with pytest.raises(ValueError, match="target_column"):
        _run_train_func(graph, settings)


def test_train_model_forwards_namespace_id_to_prepare_upload(monkeypatch):
    """A namespace_id set on the train settings must reach PrepareUploadRequest (when publishing).

    The fit runs first; the copy to the store then fails, which deletes the pending row.
    """
    graph = _make_graph(source_registration_id=1)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)

    from flowfile_core.artifacts import service as artifact_service_mod
    from flowfile_core.schemas.artifact_schema import PrepareUploadResponse

    captured: dict = {}
    deleted: list[int] = []

    def _fake_prepare_upload(self, request, owner_id, _max_retries=3):
        captured["namespace_id"] = request.namespace_id
        captured["name"] = request.name
        return PrepareUploadResponse(artifact_id=1, version=1, method="file", path="/tmp/x", storage_key="1/x")

    def _refuse_export(self, ref, uri, credential):
        raise RuntimeError("expected")

    monkeypatch.setattr(artifact_service_mod.ArtifactService, "prepare_upload", _fake_prepare_upload)
    monkeypatch.setattr(artifact_service_mod.ArtifactService, "delete_artifact", lambda self, i: deleted.append(i))
    monkeypatch.setattr(WorkerBackend, "export", _refuse_export)
    calls = _route_worker_ops(monkeypatch, _trained())

    settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1"],
            model_type="linear_regression",
            params={"add_bias": True},
            namespace_id=42,
            publish_to_catalog=True,
            model_name="m",
        ),
    )
    graph.add_train_model(settings)

    with pytest.raises(RuntimeError, match="expected"):
        graph.get_node(2).get_resulting_data()

    assert [op for op, _ in calls] == [OperationKind.ml_train]
    assert captured["namespace_id"] == 42
    assert captured["name"] == "m"
    assert deleted == [1]


def test_apply_model_forwards_namespace_id_to_lookup(monkeypatch):
    """A namespace_id on the apply settings must reach get_artifact_by_name (catalog mode).

    The lookup is ``fetch_artifact``'s: a store method it cannot hand a backend is its ``ValueError``, and nothing runs.
    """
    graph = _make_graph(source_registration_id=1)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "apply_model", node_id=2, upstream_id=1)

    from flowfile_core.artifacts import service as artifact_service_mod

    captured: dict = {}

    def _fake_get_by_name(self, name, namespace_id=None, version=None):
        captured["name"] = name
        captured["namespace_id"] = namespace_id
        captured["version"] = version
        from types import SimpleNamespace

        return SimpleNamespace(
            id=1,
            name=name,
            version=1,
            download_source=SimpleNamespace(method="not-file", path=""),
        )

    monkeypatch.setattr(artifact_service_mod.ArtifactService, "get_artifact_by_name", _fake_get_by_name)
    imports = []
    monkeypatch.setattr(WorkerBackend, "import_uri", lambda self, *args, **kwargs: imports.append(args))
    calls = _route_worker_ops(monkeypatch, lambda op, payload: pytest.fail("nothing runs"))

    settings = input_schema.NodeApplyModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        apply_input=input_schema.ApplyModelSettings(
            source="catalog",
            model_name="m",
            model_version=3,
            namespace_id=42,
            output_column="pred",
        ),
    )
    graph.add_apply_model(settings)

    with pytest.raises(ValueError, match="'not-file' is neither 'file' nor 's3_presigned'"):
        graph.get_node(2).get_resulting_data()

    assert captured == {"name": "m", "namespace_id": 42, "version": 3}
    assert (imports, calls) == ([], [])


def test_apply_model_upstream_mode_requires_existing_train_node():
    """source='upstream' with no model file yet must produce a clear error."""
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "apply_model", node_id=2, upstream_id=1)
    settings = input_schema.NodeApplyModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        apply_input=input_schema.ApplyModelSettings(
            source="upstream",
            upstream_node_id=99,
            output_column="pred",
        ),
    )
    graph.add_apply_model(settings)
    with pytest.raises(ValueError, match="not a Train Model"):
        graph.get_node(2).get_resulting_data()


def _upstream_flow(graph: FlowGraph) -> None:
    """Train Model 10 on the seeded rows, and Apply Model 20 below it scoring with node 10's model into ``pred``."""
    _wire_ml_node(graph, "train_model", node_id=10, upstream_id=1)
    graph.add_train_model(
        input_schema.NodeTrainModel(
            flow_id=graph.flow_id,
            node_id=10,
            depending_on_id=1,
            train_input=input_schema.TrainModelSettings(
                target_column="y",
                feature_columns=["x1"],
                model_type="linear_regression",
                params={"add_bias": True},
            ),
        )
    )
    _wire_ml_node(graph, "apply_model", node_id=20, upstream_id=10)
    graph.add_apply_model(
        input_schema.NodeApplyModel(
            flow_id=graph.flow_id,
            node_id=20,
            depending_on_id=10,
            apply_input=input_schema.ApplyModelSettings(
                source="upstream",
                upstream_node_id=10,
                output_column="pred",
            ),
        )
    )


def test_an_upstream_apply_sends_the_model_its_train_node_holds(tmp_path, monkeypatch):
    """Apply with source='upstream' scores with the blob the train node holds, by reference."""
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    scored = pl.DataFrame({"x1": [1.0, 3.0], "x2": [2.0, 4.0], "y": [5.0, 13.0], "pred": [3.0, 7.0]})
    score = _scored(tmp_path, scored)
    train = _trained()
    calls = _route_worker_ops(
        monkeypatch, lambda op, job: train(op, job) if op is OperationKind.ml_train else score(op, job)
    )
    _upstream_flow(graph)

    result = graph.get_node(20).get_resulting_data()

    assert [op for op, _ in calls] == [OperationKind.ml_train, OperationKind.ml_apply]
    apply = calls[-1][1]
    assert isinstance(apply, MlApplyPayload)
    held = graph.get_node(10).results.model_ref
    assert held == _model(graph.get_node(10).hash)
    assert apply.model is held
    assert apply.output_column == "pred"
    assert (apply.flow_id, apply.node_id, apply.job_id) == (graph.flow_id, 20, graph.get_node(20).hash)
    assert apply.plan.collect().columns == ["x1", "x2", "y"]
    assert result.collect()["pred"].to_list() == [3.0, 7.0]
    assert result.number_of_records == 2


def test_an_upstream_apply_whose_model_is_gone_asks_for_the_train_node_again(monkeypatch):
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    train = _trained()

    def train_then_miss(op, job) -> OpResult:
        if op is OperationKind.ml_train:
            return train(op, job)
        raise ResultMissing(f"No result {job.model.result_id} is held by this worker")

    _route_worker_ops(monkeypatch, train_then_miss)
    _upstream_flow(graph)

    with pytest.raises(ValueError, match=r"Train Model \(node 10\) fitted is no longer held\. Run .* node 10 again"):
        graph.get_node(20).get_resulting_data()


def test_a_catalog_apply_scores_with_the_fetched_model_and_releases_it(tmp_path, monkeypatch):
    from flowfile_core.artifacts import publish

    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "apply_model", node_id=2, upstream_id=1)
    fetched = ResultRef("worker", "fetched-model", [], None, "materialised", b"held-on-the-worker", kind="blob")
    fetches = []

    def fake_fetch(backend, name, version, namespace_id):
        fetches.append((backend, name, version, namespace_id))
        return fetched

    monkeypatch.setattr(publish, "fetch_artifact", fake_fetch)
    released = _record_releases(monkeypatch)
    calls = _route_worker_ops(monkeypatch, _scored(tmp_path, pl.DataFrame({"x1": [1.0, 3.0], "pred": [3.0, 7.0]})))
    graph.add_apply_model(
        input_schema.NodeApplyModel(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            apply_input=input_schema.ApplyModelSettings(
                source="catalog", model_name="m", model_version=3, namespace_id=42, output_column="pred"
            ),
        )
    )

    result = graph.get_node(2).get_resulting_data()

    ((backend, *looked_up),) = fetches
    assert isinstance(backend, WorkerBackend) and looked_up == ["m", 3, 42]
    ((op, apply),) = calls
    assert (op, apply.model) == (OperationKind.ml_apply, fetched)
    assert released == [fetched]
    assert result.collect()["pred"].to_list() == [3.0, 7.0]


def test_train_model_rejects_unknown_model_type():
    graph = _make_graph(source_registration_id=1)
    _seed_manual_input(graph, node_id=1)
    settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1"],
            model_type="quantum_regression",
            params={},
        ),
    )
    with pytest.raises(ValueError, match="Unknown model_type"):
        _run_train_func(graph, settings)


def test_train_model_rolls_back_pending_artifact_when_the_copy_fails(monkeypatch):
    """A fit that fails prepares no artifact; a copy to the store that fails deletes the pending row."""
    graph = _make_graph(source_registration_id=1)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)

    from flowfile_core.artifacts import service as artifact_service_mod
    from flowfile_core.schemas.artifact_schema import PrepareUploadResponse

    prepared: list[str] = []

    def _fake_prepare_upload(self, request, owner_id, _max_retries=3):
        prepared.append(request.name)
        return PrepareUploadResponse(
            artifact_id=999,
            version=1,
            method="file",
            path="/tmp/whatever_999_m.json",
            storage_key="999/m.json",
        )

    delete_calls: list[int] = []

    def _fake_delete_artifact(self, artifact_id):
        delete_calls.append(artifact_id)
        return 1

    monkeypatch.setattr(
        artifact_service_mod.ArtifactService,
        "prepare_upload",
        _fake_prepare_upload,
    )
    monkeypatch.setattr(
        artifact_service_mod.ArtifactService,
        "delete_artifact",
        _fake_delete_artifact,
    )

    def _refuse_export(self, ref, uri, credential):
        raise OSError("copy failed")

    monkeypatch.setattr(WorkerBackend, "export", _refuse_export)
    _refuse_worker_ops(monkeypatch, RuntimeError("worker exploded"))
    settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1"],
            model_type="linear_regression",
            params={"add_bias": True},
            publish_to_catalog=True,
            model_name="m",
        ),
    )
    graph.add_train_model(settings)
    with pytest.raises(RuntimeError, match="worker exploded"):
        graph.get_node(2).get_resulting_data()

    assert (prepared, delete_calls) == ([], []), "a failed fit prepares nothing"

    _route_worker_ops(monkeypatch, _trained())
    graph.get_node(2).reset(deep=True)
    with pytest.raises(OSError, match="copy failed"):
        graph.get_node(2).get_resulting_data()

    assert prepared == ["m"]
    assert delete_calls == [999], "pending artifact must be deleted on failure"


def test_apply_model_catalog_mode_requires_model_name():
    graph = _make_graph(source_registration_id=1)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "apply_model", node_id=2, upstream_id=1)
    settings = input_schema.NodeApplyModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        apply_input=input_schema.ApplyModelSettings(source="catalog", model_name="", output_column="pred"),
    )
    graph.add_apply_model(settings)
    with pytest.raises(ValueError, match="model_name"):
        graph.get_node(2).get_resulting_data()


def test_apply_model_upstream_mode_requires_upstream_node_id():
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "apply_model", node_id=2, upstream_id=1)
    settings = input_schema.NodeApplyModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        apply_input=input_schema.ApplyModelSettings(source="upstream", upstream_node_id=None, output_column="pred"),
    )
    graph.add_apply_model(settings)
    with pytest.raises(ValueError, match="upstream_node_id"):
        graph.get_node(2).get_resulting_data()


# Schema callback creates Float64 column even if the apply node has no
# upstream yet (defensive sanity check).


def test_flowfile_column_float64_dtype_for_prediction():
    col = FlowfileColumn.from_input("predicted_value", "Float64")
    assert col.data_type == "Float64"
    assert col.column_name == "predicted_value"


# Artifact tab — train_model surfaces in node Artifacts tab via artifact_context


def test_train_model_records_artifact_in_node_summary(monkeypatch):
    """A successful train run should publish an entry in artifact_context."""
    graph = _make_graph()
    graph._flow_settings.path = ""
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)

    calls = _route_worker_ops(monkeypatch, _trained())

    settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1"],
            model_type="linear_regression",
            params={"add_bias": True},
        ),
    )
    graph.add_train_model(settings)
    graph.get_node(2).get_resulting_data()

    ((op, train),) = calls
    assert op is OperationKind.ml_train
    assert isinstance(train, MlTrainPayload)
    assert (train.model_type, train.target_column, train.feature_columns, train.params) == (
        "linear_regression",
        "y",
        ["x1"],
        {"add_bias": True},
    )
    assert (train.flow_id, train.node_id, train.job_id) == (graph.flow_id, 2, graph.get_node(2).hash)
    assert train.plan.collect().columns == ["x1", "x2", "y"]
    assert graph.get_node(2).results.model_ref == _model(train.job_id)
    summaries = graph.artifact_context.get_node_summaries()
    assert "2" in summaries
    s = summaries["2"]
    assert s["published_count"] == 1
    art = s["published"][0]
    assert "linear_regression" in art["name"]
    assert art["type_name"] == "flowfile.ml.linear_regression"
    assert art["module"] == "flowfile.ml"


def test_train_model_artifact_replaces_on_rerun(monkeypatch):
    """Re-running a train node should replace its artifact entry, not accumulate."""
    graph = _make_graph()
    graph._flow_settings.path = ""
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)

    _route_worker_ops(monkeypatch, _trained(sha256="x" * 64))

    settings = input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1"],
            model_type="linear_regression",
            params={"add_bias": True},
        ),
    )
    graph.add_train_model(settings)
    graph.get_node(2).get_resulting_data()
    graph.get_node(2).reset()
    graph.get_node(2).get_resulting_data()

    summaries = graph.artifact_context.get_node_summaries()
    assert summaries["2"]["published_count"] == 1


def _flow_only_train_settings(graph: FlowGraph) -> input_schema.NodeTrainModel:
    return input_schema.NodeTrainModel(
        flow_id=graph.flow_id,
        node_id=2,
        depending_on_id=1,
        train_input=input_schema.TrainModelSettings(
            target_column="y",
            feature_columns=["x1"],
            model_type="linear_regression",
            params={"add_bias": True},
        ),
    )


def test_a_local_flow_still_trains_on_the_worker_backend(monkeypatch):
    """Core's own backend fits no models, so the fit goes to the worker backend whatever the flow's location."""
    graph = _make_graph()
    graph._flow_settings.path = ""
    graph.flow_settings.execution_location = "local"
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)
    calls = _route_worker_ops(monkeypatch, _trained())

    graph.add_train_model(_flow_only_train_settings(graph))
    result = graph.get_node(2).get_resulting_data()

    assert [op for op, _ in calls] == [OperationKind.ml_train]
    assert result.collect().columns == ["x1", "x2", "y"], "the node passes its input through"


def test_a_train_result_without_the_model_digest_fails_the_node(monkeypatch):
    graph = _make_graph()
    graph._flow_settings.path = ""
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)
    _route_worker_ops(monkeypatch, lambda op, train: OpResult(refs={}, metadata={"model_type": "linear_regression"}))

    graph.add_train_model(_flow_only_train_settings(graph))
    with pytest.raises(RuntimeError, match="sha256/size_bytes"):
        graph.get_node(2).get_resulting_data()


def test_a_train_result_without_its_model_fails_the_node(monkeypatch):
    graph = _make_graph()
    graph._flow_settings.path = ""
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)
    report = {"sha256": "0" * 64, "size_bytes": 14, "model_type": "linear_regression"}
    _route_worker_ops(monkeypatch, lambda op, train: OpResult(refs={}, metadata=report))

    graph.add_train_model(_flow_only_train_settings(graph))
    with pytest.raises(RuntimeError, match="did not return the model"):
        graph.get_node(2).get_resulting_data()
    assert graph.get_node(2).results.model_ref is None


def test_a_retrain_releases_the_model_it_replaces(monkeypatch):
    """A refit under the same id replaced the model where it is held; one under another id releases the old one."""
    graph = _make_graph()
    graph._flow_settings.path = ""
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)
    _route_worker_ops(monkeypatch, _trained())
    released = _record_releases(monkeypatch)
    graph.add_train_model(_flow_only_train_settings(graph))
    graph.get_node(2).get_resulting_data()
    first = graph.get_node(2).results.model_ref

    graph.get_node(2).reset(deep=True)
    graph.get_node(2).get_resulting_data()

    assert graph.get_node(2).results.model_ref == first
    assert released == []

    retrained = _flow_only_train_settings(graph)
    retrained.train_input.params = {"add_bias": False}
    graph.add_train_model(retrained)
    graph.get_node(2).get_resulting_data()

    second = graph.get_node(2).results.model_ref
    assert second == _model(graph.get_node(2).hash)
    assert second.result_id != first.result_id
    assert released == [first]


def test_a_published_model_is_the_held_model_exported_to_the_store(monkeypatch):
    from flowfile_core.artifacts import publish
    from flowfile_core.schemas.artifact_schema import FinalizeUploadResponse

    graph = _make_graph(source_registration_id=1)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)
    _route_worker_ops(monkeypatch, _trained(size_bytes=21, sha256="ab" * 32))
    published = []

    def fake_publish(backend, ref, request, *, owner_id, sha256, size_bytes):
        published.append((backend, ref, request, sha256, size_bytes))
        return FinalizeUploadResponse(artifact_id=5, version=3)

    monkeypatch.setattr(publish, "publish_result", fake_publish)
    settings = _flow_only_train_settings(graph)
    settings.train_input.publish_to_catalog = True
    settings.train_input.model_name = "m"
    settings.train_input.namespace_id = 42
    graph.add_train_model(settings)

    graph.get_node(2).get_resulting_data()

    ((backend, ref, request, sha256, size_bytes),) = published
    assert isinstance(backend, WorkerBackend)
    assert ref is graph.get_node(2).results.model_ref
    assert (request.name, request.namespace_id, request.source_registration_id) == ("m", 42, 1)
    assert (request.source_flow_id, request.source_node_id) == (graph.flow_id, 2)
    assert (sha256, size_bytes) == ("ab" * 32, 21)
    (artifact,) = graph.artifact_context.get_node_summaries()["2"]["published"]
    assert artifact["name"] == "m v3"


# Wait For node


def _wire_wait_for(graph: FlowGraph, *, node_id: int, left: int, right: int) -> None:
    promise_w = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="wait_for")
    graph.add_node_promise(promise_w)
    left_conn = input_schema.NodeConnection.create_from_simple_input(left, node_id)
    add_connection(graph, left_conn)
    right_conn = input_schema.NodeConnection.create_from_simple_input(right, node_id, input_type="right")
    add_connection(graph, right_conn)
    graph.add_wait_for(input_schema.NodeWaitFor(flow_id=graph.flow_id, node_id=node_id, depending_on_ids=[left, right]))


def test_wait_for_passes_through_left_input_schema():
    """The Wait For node must keep the left input's schema verbatim."""
    graph = _make_graph()
    _seed_manual_input(graph, node_id=1)

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{"other": 99}]),
        )
    )

    _wire_wait_for(graph, node_id=3, left=1, right=2)
    schema_columns = {c.column_name for c in graph.get_node(3).schema}
    # Left input wins — node 1's columns, not node 2's "other".
    assert schema_columns == {"x1", "x2", "y"}


def test_wait_for_runs_after_dependency_and_passes_data_through():
    """wait_for must execute after both inputs and produce the left input's data."""
    graph = _make_graph(execution_mode="Development")
    graph.execution_location = "local"
    _seed_manual_input(graph, node_id=1)

    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=2,
            raw_data_format=input_schema.RawData.from_pylist([{"unrelated": "data"}]),
        )
    )

    _wire_wait_for(graph, node_id=3, left=1, right=2)
    graph.run_graph()
    out = graph.get_node(3).get_resulting_data().collect()
    assert set(out.columns) == {"x1", "x2", "y"}
    assert out.height == 2  # _seed_manual_input writes 2 rows


# /ml/upstream-train-models picker — strict DAG ancestor scope

_USER = User(id=1, username="local_user")


def _patch_upstream_handler(monkeypatch, graph: FlowGraph) -> None:
    """Point the ml routes module at a stub handler holding this single graph."""
    from flowfile_core.ml import routes as ml_routes

    class _StubHandler:
        def get_flow(self, fid, user_id=None):
            return graph if fid == graph.flow_id and user_id == _USER.id else None

    monkeypatch.setattr(ml_routes, "flow_file_handler", _StubHandler())


def _seed_extra_source(graph: FlowGraph, node_id: int) -> None:
    promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    graph.add_node_promise(promise)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist([{"x1": 9.0, "x2": 8.0, "y": 1.0}]),
        )
    )


def test_upstream_picker_returns_directly_connected_train_model(monkeypatch):
    from flowfile_core.ml.routes import list_upstream_train_models

    graph = _make_graph(flow_id=5001)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)
    _wire_ml_node(graph, "apply_model", node_id=3, upstream_id=2)

    _patch_upstream_handler(monkeypatch, graph)
    result = list_upstream_train_models(graph.flow_id, 3, current_user=_USER)
    assert [r.node_id for r in result] == [2]


def test_upstream_picker_excludes_train_model_in_parallel_branch(monkeypatch):
    from flowfile_core.ml.routes import list_upstream_train_models

    graph = _make_graph(flow_id=5002)
    _seed_manual_input(graph, node_id=1)
    _seed_extra_source(graph, node_id=10)
    _wire_ml_node(graph, "train_model", node_id=11, upstream_id=10)
    _wire_ml_node(graph, "apply_model", node_id=2, upstream_id=1)

    _patch_upstream_handler(monkeypatch, graph)
    result = list_upstream_train_models(graph.flow_id, 2, current_user=_USER)
    assert result == []


def test_upstream_picker_finds_train_model_through_wait_for(monkeypatch):
    from flowfile_core.ml.routes import list_upstream_train_models

    graph = _make_graph(flow_id=5003)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)
    _wire_wait_for(graph, node_id=3, left=1, right=2)
    _wire_ml_node(graph, "apply_model", node_id=4, upstream_id=3)

    _patch_upstream_handler(monkeypatch, graph)
    result = list_upstream_train_models(graph.flow_id, 4, current_user=_USER)
    assert [r.node_id for r in result] == [2]


def test_upstream_picker_returns_empty_when_no_train_models(monkeypatch):
    from flowfile_core.ml.routes import list_upstream_train_models

    graph = _make_graph(flow_id=5004)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "apply_model", node_id=2, upstream_id=1)

    _patch_upstream_handler(monkeypatch, graph)
    result = list_upstream_train_models(graph.flow_id, 2, current_user=_USER)
    assert result == []


def test_upstream_picker_returns_only_ancestor_train_model_among_many(monkeypatch):
    from flowfile_core.ml.routes import list_upstream_train_models

    graph = _make_graph(flow_id=5005)
    _seed_manual_input(graph, node_id=1)
    _wire_ml_node(graph, "train_model", node_id=2, upstream_id=1)
    _seed_extra_source(graph, node_id=10)
    _wire_ml_node(graph, "train_model", node_id=11, upstream_id=10)
    _wire_ml_node(graph, "apply_model", node_id=3, upstream_id=2)

    _patch_upstream_handler(monkeypatch, graph)
    result = list_upstream_train_models(graph.flow_id, 3, current_user=_USER)
    assert [r.node_id for r in result] == [2]

"""The Train Model and Apply Model nodes on the session worker, in the protocol flag state the run is in.

A remote Development flow reads eight rows of ``y = 2·x1 + 3·x2 + 1``, fits a linear regression in Train Model node 2
and scores with the model that node holds in Apply Model node 3, by reference. With the flag on the worker holds the
model as a blob over ``/v1``; with it off the worker stages it in core's flow cache directory, the disk the two share,
as a task of its own. A model published to the filesystem artifact store is applied from the catalog in a second flow.
"""

import hashlib
import re
import uuid
from pathlib import Path

import pytest

from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, FlowRegistration, GlobalArtifact
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.output_model import RunInformation
from shared.compute_protocol import WorkerLocation
from shared.storage_config import storage
from tests.flowfile.conftest import (
    add_test_manual_input,
    create_test_flow_registration,
    create_test_graph,
    create_test_namespace,
)

pytestmark = pytest.mark.core

ROWS = [{"x1": float(i), "x2": float(i % 3), "y": 2.0 * i + 3.0 * (i % 3) + 1.0} for i in range(1, 9)]
FLOW_ID = 7480


@pytest.fixture(autouse=True)
def remote_worker():
    from flowfile_core.schemas.schemas import is_valid_execution_location_in_current_global_settings
    from tests.conftest import is_worker_running

    if not (is_worker_running() and is_valid_execution_location_in_current_global_settings("remote")):
        pytest.skip("Remote execution not active")


def _wire(graph: FlowGraph, node_type: str, node_id: int, upstream_id: int) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(upstream_id, node_id))


def _train(graph: FlowGraph, add_bias: bool = True, **publish) -> None:
    graph.add_train_model(
        input_schema.NodeTrainModel(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            train_input=input_schema.TrainModelSettings(
                target_column="y",
                feature_columns=["x1", "x2"],
                model_type="linear_regression",
                params={"add_bias": add_bias},
                **publish,
            ),
        )
    )


def _apply(graph: FlowGraph, node_id: int, output_column: str = "pred", **source) -> None:
    source = source or {"source": "upstream", "upstream_node_id": 2}
    graph.add_apply_model(
        input_schema.NodeApplyModel(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=node_id - 1,
            apply_input=input_schema.ApplyModelSettings(output_column=output_column, **source),
        )
    )


def _release_models(graph: FlowGraph) -> None:
    """Release the model each Train Model node of ``graph`` holds, then close the flow."""
    for node in graph.nodes:
        model = node.results.model_ref
        if model is not None:
            registry.backend(model.backend_id).release(model)
    graph.close_flow()


@pytest.fixture
def flow():
    """Manual input 1 → Train Model 2 → Apply Model 3 (upstream 2, into ``pred``), remote, in Development mode."""
    graph = create_test_graph(flow_id=FLOW_ID, execution_location="remote")
    add_test_manual_input(graph, ROWS, node_id=1)
    _wire(graph, "train_model", 2, 1)
    _train(graph)
    _wire(graph, "apply_model", 3, 2)
    _apply(graph, 3)
    yield graph
    _release_models(graph)


@pytest.fixture
def registration():
    """A namespace and a flow registered in it, ``(namespace_id, registration_id)``; their rows go after the test."""
    namespace_id = create_test_namespace()
    registration_id = create_test_flow_registration(namespace_id, name=f"ml_nodes_{uuid.uuid4().hex[:8]}")
    yield namespace_id, registration_id
    with get_db_context() as db:
        db.query(GlobalArtifact).filter_by(source_registration_id=registration_id).delete()
        db.query(FlowRegistration).filter_by(id=registration_id).delete()
        schema = db.get(CatalogNamespace, namespace_id)
        catalog_id = schema.parent_id
        db.delete(schema)
        db.query(CatalogNamespace).filter_by(id=catalog_id).delete()
        db.commit()


def _failures(run: RunInformation) -> dict[int, str]:
    return {result.node_id: result.error for result in run.node_step_result if not result.success}


def _predictions(graph: FlowGraph, node_id: int, column: str = "pred") -> list[float]:
    """The scored column of ``node_id``'s preview, rounded, in the order of ``ROWS``."""
    by_x1 = {row["x1"]: row[column] for row in graph.get_node(node_id).get_table_example(True).data}
    return [round(by_x1[row["x1"]], 6) for row in ROWS]


def test_an_upstream_apply_scores_with_the_model_its_train_node_holds(flow):
    run = flow.run_graph()

    assert run.success, _failures(run)
    assert _predictions(flow, 3) == [row["y"] for row in ROWS]
    train = flow.get_node(2)
    model = train.results.model_ref
    assert (model.kind, model.state, model.result_id) == ("blob", "materialised", f"{train.hash}_0")
    staged = storage.get_flow_cache_directory(flow.flow_id) / "ml_models" / f"{train.hash}_0.json"
    if settings.FLOWFILE_COMPUTE_PROTOCOL:
        assert WorkerLocation.model_validate_json(model.location).kind == "blob"
        assert not staged.exists()
    else:
        assert model.location == str(staged).encode()
        assert staged.is_file()


def test_a_retrain_releases_the_model_it_replaces(flow):
    assert flow.run_graph().success
    first = flow.get_node(2).results.model_ref
    backend = registry.backend(first.backend_id)

    _train(flow, add_bias=False)
    run = flow.run_graph()

    assert run.success, _failures(run)
    second = flow.get_node(2).results.model_ref
    assert second.result_id != first.result_id
    assert backend.exists(first) is False
    assert backend.exists(second) is True


def test_an_apply_whose_model_is_gone_asks_for_the_train_node_again(flow):
    assert flow.run_graph().success
    model = flow.get_node(2).results.model_ref
    registry.backend(model.backend_id).release(model)

    _apply(flow, 3, output_column="scored")
    run = flow.run_graph()

    assert run.success is False
    failures = _failures(run)
    assert list(failures) == [3], failures
    assert re.search(r"Train Model \(node 2\) fitted is no longer held\. Run the Train Model node 2 again", failures[3])


def test_a_published_model_is_applied_from_the_catalog_on_the_filesystem_store(registration, monkeypatch):
    from flowfile_core.artifacts import get_storage_backend

    namespace_id, registration_id = registration
    name = f"model_{uuid.uuid4().hex[:12]}"
    publishing = create_test_graph(
        flow_id=FLOW_ID + 1, source_registration_id=registration_id, execution_location="remote"
    )
    add_test_manual_input(publishing, ROWS, node_id=1)
    _wire(publishing, "train_model", 2, 1)
    _train(publishing, publish_to_catalog=True, model_name=name, namespace_id=namespace_id)
    try:
        run = publishing.run_graph()
        assert run.success, _failures(run)
    finally:
        _release_models(publishing)

    with get_db_context() as db:
        (artifact,) = db.query(GlobalArtifact).filter_by(name=name).all()
        assert (artifact.version, artifact.status, artifact.namespace_id) == (1, "active", namespace_id)
        artifact_id, storage_key, digest = artifact.id, artifact.storage_key, artifact.sha256
    store = get_storage_backend()
    permanent: Path = store.permanent / storage_key
    assert hashlib.sha256(permanent.read_bytes()).hexdigest() == digest
    assert not list(store.staging.glob(f"{artifact_id}_*"))

    released = []
    real_release = WorkerBackend.release

    def spying_release(self, ref):
        released.append(ref)
        return real_release(self, ref)

    monkeypatch.setattr(WorkerBackend, "release", spying_release)
    scoring = create_test_graph(flow_id=FLOW_ID + 2, execution_location="remote")
    add_test_manual_input(scoring, ROWS, node_id=1)
    _wire(scoring, "apply_model", 2, 1)
    _apply(scoring, 2, source="catalog", model_name=name, namespace_id=namespace_id)
    try:
        run = scoring.run_graph()

        assert run.success, _failures(run)
        assert _predictions(scoring, 2) == [row["y"] for row in ROWS]
    finally:
        scoring.close_flow()
    (fetched,) = [ref for ref in released if ref.kind == "blob"]
    assert registry.backend(fetched.backend_id).exists(fetched) is (not settings.FLOWFILE_COMPUTE_PROTOCOL)
    assert hashlib.sha256(permanent.read_bytes()).hexdigest() == digest

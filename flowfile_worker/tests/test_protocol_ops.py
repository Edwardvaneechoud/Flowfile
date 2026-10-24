"""The worker's ``POST /v1/ops/<kind>`` routes: an operation runs in its child and each output it wrote is held.

A training holds the model it fitted as a blob, and an apply scores with a blob held here.
"""

import base64
import hashlib
import json
import os
import threading
import time
import uuid
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal
from pydantic import BaseModel

from flowfile_worker import main, models, status_dict, status_dict_lock
from flowfile_worker.create.models import ReceivedTable
from flowfile_worker.protocol_routes import OpOutputs
from flowfile_worker.results import result_path, results
from flowfile_worker.secrets import encrypt_secret
from shared.compute_protocol import (
    PROTOCOL_VERSION,
    PROTOCOL_VERSION_HEADER,
    ResultRef,
    WorkerLocation,
    file_uri,
    schema_to_wire,
)
from shared.ml.trainers import get_trainer
from tests.conftest import INTERNAL_AUTH_HEADERS
from tests.utils import is_docker_available

pytestmark = pytest.mark.worker

client = TestClient(main.app, headers={**INTERNAL_AUTH_HEADERS, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)})

FLOW_ID = 4243
OTHER_FLOW_ID = 4244
NODE_ID = 9
GEOARROW_WKB = pl.Extension("geoarrow.wkb", pl.Binary, "{}")
PEOPLE = pl.DataFrame({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]})
TRAINING = pl.LazyFrame({"x1": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], "x2": [3.0, 1.0, 4.0, 1.0, 5.0, 9.0]})
TRAINING = TRAINING.with_columns(y=2 * pl.col("x1") + 3 * pl.col("x2") + 1)
SCORING = pl.LazyFrame({"x1": [10.0, 0.0, 1.5], "x2": [20.0, 1.0, -2.0]})

SPLIT_NODE_SOURCE = """
import polars as pl
from shared.node_designer import CustomNodeBase


class SplitNode(CustomNodeBase):
    node_name: str = "Split Node"
    number_of_outputs: int = 2
    output_names: list[str] = ["high", "low"]

    def process(self, *inputs):
        lf = inputs[0]
        return {"high": lf.filter(pl.col("value") > 10), "low": lf.filter(pl.col("value") <= 10)}
"""

SLEEPING_NODE_SOURCE = """
import time

from shared.node_designer import CustomNodeBase


class SleepingNode(CustomNodeBase):
    node_name: str = "Sleeping Node"

    def process(self, *inputs):
        time.sleep(30)
        return inputs[0]
"""


@pytest.fixture
def jobs():
    """Job ids a test posts under; each one's outputs are released and its entry dropped when the test ends."""
    ids: list[str] = []

    def new_id() -> str:
        ids.append(uuid.uuid4().hex)
        return ids[-1]

    yield new_id
    for job_id in ids:
        for i in range(4):
            client.delete(f"/v1/results/{job_id}_{i}")
        results.pop(job_id)
        with status_dict_lock:
            status_dict.pop(job_id, None)


def _post_op(kind: str, job_id: str | None, body: BaseModel | dict, **params):
    if job_id is not None:
        params["job_id"] = job_id
    if isinstance(body, BaseModel):
        return client.post(
            f"/v1/ops/{kind}",
            params=params,
            content=body.model_dump_json(),
            headers={"Content-Type": "application/json"},
        )
    return client.post(f"/v1/ops/{kind}", params=params, json=body)


def _answer(response) -> OpOutputs:
    assert response.status_code == 200, response.text
    return OpOutputs.model_validate_json(response.text)


def _location(ref: ResultRef) -> WorkerLocation:
    return WorkerLocation.model_validate_json(ref.location)


def _rows(ref: ResultRef) -> pl.DataFrame:
    response = client.get(f"/v1/results/{ref.result_id}/sample", params={"n": 1000})
    assert response.status_code == 200, response.text
    return pl.read_ipc_stream(response.content)


def _split_body(plan: pl.LazyFrame, splits: list[tuple[str, float]], flow_id: int = FLOW_ID) -> dict:
    return {
        "operation": base64.b64encode(plan.serialize()).decode("ascii"),
        "flowfile_flow_id": flow_id,
        "flowfile_node_id": NODE_ID,
        "splits": [list(split) for split in splits],
    }


def _train_body(target: str = "y", flow_id: int = FLOW_ID, **extra) -> dict:
    return {
        "operation": base64.b64encode(TRAINING.serialize()).decode("ascii"),
        "flowfile_flow_id": flow_id,
        "flowfile_node_id": NODE_ID,
        "model_type": "linear_regression",
        "target_column": target,
        "feature_columns": ["x1", "x2"],
        "params": {"add_bias": True},
        **extra,
    }


def _apply_body(model: str, **extra) -> dict:
    return {
        "operation": base64.b64encode(SCORING.serialize()).decode("ascii"),
        "flowfile_flow_id": FLOW_ID,
        "flowfile_node_id": NODE_ID,
        "model": model,
        "output_column": "pred",
        **extra,
    }


def _custom_node(source: str, class_name: str, inputs: list[pl.LazyFrame], output_names: list[str]):
    return models.CustomNodeExecuteInput(
        node_source=source,
        class_name=class_name,
        inputs=[plan.serialize() for plan in inputs],
        output_names=output_names,
        flowfile_flow_id=FLOW_ID,
        flowfile_node_id=NODE_ID,
    )


def _fuzzy_join_input() -> models.FuzzyJoinInput:
    left = pl.LazyFrame({"name": ["eduward", "edward", "courtney"]})
    right = pl.LazyFrame({"name_right": ["eduward", "edward", "courtney"]})
    return models.FuzzyJoinInput(
        left_df_operation=models.PolarsOperation(operation=left.serialize()),
        right_df_operation=models.PolarsOperation(operation=right.serialize()),
        fuzzy_maps=[
            models.FuzzyMapping(left_col="name", right_col="name_right", threshold_score=75.0, fuzzy_type="levenshtein")
        ],
        flowfile_flow_id=FLOW_ID,
        flowfile_node_id=NODE_ID,
    )


def test_a_file_read_holds_its_rows_as_one_materialised_output(jobs, tmp_path):
    job_id = jobs()
    csv_path = tmp_path / "people.csv"
    PEOPLE.write_csv(csv_path)

    response = _post_op(
        "file_read",
        job_id,
        ReceivedTable.create_from_path(str(csv_path), "csv"),
        flowfile_flow_id=FLOW_ID,
        flowfile_node_id=NODE_ID,
    )

    answer = _answer(response)
    assert list(answer.outputs) == ["result"]
    assert answer.metadata == {}
    ref = answer.outputs["result"]
    assert ref.result_id == f"{job_id}_0"
    assert (ref.state, ref.row_count) == ("materialised", 5)
    assert ref.columns == schema_to_wire(PEOPLE.schema)
    location = _location(ref)
    assert location.kind == "file"
    assert location.path == str(result_path(FLOW_ID, ref.result_id))
    assert os.path.isfile(location.path)

    two = client.get(f"/v1/results/{ref.result_id}/sample", params={"n": 2})
    assert_frame_equal(pl.read_ipc_stream(two.content), PEOPLE.head(2))
    assert client.get(f"/v1/results/{ref.result_id}/count").json() == {"row_count": 5}
    assert client.head(f"/v1/results/{ref.result_id}").status_code == 200
    assert client.delete(f"/v1/results/{ref.result_id}").status_code == 204
    assert not os.path.exists(location.path)
    job = client.get(f"/v1/jobs/{job_id}").json()
    assert (job["status"], job["results"]) == ("Completed", None)


def test_a_directory_scan_is_422_before_anything_runs(jobs, tmp_path):
    job_id = jobs()
    table = ReceivedTable.create_from_path(str(tmp_path), "csv").model_copy(update={"scan_mode": "directory"})

    response = _post_op("file_read", job_id, table, flowfile_flow_id=FLOW_ID)

    assert response.status_code == 422
    assert "Directory scan" in response.text
    assert job_id not in status_dict


def test_a_fuzzy_join_holds_the_matched_rows(jobs):
    job_id = jobs()

    answer = _answer(_post_op("fuzzy_join", job_id, _fuzzy_join_input()))

    ref = answer.outputs["result"]
    assert list(answer.outputs) == ["result"]
    assert (ref.result_id, ref.state, ref.row_count) == (f"{job_id}_0", "materialised", 5)
    assert [column.name for column in ref.columns] == ["name", "name_right", "name_vs_name_right_levenshtein"]
    assert _rows(ref).height == 5


def test_a_custom_node_holds_each_declared_output_in_its_own_file(jobs):
    job_id = jobs()
    rows = pl.LazyFrame({"value": [5, 15, 25]})

    answer = _answer(
        _post_op("custom_node", job_id, _custom_node(SPLIT_NODE_SOURCE, "SplitNode", [rows], ["high", "low"]))
    )

    high, low = answer.outputs["high"], answer.outputs["low"]
    assert list(answer.outputs) == ["high", "low"]
    assert [(ref.result_id, ref.state, ref.row_count) for ref in (high, low)] == [
        (f"{job_id}_0", "materialised", 2),
        (f"{job_id}_1", "materialised", 1),
    ]
    assert _location(high).path != _location(low).path
    assert _rows(high)["value"].to_list() == [15, 25]
    assert _rows(low)["value"].to_list() == [5]

    assert client.delete(f"/v1/results/{high.result_id}").status_code == 204

    assert client.head(f"/v1/results/{high.result_id}").status_code == 404
    assert client.head(f"/v1/results/{low.result_id}").status_code == 200


def test_a_random_split_holds_each_split_with_the_plans_columns(jobs):
    job_id = jobs()
    plan = (
        pl.LazyFrame({"n": list(range(50)), "geom": pl.Series([bytes([i]) for i in range(50)], dtype=GEOARROW_WKB)})
        .with_columns(pl.int_range(0, pl.len()).shuffle(seed=3).alias("rank"))
        .sort("rank")
        .drop("rank")
    )

    answer = _answer(_post_op("random_split", job_id, _split_body(plan, [("train", 80.0), ("test", 20.0)])))

    train, test = answer.outputs["train"], answer.outputs["test"]
    assert list(answer.outputs) == ["train", "test"]
    assert [(ref.result_id, ref.state, ref.row_count) for ref in (train, test)] == [
        (f"{job_id}_0", "materialised", 40),
        (f"{job_id}_1", "materialised", 10),
    ]
    assert train.columns == test.columns == schema_to_wire(plan.collect_schema())
    assert train.columns[1].data_type == "Extension('geoarrow.wkb', Binary, '{}')"
    train_rows, test_rows = _rows(train)["n"].to_list(), _rows(test)["n"].to_list()
    assert not set(train_rows) & set(test_rows)
    assert sorted(train_rows + test_rows) == list(range(50))


def test_the_last_split_takes_the_rows_rounding_leaves(jobs):
    plan = pl.LazyFrame({"n": list(range(10))})

    answer = _answer(_post_op("random_split", jobs(), _split_body(plan, [("a", 33.3), ("b", 33.3), ("c", 33.4)])))

    assert [ref.row_count for ref in answer.outputs.values()] == [3, 3, 4]


def test_a_rerun_under_the_job_id_replaces_its_outputs(jobs):
    job_id = jobs()
    first = _answer(
        _post_op("random_split", job_id, _split_body(pl.LazyFrame({"n": list(range(10))}), [("all", 100.0)]))
    )
    first_path = _location(first.outputs["all"]).path

    again = _answer(
        _post_op(
            "random_split",
            job_id,
            _split_body(pl.LazyFrame({"n": list(range(4))}), [("all", 100.0)], flow_id=OTHER_FLOW_ID),
        )
    )

    ref = again.outputs["all"]
    assert (ref.result_id, ref.row_count) == (f"{job_id}_0", 4)
    assert _location(ref).path == str(result_path(OTHER_FLOW_ID, ref.result_id))
    assert not os.path.exists(first_path)
    assert _rows(ref)["n"].to_list() == [0, 1, 2, 3]


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_a_database_read_holds_the_rows_it_read(jobs):
    body = {
        "connection": {
            "host": "localhost",
            "password": encrypt_secret("testpass"),
            "username": "testuser",
            "port": 5433,
            "database": "testdb",
        },
        "query": "SELECT * FROM public.movies",
        "flowfile_flow_id": FLOW_ID,
        "flowfile_node_id": NODE_ID,
    }

    ref = _answer(_post_op("database_read", jobs(), body)).outputs["result"]

    assert ref.row_count > 0
    assert _rows(ref).height == min(ref.row_count, 1000)


def test_a_read_that_fails_holds_nothing_and_names_the_error(jobs):
    job_id = jobs()
    body = {
        "auth_method": "oauth",
        "property_id": "123",
        "metrics": ["sessions"],
        "dimensions": ["date"],
        "flowfile_flow_id": FLOW_ID,
        "flowfile_node_id": NODE_ID,
    }

    response = _post_op("google_analytics_read", job_id, body)

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert (detail["status"], detail["error_kind"]) == ("Error", "task")
    assert "OAuth client credentials missing" in detail["error_message"]
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert not result_path(FLOW_ID, f"{job_id}_0").exists()
    assert client.get(f"/v1/jobs/{job_id}").json()["status"] == "Error"


def test_bytes_that_are_no_plan_are_an_environment_failure_and_hold_nothing(jobs):
    job_id = jobs()
    body = {
        "operation": base64.b64encode(b"not a plan").decode("ascii"),
        "flowfile_flow_id": FLOW_ID,
        "splits": [["all", 100.0]],
    }

    response = _post_op("random_split", job_id, body)

    assert response.status_code == 500
    assert response.json()["detail"]["error_kind"] == "environment"
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert not result_path(FLOW_ID, f"{job_id}_0").exists()


def test_a_cancelled_operation_answers_500_and_holds_nothing(jobs):
    job_id = jobs()
    body = _custom_node(SLEEPING_NODE_SOURCE, "SleepingNode", [pl.LazyFrame({"value": [1]})], ["main"])
    answers = []
    poster = threading.Thread(target=lambda: answers.append(_post_op("custom_node", job_id, body)))
    poster.start()
    deadline = time.monotonic() + 30
    while client.get(f"/v1/jobs/{job_id}").json().get("status") != "Processing":
        assert time.monotonic() < deadline, "the operation never started"
        time.sleep(0.05)

    assert client.post(f"/v1/jobs/{job_id}/cancel").status_code == 200
    poster.join(30)

    assert answers[0].status_code == 500
    assert answers[0].json()["detail"]["status"] == "Cancelled"
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert not result_path(FLOW_ID, f"{job_id}_0").exists()


@pytest.mark.parametrize("job_id", ["a/b", "../x", None], ids=["slash", "dot-dot", "missing"])
def test_a_job_id_that_could_name_another_file_is_422(job_id):
    response = _post_op("fuzzy_join", job_id, _fuzzy_join_input())

    assert response.status_code == 422
    assert "job_id" in response.text


def test_a_body_missing_a_required_field_is_422(jobs):
    job_id = jobs()

    response = _post_op("random_split", job_id, {"flowfile_flow_id": FLOW_ID, "splits": [["all", 100.0]]})

    assert response.status_code == 422
    assert job_id not in status_dict


def test_a_training_holds_the_model_as_a_blob_and_reports_its_digest(jobs, tmp_path):
    job_id = jobs()

    answer = _answer(_post_op("ml_train", job_id, _train_body()))

    assert list(answer.outputs) == ["model"]
    model = answer.outputs["model"]
    assert (model.result_id, model.kind, model.state, model.columns, model.row_count) == (
        f"{job_id}_0",
        "blob",
        "materialised",
        [],
        None,
    )
    location = _location(model)
    path = result_path(FLOW_ID, model.result_id).with_suffix(".blob")
    assert (location.kind, location.path) == ("blob", str(path))
    written = path.read_bytes()
    assert json.loads(written)["model_type"] == "linear_regression"
    assert answer.metadata == {
        "sha256": hashlib.sha256(written).hexdigest(),
        "size_bytes": len(written),
        "model_type": "linear_regression",
    }
    assert client.head(f"/v1/results/{model.result_id}").status_code == 200
    copy = tmp_path / "model.json"
    exported = client.post(f"/v1/results/{model.result_id}/export", json={"uri": file_uri(copy)})
    assert exported.status_code == 200, exported.text
    assert copy.read_bytes() == written
    job = client.get(f"/v1/jobs/{job_id}").json()
    assert (job["status"], job["results"]) == ("Completed", None)


def test_an_apply_scores_with_a_held_model_and_holds_the_scored_rows(jobs):
    model = _answer(_post_op("ml_train", jobs(), _train_body())).outputs["model"]
    job_id = jobs()

    answer = _answer(_post_op("ml_apply", job_id, _apply_body(model.result_id)))

    assert list(answer.outputs) == ["result"]
    assert answer.metadata == {}
    ref = answer.outputs["result"]
    assert (ref.result_id, ref.kind, ref.state, ref.row_count) == (f"{job_id}_0", "table", "materialised", 3)
    assert ref.columns == schema_to_wire(pl.Schema({"x1": pl.Float64, "x2": pl.Float64, "pred": pl.Float64}))
    fitted = json.loads(Path(_location(model).path).read_bytes())
    assert_frame_equal(_rows(ref), get_trainer("linear_regression").apply(SCORING, fitted, "pred").collect())
    assert client.head(f"/v1/results/{model.result_id}").status_code == 200


def test_an_apply_whose_model_is_not_held_is_404(jobs):
    released = _answer(_post_op("ml_train", jobs(), _train_body())).outputs["model"]
    assert client.delete(f"/v1/results/{released.result_id}").status_code == 204

    for model in (uuid.uuid4().hex, released.result_id):
        job_id = jobs()
        response = _post_op("ml_apply", job_id, _apply_body(model))

        assert response.status_code == 404
        assert model in response.json()["detail"]
        assert client.head(f"/v1/results/{job_id}_0").status_code == 404
        assert job_id not in status_dict


def test_an_apply_whose_model_is_a_table_is_422(jobs):
    split = _answer(_post_op("random_split", jobs(), _split_body(SCORING, [("all", 100.0)]))).outputs["all"]
    job_id = jobs()

    response = _post_op("ml_apply", job_id, _apply_body(split.result_id))

    assert response.status_code == 422
    assert split.result_id in response.json()["detail"]
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert job_id not in status_dict
    assert client.head(f"/v1/results/{split.result_id}").status_code == 200


def test_a_training_that_fails_names_the_error_and_holds_no_model(jobs):
    job_id = jobs()

    response = _post_op("ml_train", job_id, _train_body(target="missing"))

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert (detail["status"], detail["error_kind"]) == ("Error", "task")
    assert detail["error_message"].startswith('ColumnNotFoundError: unable to find column "missing"')
    assert not result_path(FLOW_ID, f"{job_id}_0").with_suffix(".blob").exists()
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert client.get(f"/v1/jobs/{job_id}").json()["status"] == "Error"


def test_a_rerun_of_a_training_replaces_its_model(jobs):
    job_id = jobs()
    first = _answer(_post_op("ml_train", job_id, _train_body())).outputs["model"]
    first_path = _location(first).path

    again = _answer(_post_op("ml_train", job_id, _train_body(flow_id=OTHER_FLOW_ID))).outputs["model"]

    assert again.result_id == first.result_id == f"{job_id}_0"
    assert _location(again).path == str(result_path(OTHER_FLOW_ID, again.result_id).with_suffix(".blob"))
    assert not os.path.exists(first_path)
    assert os.path.isfile(_location(again).path)


def test_an_ml_body_that_names_a_path_is_422(jobs, tmp_path):
    model = _answer(_post_op("ml_train", jobs(), _train_body())).outputs["model"]
    job_id = jobs()

    train = _post_op("ml_train", job_id, _train_body(staging_path=str(tmp_path / "model.json")))
    apply = _post_op("ml_apply", job_id, _apply_body(model.result_id, model_path=str(tmp_path / "model.json")))

    assert (train.status_code, apply.status_code) == (422, 422)
    assert "staging_path" in train.text
    assert "model_path" in apply.text
    assert job_id not in status_dict
    assert not (tmp_path / "model.json").exists()

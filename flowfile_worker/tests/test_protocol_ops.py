"""The worker's ``POST /v1/ops/<kind>`` routes: an operation runs in its child and each output it wrote is held.

A training holds the model it fitted as a blob, and an apply scores with a blob held here. A write holds nothing.
"""

import base64
import hashlib
import io
import json
import os
import sqlite3
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
from flowfile_worker.external_sources.s3_source.models import FullCloudStorageConnection, WriteSettings
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


def _polars_code_body(code: str, inputs: list[pl.LazyFrame]) -> dict:
    return {
        "code": code,
        "inputs": [base64.b64encode(plan.serialize()).decode("ascii") for plan in inputs],
        "flowfile_flow_id": FLOW_ID,
        "flowfile_node_id": NODE_ID,
    }


def _sql_query_body(sql_code: str, inputs: list[pl.LazyFrame]) -> dict:
    return {
        "sql_code": sql_code,
        "inputs": [base64.b64encode(plan.serialize()).decode("ascii") for plan in inputs],
        "flowfile_flow_id": FLOW_ID,
        "flowfile_node_id": NODE_ID,
    }


def test_polars_code_holds_a_lazy_result_deferred_with_the_plans_columns(jobs, tmp_path):
    """The inputs scan files, as a held result's plan does, so the join's plan carries no rows and is held as it is."""
    job_id = jobs()
    pl.DataFrame({"id": [1, 2, 3], "geom": [bytes([i]) for i in range(3)]}).write_ipc(tmp_path / "shapes.arrow")
    pl.DataFrame({"id": [2, 3, 4], "name": ["b", "c", "d"]}).write_ipc(tmp_path / "names.arrow")
    shapes = pl.scan_ipc(tmp_path / "shapes.arrow").with_columns(pl.col("geom").cast(GEOARROW_WKB))
    names = pl.scan_ipc(tmp_path / "names.arrow")
    body = _polars_code_body("input_df_1.join(input_df_2, on='id')", [shapes, names])

    answer = _answer(_post_op("polars_code", job_id, body))

    ref = answer.outputs["result"]
    assert list(answer.outputs) == ["result"]
    assert (ref.result_id, ref.state, ref.row_count) == (f"{job_id}_0", "deferred", None)
    assert ref.columns == schema_to_wire(shapes.join(names, on="id").collect_schema())
    assert ref.columns[1].data_type == "Extension('geoarrow.wkb', Binary, '{}')"
    response = client.get(f"/v1/results/{ref.result_id}/sample", params={"n": 10})
    assert response.status_code == 200, response.text
    assert sorted(pl.read_ipc_stream(response.content)["name"].to_list()) == ["b", "c"]


def test_polars_code_holds_an_eager_result_as_a_file(jobs):
    job_id = jobs()
    body = _polars_code_body("output_df = input_df.collect().head(2)", [PEOPLE.lazy()])

    answer = _answer(_post_op("polars_code", job_id, body))

    ref = answer.outputs["result"]
    assert (ref.result_id, ref.state, ref.row_count) == (f"{job_id}_0", "materialised", 2)
    assert _location(ref).path == str(result_path(FLOW_ID, ref.result_id))
    assert _rows(ref)["id"].to_list() == [1, 2]


def test_polars_code_holds_a_lazy_result_over_rows_in_memory_as_a_file(jobs, tmp_path):
    PEOPLE.write_ipc(tmp_path / "people.arrow")
    code = "rows = input_df.collect()\noutput_df = rows.lazy().select('id', 'name')"
    job_id = jobs()
    body = _polars_code_body(code, [pl.scan_ipc(tmp_path / "people.arrow")])

    answer = _answer(_post_op("polars_code", job_id, body))

    ref = answer.outputs["result"]
    assert (ref.result_id, ref.state, ref.row_count) == (f"{job_id}_0", "materialised", 5)
    assert ref.columns == schema_to_wire(PEOPLE.schema)
    location = _location(ref)
    assert (location.kind, location.plan) == ("file", None)
    assert location.path == str(result_path(FLOW_ID, ref.result_id))
    assert _rows(ref)["id"].to_list() == [1, 2, 3, 4, 5]


def test_polars_code_holds_a_lazy_result_over_its_input_scan_as_a_plan_that_scans_it(jobs, tmp_path):
    source = tmp_path / "people.arrow"
    PEOPLE.write_ipc(source)
    job_id = jobs()
    body = _polars_code_body("output_df = input_df.filter(pl.col('id') > 2)", [pl.scan_ipc(source)])

    answer = _answer(_post_op("polars_code", job_id, body))

    ref = answer.outputs["result"]
    assert (ref.result_id, ref.state, ref.row_count) == (f"{job_id}_0", "deferred", None)
    location = _location(ref)
    assert location.kind == "plan"
    plan = pl.LazyFrame.deserialize(io.BytesIO(location.plan))
    assert plan.collect()["id"].to_list() == [3, 4, 5]
    source.unlink()
    with pytest.raises(FileNotFoundError):
        plan.collect()


def test_polars_code_that_fails_answers_500_and_holds_nothing(jobs):
    job_id = jobs()
    body = _polars_code_body("output_df = input_df.select('missing')", [PEOPLE.lazy()])

    response = _post_op("polars_code", job_id, body)

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert (detail["status"], detail["error_kind"]) == ("Error", "task")
    assert "ColumnNotFoundError" in detail["error_message"]
    assert "missing" in detail["error_message"]
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert not result_path(FLOW_ID, f"{job_id}_0").exists()


def test_sql_query_holds_a_result_over_its_input_scans_as_a_plan_that_scans_them(jobs, tmp_path):
    pl.DataFrame({"id": [1, 2, 3], "geom": [bytes([i]) for i in range(3)]}).write_ipc(tmp_path / "shapes.arrow")
    pl.DataFrame({"id": [2, 3, 4], "name": ["b", "c", "d"]}).write_ipc(tmp_path / "names.arrow")
    shapes = pl.scan_ipc(tmp_path / "shapes.arrow")
    names = pl.scan_ipc(tmp_path / "names.arrow")
    query = "SELECT a.id, b.name FROM input_1 a JOIN input_2 b ON a.id = b.id"
    job_id = jobs()

    answer = _answer(_post_op("sql_query", job_id, _sql_query_body(query, [shapes, names])))

    ref = answer.outputs["result"]
    assert list(answer.outputs) == ["result"]
    assert (ref.result_id, ref.state, ref.row_count) == (f"{job_id}_0", "deferred", None)
    context = pl.SQLContext(input_1=shapes, input_2=names)
    assert ref.columns == schema_to_wire(context.execute(query).collect_schema())
    location = _location(ref)
    assert location.kind == "plan"
    response = client.get(f"/v1/results/{ref.result_id}/sample", params={"n": 10})
    assert response.status_code == 200, response.text
    assert sorted(pl.read_ipc_stream(response.content)["name"].to_list()) == ["b", "c"]
    plan = pl.LazyFrame.deserialize(io.BytesIO(location.plan))
    (tmp_path / "names.arrow").unlink()
    with pytest.raises(FileNotFoundError):
        plan.collect()


def test_sql_query_holds_a_result_over_rows_in_memory_as_a_file(jobs):
    job_id = jobs()

    answer = _answer(_post_op("sql_query", job_id, _sql_query_body("SELECT id, name FROM input_1", [PEOPLE.lazy()])))

    ref = answer.outputs["result"]
    assert (ref.result_id, ref.state, ref.row_count) == (f"{job_id}_0", "materialised", 5)
    location = _location(ref)
    assert (location.kind, location.plan) == ("file", None)
    assert location.path == str(result_path(FLOW_ID, ref.result_id))
    assert _rows(ref)["id"].to_list() == [1, 2, 3, 4, 5]


def test_sql_query_that_fails_answers_500_and_holds_nothing(jobs):
    job_id = jobs()

    response = _post_op("sql_query", job_id, _sql_query_body("SELECT missing FROM input_1", [PEOPLE.lazy()]))

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert (detail["status"], detail["error_kind"]) == ("Error", "task")
    assert "missing" in detail["error_message"]
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert not result_path(FLOW_ID, f"{job_id}_0").exists()


def test_a_sql_statement_the_worker_refuses_holds_nothing(jobs):
    job_id = jobs()

    response = _post_op("sql_query", job_id, _sql_query_body("DROP TABLE input_1", [PEOPLE.lazy()]))

    assert response.status_code == 500
    assert "Only SELECT queries are allowed" in response.json()["detail"]["error_message"]
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert not result_path(FLOW_ID, f"{job_id}_0").exists()


def _write_body(plan: pl.LazyFrame, **fields) -> dict:
    return {
        "operation": base64.b64encode(plan.serialize()).decode("ascii"),
        "flowfile_flow_id": FLOW_ID,
        "flowfile_node_id": NODE_ID,
        **fields,
    }


def _minio_available() -> bool:
    from test_utils.s3.fixtures import get_minio_client

    if not is_docker_available():
        return False
    try:
        get_minio_client().list_buckets()
        return True
    except Exception:
        return False


def test_a_file_write_writes_its_target_and_holds_nothing(jobs, tmp_path):
    job_id = jobs()
    target = tmp_path / "people.csv"
    body = _write_body(PEOPLE.lazy(), data_type="csv", path=str(target), write_mode="overwrite", delimiter=";")

    answer = _answer(_post_op("file_write", job_id, body))

    assert (answer.outputs, answer.metadata) == ({}, {})
    assert_frame_equal(pl.read_csv(target, separator=";"), PEOPLE)
    assert client.get(f"/v1/jobs/{job_id}").json()["status"] == "Completed"
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404


def test_a_database_write_writes_its_table_and_holds_nothing(jobs, tmp_path):
    database = tmp_path / "people.db"
    body = _write_body(
        PEOPLE.lazy(),
        connection={"database_type": "sqlite", "url": f"sqlite:///{database}"},
        table_name="people",
        if_exists="replace",
    )

    answer = _answer(_post_op("database_write", jobs(), body))

    assert answer.outputs == {}
    with sqlite3.connect(database) as db:
        assert db.execute("SELECT id, name FROM people ORDER BY id").fetchall() == list(PEOPLE.iter_rows())


@pytest.mark.skipif(not _minio_available(), reason="MinIO mock S3 not available")
def test_a_cloud_write_writes_its_object_with_the_connection_it_decrypts(jobs):
    from test_utils.s3.fixtures import get_minio_client

    connection = FullCloudStorageConnection(
        connection_name="minio-test",
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id="minioadmin",
        aws_secret_access_key=encrypt_secret("minioadmin"),
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
        aws_allow_unsafe_html=True,
    )
    key = f"v1-ops/{uuid.uuid4().hex}.parquet"
    body = models.CloudStorageScriptWrite(
        connection=connection,
        write_settings=WriteSettings(resource_path=f"s3://worker-test-bucket/{key}", file_format="parquet"),
        operation=PEOPLE.lazy().serialize(),
    ).model_dump(mode="json")
    body["connection"]["aws_secret_access_key"] = connection.aws_secret_access_key.get_secret_value()
    minio = get_minio_client()
    try:
        answer = _answer(_post_op("cloud_write", jobs(), body))

        assert answer.outputs == {}
        written = minio.get_object(Bucket="worker-test-bucket", Key=key)["Body"].read()
        assert_frame_equal(pl.read_parquet(io.BytesIO(written)), PEOPLE)
    finally:
        minio.delete_object(Bucket="worker-test-bucket", Key=key)


def test_a_write_that_fails_answers_500_names_the_error_and_holds_nothing(jobs, tmp_path):
    job_id = jobs()
    (tmp_path / "a-file").write_text("not a directory")
    target = tmp_path / "a-file" / "people.csv"

    body = _write_body(PEOPLE.lazy(), data_type="csv", path=str(target), write_mode="overwrite")

    response = _post_op("file_write", job_id, body)

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert (detail["status"], detail["error_kind"]) == ("Error", "task")
    assert detail["error_message"]
    assert client.get(f"/v1/jobs/{job_id}").json()["status"] == "Error"
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404


def test_a_write_body_naming_an_unknown_field_is_422_and_runs_nothing(jobs, tmp_path):
    job_id = jobs()
    body = _write_body(
        PEOPLE.lazy(), data_type="csv", path=str(tmp_path / "x.csv"), write_mode="overwrite", task_id="t"
    )

    response = _post_op("file_write", job_id, body)

    assert response.status_code == 422
    assert job_id not in status_dict
    assert not (tmp_path / "x.csv").exists()

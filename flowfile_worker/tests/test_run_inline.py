"""The worker's in-process mode: with ``spawner.in_process`` set, a job runs in the calling process.

Each case makes the same request twice, once on the child path and once in process, where starting a child or leasing
a pool member fails the test, and compares the two answers. A failure in process is the typed failure a child answers.
"""

from collections.abc import Callable
from typing import Any, TypeVar

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_worker import mp_context, pool, spawner
from flowfile_worker.create.models import ReceivedTable
from flowfile_worker.protocol_routes import SAMPLE_ROWS_IN_PARENT, OpOutputs
from flowfile_worker.results import result_path
from flowfile_worker.secrets import encrypt_secret
from shared.compute_protocol import SubmitResult, file_uri
from tests.test_execute_custom_node import clean_import_aliases  # noqa: F401 - autouse, undoes a custom node's aliases
from tests.test_protocol_ops import FLOW_ID as OPS_FLOW_ID
from tests.test_protocol_ops import NODE_ID as OPS_NODE_ID
from tests.test_protocol_ops import (
    PEOPLE,
    SPLIT_NODE_SOURCE,
    _answer,
    _apply_body,
    _custom_node,
    _fuzzy_join_input,
    _polars_code_body,
    _post_op,
    _rows,
    _split_body,
    _sql_query_body,
    _train_body,
    jobs,  # noqa: F401 - fixture
)
from tests.test_protocol_routes import (
    FLOW_ID,
    _meta,
    _plan,
    _post,
    _submit,
    _typed_plan,
    client,
    held,  # noqa: F401 - fixture
    one_member_pool,  # noqa: F401 - fixture
)
from tests.utils import is_docker_available

pytestmark = pytest.mark.worker

T = TypeVar("T")

RAISING_NODE_SOURCE = """
from shared.node_designer import CustomNodeBase


class RaisingNode(CustomNodeBase):
    node_name: str = "Raising Node"

    def process(self, *inputs):
        raise ValueError("no rows for this node")
"""


def _child_and_inline(monkeypatch, call: Callable[[], T]) -> tuple[T, T]:
    """*call*'s answer on the child path, then its answer with jobs run in this process, which starts no child."""
    in_child = call()
    touched: list[str] = []

    def refuse(what: str) -> Callable[..., Any]:
        def touch(*args, **kwargs):
            touched.append(what)
            raise AssertionError(f"{what} while jobs run in process")

        return touch

    with monkeypatch.context() as patched:
        patched.setattr(spawner, "in_process", True)
        patched.setattr(mp_context, "Process", refuse("a child was started"))
        patched.setattr(pool.task_pool, "acquire", refuse("a pool member was leased"))
        inline = call()
    assert touched == []
    return in_child, inline


def _op_answer(kind: str, job_id: str, body: Any, **params) -> tuple[OpOutputs, dict[str, list[dict]]]:
    """An operation's answer and the rows of each table output it holds, read before a second run replaces them.

    The rows are sorted, as a fuzzy join answers its matches in no fixed order on either path.
    """
    answer = _answer(_post_op(kind, job_id, body, **params))
    tables = {name: _rows(ref) for name, ref in answer.outputs.items() if ref.kind == "table"}
    return answer, {name: rows.sort(rows.columns).to_dicts() for name, rows in tables.items()}


@pytest.mark.parametrize(
    "hints",
    [{"must_materialise": True, "preview_rows": 2}, {"preview_rows": 3}, {"wide": True}],
    ids=["materialised-with-a-preview", "deferred-with-a-preview", "deferred-without-a-preview"],
)
def test_a_submit_answers_in_process_what_it_answers_in_a_child(held, monkeypatch, hints):
    result_id = held()
    plan = _typed_plan().serialize()

    in_child, inline = _child_and_inline(monkeypatch, lambda: _post(plan, _meta(result_id, **hints)))

    assert (in_child.status_code, inline.status_code) == (200, 200), inline.text
    child_result = SubmitResult.model_validate_json(in_child.text)
    inline_result = SubmitResult.model_validate_json(inline.text)
    assert inline_result.ref == child_result.ref
    assert inline_result.job_id == child_result.job_id == result_id
    assert inline_result.sample == child_result.sample
    assert (inline_result.sample is None) == ("preview_rows" not in hints)
    assert result_path(FLOW_ID, result_id).is_file() == (inline_result.ref.state == "materialised")
    assert client.get(f"/v1/jobs/{result_id}").json()["status"] == "Completed"


@pytest.mark.parametrize(
    ("plan", "must_materialise", "n"),
    [(_plan(), False, 3), (pl.LazyFrame({"id": range(1_500)}), True, SAMPLE_ROWS_IN_PARENT + 1)],
    ids=["deferred", "materialised-above-the-parents-share"],
)
def test_a_sample_read_in_process_is_the_sample_a_child_reads(held, monkeypatch, plan, must_materialise, n):
    result_id = held()
    _submit(result_id, plan, must_materialise=must_materialise)

    in_child, inline = _child_and_inline(
        monkeypatch, lambda: client.get(f"/v1/results/{result_id}/sample", params={"n": n})
    )

    assert (in_child.status_code, inline.status_code) == (200, 200), inline.text
    assert inline.content == in_child.content
    assert_frame_equal(pl.read_ipc_stream(inline.content), plan.head(n).collect())


def test_a_count_of_a_deferred_result_in_process_is_the_childs(held, monkeypatch):
    result_id = held()
    _submit(result_id, _plan())

    in_child, inline = _child_and_inline(monkeypatch, lambda: client.get(f"/v1/results/{result_id}/count"))

    assert inline.json() == in_child.json() == {"row_count": 5}


@pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred"])
def test_an_export_in_process_writes_what_a_child_writes(held, monkeypatch, tmp_path, must_materialise):
    result_id = held()
    _submit(result_id, _plan(), must_materialise=must_materialise)
    target = tmp_path / "out.parquet"

    def export() -> tuple[int, dict, list[dict]]:
        target.unlink(missing_ok=True)
        response = client.post(f"/v1/results/{result_id}/export", json={"uri": file_uri(target)})
        return response.status_code, response.json(), pl.read_parquet(target).to_dicts()

    in_child, inline = _child_and_inline(monkeypatch, export)

    assert inline == in_child
    assert inline[:2] == (200, {"uri": file_uri(target)})
    assert client.head(f"/v1/results/{result_id}").status_code == 200


def _people_csv(tmp_path) -> str:
    path = tmp_path / "people.csv"
    PEOPLE.write_csv(path)
    return str(path)


def _people_scan(tmp_path) -> pl.LazyFrame:
    path = tmp_path / "people.arrow"
    PEOPLE.write_ipc(path)
    return pl.scan_ipc(path)


OPERATIONS: dict[str, Callable[[Any], tuple[str, Any, dict]]] = {
    "file_read": lambda tmp_path: (
        "file_read",
        ReceivedTable.create_from_path(_people_csv(tmp_path), "csv"),
        {"flowfile_flow_id": OPS_FLOW_ID, "flowfile_node_id": OPS_NODE_ID},
    ),
    "fuzzy_join": lambda tmp_path: ("fuzzy_join", _fuzzy_join_input(), {}),
    "custom_node": lambda tmp_path: (
        "custom_node",
        _custom_node(SPLIT_NODE_SOURCE, "SplitNode", [pl.LazyFrame({"value": [5, 15, 25]})], ["high", "low"]),
        {},
    ),
    "random_split": lambda tmp_path: (
        "random_split",
        _split_body(pl.LazyFrame({"n": list(range(50))}), [("train", 80.0), ("test", 20.0)]),
        {},
    ),
    "polars_code-lazy": lambda tmp_path: (
        "polars_code",
        _polars_code_body("output_df = input_df.filter(pl.col('id') > 2)", [_people_scan(tmp_path)]),
        {},
    ),
    "polars_code-eager": lambda tmp_path: (
        "polars_code",
        _polars_code_body("output_df = input_df.collect().head(2)", [PEOPLE.lazy()]),
        {},
    ),
    "sql_query": lambda tmp_path: (
        "sql_query",
        _sql_query_body("SELECT id, name FROM input_1 WHERE id > 1", [PEOPLE.lazy()]),
        {},
    ),
}


@pytest.mark.parametrize("operation", list(OPERATIONS))
def test_an_operation_answers_in_process_what_it_answers_in_a_child(jobs, monkeypatch, tmp_path, operation):
    kind, body, params = OPERATIONS[operation](tmp_path)
    job_id = jobs()

    in_child, inline = _child_and_inline(monkeypatch, lambda: _op_answer(kind, job_id, body, **params))

    assert inline == in_child
    assert all(rows for rows in inline[1].values())


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_a_database_read_in_process_holds_what_a_child_holds(jobs, monkeypatch):
    body = {
        "connection": {
            "host": "localhost",
            "password": encrypt_secret("testpass"),
            "username": "testuser",
            "port": 5433,
            "database": "testdb",
        },
        "query": "SELECT * FROM public.movies",
        "flowfile_flow_id": OPS_FLOW_ID,
        "flowfile_node_id": OPS_NODE_ID,
    }
    job_id = jobs()

    in_child, inline = _child_and_inline(monkeypatch, lambda: _op_answer("database_read", job_id, body))

    assert inline == in_child
    assert inline[0].outputs["result"].row_count > 0


def test_a_training_in_process_holds_the_model_a_child_holds(jobs, monkeypatch):
    job_id = jobs()

    in_child, inline = _child_and_inline(monkeypatch, lambda: _answer(_post_op("ml_train", job_id, _train_body())))

    assert inline.outputs == in_child.outputs
    assert inline.outputs["model"].kind == "blob"
    assert set(inline.metadata) == set(in_child.metadata) == {"sha256", "size_bytes", "model_type"}
    assert client.head(f"/v1/results/{job_id}_0").status_code == 200


def test_an_apply_in_process_scores_as_a_child_scores(jobs, monkeypatch):
    model = _answer(_post_op("ml_train", jobs(), _train_body())).outputs["model"]
    job_id = jobs()

    in_child, inline = _child_and_inline(
        monkeypatch, lambda: _op_answer("ml_apply", job_id, _apply_body(model.result_id))
    )

    assert inline == in_child
    assert inline[0].outputs["result"].row_count == 3


@pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred-without-a-preview"])
def test_a_plan_that_fails_on_its_data_in_process_is_the_childs_task_failure(held, monkeypatch, must_materialise):
    result_id = held()
    plan = pl.LazyFrame({"a": [1]}).select("missing").serialize()

    in_child, inline = _child_and_inline(
        monkeypatch, lambda: _post(plan, _meta(result_id, must_materialise=must_materialise))
    )

    assert (in_child.status_code, inline.status_code) == (500, 500)
    assert inline.json() == in_child.json()
    detail = inline.json()["detail"]
    assert (detail["status"], detail["error_kind"]) == ("Error", "task")
    assert detail["error_message"].startswith("ColumnNotFoundError: ")
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert not result_path(FLOW_ID, result_id).exists()


@pytest.mark.parametrize("source", ["unloadable_plan", "not-a-plan"])
def test_a_plan_that_cannot_load_in_process_is_the_childs_environment_failure(held, monkeypatch, request, source):
    result_id = held()
    plan = request.getfixturevalue(source) if source == "unloadable_plan" else b"not a plan"

    in_child, inline = _child_and_inline(monkeypatch, lambda: _post(plan, _meta(result_id, must_materialise=True)))

    assert (in_child.status_code, inline.status_code) == (500, 500)
    assert inline.json() == in_child.json()
    assert inline.json()["detail"]["error_kind"] == "environment"
    assert client.head(f"/v1/results/{result_id}").status_code == 404


@pytest.mark.parametrize(
    ("kind", "body", "message"),
    [
        (
            "custom_node",
            _custom_node(RAISING_NODE_SOURCE, "RaisingNode", [pl.LazyFrame({"value": [1]})], ["main"]),
            "ValueError: no rows for this node",
        ),
        ("polars_code", _polars_code_body("output_df = input_df.select('missing')", [PEOPLE.lazy()]), "ColumnNotFound"),
    ],
    ids=["custom_node", "polars_code"],
)
def test_an_operation_that_fails_in_process_answers_the_childs_500_and_holds_nothing(
    jobs, monkeypatch, kind, body, message
):
    job_id = jobs()

    in_child, inline = _child_and_inline(monkeypatch, lambda: _post_op(kind, job_id, body))

    assert (in_child.status_code, inline.status_code) == (500, 500)
    assert inline.json() == in_child.json()
    detail = inline.json()["detail"]
    assert (detail["status"], detail["error_kind"]) == ("Error", "task")
    assert detail["error_message"].startswith(message)
    assert client.head(f"/v1/results/{job_id}_0").status_code == 404
    assert not result_path(OPS_FLOW_ID, f"{job_id}_0").exists()


def test_the_job_of_a_submit_that_failed_in_process_carries_the_error(held, monkeypatch):
    result_id = held()
    monkeypatch.setattr(spawner, "in_process", True)

    _post(pl.LazyFrame({"a": [1]}).select("missing").serialize(), _meta(result_id))

    job = client.get(f"/v1/jobs/{result_id}").json()
    assert (job["status"], job["progress"], job["error_kind"]) == ("Error", -1, "task")
    assert job["error_message"].startswith("ColumnNotFoundError")


def test_jobs_run_in_process_leave_the_pool_unused(held, jobs, monkeypatch, one_member_pool, tmp_path):
    monkeypatch.setattr(spawner, "in_process", True)
    result_id = held()

    _submit(result_id, _plan(), must_materialise=True)
    export = client.post(f"/v1/results/{result_id}/export", json={"uri": file_uri(tmp_path / "out.parquet")})
    split = _post_op("random_split", jobs(), _split_body(_plan(), [("all", 100.0)]))

    assert (export.status_code, split.status_code) == (200, 200), (export.text, split.text)
    assert one_member_pool.stats()["total"] == 0
    assert one_member_pool.describe()["tasks_completed"] == 0

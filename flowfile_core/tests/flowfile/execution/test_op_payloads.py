"""The transform and model operations take one payload dataclass under ``"payload"``, read the same by both backends.

The worker side is checked at the fetcher boundary: the worker backend's fetchers are replaced by stand-ins that keep
what they were built with and write their result where a worker task writes it, so these tests show what each payload
becomes on its way to the worker without one running (``test_worker_backend.py`` runs the same operations against a
real worker). The local side runs in process. The builders are checked on a flow whose default backend is a recording
in-process backend, so a remote flow's operations are seen arriving with their payloads.
"""

import dataclasses
import io
import logging
import os
import uuid
from types import SimpleNamespace

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.auth.models import SecretInput
from flowfile_core.configs.node_store import add_to_custom_node_store, remove_from_custom_node_store
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile import flow_graph as flow_graph_package
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends import local as local_module
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import OperationUnsupported
from flowfile_core.flowfile.execution.op_payloads import (
    PAYLOAD_KEY,
    CustomNodePayload,
    FuzzyJoinPayload,
    MlApplyPayload,
    MlTrainPayload,
    RandomSplitPayload,
    unpack,
)
from flowfile_core.flowfile.execution.protocol import JobHandle, OperationKind, OpResult
from flowfile_core.flowfile.execution.write_payloads import FileWritePayload
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import (
    CustomNodeExecuteInput,
    custom_node_task_id,
)
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, SecretSelector, Section, TextInput
from flowfile_core.schemas import input_schema
from flowfile_core.secret_manager.secret_manager import store_secret
from tests.flowfile.conftest import add_test_manual_input, create_test_graph

pytestmark = pytest.mark.core

SECRET = "plain-api-key-for-the-node"
NAMES = pl.LazyFrame({"name": ["eduward", "edward", "courtney"]})
NUMBERS = pl.DataFrame({"n": list(range(100))})
MATCHED_PAIRS = [
    ("courtney", "courtney"),
    ("eduward", "eduward"),
    ("eduward", "edward"),
    ("edward", "eduward"),
    ("edward", "edward"),
]
SPLITTER_SOURCE = "class Splitter(CustomNodeBase): ...\n"


class Splitter(CustomNodeBase):
    """Splits its input at a threshold read from its settings; the small side comes back computed."""

    node_name: str = "Payload Test Splitter"
    number_of_outputs: int = 2
    output_names: list[str] = ["big", "small"]
    settings_schema: NodeSettings = NodeSettings(
        main=Section(
            title="Main",
            threshold=TextInput(label="Threshold", default="10"),
            api_key=SecretSelector(label="API key"),
        )
    )

    def process(self, *inputs: pl.LazyFrame):
        threshold = int(self.settings_schema.main.threshold.value)
        rows = inputs[0]
        return {"big": rows.filter(pl.col("n") > threshold), "small": rows.filter(pl.col("n") <= threshold).collect()}


class Doubler(CustomNodeBase):
    """Doubles ``n``; one output."""

    node_name: str = "Payload Test Doubler"

    def process(self, *inputs: pl.LazyFrame):
        return inputs[0].with_columns(pl.col("n") * 2)


class Silent(CustomNodeBase):
    """Returns nothing."""

    node_name: str = "Payload Test Silent"

    def process(self, *inputs: pl.LazyFrame):
        return None


def fuzzy_settings() -> input_schema.NodeFuzzyMatch:
    return input_schema.NodeFuzzyMatch(
        flow_id=1,
        node_id=2,
        join_input={
            "join_mapping": [
                {"left_col": "name", "right_col": "name", "threshold_score": 75, "fuzzy_type": "levenshtein"}
            ],
            "left_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
            "right_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
            "how": "inner",
        },
    )


def fuzzy_join(**overrides) -> FuzzyJoinPayload:
    return FuzzyJoinPayload(
        **{"left": NAMES, "right": NAMES, "settings": fuzzy_settings(), "flow_id": 7, "node_id": 3, **overrides}
    )


def random_split(seed: int | None = 7, **overrides) -> RandomSplitPayload:
    settings = input_schema.NodeRandomSplit(flow_id=1, node_id=3, seed=seed)
    return RandomSplitPayload(**{"plan": NUMBERS.lazy(), "settings": settings, "flow_id": 7, "node_id": 3, **overrides})


def custom_node(node_class: type[CustomNodeBase] = Splitter, settings_values=None, **overrides) -> CustomNodePayload:
    settings_values = {"main": {"threshold": "89"}} if settings_values is None else settings_values
    instance = node_class.from_settings(settings_values)
    return CustomNodePayload(
        **{
            "inputs": (NUMBERS.lazy(),),
            "custom_node": instance,
            "node_source": SPLITTER_SOURCE,
            "class_name": node_class.__name__,
            "settings_values": settings_values,
            "output_names": list(instance.output_names),
            "user_id": None,
            "flow_id": 7,
            "node_id": 3,
            **overrides,
        }
    )


def ml_train(**overrides) -> MlTrainPayload:
    return MlTrainPayload(
        **{
            "plan": pl.LazyFrame({"x": [1.0, 2.0], "y": [3.0, 5.0]}),
            "staging_path": "/models/m.json",
            "model_type": "linear_regression",
            "target_column": "y",
            "feature_columns": ["x"],
            "params": {"add_bias": True},
            "flow_id": 7,
            "node_id": 3,
            **overrides,
        }
    )


def ml_apply(**overrides) -> MlApplyPayload:
    return MlApplyPayload(
        **{
            "plan": pl.LazyFrame({"x": [1.0, 2.0]}),
            "model_path": "/models/m.json",
            "output_column": "pred",
            "flow_id": 7,
            "node_id": 3,
            **overrides,
        }
    )


PAYLOADS = {
    OperationKind.fuzzy_join: fuzzy_join,
    OperationKind.random_split: random_split,
    OperationKind.custom_node: custom_node,
    OperationKind.ml_train: ml_train,
    OperationKind.ml_apply: ml_apply,
}
LOCAL_KINDS = (OperationKind.fuzzy_join, OperationKind.random_split, OperationKind.custom_node)
BACKEND_KINDS = [(LocalBackend, kind) for kind in LOCAL_KINDS] + [(WorkerBackend, kind) for kind in PAYLOADS]


def kind_id(value) -> str:
    return value.value if isinstance(value, OperationKind) else value.__name__


class StandInTask:
    """Stands in for a worker fetcher: keeps what it was built with and reports a task that finished.

    ``status.file_ref`` names the Arrow IPC file it wrote, where the backend reads a stored result from.
    """

    on_wait = None

    def __init__(self, args, kwargs, file_ref, status, result):
        self.args = args
        self.kwargs = kwargs
        self.file_ref = file_ref
        self.status = status
        self.result = result
        self.error_code = 0
        self.error_description = None
        self.error_kind = None
        self.cancelled = False

    def get_result(self):
        if self.on_wait is not None:
            self.on_wait(self)
        return self.result

    def get_payload(self):
        """What the finished task reported; the backend reads it after the wait ``get_result`` stands for."""
        return self.result

    def cancel(self):
        self.cancelled = True


class WorkerTasks:
    """Replaces the worker backend's fetchers with ``StandInTask``s; ``built`` lists them in the order built."""

    def __init__(self, monkeypatch, directory):
        self.built: list[StandInTask] = []
        self.monkeypatch = monkeypatch
        self.directory = directory

    def write(self, name: str, rows: pl.DataFrame) -> str:
        path = self.directory / f"{name.replace('#', '_')}.arrow"
        rows.write_ipc(path)
        return str(path)

    def replace(self, fetcher: str, *, rows=None, result=None, file_ref=lambda args, kwargs: kwargs["file_ref"]):
        """Build a stand-in where the backend builds ``fetcher``.

        ``rows`` (a frame, or a function of the build's keyword arguments) is written where the task writes its
        result; ``result`` (a value, or a function of the build's arguments) is what the task reports.
        """

        def build(*args, **kwargs):
            task_id = file_ref(args, kwargs)
            status = None
            if rows is not None:
                frame = rows(kwargs) if callable(rows) else rows
                status = SimpleNamespace(file_ref=self.write(task_id, frame), number_of_records=frame.height)
            reported = result(args, kwargs) if callable(result) else result
            task = StandInTask(args, kwargs, task_id, status, reported)
            self.built.append(task)
            return task

        self.monkeypatch.setattr(worker_module, fetcher, build)

    def replace_all(self) -> None:
        """Replace every fetcher these operations use with one that writes or reports nothing."""
        for fetcher in ("ExternalFuzzyMatchFetcher", "ExternalDfFetcher", "MLTrainFetcher", "MLApplyFetcher"):
            self.replace(fetcher)
        self.replace("ExternalCustomNodeFetcher", file_ref=lambda args, kwargs: args[0].task_id)


@pytest.fixture
def tasks(monkeypatch, tmp_path) -> WorkerTasks:
    return WorkerTasks(monkeypatch, tmp_path)


def stored_rows(kwargs) -> pl.DataFrame:
    """What a worker store task writes: its plan, run."""
    return pl.LazyFrame.deserialize(io.BytesIO(kwargs["plan_bytes"])).collect()


def custom_node_outputs(tasks: WorkerTasks, frames: dict[str, pl.DataFrame]):
    """The payload a worker custom-node task reports: each output's file and row count."""

    def report(args, kwargs):
        task_id = args[0].task_id
        return {
            "outputs": {
                name: {"path": tasks.write(f"{task_id}__{name}", frame), "row_count": frame.height}
                for name, frame in frames.items()
            }
        }

    return report


@pytest.fixture
def user_id() -> int:
    with get_db_context() as db:
        return db.query(db_models.User).filter(db_models.User.username == "local_user").one().id


@pytest.fixture
def stored_secret(user_id) -> str:
    """A secret stored for the local user; its name."""
    name = f"payload_secret_{uuid.uuid4().hex[:8]}"
    with get_db_context() as db:
        store_secret(db, SecretInput(name=name, value=SecretStr(SECRET)), user_id)
    yield name
    with get_db_context() as db:
        db.query(db_models.Secret).filter(db_models.Secret.name == name).delete()
        db.commit()


# The payloads


@pytest.mark.parametrize("kind", list(PAYLOADS), ids=kind_id)
def test_each_payload_comes_back_from_unpack(kind):
    payload = PAYLOADS[kind]()

    assert unpack({PAYLOAD_KEY: payload}, type(payload)) is payload


@pytest.mark.parametrize("kind", list(PAYLOADS), ids=kind_id)
def test_a_payload_is_frozen_keyword_only_and_gets_a_fresh_job_id(kind):
    payload = PAYLOADS[kind]()
    fields = {field.name: getattr(payload, field.name) for field in dataclasses.fields(payload)}

    with pytest.raises(dataclasses.FrozenInstanceError):
        payload.job_id = "another"
    with pytest.raises(TypeError):
        type(payload)(*fields.values())
    assert PAYLOADS[kind]().job_id != payload.job_id
    assert len(payload.job_id) == 32
    assert PAYLOADS[kind](job_id="stable").job_id == "stable"


@pytest.mark.parametrize(("backend_type", "kind"), BACKEND_KINDS, ids=kind_id)
def test_an_operation_without_its_payload_key_names_the_key(backend_type, kind, tasks):
    tasks.replace_all()
    expected = type(PAYLOADS[kind]()).__name__
    message = rf"Expected a {expected} under the payload key 'payload', found no such key \(keys: \['left', 'right'\]\)"

    with pytest.raises(TypeError, match=message):
        backend_type().run_op(kind, {"left": NAMES, "right": NAMES})
    assert tasks.built == []


@pytest.mark.parametrize(("backend_type", "kind"), BACKEND_KINDS, ids=kind_id)
def test_an_operation_given_another_payload_names_the_one_it_expects(backend_type, kind, tasks, tmp_path):
    tasks.replace_all()
    expected = type(PAYLOADS[kind]()).__name__
    settings = input_schema.OutputSettings(
        name="rows.parquet", directory=str(tmp_path), file_type="parquet", table_settings={"file_type": "parquet"}
    )
    wrong = FileWritePayload(plan=NUMBERS.lazy(), settings=settings, flow_id=1, node_id=1)

    with pytest.raises(TypeError, match=f"Expected a {expected} under the payload key 'payload', found a FileWrite"):
        backend_type().run_op(kind, {PAYLOAD_KEY: wrong})
    assert tasks.built == []


# Core's own backend


def test_the_local_backend_fuzzy_joins_the_two_plans_and_reports_to_the_node_logger():
    records: list[str] = []
    node_logger = logging.getLogger(f"payload-test-{uuid.uuid4().hex}")
    node_logger.setLevel(logging.INFO)
    node_logger.addHandler(type("Collect", (logging.Handler,), {"emit": lambda self, r: records.append(r)})())
    join = fuzzy_join(node_logger=SimpleNamespace(logger=node_logger))
    settings_before = join.settings.join_input.model_dump()
    backend = LocalBackend()

    ref = backend.run_op(OperationKind.fuzzy_join, {PAYLOAD_KEY: join}).refs["result"]

    assert ref.state == "deferred"
    matches = backend.plan_for(ref).collect().sort("name", "name_right")
    assert matches.select("name", "name_right").rows() == MATCHED_PAIRS
    assert records, "the matcher reports its progress to the payload's node logger"
    assert join.settings.join_input.model_dump() == settings_before


def test_the_local_backend_splits_one_shuffle_into_a_deferred_ref_per_split():
    backend = LocalBackend()

    refs = backend.run_op(OperationKind.random_split, {PAYLOAD_KEY: random_split(seed=7)}).refs

    assert list(refs) == ["train", "test"]
    assert {ref.state for ref in refs.values()} == {"deferred"}
    train, test = (backend.plan_for(ref).collect()["n"].to_list() for ref in refs.values())
    assert (len(train), len(test)) == (80, 20)
    assert sorted(train + test) == list(range(100))
    expected = FlowDataEngine(NUMBERS.lazy()).random_split([("train", 80.0), ("test", 20.0)], seed=7)
    assert train == expected.outputs["train"].data_frame.collect()["n"].to_list()


def test_the_local_backend_runs_a_custom_node_from_its_settings_values():
    backend = LocalBackend()

    refs = backend.run_op(OperationKind.custom_node, {PAYLOAD_KEY: custom_node()}).refs

    assert list(refs) == ["output-0", "output-1"]
    big, small = refs.values()
    assert backend.plan_for(big).collect()["n"].to_list() == list(range(90, 100))
    assert backend.plan_for(small).collect()["n"].to_list() == list(range(90))
    assert (big.state, big.row_count) == ("deferred", None)
    assert (small.state, small.row_count) == ("deferred", 90), "a computed output is held as a plan with its count"


@pytest.mark.parametrize(
    ("node_class", "expected"),
    [(Doubler, ["result"]), (Silent, [])],
    ids=["one-frame", "nothing"],
)
def test_the_local_backend_keys_a_custom_node_result_by_what_process_returned(node_class, expected):
    backend = LocalBackend()
    job = custom_node(node_class, settings_values={})

    refs = backend.run_op(OperationKind.custom_node, {PAYLOAD_KEY: job}).refs

    assert list(refs) == expected
    if refs:
        assert backend.plan_for(refs["result"]).collect()["n"].to_list()[:3] == [0, 2, 4]


def test_the_local_backend_refuses_a_dict_without_a_declared_output():
    job = custom_node(output_names=["big", "small", "medium"])

    with pytest.raises(ValueError, match="did not return declared output 'medium'"):
        LocalBackend().run_op(OperationKind.custom_node, {PAYLOAD_KEY: job})


@pytest.mark.parametrize("user", [None, "local"], ids=["no-user", "user"])
def test_the_local_backend_reads_no_secret_the_node_does_not_access(user, user_id):
    """In core a secret is read when ``process`` reads it, so a selected secret that does not exist breaks nothing."""
    job = custom_node(
        settings_values={"main": {"threshold": "89", "api_key": "no_such_secret"}},
        user_id=None if user is None else user_id,
    )

    refs = LocalBackend().run_op(OperationKind.custom_node, {PAYLOAD_KEY: job}).refs

    assert list(refs) == ["output-0", "output-1"]


def test_the_local_backend_writes_a_parquet_file_that_is_complete_on_return(tmp_path, monkeypatch):
    synced = []
    real_fsync = os.fsync

    def recording_fsync(descriptor):
        synced.append(os.fstat(descriptor).st_ino)
        real_fsync(descriptor)

    monkeypatch.setattr(local_module.os, "fsync", recording_fsync)
    target = tmp_path / "inputs" / "main_0.parquet"
    target.parent.mkdir()
    settings = input_schema.OutputSettings(
        name=target.name, directory=str(target.parent), file_type="parquet", table_settings={"file_type": "parquet"}
    )
    write = FileWritePayload(plan=NUMBERS.lazy().filter(pl.col("n") % 2 == 0), settings=settings, flow_id=1, node_id=1)

    LocalBackend().run_op(OperationKind.file_write, {PAYLOAD_KEY: write})

    assert synced == [target.stat().st_ino]
    assert pl.read_parquet(target)["n"].to_list() == list(range(0, 100, 2))


def test_the_local_backend_syncs_only_a_parquet_write(tmp_path, monkeypatch):
    synced = []
    monkeypatch.setattr(local_module.os, "fsync", lambda descriptor: synced.append(descriptor))
    settings = input_schema.OutputSettings(
        name="rows.csv", directory=str(tmp_path), file_type="csv", table_settings={"file_type": "csv"}
    )
    write = FileWritePayload(plan=NUMBERS.lazy(), settings=settings, flow_id=1, node_id=1)

    LocalBackend().run_op(OperationKind.file_write, {PAYLOAD_KEY: write})

    assert synced == []
    assert pl.read_csv(tmp_path / "rows.csv").height == 100


# The worker backend, at the fetcher boundary


def test_the_worker_backend_prepares_both_sides_and_files_the_match_under_the_job_id(tasks):
    matched = pl.DataFrame({"name": ["edward"], "name_right": ["eduward"]})
    tasks.replace("ExternalFuzzyMatchFetcher", rows=matched)
    join = fuzzy_join(job_id="node-hash_fm")
    settings_before = join.settings.join_input.model_dump()

    result = WorkerBackend().run_op(OperationKind.fuzzy_join, {PAYLOAD_KEY: join})

    (task,) = tasks.built
    sent = task.kwargs
    assert (sent["file_ref"], sent["flow_id"], sent["node_id"], sent["wait_on_completion"]) == (
        "node-hash_fm",
        7,
        3,
        False,
    )
    assert sent["left_df"].collect().columns == ["name"]
    assert sent["right_df"].collect().columns == ["name_right"]
    assert pl.LazyFrame.deserialize(io.BytesIO(sent["right_df"].serialize())).collect()["name_right"].to_list() == [
        "eduward",
        "edward",
        "courtney",
    ]
    ((left_col, right_col),) = [(mapping.left_col, mapping.right_col) for mapping in sent["fuzzy_maps"]]
    assert (left_col, right_col) == ("name", "name_right")
    ref = result.refs["result"]
    assert list(result.refs) == ["result"]
    assert (ref.backend_id, ref.result_id, ref.state, ref.row_count) == ("worker", "node-hash_fm", "materialised", 1)
    assert ref.location.decode() == task.status.file_ref
    assert join.settings.join_input.model_dump() == settings_before


def test_the_worker_backend_refuses_an_invalid_join_before_it_starts_a_task(tasks):
    tasks.replace_all()
    settings = fuzzy_settings()
    settings.join_input.join_mapping[0].left_col = "missing"

    with pytest.raises(Exception, match="missing"):
        WorkerBackend().run_op(OperationKind.fuzzy_join, {PAYLOAD_KEY: fuzzy_join(settings=settings)})
    assert tasks.built == []


def test_the_worker_backend_stores_one_shuffle_and_splits_it_like_core(tasks):
    tasks.replace("ExternalDfFetcher", rows=stored_rows)
    split = random_split(seed=7, job_id="split-job")
    backend = WorkerBackend()

    refs = backend.run_op(OperationKind.random_split, {PAYLOAD_KEY: split}).refs

    (store,) = tasks.built
    assert (store.kwargs["file_ref"], store.kwargs["flow_id"], store.kwargs["node_id"]) == ("split-job", 7, 3)
    assert sorted(stored_rows(store.kwargs)["n"].to_list()) == list(range(100))
    assert list(refs) == ["train", "test"]
    assert [ref.result_id for ref in refs.values()] == ["split-job#train", "split-job#test"]
    assert [(ref.state, ref.row_count) for ref in refs.values()] == [("deferred", 80), ("deferred", 20)]
    assert [column.column_name for column in refs["train"].schema] == ["n"]
    on_worker = {name: backend.plan_for(ref).collect()["n"].to_list() for name, ref in refs.items()}
    local = LocalBackend()
    local_refs = local.run_op(OperationKind.random_split, {PAYLOAD_KEY: random_split(seed=7)}).refs
    assert on_worker == {name: local.plan_for(ref).collect()["n"].to_list() for name, ref in local_refs.items()}

    for ref in refs.values():
        backend.release(ref)

    assert os.path.isfile(store.status.file_ref), "releasing a split leaves the shuffled rows its siblings read"


def test_the_worker_backend_draws_a_seed_when_the_settings_have_none(tasks):
    tasks.replace("ExternalDfFetcher", rows=stored_rows)

    refs = WorkerBackend().run_op(OperationKind.random_split, {PAYLOAD_KEY: random_split(seed=None)}).refs

    assert [ref.row_count for ref in refs.values()] == [80, 20]


def test_the_worker_backend_sends_a_custom_node_with_its_inputs_serialised_and_its_secrets_encrypted(
    tasks, stored_secret, user_id
):
    big, small = pl.DataFrame({"n": [95, 96]}), pl.DataFrame({"n": [1, 2, 3]})
    tasks.replace(
        "ExternalCustomNodeFetcher",
        file_ref=lambda args, kwargs: args[0].task_id,
        result=custom_node_outputs(tasks, {"big": big, "small": small}),
    )
    settings_values = {"main": {"threshold": "89", "api_key": stored_secret}}
    job = custom_node(settings_values=settings_values, user_id=user_id, job_id=custom_node_task_id("node-hash"))

    refs = WorkerBackend().run_op(OperationKind.custom_node, {PAYLOAD_KEY: job}).refs

    (task,) = tasks.built
    (request,) = task.args
    assert isinstance(request, CustomNodeExecuteInput)
    assert request.task_id == "node-hash-custom-node"
    assert (request.node_source, request.class_name, request.output_names) == (
        SPLITTER_SOURCE,
        "Splitter",
        ["big", "small"],
    )
    assert request.settings_values == settings_values
    assert (request.user_id, request.flowfile_flow_id, request.flowfile_node_id) == (user_id, 7, 3)
    (sent_input,) = request.inputs
    assert pl.LazyFrame.deserialize(io.BytesIO(sent_input)).collect().equals(NUMBERS)
    assert list(request.secrets) == [stored_secret]
    assert request.secrets[stored_secret].startswith("$ffsec$")
    assert SECRET not in request.model_dump_json()
    assert list(refs) == ["output-0", "output-1"]
    assert [ref.result_id for ref in refs.values()] == ["node-hash-custom-node#big", "node-hash-custom-node#small"]
    assert [(ref.state, ref.row_count) for ref in refs.values()] == [("materialised", 2), ("materialised", 3)]


def test_the_worker_backend_refuses_a_custom_node_whose_task_left_out_a_declared_output(tasks):
    tasks.replace(
        "ExternalCustomNodeFetcher",
        file_ref=lambda args, kwargs: args[0].task_id,
        result=custom_node_outputs(tasks, {"big": NUMBERS}),
    )

    with pytest.raises(ValueError, match="Worker did not return declared output 'small'"):
        WorkerBackend().run_op(OperationKind.custom_node, {PAYLOAD_KEY: custom_node()})


def test_the_worker_backend_runs_no_custom_node_without_a_node_file(tasks):
    tasks.replace_all()

    with pytest.raises(OperationUnsupported, match="node file"):
        WorkerBackend().run_op(OperationKind.custom_node, {PAYLOAD_KEY: custom_node(node_source=None)})
    assert tasks.built == []


def test_the_worker_backend_refuses_a_selected_secret_it_cannot_find_before_it_starts_a_task(tasks, user_id):
    tasks.replace_all()
    job = custom_node(settings_values={"main": {"api_key": "no_such_secret"}}, user_id=user_id)

    with pytest.raises(ValueError, match="no_such_secret"):
        WorkerBackend().run_op(OperationKind.custom_node, {PAYLOAD_KEY: job})
    assert tasks.built == []


def test_the_worker_backend_trains_under_the_job_id_and_reports_the_model_as_metadata(tasks):
    report = {"sha256": "ab" * 32, "size_bytes": 14, "model_type": "linear_regression"}
    tasks.replace("MLTrainFetcher", result=report)
    train = ml_train(job_id="train-hash")

    result = WorkerBackend().run_op(OperationKind.ml_train, {PAYLOAD_KEY: train})

    (task,) = tasks.built
    assert task.kwargs == {
        "lf": train.plan,
        "staging_path": "/models/m.json",
        "model_type": "linear_regression",
        "target_column": "y",
        "feature_columns": ["x"],
        "params": {"add_bias": True},
        "flow_id": 7,
        "node_id": 3,
        "file_ref": "train-hash",
        "wait_on_completion": False,
    }
    assert result == OpResult(refs={}, metadata=report)


def test_the_worker_backend_scores_under_the_job_id_into_a_stored_result(tasks):
    scored = pl.DataFrame({"x": [1.0, 2.0], "pred": [3.0, 5.0]})
    tasks.replace("MLApplyFetcher", rows=scored)
    apply = ml_apply(job_id="apply-hash")
    backend = WorkerBackend()

    ref = backend.run_op(OperationKind.ml_apply, {PAYLOAD_KEY: apply}).refs["result"]

    (task,) = tasks.built
    assert task.kwargs == {
        "lf": apply.plan,
        "model_path": "/models/m.json",
        "output_column": "pred",
        "flow_id": 7,
        "node_id": 3,
        "file_ref": "apply-hash",
        "wait_on_completion": False,
    }
    assert (ref.result_id, ref.state, ref.row_count) == ("apply-hash", "materialised", 2)
    assert backend.plan_for(ref).collect().equals(scored)


# A cancel reaches the operation the node waits on


@pytest.fixture
def remote_node():
    graph = create_test_graph(flow_id=92, execution_location="remote")
    add_test_manual_input(graph, [{"n": 1}], node_id=1)
    return graph.get_node(1)


CANCELLED_KINDS = {
    OperationKind.fuzzy_join: ("ExternalFuzzyMatchFetcher", {"rows": pl.DataFrame({"name": ["a"]})}),
    OperationKind.random_split: ("ExternalDfFetcher", {"rows": stored_rows}),
    OperationKind.custom_node: ("ExternalCustomNodeFetcher", {"file_ref": lambda args, kwargs: args[0].task_id}),
    OperationKind.ml_train: ("MLTrainFetcher", {"result": {}}),
    OperationKind.ml_apply: ("MLApplyFetcher", {"rows": pl.DataFrame({"pred": [1.0]})}),
}


@pytest.mark.parametrize("kind", list(CANCELLED_KINDS), ids=kind_id)
def test_cancelling_a_node_reaches_the_operation_it_is_waiting_on(kind, remote_node, tasks, monkeypatch):
    fetcher, stand_in = CANCELLED_KINDS[kind]
    if kind is OperationKind.custom_node:
        stand_in["result"] = custom_node_outputs(tasks, {"big": NUMBERS, "small": NUMBERS})
    tasks.replace(fetcher, **stand_in)
    backend = registry.backend(registry.WORKER_BACKEND_ID)
    payload = PAYLOADS[kind]()
    seen = []

    def cancel_while_waiting(task):
        seen.append(remote_node._submit_in_flight)
        remote_node.cancel()

    monkeypatch.setattr(StandInTask, "on_wait", staticmethod(cancel_while_waiting))

    remote_node.run_op(backend, kind, {PAYLOAD_KEY: payload}, job_id=payload.job_id)

    (task,) = tasks.built
    assert seen == [JobHandle(backend.id, payload.job_id)]
    assert task.cancelled
    assert remote_node._submit_in_flight is None


# The builders


class RecordingBackend(LocalBackend):
    """Core's own process as a compute under another id, keeping every operation it is asked to run."""

    id = "recording"

    def __init__(self) -> None:
        super().__init__()
        self.operations: list[tuple[OperationKind, object]] = []

    def run_op(self, op, payload):
        self.operations.append((op, payload.get(PAYLOAD_KEY)))
        return super().run_op(op, payload)


@pytest.fixture
def recording_default_backend(monkeypatch):
    """A ``"remote"`` flow's backend: a ``RecordingBackend``, registered as the default for the test."""
    registry.reset()
    registry.register_backend(RecordingBackend.id, RecordingBackend)
    monkeypatch.setenv(registry.BACKEND_ENV_VAR, RecordingBackend.id)
    yield registry.backend(RecordingBackend.id)
    registry.reset()


@pytest.fixture
def splitter_with_a_node_file(monkeypatch):
    """``Splitter`` placeable as a custom node whose registry entry has a node file."""
    add_to_custom_node_store(Splitter)
    entry = SimpleNamespace(source_hash="0" * 64, source_text=SPLITTER_SOURCE, class_name="Splitter", manifest=None)
    entries = {Splitter().item: entry}
    monkeypatch.setattr(flow_graph_package, "user_defined_registry", SimpleNamespace(get=entries.get))
    yield
    remove_from_custom_node_store(Splitter().item)


def build_transform_flow(flow_id: int, execution_location: str, node_class: type[CustomNodeBase] = Splitter):
    """Numbers into a random split, a fuzzy match of names against themselves and a custom node."""
    graph = create_test_graph(flow_id=flow_id, execution_location=execution_location)
    add_test_manual_input(graph, NUMBERS.to_dicts(), node_id=1)
    add_test_manual_input(graph, NAMES.collect().to_dicts(), node_id=2)

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=3, node_type="random_split"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 3))
    graph.add_random_split(input_schema.NodeRandomSplit(flow_id=flow_id, node_id=3, depending_on_id=1, seed=7))

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=4, node_type="fuzzy_match"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 4))
    right = input_schema.NodeConnection.create_from_simple_input(2, 4)
    right.input_connection.connection_class = "input-1"
    add_connection(graph, right)
    settings = fuzzy_settings()
    graph.add_fuzzy_match(
        input_schema.NodeFuzzyMatch(flow_id=flow_id, node_id=4, depending_on_ids=[2, 2], join_input=settings.join_input)
    )

    item = node_class().item
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=5, node_type=item, is_user_defined=True))
    settings_values = {"main": {"threshold": "89"}}
    graph.add_user_defined_node(
        custom_node=node_class.from_settings(settings_values),
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=flow_id, node_id=5, settings=settings_values, is_user_defined=True
        ),
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 5))
    return graph


def assert_transform_results(graph) -> None:
    run_info = graph.run_graph()
    assert run_info.success, [result.error for result in run_info.node_step_result if not result.success]
    split = graph.get_node(3)
    train, test = (split.get_output(handle).collect()["n"].to_list() for handle in ("output-0", "output-1"))
    assert (len(train), len(test)) == (80, 20)
    assert sorted(train + test) == list(range(100))
    matches = graph.get_node(4).get_resulting_data().collect().sort("name", "name_right")
    assert matches.select("name", "name_right").rows() == MATCHED_PAIRS
    custom = graph.get_node(5)
    assert custom.get_output("output-0").collect()["n"].to_list() == list(range(90, 100))
    assert custom.get_output("output-1").collect()["n"].to_list() == list(range(90))


def test_a_remote_flow_runs_its_transforms_on_the_context_backend(recording_default_backend, splitter_with_a_node_file):
    graph = build_transform_flow(93, "remote")

    assert_transform_results(graph)

    sent = dict(recording_default_backend.operations)
    assert set(sent) == {OperationKind.random_split, OperationKind.fuzzy_join, OperationKind.custom_node}
    hashes = {node_id: graph.get_node(node_id).hash for node_id in (3, 4, 5)}
    split, join, job = sent[OperationKind.random_split], sent[OperationKind.fuzzy_join], sent[OperationKind.custom_node]
    assert isinstance(split, RandomSplitPayload) and (split.flow_id, split.node_id) == (93, 3)
    assert split.settings.seed == 7
    assert isinstance(join, FuzzyJoinPayload) and join.job_id == f"{hashes[4]}_fm"
    assert isinstance(job, CustomNodePayload)
    assert (job.node_source, job.class_name, job.job_id) == (SPLITTER_SOURCE, "Splitter", f"{hashes[5]}-custom-node")
    assert job.settings_values == {"main": {"threshold": "89"}}


def test_a_custom_node_without_a_node_file_runs_in_core_in_a_remote_flow(recording_default_backend):
    add_to_custom_node_store(Splitter)
    try:
        graph = build_transform_flow(94, "remote")
        assert_transform_results(graph)
    finally:
        remove_from_custom_node_store(Splitter().item)

    assert OperationKind.custom_node not in dict(recording_default_backend.operations)


def test_a_local_flow_runs_its_transforms_in_core_and_leaves_core_holding_only_node_results(
    splitter_with_a_node_file, monkeypatch
):
    def _no_worker(*args, **kwargs):
        raise AssertionError("a local flow must not reach the worker")

    monkeypatch.setattr(WorkerBackend, "run_op", _no_worker)
    monkeypatch.setattr(WorkerBackend, "submit", _no_worker)
    local = registry.backend(registry.LOCAL_BACKEND_ID)
    held_before = set(local._results)
    graph = build_transform_flow(95, "local")

    assert_transform_results(graph)

    held_now = set(local._results) - held_before
    assert held_now <= {node.hash for node in graph.nodes}, "an operation's results are released once read"

"""A run whose node needs what its backend does not declare is refused before any node starts.

With the compute protocol flag on, ``run_graph`` places the nodes it will run: each on the run's backend, the model
nodes on the worker their builders run them on, and compares what each needs with the ``Capabilities`` that backend
declares. A refused node is a failed row on the run report naming the backend and the reason; no node runs and the
backend is sent no work. A backend that cannot be asked for its capabilities is not in the map placement is given: its
nodes are not refused and meet its typed error when they run. The backends here are core's own under another id,
declaring what each test hands them, so no worker is needed. With the flag off nothing is placed.

A row's ``compute_error`` says what kind of failure it is: ``"Refusal"`` on a refused row, the class name of the typed
compute error that failed a node on that node's row, in both flag states, and ``None`` for any other failure.
"""

import threading

import polars as pl
import pytest

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, VersionMismatch
from flowfile_core.flowfile.execution.placement import Placement, SingleBackendPolicy, place
from flowfile_core.flowfile.execution.protocol import OperationKind
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas
from shared.compute_protocol import Capabilities
from tests.flowfile.conftest import add_test_catalog_writer, create_test_namespace

BACKEND_ID = "declaring"
LOCAL_DISK = 'namespace "TestCat.TestSch" is on local disk; remote execution needs an object-storage namespace'


def everything() -> Capabilities:
    """What core's own backend declares: its thirteen operations, ``file`` and object storage, every plugin."""
    return LocalBackend().capabilities()


def without(capabilities: Capabilities, *, operation=None, scheme=None, plugin=None) -> Capabilities:
    """``capabilities`` less one operation, one scheme from both scheme sets, or one plugin."""
    update = {}
    if operation is not None:
        update["operations"] = capabilities.operations - {operation}
    if scheme is not None:
        update["read_schemes"] = capabilities.read_schemes - {scheme}
        update["write_schemes"] = capabilities.write_schemes - {scheme}
    if plugin is not None:
        update["plugins"] = {name: version for name, version in capabilities.plugins.items() if name != plugin}
    return capabilities.model_copy(update=update)


class DeclaringBackend(LocalBackend):
    """Core's own backend under another id, declaring the capabilities it is given.

    Given an error instead, it is unreachable: ``capabilities()``, ``submit`` and ``run_op`` raise it. ``work`` records
    each ``submit`` and ``run_op`` it is sent; ``asked`` counts its ``capabilities()`` calls.
    """

    def __init__(self, backend_id: str, declared: Capabilities | Exception):
        super().__init__()
        self.id = backend_id
        self.declared = declared
        self.asked = 0
        self.work: list[str] = []
        self._work_lock = threading.Lock()

    def capabilities(self) -> Capabilities:
        self.asked += 1
        if isinstance(self.declared, Exception):
            raise self.declared
        return self.declared

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        with self._work_lock:
            self.work.append(f"submit {node_id}")
        if isinstance(self.declared, Exception):
            raise self.declared
        return super().submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)

    def run_op(self, op, payload):
        with self._work_lock:
            self.work.append(f"run_op {op.value}")
        if isinstance(self.declared, Exception):
            raise self.declared
        return super().run_op(op, payload)


@pytest.fixture
def protocol_on(monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)


@pytest.fixture
def protocol_off(monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", False)


@pytest.fixture
def declare(monkeypatch):
    """``declare(declared, backend_id)`` registers a ``DeclaringBackend`` under the id and returns it.

    ``declaring`` is the default backend, the one a ``"remote"`` flow runs on.
    """
    registry.reset()
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", BACKEND_ID)

    def register(declared: Capabilities | Exception, backend_id: str = BACKEND_ID) -> DeclaringBackend:
        backend = DeclaringBackend(backend_id, declared)
        registry.register_backend(backend_id, lambda: backend)
        return backend

    yield register
    registry.reset()


def make_graph(flow_id: int, execution_location: str = "remote") -> FlowGraph:
    flow_settings = schemas.FlowSettings(
        flow_id=flow_id,
        name=f"placement_{flow_id}",
        path=".",
        execution_location=execution_location,
        execution_mode="Development",
    )
    return FlowGraph(flow_settings)


def add_promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def connect(graph: FlowGraph, from_id: int, to_id: int, input_type: str = "main") -> None:
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id, input_type=input_type))


def add_manual_input(graph: FlowGraph, node_id: int, rows: list[dict]) -> None:
    add_promise(graph, node_id, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(rows)
        )
    )


def fuzzy_flow(flow_id: int) -> FlowGraph:
    """Manual inputs 1 and 2 → fuzzy match 3 on ``name``."""
    graph = make_graph(flow_id)
    add_manual_input(graph, 1, [{"name": "eduward"}, {"name": "edward"}, {"name": "courtney"}])
    add_manual_input(graph, 2, [{"name": "edward"}, {"name": "courtney"}])
    add_promise(graph, 3, "fuzzy_match")
    graph.add_fuzzy_match(
        input_schema.NodeFuzzyMatch.model_validate(
            {
                "flow_id": flow_id,
                "node_id": 3,
                "depending_on_ids": [1, 2],
                "join_input": {
                    "join_mapping": [
                        {"left_col": "name", "right_col": "name", "threshold_score": 75, "fuzzy_type": "levenshtein"}
                    ],
                    "left_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
                    "right_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
                    "how": "inner",
                },
            }
        )
    )
    connect(graph, 1, 3)
    connect(graph, 2, 3, input_type="right")
    return graph


def train_flow(flow_id: int) -> FlowGraph:
    """A local flow: manual input 1 → train model 2, which its builder runs on the worker."""
    graph = make_graph(flow_id, execution_location="local")
    add_manual_input(graph, 1, [{"x": float(i), "y": 2.0 * i} for i in range(10)])
    add_promise(graph, 2, "train_model")
    graph.add_train_model(
        input_schema.NodeTrainModel(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            train_input=input_schema.TrainModelSettings(target_column="y", feature_columns=["x"]),
        )
    )
    connect(graph, 1, 2)
    return graph


def assert_refused_before_any_node(
    graph: FlowGraph, run_info, backend: DeclaringBackend, refused: dict[int, list[str]]
):
    """The run failed with exactly ``refused`` (node id -> phrases its error holds) and nothing ran or holds the run."""
    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors.keys() == refused.keys(), errors
    for node_id, phrases in refused.items():
        assert errors[node_id].startswith(f'cannot run on backend "{backend.id}": '), errors[node_id]
        for phrase in phrases:
            assert phrase in errors[node_id], errors[node_id]
    assert all(result.success is False for result in run_info.node_step_result)
    assert all(result.compute_error == "Refusal" for result in run_info.node_step_result)
    assert run_info.success is False
    assert run_info.nodes_completed == 0
    assert run_info.is_running is False
    assert graph.flow_settings.is_running is False
    assert backend.work == []


def test_a_catalog_write_to_a_local_disk_namespace_is_refused_on_a_backend_without_local_files(
    protocol_on, declare, catalog_clean_state
):
    backend = declare(without(everything(), scheme="file"))
    graph = make_graph(8401)
    add_manual_input(graph, 1, [{"id": 1, "name": "ada"}])
    add_test_catalog_writer(graph, node_id=2, depending_on_id=1, table_name="dim", namespace_id=create_test_namespace())

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {2: [LOCAL_DISK]})


def test_a_fuzzy_match_is_refused_on_a_backend_without_the_fuzzy_join_operation(protocol_on, declare):
    backend = declare(without(everything(), operation="fuzzy_join"))
    graph = fuzzy_flow(8402)

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {3: ["it runs the fuzzy_join operation"]})


def test_a_fuzzy_match_is_refused_on_a_backend_without_its_plugin(protocol_on, declare):
    backend = declare(without(everything(), plugin="polars_simed"))
    graph = fuzzy_flow(8403)

    run_info = graph.run_graph()

    assert_refused_before_any_node(
        graph, run_info, backend, {3: ["it needs plugins this backend does not have: polars_simed"]}
    )


def test_a_python_script_node_is_refused_on_a_backend_off_cores_host(protocol_on, declare):
    backend = declare(without(everything(), scheme="file"))
    graph = make_graph(8404)
    add_manual_input(graph, 1, [{"id": 1}])
    add_promise(graph, 2, "python_script")
    graph.add_python_script(
        input_schema.NodePythonScript(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_ids=[1],
            python_script_input=input_schema.PythonScriptInput(code="print(1)", kernel_id="k"),
        )
    )
    connect(graph, 1, 2)

    run_info = graph.run_graph()

    assert_refused_before_any_node(
        graph, run_info, backend, {2: ["it runs on a notebook kernel", "the kernel shared volume on core's host"]}
    )


def test_a_flow_its_backend_can_run_runs_as_it_does_unplaced(declare, monkeypatch):
    backend = declare(everything())
    rows = {}
    for flag, flow_id in ((True, 8405), (False, 8406)):
        monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
        graph = fuzzy_flow(flow_id)

        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.nodes_completed == 3
        rows[flag] = graph.get_node(3).get_resulting_data().collect().sort(pl.all()).rows()
    assert rows[True] == rows[False]
    assert len(rows[True]) == 3
    assert backend.asked == 1
    assert "run_op fuzzy_join" in backend.work


def test_with_the_flag_off_nothing_is_placed(protocol_off, declare):
    backend = declare(without(everything(), operation="fuzzy_join"))
    graph = fuzzy_flow(8407)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert backend.asked == 0
    assert "run_op fuzzy_join" in backend.work


def test_a_backend_missing_from_the_map_leaves_its_nodes_unplaced_and_unrefused():
    graph = train_flow(8408)

    placement = place(graph, {registry.LOCAL_BACKEND_ID: everything()}, SingleBackendPolicy(registry.LOCAL_BACKEND_ID))

    assert placement == Placement(assignment={1: registry.LOCAL_BACKEND_ID}, boundaries=[], refusals=[])


def test_the_nodes_of_a_backend_that_cannot_be_asked_meet_its_error_when_they_run(protocol_on, declare):
    backend = declare(ComputeUnavailable("Connection refused"))
    graph = fuzzy_flow(8412)

    run_info = graph.run_graph()

    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors.keys() == {1, 2}, errors
    for error in errors.values():
        assert error.startswith("Connection refused"), error
    assert {result.compute_error for result in run_info.node_step_result} == {"ComputeUnavailable"}
    assert graph.get_node(3).results.errors is None
    assert run_info.success is False
    assert sorted(backend.work) == ["submit 1", "submit 2"]
    assert backend.asked == 1


@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
def test_a_node_a_version_mismatch_failed_carries_the_error_s_class_on_its_row(declare, monkeypatch, flag):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
    declare(VersionMismatch("polars_version: core 1, worker 2"))
    graph = fuzzy_flow(8414 if flag else 8413)

    run_info = graph.run_graph()

    results = {result.node_id: result for result in run_info.node_step_result}
    assert results.keys() == {1, 2}, results
    for result in results.values():
        assert result.success is False
        assert result.error.startswith("polars_version: core 1, worker 2"), result.error
        assert result.compute_error == "VersionMismatch"
    assert run_info.success is False


def test_a_node_another_error_failed_carries_no_compute_error(protocol_off, declare):
    declare(RuntimeError("boom"))
    graph = fuzzy_flow(8415)

    run_info = graph.run_graph()

    results = {result.node_id: result for result in run_info.node_step_result}
    assert results.keys() == {1, 2}, results
    for result in results.values():
        assert result.success is False
        assert "boom" in result.error, result.error
        assert result.compute_error is None
    assert run_info.success is False


@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
def test_a_fetched_node_a_compute_error_failed_carries_the_error_s_class_on_its_row(declare, monkeypatch, flag):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)
    declare(ComputeUnavailable("Connection refused"))
    graph = fuzzy_flow(8417 if flag else 8416)

    run_info = graph.trigger_fetch_node(1)

    [result] = run_info.node_step_result
    assert result.node_id == 1
    assert result.success is False
    assert result.error.startswith("Connection refused"), result.error
    assert result.compute_error == "ComputeUnavailable"


def test_a_node_the_run_skips_is_not_placed(protocol_on, declare):
    backend = declare(without(everything(), operation="fuzzy_join"))
    graph = fuzzy_flow(8409)
    add_promise(graph, 4, "select")
    connect(graph, 3, 4)

    run_info = graph.run_graph()

    assert_refused_before_any_node(graph, run_info, backend, {3: ["it runs the fuzzy_join operation"]})
    assert run_info.number_of_nodes == 3


def test_the_model_nodes_are_placed_on_the_worker_their_builder_runs_them_on():
    graph = train_flow(8410)
    worker = everything().model_copy(update={"operations": frozenset(kind.value for kind in OperationKind)})
    backends = {registry.LOCAL_BACKEND_ID: everything(), registry.WORKER_BACKEND_ID: worker}

    placement = place(graph, backends, SingleBackendPolicy(registry.LOCAL_BACKEND_ID))

    assert placement == Placement(
        assignment={1: registry.LOCAL_BACKEND_ID, 2: registry.WORKER_BACKEND_ID}, boundaries=[], refusals=[]
    )


def test_a_local_flow_with_a_model_node_is_refused_on_a_worker_off_cores_host(protocol_on, declare):
    worker = without(everything(), scheme="file").model_copy(
        update={"operations": frozenset(kind.value for kind in OperationKind)}
    )
    backend = declare(worker, registry.WORKER_BACKEND_ID)
    graph = train_flow(8411)

    run_info = graph.run_graph()

    assert_refused_before_any_node(
        graph, run_info, backend, {2: ["it writes storage this backend does not write: file"]}
    )
    assert "operation" not in run_info.node_step_result[0].error
    assert backend.asked == 1

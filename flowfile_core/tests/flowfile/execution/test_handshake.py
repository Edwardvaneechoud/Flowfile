"""Core checks each compute backend once, on its first use, through its own interface, and places nodes by what it
offers.

A backend's check reads its ``capabilities()`` and runs each probe plan on it as an ordinary job. A Polars version, an
index-width runtime or a protocol version that differs refuses the backend whole, before it is sent any work, and the
app version is not compared; a plugin it lacks, has in another version or cannot load, and core's Python functions
where they do not run there, only refuse the nodes that need them, with the reason. The session worker is the backend
``worker``; ``worker:team`` is that worker reached by another name, or core's own process under that id, faked to report
or fail what a case needs. Every gate case runs under both states of the protocol flag; placement runs with the flag on
only.
"""

import json
import socket
from urllib.parse import urlsplit

import polars as pl
import pytest
import requests

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, PlanRejected, VersionMismatch
from flowfile_core.flowfile.execution.placement import PYTHON_FUNCTIONS, Refusal, SingleBackendPolicy, place
from flowfile_core.flowfile.execution.protocol import SubmitHints
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake, streaming
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.flow_node.flow_node import schema_prefetch_blocked
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageReadSettings
from shared.compute_protocol import PROTOCOL_VERSION, local_capabilities
from tests.flowfile.execution.test_placement import FLOW_REMEDY

HINT = ". Is the compute backend running? Start it, or check the Backends page."
TEAM_HINT = '. Is backend "worker:team" running? Start it, or check the Backends page.'
TEAM = "worker:team"
PROBES = ["polars_grouper", "polars_ds", PYTHON_FUNCTIONS]

WORKER_URL = registry.worker_url()
"""The session worker's URL: the one the registry's worker backend, and so a remote flow, calls."""

OLDER_ANSWER = {
    **local_capabilities().model_dump(
        mode="json", exclude={"operations", "read_schemes", "write_schemes", "credential_resolution"}
    ),
    "app_version": "0.23.0",
}
"""The capabilities of a worker built before it declared its operations, schemes and credential resolution."""

OLDER_RELEASE_REFUSAL = (
    f"The worker at {WORKER_URL} does not match core (app_version: core {local_capabilities().app_version}, "
    "worker 0.23.0): its capabilities answer is not one this release reads (credential_resolution: Field required)"
)
NO_WORKER_REFUSAL = (
    f"The server at {WORKER_URL} is not a Flowfile worker: its capabilities answer carries no app_version"
)


@pytest.fixture(params=[False, True], ids=["flag-off", "flag-on"])
def flag(request, monkeypatch) -> bool:
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", request.param)
    return request.param


@pytest.fixture
def protocol_on(monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)


@pytest.fixture(autouse=True)
def unchecked_backends():
    """Every test starts with no backend checked, and a call naming the session worker's URL alone gating on the
    registry's worker backend."""
    handshake.reset_handshake()
    handshake.owns(registry.backend(registry.WORKER_BACKEND_ID))
    yield
    handshake.reset_handshake()
    registry.reset()


def counted(monkeypatch, backend, reported=None) -> list[int]:
    """One entry per ``backend.capabilities()`` call; with ``reported``, the call answers that and sends nothing."""
    calls: list[int] = []
    real = backend.capabilities

    def capabilities():
        calls.append(1)
        return real() if reported is None else reported

    monkeypatch.setattr(backend, "capabilities", capabilities)
    return calls


@pytest.fixture
def probes(monkeypatch) -> list[tuple[str, str]]:
    """The backend id and probe of each probe job a check runs."""
    ran: list[tuple[str, str]] = []
    real = handshake._run_probe

    def recording(backend, name, plan):
        ran.append((backend.id, name))
        return real(backend, name, plan)

    monkeypatch.setattr(handshake, "_run_probe", recording)
    return ran


@pytest.fixture
def sent(monkeypatch) -> list[str]:
    """The URL of each REST request core sends and of each submit socket it dials."""
    urls: list[str] = []
    real_send = requests.Session.send
    real_connects = {module: module.connect for module in (streaming, worker_module)}

    def counting_send(self, request, **kwargs):
        urls.append(request.url)
        return real_send(self, request, **kwargs)

    def recording(module):
        def connect(url, *args, **kwargs):
            urls.append(url)
            return real_connects[module](url, *args, **kwargs)

        return connect

    monkeypatch.setattr(requests.Session, "send", counting_send)
    for module in real_connects:
        monkeypatch.setattr(module, "connect", recording(module))
    return urls


@pytest.fixture
def closed_port_url() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def capabilities_answer(monkeypatch):
    """Make ``GET /capabilities`` and ``GET /v1/capabilities`` answer 200 with the given body."""

    def answer(body: bytes) -> None:
        response = requests.Response()
        response.status_code = 200
        response._content = body
        real_send = requests.Session.send

        def send(self, request, **kwargs):
            if request.url.endswith("/capabilities"):
                return response
            return real_send(self, request, **kwargs)

        monkeypatch.setattr(requests.Session, "send", send)

    return answer


def same_worker_by_another_name() -> str:
    """The session worker's URL with its host spelled another way, so a backend made on it owns a URL of its own."""
    parts = urlsplit(WORKER_URL)
    host = "localhost" if parts.hostname == "127.0.0.1" else "127.0.0.1"
    return f"{parts.scheme}://{host}:{parts.port}{parts.path}"


@pytest.fixture
def team_on_other_polars(monkeypatch) -> tuple[WorkerBackend, list[int]]:
    """``worker:team``: the session worker by another name, reporting core's capabilities but Polars 0.0.1."""
    backend = WorkerBackend(same_worker_by_another_name(), backend_id=TEAM)
    asked = counted(monkeypatch, backend, local_capabilities().model_copy(update={"polars_version": "0.0.1"}))
    registry.register_backend(TEAM, lambda: backend)
    return backend, asked


class CoreAsTeam(EmbeddedWorkerBackend):
    """The worker embedded in this process as ``worker:team``, reporting this process's capabilities with
    ``changes``, whose probe job ``failing`` fails as a plan the compute cannot load."""

    id = TEAM

    def __init__(self, failing: str | None = None, **changes):
        super().__init__()
        self.failing = failing
        self.changes = changes

    def capabilities(self):
        return super().capabilities().model_copy(update=self.changes)

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        if self.failing is not None and result_id.startswith(f"handshake-{self.failing}-"):
            raise PlanRejected(f"the {self.failing} probe could not be loaded")
        return super().submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)


def register_team(backend: EmbeddedWorkerBackend) -> EmbeddedWorkerBackend:
    registry.register_backend(TEAM, lambda: backend)
    return backend


def _graph(flow_id: int, location: str):
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id, name="handshake", path=".", execution_location=location, execution_mode="Development"
        )
    )
    return handler.get_flow(flow_id)


def _add(graph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def _connect(graph, from_id: int, to_id: int) -> None:
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id))


def _manual_input(graph, *, cache_results: bool = False) -> None:
    _add(graph, 1, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            cache_results=cache_results,
            raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]),
        )
    )


def one_node_graph(flow_id: int, location: str = "remote", *, cache_results: bool = False):
    graph = _graph(flow_id, location)
    _manual_input(graph, cache_results=cache_results)
    return graph


def formula_and_select_graph(flow_id: int, location: str = TEAM):
    """A manual input read by a formula node (2) and by a select (3)."""
    graph = _graph(flow_id, location)
    _manual_input(graph)
    _add(graph, 2, "formula")
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            functions=[transform_schema.FunctionInput(transform_schema.FieldInput(name="b"), "[a] + 1")],
        )
    )
    _connect(graph, 1, 2)
    _add(graph, 3, "select")
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=flow_id, node_id=3, depending_on_id=1, select_input=[transform_schema.SelectInput(old_name="a")]
        )
    )
    _connect(graph, 1, 3)
    return graph


def add_cloud_reader(graph, node_id: int, **read) -> None:
    """A cloud storage reader of user 1 with no connection, built without asking any backend for its schema."""
    _add(graph, node_id, "cloud_storage_reader")
    token = schema_prefetch_blocked.set(True)
    try:
        graph.add_cloud_storage_reader(
            input_schema.NodeCloudStorageReader(
                flow_id=graph.flow_id,
                node_id=node_id,
                user_id=1,
                cloud_storage_settings=CloudStorageReadSettings(**read),
            )
        )
    finally:
        schema_prefetch_blocked.reset(token)


def row(run_info, node_id: int):
    return next(result for result in run_info.node_step_result if result.node_id == node_id)


def test_two_workers_are_each_checked_on_first_use_and_the_mismatched_one_is_sent_nothing(
    flag, team_on_other_polars, probes, sent, monkeypatch
):
    team, team_asked = team_on_other_polars
    worker_asked = counted(monkeypatch, registry.backend(registry.WORKER_BACKEND_ID))
    graph = one_node_graph(9201)

    assert graph.run_graph().success is True
    assert worker_asked == [1]
    assert probes == [(registry.WORKER_BACKEND_ID, probe) for probe in PROBES]
    assert team_asked == []

    probes.clear()
    sent.clear()
    refused = one_node_graph(9202, TEAM, cache_results=True).run_graph()

    result = row(refused, 1)
    mismatch = f'Backend "{TEAM}" does not match core (polars_version: core {pl.__version__}, backend 0.0.1)'
    assert refused.success is False
    # with the flag on placement refuses the node, so no node starts; with it off the node's gate raises
    assert result.error == (f'cannot run on backend "{TEAM}": {mismatch}. {FLOW_REMEDY}' if flag else mismatch)
    assert result.compute_error == ("Refusal" if flag else "VersionMismatch")
    assert team_asked == [1]
    assert probes == []
    assert [url for url in sent if url.split("://", 1)[1].startswith(team.url.split("://", 1)[1])] == []

    assert graph.run_graph().success is True
    assert worker_asked == [1]
    assert handshake.last_verdict(registry.WORKER_BACKEND_ID).ok is True
    assert handshake.last_verdict(TEAM).ok is False


def test_a_refused_backend_is_checked_again_on_its_next_use(flag, monkeypatch):
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    asked = counted(monkeypatch, worker, local_capabilities().model_copy(update={"polars_version": "0.0.1"}))

    for _ in range(2):
        with pytest.raises(VersionMismatch):
            subops.get_status("no-such-task", worker_url=WORKER_URL)

    assert asked == [1, 1]


def test_a_call_naming_the_url_alone_is_gated_by_the_registry_s_worker(flag, monkeypatch, probes):
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    asked = counted(monkeypatch, worker)

    assert subops.results_exists("no-such-task", worker_url=WORKER_URL) is False
    assert subops.results_exists("no-such-task", worker_url=WORKER_URL) is False

    assert asked == [1]
    assert probes == [(registry.WORKER_BACKEND_ID, probe) for probe in PROBES]
    verdict = handshake.last_verdict(registry.WORKER_BACKEND_ID)
    assert verdict.ok is True
    assert verdict.absent == {}
    assert verdict.differences == []
    assert verdict.offered.plugins == local_capabilities().plugins
    assert handshake.verdict_for(worker) is verdict


def test_a_call_that_cannot_reach_the_worker_rearms_its_check(flag, monkeypatch):
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    asked = counted(monkeypatch, worker)
    assert subops.results_exists("no-such-task", worker_url=WORKER_URL) is False
    real_send = requests.Session.send

    def refused_once(self, request, **kwargs):
        monkeypatch.setattr(requests.Session, "send", real_send)
        raise requests.ConnectionError("[Errno 61] Connection refused")

    monkeypatch.setattr(requests.Session, "send", refused_once)
    with pytest.raises(ComputeUnavailable):
        subops.results_exists("no-such-task", worker_url=WORKER_URL)

    assert handshake.last_verdict(registry.WORKER_BACKEND_ID) is None
    assert subops.results_exists("no-such-task", worker_url=WORKER_URL) is False
    assert asked == [1, 1]


def test_a_submit_socket_that_cannot_be_opened_rearms_the_check_of_its_url(protocol_on, monkeypatch):
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    asked = counted(monkeypatch, worker)
    handshake.ensure_handshake(WORKER_URL)

    real_connect = worker_module.connect

    def unreachable_once(url, **kwargs):
        monkeypatch.setattr(worker_module, "connect", real_connect)
        raise OSError("[Errno 61] Connection refused")

    monkeypatch.setattr(worker_module, "connect", unreachable_once)
    plan = pl.LazyFrame({"a": [1]})
    hints = SubmitHints(must_materialise=True, wide=False, fanout=1, preview_rows=None)
    result = worker.submit(plan, [], hints, "t-handshake-rearm", flow_id=-1, node_id=-1)
    worker.release(result.ref)

    # the socket's failure dropped the verdict, so the post it fell back to checked the worker again
    assert asked == [1, 1]


@pytest.mark.parametrize(
    ("changes", "failing", "which"),
    [
        ({}, "polars_ds", "this backend could not load from the path core's plans carry (core {lib}, backend {lib}): "),
        ({"plugins": "other"}, None, "this backend has in version 0.0.1 where core has {version}"),
        ({"plugins": "missing"}, None, "this backend does not have"),
    ],
    ids=["not-loadable", "other-version", "missing"],
)
def test_a_formula_is_refused_where_polars_ds_is_absent_and_a_select_still_runs(protocol_on, changes, failing, which):
    core = local_capabilities()
    plugins = dict(core.plugins)
    if changes.get("plugins") == "other":
        plugins["polars_ds"] = "0.0.1"
    elif changes.get("plugins") == "missing":
        del plugins["polars_ds"]
    register_team(CoreAsTeam(failing, plugins=plugins))
    graph = formula_and_select_graph(9211)

    refused = graph.run_graph()

    reason = which.format(lib=core.plugin_libs["polars_ds"], version=core.plugins["polars_ds"])
    [(node_id, error)] = [(result.node_id, result.error) for result in refused.node_step_result]
    assert node_id == 2
    assert error.startswith(f'cannot run on backend "{TEAM}": it needs polars_ds, which {reason}'), error
    assert refused.success is False

    graph.delete_node(2)
    ran = graph.run_graph()

    assert ran.success is True, [result.error for result in ran.node_step_result]
    assert graph.get_node(3).get_resulting_data().collect().to_dicts() == [{"a": 1}]


def test_a_formula_is_refused_where_core_s_python_functions_do_not_run_and_a_select_still_runs(protocol_on):
    python = local_capabilities().python_version
    register_team(CoreAsTeam(PYTHON_FUNCTIONS))
    graph = formula_and_select_graph(9212)

    refused = graph.run_graph()

    [(node_id, error)] = [(result.node_id, result.error) for result in refused.node_step_result]
    assert node_id == 2
    assert error == (
        f'cannot run on backend "{TEAM}": its plan may carry a Python function, which core\'s Python functions do not '
        f"run on this backend (Python: core {python}, backend {python}): the python probe could not be loaded. "
        f"{FLOW_REMEDY}"
    )

    graph.delete_node(2)
    assert graph.run_graph().success is True


def test_a_cloud_change_read_is_refused_where_core_s_python_functions_do_not_run_and_a_plain_read_is_placed():
    backend = register_team(CoreAsTeam(PYTHON_FUNCTIONS))
    verdict = handshake.verdict_for(backend)
    graph = _graph(9213, TEAM)
    add_cloud_reader(
        graph,
        1,
        resource_path="s3://handshake-bucket/table",
        file_format="delta",
        cdc_mode="since_version",
        cdc_from_version=1,
    )
    add_cloud_reader(graph, 2, resource_path="s3://handshake-bucket/data.parquet")

    placement = place(graph, {TEAM: verdict.offered}, SingleBackendPolicy(TEAM), absent={TEAM: verdict.absent})

    assert placement.assignment == {1: TEAM, 2: TEAM}
    assert placement.refusals == [
        Refusal(1, TEAM, f"its plan may carry a Python function, which {verdict.absent[PYTHON_FUNCTIONS]}")
    ]


def test_a_python_that_differs_is_listed_and_a_formula_still_runs_where_the_probe_passes(protocol_on):
    core = local_capabilities()
    backend = register_team(CoreAsTeam(python_version="3.99.0", platform="plan9-mips", plugin_libs={}))
    graph = formula_and_select_graph(9214)

    ran = graph.run_graph()

    assert ran.success is True, [result.error for result in ran.node_step_result]
    verdict = handshake.last_verdict(TEAM)
    assert verdict is handshake.verdict_for(backend)
    assert verdict.ok is True
    assert verdict.differences == [f"python_version: core {core.python_version}, backend 3.99.0"]
    assert verdict.absent == {}
    assert verdict.backend.platform == "plan9-mips"


def test_a_backend_of_any_kind_refused_whole_refuses_its_nodes_at_placement(protocol_on, monkeypatch):
    backend = register_team(CoreAsTeam(polars_version="0.0.1"))
    submitted = []
    monkeypatch.setattr(backend, "submit", lambda *args, **kwargs: submitted.append(args))
    graph = formula_and_select_graph(9215)

    refused = graph.run_graph()

    mismatch = f'Backend "{TEAM}" does not match core (polars_version: core {pl.__version__}, backend 0.0.1)'
    assert refused.success is False
    assert sorted((result.node_id, result.error) for result in refused.node_step_result) == [
        (node_id, f'cannot run on backend "{TEAM}": {mismatch}. {FLOW_REMEDY}') for node_id in (1, 2, 3)
    ]
    assert submitted == []
    assert handshake.last_verdict(TEAM).error == mismatch


@pytest.mark.parametrize("field", ["polars_version", "polars_runtime"])
def test_another_polars_or_runtime_refuses_the_backend_naming_both_sides(flag, monkeypatch, sent, field):
    core = local_capabilities()
    other = {"polars_version": "0.0.1", "polars_runtime": "64" if core.polars_runtime == "32" else "32"}[field]
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    counted(monkeypatch, worker, core.model_copy(update={field: other}))

    with pytest.raises(VersionMismatch) as refused:
        subops.results_exists("no-such-task", worker_url=WORKER_URL)

    assert str(refused.value) == (
        f'Backend "worker" does not match core ({field}: core {getattr(core, field)}, backend {other})'
    )
    assert sent == []


def test_a_verdict_lists_each_difference_and_refuses_on_the_protocol_version(monkeypatch):
    core = local_capabilities()
    reported = core.model_copy(
        update={"app_version": "0.0.0", "protocol_version": core.protocol_version + 1, "python_version": "3.99.0"}
    )
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    counted(monkeypatch, worker, reported)

    verdict = handshake.verify(worker)

    assert verdict.ok is False
    assert verdict.differences == [
        f"protocol_version: core {core.protocol_version}, backend {core.protocol_version + 1}",
        f"python_version: core {core.python_version}, backend 3.99.0",
    ]
    assert verdict.error == (
        f'Backend "worker" does not match core '
        f"(protocol_version: core {core.protocol_version}, backend {core.protocol_version + 1})"
    )
    assert verdict.offered is None
    assert verdict.backend.app_version == "0.0.0"


def test_two_workers_of_other_app_versions_and_one_contract_both_run(flag, monkeypatch):
    core = local_capabilities()
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    counted(monkeypatch, worker, core.model_copy(update={"app_version": "0.0.1"}))
    team = WorkerBackend(same_worker_by_another_name(), backend_id=TEAM)
    counted(monkeypatch, team, core.model_copy(update={"app_version": "0.0.2"}))
    registry.register_backend(TEAM, lambda: team)

    assert one_node_graph(9216, cache_results=True).run_graph().success is True
    assert one_node_graph(9217, TEAM, cache_results=True).run_graph().success is True

    for backend_id, app_version in ((registry.WORKER_BACKEND_ID, "0.0.1"), (TEAM, "0.0.2")):
        verdict = handshake.last_verdict(backend_id)
        assert verdict.ok is True
        assert verdict.differences == []
        assert verdict.backend.app_version == app_version


def test_another_contract_number_refuses_the_backend_naming_both_sides(flag, monkeypatch, sent):
    core = local_capabilities()
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    counted(monkeypatch, worker, core.model_copy(update={"protocol_version": core.protocol_version + 1}))

    with pytest.raises(VersionMismatch) as refused:
        subops.results_exists("no-such-task", worker_url=WORKER_URL)

    assert str(refused.value) == (
        f'Backend "worker" does not match core '
        f"(protocol_version: core {core.protocol_version}, backend {core.protocol_version + 1})"
    )
    assert sent == []


def test_an_unreadable_answer_of_another_contract_names_both_numbers(flag, capabilities_answer):
    capabilities_answer(json.dumps({**OLDER_ANSWER, "protocol_version": PROTOCOL_VERSION + 1}).encode())

    with pytest.raises(VersionMismatch) as excinfo:
        handshake.ensure_handshake(WORKER_URL)

    assert str(excinfo.value) == OLDER_RELEASE_REFUSAL.replace(
        "(app_version:", f"(protocol_version: core {PROTOCOL_VERSION}, worker {PROTOCOL_VERSION + 1}; app_version:"
    )


def test_a_worker_of_an_earlier_release_is_refused_naming_both_app_versions(flag, capabilities_answer, probes):
    capabilities_answer(json.dumps(OLDER_ANSWER).encode())

    with pytest.raises(VersionMismatch) as excinfo:
        handshake.ensure_handshake(WORKER_URL)

    assert str(excinfo.value) == OLDER_RELEASE_REFUSAL
    assert probes == []
    assert handshake.last_verdict(registry.WORKER_BACKEND_ID).error == OLDER_RELEASE_REFUSAL


@pytest.mark.parametrize(
    "body", [b'{"status": "ok"}', b"<!doctype html><html><body>Welcome</body></html>"], ids=["other-json", "html-page"]
)
def test_a_url_that_answers_no_workers_capabilities_is_unavailable(flag, capabilities_answer, body):
    capabilities_answer(body)

    with pytest.raises(ComputeUnavailable) as excinfo:
        handshake.ensure_handshake(WORKER_URL, backend_id=TEAM)

    assert str(excinfo.value) == NO_WORKER_REFUSAL
    assert excinfo.value.backend_id == TEAM


@pytest.mark.parametrize(
    ("answer", "error", "message"),
    [(OLDER_ANSWER, VersionMismatch, OLDER_RELEASE_REFUSAL), ({"status": "ok"}, ComputeUnavailable, NO_WORKER_REFUSAL)],
    ids=["earlier-release", "no-worker"],
)
def test_a_node_on_an_earlier_release_or_on_no_worker_fails_with_the_typed_error(
    flag, capabilities_answer, answer, error, message
):
    capabilities_answer(json.dumps(answer).encode())

    run_info = one_node_graph(9205, cache_results=True).run_graph()

    refused_at_placement = flag and error is VersionMismatch
    assert run_info.success is False
    assert row(run_info, 1).error == (
        f'cannot run on backend "{registry.WORKER_BACKEND_ID}": {message}. {FLOW_REMEDY}'
        if refused_at_placement
        else message
    )
    assert row(run_info, 1).compute_error == ("Refusal" if refused_at_placement else error.__name__)


def test_a_worker_without_a_capabilities_route_is_an_older_release(monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", False)
    older_worker = f"{WORKER_URL}/no-such-prefix"

    with pytest.raises(VersionMismatch) as excinfo:
        subops.get_status("no-such-task", worker_url=older_worker)

    assert str(excinfo.value) == (
        f"The worker at {older_worker} does not report its capabilities (GET /capabilities answered 404), "
        "so it is an older release than core"
    )


def test_cleanup_calls_to_a_mismatched_worker_return_false(flag, monkeypatch, sent):
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    asked = counted(monkeypatch, worker, local_capabilities().model_copy(update={"polars_version": "0.0.1"}))

    assert subops.clear_task_from_worker("no-such-task", worker_url=WORKER_URL) is False
    assert subops.cancel_task("no-such-task", worker_url=WORKER_URL) is False

    assert asked == [1, 1]
    assert sent == []


def test_unreachable_worker_on_the_first_call_is_compute_unavailable(flag, closed_port_url):
    with pytest.raises(ComputeUnavailable) as excinfo:
        subops.get_status("no-such-task", worker_url=closed_port_url)

    message = str(excinfo.value)
    assert message.startswith(f"Could not reach the worker at {closed_port_url}: ")
    assert message.endswith(HINT)
    assert isinstance(excinfo.value.__cause__, requests.ConnectionError)


def test_the_gate_names_the_backend_it_is_given_when_the_worker_cannot_be_reached(flag, closed_port_url):
    with pytest.raises(ComputeUnavailable) as at_the_gate:
        handshake.ensure_handshake(closed_port_url, backend_id=TEAM)
    with pytest.raises(ComputeUnavailable) as through_the_session:
        subops._worker_session.request("GET", "/status/x", worker_url=closed_port_url, backend_id=TEAM)

    for raised in (at_the_gate.value, through_the_session.value):
        assert raised.backend_id == TEAM
        assert str(raised).startswith(f"Could not reach the worker at {closed_port_url}: ")
        assert str(raised).endswith(TEAM_HINT)
    assert handshake.last_verdict(TEAM) is None


def test_compute_unavailable_adds_the_hint_once_and_only_for_a_refused_connection():
    assert str(ComputeUnavailable("[Errno 61] Connection refused")) == f"[Errno 61] Connection refused{HINT}"
    rebuilt = f"node 1: [Errno 61] Connection refused{HINT}"
    assert str(ComputeUnavailable(rebuilt)) == rebuilt
    named = str(ComputeUnavailable("[Errno 61] Connection refused", backend_id=TEAM))
    assert named == f"[Errno 61] Connection refused{TEAM_HINT}"
    assert str(ComputeUnavailable(f"node 1: {named}")) == f"node 1: {named}"
    windows = "[WinError 10061] No connection could be made because the target machine actively refused it"
    assert str(ComputeUnavailable(windows)) == f"{windows}{HINT}"
    assert str(ComputeUnavailable("Read timed out")) == "Read timed out"

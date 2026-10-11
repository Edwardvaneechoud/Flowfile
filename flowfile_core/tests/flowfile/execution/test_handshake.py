"""Core checks the worker's versions and plugin libraries before its first worker call, once per process."""

import socket

import polars as pl
import pytest
import requests

from flowfile_core.flowfile.execution.errors import ComputeUnavailable, VersionMismatch
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake, streaming
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas
from shared.compute_protocol import EXPRESSION_PLUGINS, local_capabilities

HINT = ". Is the worker running? Start it, or switch the flow to local execution."


@pytest.fixture(autouse=True)
def unchecked_worker(monkeypatch):
    """Every test starts in a process that has not checked the worker yet, with offload on."""
    monkeypatch.setattr(subops, "OFFLOAD_TO_WORKER", True)
    handshake.reset_handshake()
    yield
    handshake.reset_handshake()


@pytest.fixture
def capability_fetches(monkeypatch) -> list[str]:
    """The worker URL of each ``GET /capabilities`` core makes, still answered by the real worker."""
    fetches = []
    real_fetch = handshake.fetch_worker_capabilities

    def recording_fetch(worker_url):
        fetches.append(worker_url)
        return real_fetch(worker_url)

    monkeypatch.setattr(handshake, "fetch_worker_capabilities", recording_fetch)
    return fetches


@pytest.fixture
def worker_reports(monkeypatch):
    """Make the worker report this process's capabilities with the given fields changed."""

    def report(**changes) -> list[int]:
        fetches = []
        reported = local_capabilities().model_copy(update=changes)

        def fetch(worker_url):
            fetches.append(1)
            return reported

        monkeypatch.setattr(handshake, "fetch_worker_capabilities", fetch)
        return fetches

    return report


@pytest.fixture
def sent_to_worker(monkeypatch) -> dict[str, int]:
    """Count REST requests that leave core and plans the probe submits."""
    sent = {"requests": 0, "probe_plans": 0}
    real_send = requests.Session.send
    real_fetcher = subops.ExternalDfFetcher

    def counting_send(self, *args, **kwargs):
        sent["requests"] += 1
        return real_send(self, *args, **kwargs)

    def counting_fetcher(*args, **kwargs):
        sent["probe_plans"] += 1
        return real_fetcher(*args, **kwargs)

    monkeypatch.setattr(requests.Session, "send", counting_send)
    monkeypatch.setattr(subops, "ExternalDfFetcher", counting_fetcher)
    return sent


@pytest.fixture
def websocket_connects(monkeypatch) -> list[str]:
    """The URL of each ``/ws/submit`` connection core opens."""
    connects = []
    real_connect = streaming.connect

    def recording_connect(url, *args, **kwargs):
        connects.append(url)
        return real_connect(url, *args, **kwargs)

    monkeypatch.setattr(streaming, "connect", recording_connect)
    return connects


@pytest.fixture
def closed_port_url() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}"


def _one_node_remote_graph(flow_id: int):
    """A remote flow whose node caches its result, so its run stores the result on the worker."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="handshake", path=".", execution_location="remote")
    )
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            cache_results=True,
            raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]),
        )
    )
    return graph


def _development_remote_graph(flow_id: int):
    """A remote Development-mode flow whose node sends its plan to the worker over ``/ws/submit``."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id, name="handshake", path=".", execution_location="remote", execution_mode="Development"
        )
    )
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    return graph


def test_remote_run_against_the_session_worker_passes_the_handshake(capability_fetches):
    graph = _one_node_remote_graph(9201)
    node = graph.get_node(1)
    subops.clear_task_from_worker(node.hash)
    handshake.reset_handshake()
    capability_fetches.clear()

    run_info = graph.run_graph()

    assert run_info.success is True
    assert node.results.resulting_data.to_pylist() == [{"a": 1}]
    assert capability_fetches == [subops.WORKER_URL]
    assert handshake._handshake_done is True
    subops.clear_task_from_worker(node.hash)


def test_cached_remote_node_fails_on_a_mismatched_worker_without_running_in_core(
    worker_reports, sent_to_worker, websocket_connects, monkeypatch
):
    fetches = worker_reports(polars_version="0.0.1")
    graph = _one_node_remote_graph(9204)
    node = graph.get_node(1)
    calls = {"function": 0, "execution": 0, "collect": 0}
    real_function = node._function
    real_execute = FlowNode._do_execute
    real_collect = pl.LazyFrame.collect

    def counting_function(*args, **kwargs):
        calls["function"] += 1
        return real_function(*args, **kwargs)

    def counting_execute(self, *args, **kwargs):
        calls["execution"] += 1
        return real_execute(self, *args, **kwargs)

    def counting_collect(self, *args, **kwargs):
        calls["collect"] += 1
        return real_collect(self, *args, **kwargs)

    monkeypatch.setattr(node, "_function", counting_function)
    monkeypatch.setattr(FlowNode, "_do_execute", counting_execute)
    monkeypatch.setattr(pl.LazyFrame, "collect", counting_collect)

    run_info = graph.run_graph()

    result = next(r for r in run_info.node_step_result if r.node_id == 1)
    assert run_info.success is False
    assert result.success is False
    assert result.error == (
        f"The worker at {subops.WORKER_URL} does not match core (polars_version: core {pl.__version__}, worker 0.0.1)"
    )
    assert node.results.errors == result.error
    assert node._last_exception_class == "VersionMismatch"
    # The node holds no stored result yet, so the run builds its plan and the store's handshake refuses it.
    assert calls == {"function": 1, "execution": 1, "collect": 0}
    assert node.results.result_ref is None
    assert fetches == [1]
    assert sent_to_worker == {"requests": 0, "probe_plans": 0}
    assert websocket_connects == []


def test_handshake_runs_once_per_process(capability_fetches, sent_to_worker):
    assert subops.results_exists("no-such-task") is False
    assert subops.results_exists("no-such-task") is False

    assert capability_fetches == [subops.WORKER_URL]
    assert sent_to_worker["probe_plans"] == len(EXPRESSION_PLUGINS)
    assert handshake._handshake_done is True


def test_other_polars_version_is_refused_before_anything_is_sent(worker_reports, sent_to_worker):
    fetches = worker_reports(polars_version="0.0.1")

    with pytest.raises(VersionMismatch) as excinfo:
        subops.results_exists("no-such-task")

    assert str(excinfo.value) == (
        f"The worker at {subops.WORKER_URL} does not match core (polars_version: core {pl.__version__}, worker 0.0.1)"
    )
    assert fetches == [1]
    assert sent_to_worker == {"requests": 0, "probe_plans": 0}
    assert handshake._handshake_done is False


def test_refused_worker_is_checked_again_on_the_next_call(worker_reports):
    fetches = worker_reports(python_version="2.7.18")

    for _ in range(2):
        with pytest.raises(VersionMismatch):
            subops.get_status("no-such-task")

    assert fetches == [1, 1]


def test_every_difference_is_named(worker_reports):
    core = local_capabilities()
    worker_reports(app_version="0.0.0", protocol_version=core.protocol_version + 1, platform="plan9-mips")

    with pytest.raises(VersionMismatch) as excinfo:
        subops.results_exists("no-such-task")

    assert str(excinfo.value) == (
        f"The worker at {subops.WORKER_URL} does not match core ("
        f"app_version: core {core.app_version}, worker 0.0.0; "
        f"protocol_version: core {core.protocol_version}, worker {core.protocol_version + 1}; "
        f"platform: core {core.platform}, worker plan9-mips)"
    )


def test_other_plugin_library_path_is_refused_with_the_plugin_and_both_paths(worker_reports, sent_to_worker):
    core_libs = local_capabilities().plugin_libs
    moved = "lib/python3.99/site-packages/polars_ds/_polars_ds.abi3.so"
    fetches = worker_reports(plugin_libs={**core_libs, "polars_ds": moved})

    with pytest.raises(VersionMismatch) as excinfo:
        subops.results_exists("no-such-task")

    assert str(excinfo.value) == (
        f"The worker at {subops.WORKER_URL} does not match core "
        f"(plugin_libs[polars_ds]: core {core_libs['polars_ds']}, worker {moved})"
    )
    assert fetches == [1]
    assert sent_to_worker == {"requests": 0, "probe_plans": 0}


def test_other_plugin_version_is_refused_with_the_plugin_and_both_versions(worker_reports):
    core_plugins = local_capabilities().plugins
    worker_reports(plugins={**core_plugins, "polars_grouper": "0.0.1"})

    with pytest.raises(VersionMismatch) as excinfo:
        subops.results_exists("no-such-task")

    assert str(excinfo.value) == (
        f"The worker at {subops.WORKER_URL} does not match core "
        f"(plugins[polars_grouper]: core {core_plugins['polars_grouper']}, worker 0.0.1)"
    )


def test_plugin_on_one_side_only_is_a_mismatch(worker_reports):
    core_libs = local_capabilities().plugin_libs
    worker_reports(plugin_libs={"polars_grouper": core_libs["polars_grouper"]})

    with pytest.raises(VersionMismatch) as excinfo:
        subops.results_exists("no-such-task")

    assert str(excinfo.value) == (
        f"The worker at {subops.WORKER_URL} does not match core "
        f"(plugin_libs[polars_ds]: core {core_libs['polars_ds']}, worker not installed)"
    )


def test_plugin_plan_the_worker_cannot_run_is_refused_naming_the_plugin(capability_fetches, monkeypatch):
    real_plans = handshake._probe_plans()
    failing = pl.LazyFrame(schema={"x": pl.Int32}).select(pl.col("not_a_column"))
    monkeypatch.setattr(handshake, "_probe_plans", lambda: {**real_plans, "polars_ds": failing})
    libs = local_capabilities().plugin_libs

    with pytest.raises(VersionMismatch) as excinfo:
        subops.results_exists("no-such-task")

    message = str(excinfo.value)
    assert message.startswith(
        f"The worker at {subops.WORKER_URL} could not run a polars_ds plan "
        f"(library: core {libs['polars_ds']}, worker {libs['polars_ds']}): "
    )
    assert "not_a_column" in message
    assert "polars_grouper" not in message
    assert capability_fetches == [subops.WORKER_URL]
    assert handshake._handshake_done is False


def test_worker_without_a_capabilities_route_is_an_older_release(monkeypatch):
    older_worker = f"{subops.WORKER_URL}/no-such-prefix"
    monkeypatch.setattr(subops, "WORKER_URL", older_worker)

    with pytest.raises(VersionMismatch) as excinfo:
        subops.get_status("no-such-task")

    assert str(excinfo.value) == (
        f"The worker at {older_worker} does not report its capabilities (GET /capabilities answered 404), "
        "so it is an older release than core"
    )
    assert handshake._handshake_done is False


def test_websocket_submit_to_a_mismatched_worker_is_refused(
    worker_reports, sent_to_worker, websocket_connects, monkeypatch
):
    real_fetch = handshake.fetch_worker_capabilities
    fetches = worker_reports(polars_version="0.0.1")
    graph = _development_remote_graph(9203)
    node = graph.get_node(1)

    refused = graph.run_graph()

    result = next(r for r in refused.node_step_result if r.node_id == 1)
    assert refused.success is False
    assert result.error == (
        f"The worker at {subops.WORKER_URL} does not match core (polars_version: core {pl.__version__}, worker 0.0.1)"
    )
    assert node._last_exception_class == "VersionMismatch"
    assert websocket_connects == []
    assert sent_to_worker == {"requests": 0, "probe_plans": 0}
    assert fetches == [1]

    monkeypatch.setattr(handshake, "fetch_worker_capabilities", real_fetch)
    accepted = graph.run_graph()

    assert accepted.success is True
    assert node.results.resulting_data.to_pylist() == [{"a": 1}]
    assert sent_to_worker["probe_plans"] == len(EXPRESSION_PLUGINS)
    assert len(websocket_connects) == len(EXPRESSION_PLUGINS) + 1
    assert handshake._handshake_done is True


def test_failed_websocket_connect_rearms_the_handshake(capability_fetches, closed_port_url, monkeypatch):
    assert subops.results_exists("no-such-task") is False
    assert handshake._handshake_done is True
    monkeypatch.setattr(streaming, "_get_ws_url", lambda: closed_port_url.replace("http://", "ws://"))

    with pytest.raises(OSError):
        streaming.streaming_start(task_id="t", operation_type="store", flow_id=-1, node_id=-1, lf_bytes=b"")

    assert handshake._handshake_done is False
    assert capability_fetches == [subops.WORKER_URL]


def test_cleanup_calls_to_a_mismatched_worker_return_false(worker_reports, sent_to_worker):
    fetches = worker_reports(polars_version="0.0.1")

    assert subops.clear_task_from_worker("no-such-task") is False
    assert subops.cancel_task("no-such-task") is False

    assert fetches == [1, 1]
    assert sent_to_worker == {"requests": 0, "probe_plans": 0}


def test_unreachable_worker_rearms_the_handshake(capability_fetches, closed_port_url, monkeypatch):
    live_url = subops.WORKER_URL
    assert subops.results_exists("no-such-task") is False
    assert capability_fetches == [live_url]

    monkeypatch.setattr(subops, "WORKER_URL", closed_port_url)
    with pytest.raises(ComputeUnavailable) as excinfo:
        subops.results_exists("no-such-task")
    assert str(excinfo.value).startswith(f"Could not reach the worker at {closed_port_url}: ")
    assert capability_fetches == [live_url]
    assert handshake._handshake_done is False

    monkeypatch.setattr(subops, "WORKER_URL", live_url)
    assert subops.results_exists("no-such-task") is False
    assert capability_fetches == [live_url, live_url]
    assert handshake._handshake_done is True


def test_unreachable_worker_on_the_first_call_is_compute_unavailable(closed_port_url, monkeypatch):
    monkeypatch.setattr(subops, "WORKER_URL", closed_port_url)

    with pytest.raises(ComputeUnavailable) as excinfo:
        subops.get_status("no-such-task")

    message = str(excinfo.value)
    assert message.startswith(f"Could not reach the worker at {closed_port_url}: ")
    assert message.endswith(HINT)
    assert isinstance(excinfo.value.__cause__, requests.ConnectionError)
    assert handshake._handshake_done is False


def test_compute_unavailable_adds_the_hint_only_for_a_refused_connection():
    assert str(ComputeUnavailable("[Errno 61] Connection refused")) == f"[Errno 61] Connection refused{HINT}"
    assert str(ComputeUnavailable("Read timed out")) == "Read timed out"

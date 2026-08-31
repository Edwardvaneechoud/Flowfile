"""Unit tests for the telemetry observer and the event seam it hangs off.

No DB, no flow execution: the graph is a duck-typed fake and events are
published the way product code publishes them, so these tests pin the payload
contract rather than the engine. Events are captured at the shared client's
``_post`` seam with the background thread disabled, so every assertion sees
exactly the bytes that would have gone over the wire.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from flowfile_core import events
from flowfile_core import telemetry as glue
from shared import telemetry as client

POISON = (
    "/Users/x/secret.csv",
    "SELECT * FROM t",
    "password=hunter2",
    "Quarterly Revenue (confidential)",
    "customer_email",
)


class FakeTemplate:
    def __init__(self, input_count: int) -> None:
        self.input = input_count


class FakeNode:
    def __init__(self, node_type: str, input_count: int = 1, node_id: int = 1, **extra) -> None:
        self.node_type = node_type
        self.node_id = node_id
        self.node_template = FakeTemplate(input_count)
        for key, value in extra.items():
            setattr(self, key, value)


class FakeSettings:
    def __init__(self, is_canceled: bool = False) -> None:
        self.is_canceled = is_canceled


class FakeGraph:
    def __init__(self, nodes: list[FakeNode], is_canceled: bool = False) -> None:
        self.nodes = nodes
        self.flow_settings = FakeSettings(is_canceled)


class FakeNodeResult:
    def __init__(self, node_id: int, success: bool | None, skipped: bool = False) -> None:
        self.node_id = node_id
        self.success = success
        self.skipped = skipped


class FakeRunInfo:
    def __init__(self, success: bool, start_time=None, end_time=None, node_step_result=None) -> None:
        self.success = success
        self.start_time = start_time
        self.end_time = end_time
        self.node_step_result = node_step_result or []


@pytest.fixture
def sent(tmp_path, monkeypatch) -> Iterator[list[dict]]:
    """All four gates open, delivery captured, worker thread disabled."""
    monkeypatch.setattr(client, "_settings_file", lambda: tmp_path / "telemetry.yaml")
    client._reset_for_tests()
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.delenv(client.ENV_KILL_SWITCH, raising=False)
    monkeypatch.setenv(client.ENV_ENDPOINT, "https://example.invalid/events")
    client.set_consent(True)

    captured: list[dict] = []

    def _capture(url: str, json_body: dict, timeout: float | None = None):
        captured.extend(json_body["events"])
        return None

    monkeypatch.setattr(client, "_ensure_worker", lambda: None)
    monkeypatch.setattr(client, "_post", _capture)
    yield captured
    client._reset_for_tests()


@pytest.fixture
def subscribed() -> Iterator[None]:
    """A clean event bus with exactly the telemetry observer on it."""
    events._reset_for_tests()
    glue._subscribed = False
    glue._snapshots.clear()
    glue.install_headless()
    yield
    events._reset_for_tests()
    glue._snapshots.clear()
    glue._subscribed = False
    glue._subscribe()  # leave the process wired up the way main.py left it


def drain(captured: list[dict]) -> list[dict]:
    client.flush(1.0)
    return captured


def names(captured: list[dict]) -> list[str]:
    return [event["event"] for event in drain(captured)]


def test_snapshot_collapses_unknown_node_types_to_custom() -> None:
    graph = FakeGraph([FakeNode("manual_input", 0), FakeNode("my_sneaky_node"), FakeNode("select")])
    snapshot = glue.run_snapshot(graph)
    assert snapshot["node_types"] == ["custom", "manual_input", "select"]
    assert snapshot["node_count"] == 3


def test_snapshot_sample_data_flags() -> None:
    sample_only = FakeGraph([FakeNode("manual_input", 0), FakeNode("select")])
    assert glue.run_snapshot(sample_only)["used_sample_data"] is True

    with_read = FakeGraph([FakeNode("manual_input", 0), FakeNode("read", 0), FakeNode("select")])
    assert glue.run_snapshot(with_read)["used_sample_data"] is False


def test_snapshot_detects_catalog_usage() -> None:
    assert glue.run_snapshot(FakeGraph([FakeNode("catalog_reader", 0)]))["uses_catalog"] is True
    assert glue.run_snapshot(FakeGraph([FakeNode("manual_input", 0)]))["uses_catalog"] is False


def test_snapshot_never_raises_on_a_broken_graph() -> None:
    class Exploding:
        @property
        def nodes(self):
            raise RuntimeError("boom")

    assert glue.run_snapshot(Exploding()) is None


class TestEventBus:
    """flowfile_core.events: the neutral seam product code publishes on."""

    def setup_method(self) -> None:
        events._reset_for_tests()

    def teardown_method(self) -> None:
        events._reset_for_tests()
        glue._subscribed = False
        glue._subscribe()

    def test_publishing_without_subscribers_is_a_no_op(self) -> None:
        events.publish("flow_run_started", graph=object())

    def test_handlers_receive_the_payload_in_registration_order(self) -> None:
        seen: list[str] = []
        events.subscribe("thing", lambda value: seen.append(f"first:{value}"))
        events.subscribe("thing", lambda value: seen.append(f"second:{value}"))
        events.publish("thing", value="x")
        assert seen == ["first:x", "second:x"]

    def test_a_raising_handler_is_swallowed_and_logged_at_debug(self, caplog) -> None:
        def _boom() -> None:
            raise RuntimeError("subscriber exploded")

        reached: list[bool] = []
        events.subscribe("thing", _boom)
        events.subscribe("thing", lambda: reached.append(True))

        with caplog.at_level(logging.DEBUG, logger=events.logger.name):
            events.publish("thing")

        assert reached == [True], "one bad subscriber must not stop the others"
        assert [r.levelno for r in caplog.records if r.name == events.logger.name] == [logging.DEBUG]

    def test_a_handler_with_the_wrong_signature_cannot_reach_the_publisher(self) -> None:
        events.subscribe("thing", lambda: None)
        events.publish("thing", unexpected=1)

    def test_reset_for_tests_clears_every_subscription(self) -> None:
        seen: list[int] = []
        events.subscribe("thing", lambda: seen.append(1))
        events._reset_for_tests()
        events.publish("thing")
        assert seen == []


class TestRunEvents:
    """The domain events product code publishes, seen through the observer."""

    def test_started_emits_started_and_catalog_used(self, sent, subscribed) -> None:
        graph = FakeGraph([FakeNode("catalog_reader", 0)])
        events.publish("flow_run_started", graph=graph)

        assert glue._snapshots[graph]["uses_catalog"] is True
        assert names(sent) == ["flow_run_started", "catalog_used"]

    @pytest.mark.parametrize("attrs", [{"_subflow_depth": 1}, {"_system_run": True}])
    def test_subflow_and_system_runs_are_silent(self, sent, subscribed, attrs) -> None:
        graph = FakeGraph([FakeNode("read", 0), FakeNode("select"), FakeNode("output")])
        for key, value in attrs.items():
            setattr(graph, key, value)

        events.publish("flow_run_started", graph=graph)
        events.publish("flow_run_finished", graph=graph, run_info=FakeRunInfo(success=True))
        events.publish("flow_run_crashed", graph=graph, error=ValueError("nope"))

        assert names(sent) == []
        assert len(glue._snapshots) == 0

    def test_finished_success_is_bucketed_from_the_start_snapshot(self, sent, subscribed) -> None:
        import datetime

        nodes = [FakeNode("read", 0), FakeNode("select"), FakeNode("output"), FakeNode("select", node_id=4)]
        graph = FakeGraph(nodes)
        start = datetime.datetime(2026, 1, 1, 12, 0, 0)
        run_info = FakeRunInfo(success=True, start_time=start, end_time=start + datetime.timedelta(seconds=3.5))

        events.publish("flow_run_started", graph=graph)
        events.publish("flow_run_finished", graph=graph, run_info=run_info)

        emitted = drain(sent)
        assert [e["event"] for e in emitted] == ["flow_run_started", "flow_run_succeeded", "activation"]
        succeeded = emitted[1]
        assert succeeded["props"] == {
            "node_count_bucket": "4-7",
            "node_types": ["output", "read", "select"],
            "duration_bucket": "1-10s",
            "used_sample_data": False,
        }
        assert set(succeeded) == {"event", "install_id", "app_version", "platform", "mode", "ts", "props"}
        assert graph not in glue._snapshots, "the snapshot is dropped when the run ends"

    def test_finished_reports_the_error_class_of_a_node_that_failed_this_run(self, sent, subscribed) -> None:
        ok = FakeNode("manual_input", 0, node_id=1)
        failing = FakeNode("select", node_id=2, _last_exception_class="ComputeError")
        graph = FakeGraph([ok, failing])
        run_info = FakeRunInfo(
            success=False,
            node_step_result=[FakeNodeResult(1, True), FakeNodeResult(2, False)],
        )

        events.publish("flow_run_started", graph=graph)
        events.publish("flow_run_finished", graph=graph, run_info=run_info)

        emitted = drain(sent)
        assert [e["event"] for e in emitted] == ["flow_run_started", "flow_run_failed"]
        assert emitted[1]["props"] == {"error_class": "ComputeError"}

    def test_only_this_runs_failures_are_consulted(self, sent, subscribed) -> None:
        """A class left on a node that did not fail this run must not be reported."""
        stale = FakeNode("read", 0, node_id=1, _last_exception_class="ComputeError")
        failing = FakeNode("select", node_id=2, _last_exception_class="ValueError")
        graph = FakeGraph([stale, failing])
        run_info = FakeRunInfo(
            success=False,
            node_step_result=[FakeNodeResult(1, True), FakeNodeResult(2, False)],
        )

        events.publish("flow_run_finished", graph=graph, run_info=run_info)
        assert drain(sent)[0]["props"] == {"error_class": "ValueError"}

    def test_a_deliberately_skipped_node_is_not_a_failure(self, sent, subscribed) -> None:
        skipped = FakeNode("select", node_id=2, _last_exception_class="ComputeError")
        graph = FakeGraph([FakeNode("read", 0, node_id=1), skipped])
        run_info = FakeRunInfo(
            success=False,
            node_step_result=[FakeNodeResult(1, True), FakeNodeResult(2, False, skipped=True)],
        )

        events.publish("flow_run_finished", graph=graph, run_info=run_info)
        assert drain(sent)[0]["props"] == {"error_class": "OtherError"}

    def test_failed_run_without_a_recorded_class_falls_back(self, sent, subscribed) -> None:
        graph = FakeGraph([FakeNode("read", 0, node_id=1)])
        run_info = FakeRunInfo(success=False, node_step_result=[FakeNodeResult(1, False)])

        events.publish("flow_run_finished", graph=graph, run_info=run_info)
        assert drain(sent)[0]["props"] == {"error_class": "OtherError"}

    def test_canceled_runs_emit_no_completion_event(self, sent, subscribed) -> None:
        graph = FakeGraph([FakeNode("read", 0), FakeNode("select"), FakeNode("output")], is_canceled=True)
        events.publish("flow_run_started", graph=graph)
        events.publish("flow_run_finished", graph=graph, run_info=FakeRunInfo(success=True))
        assert names(sent) == ["flow_run_started"]

    def test_crashed_emits_failed_with_the_classified_exception(self, sent, subscribed) -> None:
        graph = FakeGraph([FakeNode("read", 0)])
        events.publish("flow_run_started", graph=graph)
        events.publish("flow_run_crashed", graph=graph, error=ValueError("boom"))

        emitted = drain(sent)
        assert [e["event"] for e in emitted] == ["flow_run_started", "flow_run_failed"]
        assert emitted[1]["props"] == {"error_class": "ValueError"}
        assert graph not in glue._snapshots

    def test_crash_of_an_unknown_exception_class_is_collapsed(self, sent, subscribed) -> None:
        class UserDefinedWeirdError(Exception):
            pass

        events.publish("flow_run_crashed", graph=FakeGraph([]), error=UserDefinedWeirdError("x"))
        assert drain(sent)[0]["props"] == {"error_class": "OtherError"}

    def test_kernel_exec_emits_kernel_used_once_per_process(self, sent, subscribed) -> None:
        events.publish("kernel_exec")
        events.publish("kernel_exec")
        assert names(sent) == ["kernel_used"]

    def test_app_started(self, sent, subscribed) -> None:
        events.publish("app_started")
        assert names(sent) == ["app_started"]

    def test_a_broken_graph_never_reaches_the_publisher(self, sent, subscribed) -> None:
        class Exploding:
            @property
            def flow_settings(self):
                raise RuntimeError("boom")

            @property
            def nodes(self):
                raise RuntimeError("boom")

        events.publish("flow_run_started", graph=Exploding())
        events.publish("flow_run_finished", graph=Exploding(), run_info=FakeRunInfo(success=True))


def test_succeeded_payload_is_bucketed(sent) -> None:
    snapshot = {
        "node_count": 5,
        "node_types": ["read", "select", "write_output"],
        "used_sample_data": False,
        "uses_catalog": False,
    }
    glue.emit_run_events(snapshot, outcome="succeeded", duration_seconds=3.5)
    events_sent = drain(sent)
    succeeded = next(e for e in events_sent if e["event"] == "flow_run_succeeded")
    assert succeeded["props"] == {
        "node_count_bucket": "4-7",
        "node_types": ["read", "select", "write_output"],
        "duration_bucket": "1-10s",
        "used_sample_data": False,
    }
    assert set(succeeded) == {"event", "install_id", "app_version", "platform", "mode", "ts", "props"}


@pytest.mark.parametrize("node_count", [2, 4])
@pytest.mark.parametrize("used_sample_data", [True, False])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_activation_only_on_real_multi_node_success(sent, node_count, used_sample_data, outcome) -> None:
    snapshot = {
        "node_count": node_count,
        "node_types": ["read"],
        "used_sample_data": used_sample_data,
        "uses_catalog": False,
    }
    glue.emit_run_events(snapshot, outcome=outcome, duration_seconds=1.0, error_class="ValueError")
    emitted = names(sent)
    expected = outcome == "succeeded" and node_count >= 3 and not used_sample_data
    assert ("activation" in emitted) is expected


def test_activation_is_emitted_once_per_process(sent) -> None:
    snapshot = {"node_count": 9, "node_types": ["read"], "used_sample_data": False, "uses_catalog": False}
    glue.emit_run_events(snapshot, outcome="succeeded", duration_seconds=1.0)
    glue.emit_run_events(snapshot, outcome="succeeded", duration_seconds=1.0)
    assert names(sent).count("activation") == 1


def test_canceled_outcome_emits_nothing(sent) -> None:
    snapshot = {"node_count": 9, "node_types": ["read"], "used_sample_data": False, "uses_catalog": False}
    glue.emit_run_events(snapshot, outcome="canceled", duration_seconds=1.0)
    assert names(sent) == []


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("ValueError", "ValueError"),
        ("KernelDependencyError", "KernelDependencyError"),
        ("UserDefinedWeirdError", "OtherError"),
        ("SELECT * FROM t", "OtherError"),
        (None, "OtherError"),
    ],
)
def test_classify_error(raw, expected) -> None:
    assert glue.classify_error(raw) == expected


def test_no_user_content_ever_reaches_the_wire(sent, subscribed) -> None:
    """The poison test: nothing a user typed may appear in any emitted payload."""
    nodes = [
        FakeNode(
            "read",
            0,
            node_id=1,
            name="/Users/x/secret.csv",
            description="password=hunter2",
            setting_input={"file_path": "/Users/x/secret.csv", "query": "SELECT * FROM t"},
        ),
        FakeNode(
            "customer_email",
            1,
            node_id=2,
            name="Quarterly Revenue (confidential)",
            description="SELECT * FROM t",
        ),
        FakeNode("polars_code", 1, node_id=3, name="password=hunter2"),
    ]
    nodes[0]._last_exception_class = "SELECT * FROM t"
    graph = FakeGraph(nodes)
    run_info = FakeRunInfo(success=False, node_step_result=[FakeNodeResult(1, False)])

    events.publish("flow_run_started", graph=graph)
    events.publish("flow_run_finished", graph=graph, run_info=run_info)
    glue.emit("export_code_used", {"target": "polars"})
    glue.emit("flow_created", {"flow_name": "Quarterly Revenue (confidential)"})

    blob = json.dumps(drain(sent))
    for poison in POISON:
        assert poison not in blob, f"leaked {poison!r}"
    assert "flow_name" not in blob


def test_unknown_events_and_props_are_dropped(sent) -> None:
    glue.emit("definitely_not_an_event")
    glue.emit("flow_run_failed", {"error_class": "ValueError", "stack_trace": "/Users/x/secret.csv"})
    emitted = drain(sent)
    assert [e["event"] for e in emitted] == ["flow_run_failed"]
    assert emitted[0]["props"] == {"error_class": "ValueError"}


class TestRouteMiddleware:
    """HTTP events come from one declarative table, not from handler bodies."""

    @staticmethod
    def _app() -> FastAPI:
        app = FastAPI()

        @app.post("/editor/create_flow/")
        def _create_flow(fail: bool = False):
            if fail:
                raise HTTPException(422, "nope")
            return 1

        @app.get("/editor/code_to_polars")
        def _code_to_polars(fail: bool = False):
            if fail:
                raise HTTPException(422, "nope")
            return "df = pl.LazyFrame()"

        @app.get("/editor/code_to_project/zip")
        def _zip():
            raise RuntimeError("kaboom")

        @app.get("/editor/unmapped_route")
        def _unmapped():
            return "nothing to see"

        app.add_middleware(glue.TelemetryMiddleware)
        return app

    @pytest.fixture
    def http(self) -> Iterator[TestClient]:
        with TestClient(self._app(), raise_server_exceptions=False) as testclient:
            yield testclient

    def test_a_mapped_route_emits_its_event_with_its_props(self, sent, http) -> None:
        assert http.get("/editor/code_to_polars").status_code == 200
        emitted = drain(sent)
        assert [e["event"] for e in emitted] == ["export_code_used"]
        assert emitted[0]["props"] == {"target": "polars"}

    def test_a_mapped_route_without_props(self, sent, http) -> None:
        assert http.post("/editor/create_flow/").status_code == 200
        emitted = drain(sent)
        assert [e["event"] for e in emitted] == ["flow_created"]
        assert emitted[0]["props"] == {}

    @pytest.mark.parametrize(
        ("method", "path"),
        [("get", "/editor/code_to_polars?fail=true"), ("post", "/editor/create_flow/?fail=true")],
    )
    def test_a_client_error_emits_nothing(self, sent, http, method, path) -> None:
        assert getattr(http, method)(path).status_code == 422
        assert names(sent) == []

    def test_a_server_error_emits_nothing(self, sent, http) -> None:
        assert http.get("/editor/code_to_project/zip").status_code == 500
        assert names(sent) == []

    def test_an_unmapped_route_emits_nothing(self, sent, http) -> None:
        assert http.get("/editor/unmapped_route").status_code == 200
        assert names(sent) == []

    def test_an_unmapped_method_on_a_mapped_path_emits_nothing(self, sent, http) -> None:
        assert http.post("/editor/code_to_polars").status_code == 405
        assert names(sent) == []


class TestInstall:
    def test_install_is_idempotent(self, monkeypatch) -> None:
        monkeypatch.setattr(glue, "_middleware_installed", False)
        monkeypatch.setattr(glue, "_subscribed", False)
        events._reset_for_tests()

        app = FastAPI()
        glue.install(app)
        glue.install(app)

        try:
            assert [m.cls for m in app.user_middleware] == [glue.TelemetryMiddleware]
            assert [len(handlers) for handlers in events._handlers.values()] == [1, 1, 1, 1, 1]
        finally:
            events._reset_for_tests()
            glue._subscribed = False
            glue._subscribe()

    def test_install_headless_subscribes_without_an_app(self, monkeypatch) -> None:
        monkeypatch.setattr(glue, "_subscribed", False)
        events._reset_for_tests()
        try:
            glue.install_headless()
            glue.install_headless()
            assert set(events._handlers) == {
                "flow_run_started",
                "flow_run_finished",
                "flow_run_crashed",
                "kernel_exec",
                "app_started",
            }
            assert all(len(handlers) == 1 for handlers in events._handlers.values())
        finally:
            events._reset_for_tests()
            glue._subscribed = False
            glue._subscribe()


def test_every_mapped_route_exists_on_the_app() -> None:
    """Route drift would silently stop an HTTP event; this is the tripwire."""
    from flowfile_core.main import app

    real = {(method, route.path) for route in app.routes for method in getattr(route, "methods", None) or ()}
    missing = sorted(key for key in glue.ROUTE_EVENTS if key not in real)
    assert missing == [], f"telemetry route table names routes that do not exist: {missing}"


class FakeSpawnRepo:
    """Just enough repository for FlowRunService.spawn_flow_run — no DB."""

    def __init__(self) -> None:
        self.runs: list = []

    def create_run(self, run):
        run.id = 4242
        self.runs.append(run)
        return run

    def update_run(self, run):
        return run


class FakeRegistration:
    id = 7
    flow_uuid = "uuid-7"
    name = "Daily FX Sync"
    flow_path = "/tmp/demo_fx_sync.yaml"


class FakeTriggerService:
    """Stands in for CatalogService in the demo seeder's populate call."""

    def __init__(self, boom: bool = False) -> None:
        self.calls: list[tuple] = []
        self.boom = boom

    def trigger_schedule_now(self, schedule_id: int, user_id: int, **kwargs):
        self.calls.append((schedule_id, user_id, kwargs))
        if self.boom:
            raise RuntimeError("no scheduler here")
        return None


def _spawn_recorder(monkeypatch) -> list[tuple]:
    from flowfile_core.catalog.services import runs as runs_service

    calls: list[tuple] = []

    def _fake(*args, **kwargs):
        calls.append((args, kwargs))
        return 999

    monkeypatch.setattr(runs_service, "spawn_flow_subprocess", _fake)
    return calls


def test_demo_seed_populate_suppresses_telemetry_in_the_spawned_child() -> None:
    """The FX populate spawns a fresh process, which no in-process marker can reach."""
    from flowfile_core.catalog import demo_seed

    service = FakeTriggerService()
    assert demo_seed._trigger_immediate_populate(service, 11, 1) == "triggered"
    assert service.calls == [(11, 1, {"suppress_telemetry": True})]


def test_demo_seed_populate_never_fails_the_seed() -> None:
    from flowfile_core.catalog import demo_seed

    service = FakeTriggerService(boom=True)
    assert demo_seed._trigger_immediate_populate(service, 11, 1) == "skipped"


def test_spawn_flow_run_forwards_the_suppression_flag(monkeypatch) -> None:
    from flowfile_core.catalog.services.runs import FlowRunService

    calls = _spawn_recorder(monkeypatch)
    FlowRunService(FakeSpawnRepo()).spawn_flow_run(
        FakeRegistration(), user_id=1, run_type="on_demand", schedule_id=3, suppress_telemetry=True
    )
    assert calls[-1][1] == {"suppress_telemetry": True}


def test_spawn_flow_run_does_not_suppress_by_default(monkeypatch) -> None:
    """The scheduler and every other caller must keep spawning a plain child."""
    from flowfile_core.catalog.services.runs import FlowRunService

    calls = _spawn_recorder(monkeypatch)
    FlowRunService(FakeSpawnRepo()).spawn_flow_run(FakeRegistration(), user_id=1, run_type="scheduled")
    assert calls[-1][1] == {"suppress_telemetry": False}


def test_spawn_flow_subprocess_kills_telemetry_without_touching_the_parent(monkeypatch, tmp_path) -> None:
    import os

    from shared import subprocess_utils

    captured: dict = {}

    class FakeProc:
        pid = 4242

    def _fake_popen(cmd, **kwargs):
        captured.update(kwargs)
        return FakeProc()

    monkeypatch.setattr(subprocess_utils.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(subprocess_utils, "run_log_path", lambda run_id: tmp_path / f"run_{run_id}.log")
    monkeypatch.setenv("FLOWFILE_SPAWN_MARKER", "inherited")
    before = os.environ.get(client.ENV_KILL_SWITCH)

    assert subprocess_utils.spawn_flow_subprocess("/tmp/flow.yaml", 1, suppress_telemetry=True) == 4242
    child_env = captured["env"]
    assert child_env[client.ENV_KILL_SWITCH] == "0"
    assert child_env["FLOWFILE_SPAWN_MARKER"] == "inherited", "the child still inherits the parent environment"
    assert os.environ.get(client.ENV_KILL_SWITCH) == before, "the parent environment must not be mutated"

    assert subprocess_utils.spawn_flow_subprocess("/tmp/flow.yaml", 2) == 4242
    assert captured["env"] is None, "without suppression the child inherits unchanged"


def test_spawn_flow_subprocess_still_merges_extra_env(monkeypatch, tmp_path) -> None:
    from shared import subprocess_utils

    captured: dict = {}

    class FakeProc:
        pid = 4242

    def _fake_popen(cmd, **kwargs):
        captured.update(kwargs)
        return FakeProc()

    monkeypatch.setattr(subprocess_utils.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(subprocess_utils, "run_log_path", lambda run_id: tmp_path / f"run_{run_id}.log")

    subprocess_utils.spawn_flow_subprocess("/tmp/flow.yaml", 1, {"FLOWFILE_EXTRA": "1"}, suppress_telemetry=True)
    assert captured["env"]["FLOWFILE_EXTRA"] == "1"
    assert captured["env"][client.ENV_KILL_SWITCH] == "0"

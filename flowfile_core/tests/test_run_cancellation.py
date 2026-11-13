"""A run a person stops reads as cancelled, never as failed, and a run that failed or finished keeps its outcome.

The Catalog's Cancel Run on a spawned run, cancel in the designer while a flow runs, and the readers of a run's
outcome: the run routes, the status counts, the flow's last run and the notification rules.
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog.services.runs import FlowRunService
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.routes.routes import _run_and_track
from flowfile_core.schemas import input_schema, transform_schema
from shared import run_completion
from shared.notifications.processor import process_pending_notifications
from shared.run_logs import run_log_path
from tests.flowfile.conftest import add_test_manual_input, create_test_graph

SLOW = "time.sleep(120)\noutput_df = input_df"
# A job of an in-process run cannot be stopped mid-node, so the designer tests wait this node out.
BRIEF = "time.sleep(5)\noutput_df = input_df"
BROKEN = 'output_df = input_df.select(pl.col("no_such_column"))'
RUNNING_MARK = "Starting Polars code"


def _client() -> TestClient:
    with TestClient(main.app) as client:
        token = client.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _client()


@pytest.fixture(autouse=True)
def clean_runs():
    """No run, registration or alert of another test is in the tables these tests count."""
    with get_db_context() as db:
        for model in (
            db_models.NotificationOutbox,
            db_models.NotificationRule,
            db_models.NotificationChannel,
            db_models.FlowRun,
            db_models.FlowRegistration,
        ):
            db.query(model).delete()
        db.commit()
    yield


def _add_polars_code(graph: FlowGraph, node_id: int, code: str, depends_on: int) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_ids=[depends_on],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))


def _graph(flow_id: int, code: str, execution_location: str = "local") -> FlowGraph:
    """Manual input -> Polars code -> record count, so a cancel leaves one node done, one stopped, one not started."""
    graph = create_test_graph(flow_id, name=f"cancel_{flow_id}", execution_location=execution_location)
    add_test_manual_input(graph, [{"a": i} for i in range(4)])
    _add_polars_code(graph, 2, code, depends_on=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=3, node_type="record_count"))
    graph.add_record_count(input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=3, depending_on_id=2))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    return graph


def _register(flow_path: Path | str, name: str) -> int:
    with get_db_context() as db:
        registration = db_models.FlowRegistration(
            flow_uuid=str(uuid.uuid4()), name=name, flow_path=str(flow_path), owner_id=1
        )
        db.add(registration)
        db.commit()
        db.refresh(registration)
        return registration.id


def _registered_flow(tmp_path: Path, flow_id: int, code: str) -> int:
    graph = _graph(flow_id, code)
    flow_path = tmp_path / f"flow_{flow_id}.yaml"
    graph.save_flow(str(flow_path))
    return _register(flow_path, f"cancel_{flow_id}")


def _run_row(run_id: int) -> db_models.FlowRun:
    with get_db_context() as db:
        run = db.get(db_models.FlowRun, run_id)
        db.expunge(run)
        return run


def _alert_on_everything(registration_id: int) -> None:
    """A webhook channel and a rule that fires on failure, success and recovery of the flow's runs."""
    with get_db_context() as db:
        now = _now()
        channel = db_models.NotificationChannel(
            owner_id=1,
            name=f"alerts-{registration_id}",
            channel_type="generic",
            webhook_url_encrypted="not-a-real-url",
            enabled=True,
            created_at=now,
            updated_at=now,
        )
        db.add(channel)
        db.flush()
        db.add(
            db_models.NotificationRule(
                owner_id=1,
                channel_id=channel.id,
                registration_id=registration_id,
                on_failure=True,
                on_success=True,
                on_recovery=True,
                enabled=True,
                created_at=now,
                updated_at=now,
            )
        )
        db.commit()


def _now() -> datetime:
    """Naive UTC, as the catalog database stores it."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _events(run_id: int) -> list[str]:
    with get_db_context() as db:
        rows = db.query(db_models.NotificationOutbox).filter_by(run_id=run_id).all()
        return sorted(row.event_type for row in rows)


@pytest.fixture
def run_children(monkeypatch):
    """The ``Popen`` of every run child the run service spawns, so a test can wait for it and nothing outlives it."""
    from shared import subprocess_utils

    children: list[subprocess.Popen] = []

    def popen(cmd, **kwargs):
        children.append(subprocess.Popen(cmd, **kwargs))
        return children[-1]

    monkeypatch.setattr(subprocess_utils, "subprocess", SimpleNamespace(Popen=popen))
    yield children
    for child in children:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=30)


def _start(registration_id: int) -> int:
    response = client.post(f"/catalog/flows/{registration_id}/run")
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _wait_for_log(run_id: int, mark: str, timeout: float = 180) -> None:
    log = run_log_path(run_id)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if log.exists() and mark in log.read_text(errors="replace"):
            return
        time.sleep(0.2)
    raise AssertionError(
        f"run {run_id} never logged {mark!r}:\n{log.read_text(errors='replace') if log.exists() else ''}"
    )


def test_a_run_cancelled_from_the_catalog_reads_cancelled_everywhere(tmp_path, run_children):
    registration_id = _registered_flow(tmp_path, 99471, SLOW)
    _alert_on_everything(registration_id)
    run_id = _start(registration_id)
    _wait_for_log(run_id, RUNNING_MARK)

    assert client.post(f"/catalog/runs/{run_id}/cancel").status_code == 204
    assert run_children[0].wait(timeout=60) != 0
    process_pending_notifications()

    row = _run_row(run_id)
    assert (row.success, row.cancelled) == (False, True)
    assert row.ended_at is not None and row.notification_processed_at is not None
    assert row.node_results_json is None, "the child is stopped at once and reports no node"

    detail = client.get(f"/catalog/runs/{run_id}").json()
    assert (detail["success"], detail["cancelled"]) == (False, True)
    page = client.get("/catalog/runs", params={"registration_id": registration_id}).json()
    assert [(item["id"], item["cancelled"]) for item in page["items"]] == [(run_id, True)]
    assert (page["total"], page["total_cancelled"], page["total_failed"], page["total_running"]) == (1, 1, 0, 0)
    flow = client.get(f"/catalog/flows/{registration_id}").json()
    assert (flow["last_run_success"], flow["last_run_cancelled"]) == (False, True)
    assert client.get("/catalog/active-runs").json() == []
    assert _events(run_id) == [], "a cancel fires no rule"


def test_a_run_that_fails_reads_failed_and_alerts(tmp_path, run_children):
    registration_id = _registered_flow(tmp_path, 99472, BROKEN)
    _alert_on_everything(registration_id)
    run_id = _start(registration_id)
    assert run_children[0].wait(timeout=300) == 1
    process_pending_notifications()

    row = _run_row(run_id)
    assert (row.success, row.cancelled) == (False, False)
    failed = [node for node in json.loads(row.node_results_json) if node["success"] is False]
    assert [node["node_id"] for node in failed] == [2]

    detail = client.get(f"/catalog/runs/{run_id}").json()
    assert (detail["success"], detail["cancelled"]) == (False, False)
    page = client.get("/catalog/runs", params={"registration_id": registration_id}).json()
    assert (page["total_failed"], page["total_cancelled"]) == (1, 0)
    flow = client.get(f"/catalog/flows/{registration_id}").json()
    assert (flow["last_run_success"], flow["last_run_cancelled"]) == (False, False)
    assert _events(run_id) == ["run_failed"]


def test_cancelling_a_run_that_already_failed_keeps_it_failed(tmp_path, run_children):
    registration_id = _registered_flow(tmp_path, 99473, BROKEN)
    run_id = _start(registration_id)
    assert run_children[0].wait(timeout=300) == 1
    before = _run_row(run_id)

    assert client.post(f"/catalog/runs/{run_id}/cancel").status_code == 204

    after = _run_row(run_id)
    assert (after.success, after.cancelled) == (False, False)
    assert (after.ended_at, after.duration_seconds) == (before.ended_at, before.duration_seconds)
    assert client.get(f"/catalog/runs/{run_id}").json()["cancelled"] is False


def _finished_run(registration_id: int, *, success: bool, cancelled: bool = False, minutes_ago: int = 0) -> int:
    """A closed manual run row of the flow, not yet evaluated for notifications."""
    with get_db_context() as db:
        registration = db.get(db_models.FlowRegistration, registration_id)
        ended = _now() - timedelta(minutes=minutes_ago)
        run = db_models.FlowRun(
            registration_id=registration.id,
            flow_uuid=registration.flow_uuid,
            flow_name=registration.name,
            flow_path=registration.flow_path,
            user_id=1,
            started_at=ended - timedelta(seconds=5),
            ended_at=ended,
            success=success,
            cancelled=cancelled,
            run_type="manual",
        )
        db.add(run)
        db.commit()
        return run.id


def test_a_cancelled_run_is_no_failure_to_the_notification_rules(tmp_path):
    registration_id = _register(tmp_path / "unused.yaml", "alerted")
    _alert_on_everything(registration_id)

    cancelled_only = _finished_run(registration_id, success=False, cancelled=True, minutes_ago=4)
    after_cancel = _finished_run(registration_id, success=True, minutes_ago=3)
    process_pending_notifications()
    assert _events(cancelled_only) == []
    assert _events(after_cancel) == ["run_success"], "a success after a cancel recovers from nothing"

    failed = _finished_run(registration_id, success=False, minutes_ago=2)
    cancelled = _finished_run(registration_id, success=False, cancelled=True, minutes_ago=1)
    recovered = _finished_run(registration_id, success=True)
    process_pending_notifications()
    assert _events(failed) == ["run_failed"]
    assert _events(cancelled) == []
    assert _events(recovered) == ["run_recovered", "run_success"], "the cancel between is looked past"


def _designer_run(graph: FlowGraph, registration_id: int) -> threading.Thread:
    """The designer's tracked run of ``graph``, on a claimed slot as the run route starts it."""
    graph.flow_settings.source_registration_id = registration_id
    assert graph.try_claim_run()
    thread = threading.Thread(target=_run_and_track, args=(graph, 1))
    thread.start()
    return thread


def _wait_until_running(graph: FlowGraph, node_id: int, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = graph.latest_run_info
        if info is not None and node_id in [result.node_id for result in info.node_step_result]:
            return
        time.sleep(0.1)
    raise AssertionError(f"node {node_id} never started")


def _node_rows(run: dict) -> dict[int, tuple[bool | None, bool]]:
    """Each reported node's ``(success, cancelled)``."""
    return {node["node_id"]: (node["success"], node["cancelled"]) for node in json.loads(run["node_results_json"])}


def _last_run(registration_id: int) -> dict:
    page = client.get("/catalog/runs", params={"registration_id": registration_id}).json()
    assert len(page["items"]) == 1, page
    return client.get(f"/catalog/runs/{page['items'][0]['id']}").json()


def test_a_run_cancelled_in_the_designer_reads_cancelled_with_its_stopped_node(tmp_path):
    graph = _graph(99474, BRIEF)
    flow_path = tmp_path / "designer.yaml"
    graph.save_flow(str(flow_path))
    registration_id = _register(flow_path, "designer_cancel")

    thread = _designer_run(graph, registration_id)
    _wait_until_running(graph, 2)
    time.sleep(1)
    graph.cancel()
    thread.join(timeout=120)
    assert not thread.is_alive()

    assert graph.latest_run_info.cancelled
    run = _last_run(registration_id)
    assert (run["run_type"], run["success"], run["cancelled"]) == ("in_designer_run", False, True)
    assert _node_rows(run) == {1: (True, False), 2: (None, True)}, "node 1 is done, node 2 stopped, node 3 has no row"
    flow = client.get(f"/catalog/flows/{registration_id}").json()
    assert (flow["last_run_success"], flow["last_run_cancelled"]) == (False, True)


def test_a_designer_run_that_failed_before_the_cancel_stays_failed(tmp_path):
    """One branch fails on its own, the other is still running when the cancel comes; the run stays failed."""
    graph = create_test_graph(99475, name="cancel_99475")
    add_test_manual_input(graph, [{"a": i} for i in range(4)])
    _add_polars_code(graph, 2, BROKEN, depends_on=1)
    _add_polars_code(graph, 3, BRIEF, depends_on=1)
    flow_path = tmp_path / "designer_failed.yaml"
    graph.save_flow(str(flow_path))
    registration_id = _register(flow_path, "designer_failed_then_cancelled")

    thread = _designer_run(graph, registration_id)
    _wait_until_running(graph, 3)
    deadline = time.monotonic() + 120
    while graph.latest_run_info.node_step_result[1].success is not False:
        assert time.monotonic() < deadline, "node 2 never failed"
        time.sleep(0.1)
    time.sleep(1)
    graph.cancel()
    thread.join(timeout=120)
    assert not thread.is_alive()

    assert not graph.latest_run_info.cancelled
    run = _last_run(registration_id)
    assert (run["success"], run["cancelled"]) == (False, False)
    assert _node_rows(run) == {1: (True, False), 2: (False, False), 3: (None, True)}


def test_a_designer_run_cancelled_on_the_worker_reads_cancelled(tmp_path, flowfile_worker):
    """The worker stops the node's job mid-run, long before it would end; the job's error is no failure."""
    graph = _graph(99476, SLOW, execution_location="remote")
    flow_path = tmp_path / "designer_worker.yaml"
    graph.save_flow(str(flow_path))
    registration_id = _register(flow_path, "designer_worker_cancel")

    thread = _designer_run(graph, registration_id)
    _wait_until_running(graph, 2)
    time.sleep(3)
    graph.cancel()
    thread.join(timeout=60)
    assert not thread.is_alive(), "the cancel did not stop the node's job"

    run = _last_run(registration_id)
    assert (run["success"], run["cancelled"]) == (False, True)
    assert _node_rows(run) == {1: (True, False), 2: (None, True)}
    assert json.loads(run["node_results_json"])[1]["error"] == ""


def test_a_run_cancelled_before_any_node_ran_reads_cancelled_not_success(tmp_path):
    graph = _graph(99477, BRIEF)
    flow_path = tmp_path / "designer_early.yaml"
    graph.save_flow(str(flow_path))
    registration_id = _register(flow_path, "designer_cancel_before_start")
    graph.flow_settings.source_registration_id = registration_id

    assert graph.try_claim_run()
    graph.cancel()
    _run_and_track(graph, 1)

    assert (graph.latest_run_info.success, graph.latest_run_info.cancelled) == (False, True)
    run = _last_run(registration_id)
    assert (run["success"], run["cancelled"], run["nodes_completed"]) == (False, True, 0)
    assert _node_rows(run) == {}


def test_a_cancelled_data_fetch_reads_cancelled(tmp_path):
    graph = _graph(99478, BRIEF)

    thread = threading.Thread(target=graph.trigger_fetch_node, args=(2,))
    thread.start()
    _wait_until_running(graph, 2)
    time.sleep(1)
    graph.cancel()
    thread.join(timeout=120)
    assert not thread.is_alive()

    info = graph.get_run_info()
    assert (info.run_type, info.success, info.cancelled) == ("fetch_one", False, True)
    assert [(node.node_id, node.success, node.cancelled) for node in info.node_step_result] == [(2, None, True)]


def _active_run(registration_id: int) -> int:
    """A manual run row of the flow that has not ended and has no process."""
    with get_db_context() as db:
        registration = db.get(db_models.FlowRegistration, registration_id)
        run = db_models.FlowRun(
            registration_id=registration.id,
            flow_uuid=registration.flow_uuid,
            flow_name=registration.name,
            flow_path=registration.flow_path,
            user_id=1,
            started_at=_now() - timedelta(seconds=5),
            run_type="manual",
        )
        db.add(run)
        db.commit()
        return run.id


def test_a_run_that_reports_its_end_after_a_cancel_keeps_its_outcome_and_alerts(tmp_path):
    """The cancel did not stop it, so its failure is a failure: no mark, and the rules see it."""
    registration_id = _register(tmp_path / "unused.yaml", "reported_after_cancel")
    _alert_on_everything(registration_id)
    run_id = _active_run(registration_id)
    assert client.post(f"/catalog/runs/{run_id}/cancel").status_code == 204
    assert _run_row(run_id).cancelled

    run_completion.complete_run(run_id, success=False, nodes_completed=1, number_of_nodes=3)
    process_pending_notifications()

    row = _run_row(run_id)
    assert (row.success, row.cancelled) == (False, False)
    assert _events(run_id) == ["run_failed"]


def test_a_cancel_recorded_while_a_run_reports_its_end_does_not_mark_it(tmp_path, monkeypatch):
    """The cancel commits between the completion's load of the row and its write; the reported failure stands."""
    registration_id = _register(tmp_path / "unused.yaml", "cancel_during_completion")
    _alert_on_everything(registration_id)
    run_id = _active_run(registration_id)
    close_run = run_completion._close_run

    def cancel_then_close(run, **kwargs):
        assert client.post(f"/catalog/runs/{run_id}/cancel").status_code == 204
        close_run(run, **kwargs)

    monkeypatch.setattr(run_completion, "_close_run", cancel_then_close)
    run_completion.complete_run(run_id, success=False, nodes_completed=1, number_of_nodes=3)
    process_pending_notifications()

    row = _run_row(run_id)
    assert (row.success, row.cancelled) == (False, False)
    assert _events(run_id) == ["run_failed"]


def test_a_run_cancelled_while_it_starts_is_stopped(tmp_path, run_children, monkeypatch):
    """The cancel lands before the run's pid is recorded, so it signals nothing; the start stops the child."""
    registration_id = _registered_flow(tmp_path, 99479, SLOW)
    spawn = FlowRunService._spawn_flow_subprocess

    def spawn_then_cancel(flow_path, run_id, **kwargs):
        pid = spawn(flow_path, run_id, **kwargs)
        assert client.post(f"/catalog/runs/{run_id}/cancel").status_code == 204
        return pid

    monkeypatch.setattr(FlowRunService, "_spawn_flow_subprocess", staticmethod(spawn_then_cancel))
    run_id = _start(registration_id)

    assert run_children[0].wait(timeout=60) != 0
    row = _run_row(run_id)
    assert (row.success, row.cancelled, row.pid) == (False, True, run_children[0].pid)

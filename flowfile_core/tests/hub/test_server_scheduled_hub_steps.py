"""On a server joined to its hub with keys of its own, a schedule whose flow places a step on one of the hub's servers
is refused when it is saved or enabled, and a scheduled run that reaches such a step anyway stops before any node
starts, with one sentence in its run log; a schedule whose steps all run on this server is unchanged.

A scheduled run has nobody behind it, and the hub mints a run's token for its servers only through a person's own
sign-in, so such a run could never start. The server joins a ``FakeJoinHub`` with keys under ``tmp_path`` (docker
mode); ``worker:team`` is a row filled from the hub that no test reaches, since every refusal comes before a node runs.
"""

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend, FlowRegistration, FlowRun, FlowSchedule
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from shared.storage_config import storage
from tests import test_hub_sign_in
from tests.conftest import protocol_states
from tests.flowfile.execution.graph_helpers import add_manual_input, add_sort_on, make_graph, ranks
from tests.test_hub_sign_in import NODE, bearer

node_file = test_hub_sign_in.node_file
hub = test_hub_sign_in.hub
protocol = protocol_states()
client = TestClient(main.app)

BACKEND = "worker:team"
REFUSED = (
    'Scheduled runs can\'t use the hub\'s servers yet, so run the steps "Manual input 1" and "Sort the ranks" on this '
    "server (set their Compute backend under General Settings)."
)


def _frozen_runner(flow_path: str, run_id: int) -> int:
    from flowfile_core.run_flow_cli import run_flow_cli

    return run_flow_cli(flow_path, run_id)


def _dev_runner(flow_path: str, run_id: int) -> int:
    from flowfile.__main__ import run_flow

    return run_flow(flow_path, run_id=run_id)


@pytest.fixture
def hub_row(monkeypatch):
    """The hub-filled row ``worker:team``, never reached; no ``FLOWFILE_COMPUTE_BACKEND`` for the headless runs."""
    monkeypatch.delenv("FLOWFILE_COMPUTE_BACKEND", raising=False)
    registry.reset()
    handshake.reset_handshake()
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=BACKEND, url="http://127.0.0.1:9", hub_node_id=5))
        db.commit()
    yield BACKEND
    registry.reset()
    handshake.reset_handshake()
    with get_db_context() as db:
        row = db.get(ConfiguredBackend, BACKEND)
        if row is not None:
            db.delete(row)
        db.commit()


@pytest.fixture
def person(hub, hub_row) -> tuple[int, str]:
    """A person signed in to this server through the hub: their account here and core's access token."""
    username = f"sam-{uuid.uuid4().hex[:8]}"
    signed_in = client.post("/auth/hub/signin", json={"token": hub.sign_in_token(NODE, uid=11, username=username)})
    assert signed_in.status_code == 200, signed_in.text
    access_token = signed_in.json()["access_token"]
    me = client.get("/auth/users/me", headers=bearer(access_token))
    assert me.status_code == 200, me.text
    return me.json()["id"], access_token


@pytest.fixture
def registered():
    """Saves a flow (manual input → sort, the sort described) in the server's flows folder on ``location`` and
    registers it for an owner; its file, schedules, runs and registration are removed afterwards."""
    made: list[tuple[int, Path]] = []

    def register(owner_id: int, location: str) -> FlowRegistration:
        graph = make_graph(abs(hash(uuid.uuid4().hex)) % 10**8, execution_location=location)
        add_manual_input(graph, 1, ranks(3))
        add_sort_on(graph, 2, 1, "rank", description="Sort the ranks")
        path = storage.flows_directory / f"scheduled_{uuid.uuid4().hex[:8]}.yaml"
        graph.save_flow(str(path))
        with get_db_context() as db:
            flow = FlowRegistration(name=path.stem, flow_path=str(path), owner_id=owner_id, flow_uuid=str(uuid.uuid4()))
            db.add(flow)
            db.commit()
            db.refresh(flow)
            db.expunge(flow)
        made.append((flow.id, path))
        return flow

    yield register
    with get_db_context() as db:
        for flow_id, path in made:
            db.query(FlowRun).filter(FlowRun.registration_id == flow_id).delete()
            db.query(FlowSchedule).filter(FlowSchedule.registration_id == flow_id).delete()
            db.query(FlowRegistration).filter(FlowRegistration.id == flow_id).delete()
            path.unlink(missing_ok=True)
        db.commit()


def interval_schedule(flow_id: int, enabled: bool = True) -> dict:
    return {"registration_id": flow_id, "schedule_type": "interval", "interval_seconds": 3600, "enabled": enabled}


def scheduled_run(flow: FlowRegistration, owner_id: int) -> int:
    """A schedule of ``flow`` written as an older core saved it, unchecked, and the run record its scheduler opens."""
    with get_db_context() as db:
        schedule = FlowSchedule(
            registration_id=flow.id, owner_id=owner_id, schedule_type="interval", interval_seconds=3600, enabled=True
        )
        db.add(schedule)
        db.commit()
        run = FlowRun(
            registration_id=flow.id,
            flow_uuid=flow.flow_uuid,
            flow_name=flow.name,
            flow_path=flow.flow_path,
            user_id=owner_id,
            started_at=datetime.now(timezone.utc),
            number_of_nodes=0,
            run_type="scheduled",
            schedule_id=schedule.id,
        )
        db.add(run)
        db.commit()
        return run.id


def ran(run_id: int) -> FlowRun:
    with get_db_context() as db:
        run = db.get(FlowRun, run_id)
        db.expunge(run)
        return run


def headless(runner, flow: FlowRegistration, run_id: int) -> int:
    previous = registry.default_override()
    try:
        return runner(flow.flow_path, run_id)
    finally:
        registry.set_default_override(previous)


def test_saving_a_schedule_whose_flow_runs_on_the_hubs_server_is_refused_with_one_sentence(person, registered):
    owner, token = person
    flow = registered(owner, BACKEND)

    created = client.post("/catalog/schedules", json=interval_schedule(flow.id), headers=bearer(token))

    assert created.status_code == 422, created.text
    assert created.json()["detail"] == REFUSED
    with get_db_context() as db:
        assert db.query(FlowSchedule).filter(FlowSchedule.registration_id == flow.id).count() == 0


def test_enabling_such_a_schedule_is_refused_and_it_stays_disabled(person, registered):
    owner, token = person
    flow = registered(owner, BACKEND)
    created = client.post("/catalog/schedules", json=interval_schedule(flow.id, enabled=False), headers=bearer(token))
    assert created.status_code == 201, created.text
    schedule_id = created.json()["id"]

    enabled = client.put(f"/catalog/schedules/{schedule_id}", json={"enabled": True}, headers=bearer(token))

    assert enabled.status_code == 422, enabled.text
    assert enabled.json()["detail"] == REFUSED
    assert client.get(f"/catalog/schedules/{schedule_id}", headers=bearer(token)).json()["enabled"] is False


def test_a_schedule_whose_steps_all_run_on_this_server_is_saved_and_enabled_as_before(person, registered):
    owner, token = person
    flow = registered(owner, "auto")

    created = client.post("/catalog/schedules", json=interval_schedule(flow.id, enabled=False), headers=bearer(token))
    assert created.status_code == 201, created.text
    enabled = client.put(f"/catalog/schedules/{created.json()['id']}", json={"enabled": True}, headers=bearer(token))

    assert enabled.status_code == 200, enabled.text
    assert enabled.json()["enabled"] is True


def test_a_server_that_never_joined_saves_the_schedule_as_before(node_file, hub_row, registered):
    flow = registered(1, BACKEND)

    with get_db_context() as db:
        schedule = CatalogService(SQLAlchemyCatalogRepository(db)).create_schedule(
            registration_id=flow.id, owner_id=1, schedule_type="interval", interval_seconds=3600
        )

    assert schedule.enabled is True


@pytest.mark.parametrize("runner", [_frozen_runner, _dev_runner], ids=["frozen-verb", "dev-cli"])
def test_a_scheduled_run_that_reaches_the_hubs_server_stops_before_any_node_starts_and_logs_the_sentence(
    protocol, person, registered, runner, capfd, caplog
):
    owner, _ = person
    run_id = scheduled_run(registered(owner, BACKEND), owner)

    exit_code = headless(runner, ran(run_id), run_id)

    log = capfd.readouterr().err + caplog.text
    run = ran(run_id)
    assert exit_code == 1
    assert run.success is False and run.nodes_completed == 0
    assert f"2 node(s) cannot run: {REFUSED.rstrip('.')}; no node was started" in log
    assert REFUSED in log
    assert "node 1" not in REFUSED and "manual_input" not in REFUSED
    assert "signed in" not in log
    assert registry.hub_server(BACKEND) is None


@pytest.mark.parametrize("runner", [_frozen_runner, _dev_runner], ids=["frozen-verb", "dev-cli"])
def test_a_scheduled_run_whose_steps_all_run_on_this_server_runs_as_before(
    protocol, person, registered, runner, capfd, caplog
):
    owner, _ = person
    run_id = scheduled_run(registered(owner, "auto"), owner)

    exit_code = headless(runner, ran(run_id), run_id)

    run = ran(run_id)
    assert exit_code == 0, capfd.readouterr().err + caplog.text
    assert run.success is True and run.nodes_completed == 2

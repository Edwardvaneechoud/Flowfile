"""A run's record for a run executed elsewhere: where it ran, where its log is (a 307 to the executor's log route
when this core can name it, else a 404 that says so) and the published version in place of a snapshot."""

from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import run_records
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.services.runs import FlowRunService
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRun


@pytest.fixture
def api() -> TestClient:
    c = TestClient(main.app)
    token = c.post("/auth/token").json()["access_token"]
    c.headers["Authorization"] = f"Bearer {token}"
    return c


@pytest.fixture
def record():
    """A completed record of a run executed on the server ``bmax`` under its run row 17, version 3 of its flow."""
    started = datetime.utcnow() - timedelta(minutes=2)
    with get_db_context() as db:
        row = FlowRunService(SQLAlchemyCatalogRepository(db)).create_completed_run(
            registration_id=None,
            flow_name="nightly",
            flow_path=None,
            user_id=1,
            started_at=started,
            ended_at=started + timedelta(seconds=4),
            success=True,
            nodes_completed=2,
            number_of_nodes=2,
            run_type="in_designer_run",
            flow_uuid="0b1c2d3e-0000-4000-8000-0000000000ee",
            executed_on="bmax",
            remote_run_id="run-abc",
            log_ref=run_records.log_ref("bmax", 17),
            flow_version=3,
        )
        run_id = row.id
    yield run_id
    with get_db_context() as db:
        db.query(FlowRun).filter_by(id=run_id).delete()
        db.commit()


@pytest.fixture
def hooks(monkeypatch):
    """What a core that keeps other machines' records sets: the executor's address and the version's document."""
    monkeypatch.setattr(
        run_records, "backend_core_url", lambda name: "http://bmax.example:63578" if name == "bmax" else None
    )
    monkeypatch.setattr(
        run_records, "version_document", lambda uuid, v: '{"flowfile_name": "nightly v3"}' if v == 3 else None
    )


def test_log_refs_round_trip():
    assert run_records.parse_log_ref(run_records.log_ref("bmax", 17)) == ("bmax", 17)
    assert run_records.parse_log_ref("backend:a.b-c:9") == ("a.b-c", 9)
    assert run_records.parse_log_ref("elsewhere") is None and run_records.parse_log_ref("backend:x:y") is None


def test_a_plain_core_names_no_other_machine(api, record):
    detail = api.get(f"/catalog/runs/{record}").json()
    assert (detail["executed_on"], detail["remote_run_id"], detail["flow_version"]) == ("bmax", "run-abc", 3)
    assert detail["log_ref"] == "backend:bmax:17" and detail["log_url"] is None
    assert detail["has_log"] is True and detail["has_snapshot"] is False and detail["flow_snapshot"] is None
    log = api.get(f"/catalog/runs/{record}/log", follow_redirects=False)
    assert log.status_code == 404 and log.json()["detail"] == run_records.LOG_ELSEWHERE.format(backend="bmax")


def test_a_core_that_knows_the_executor_redirects_to_its_log_and_serves_the_version(api, record, hooks):
    detail = api.get(f"/catalog/runs/{record}").json()
    assert detail["log_url"] == "http://bmax.example:63578/catalog/runs/17/log"
    assert detail["has_snapshot"] is True and detail["flow_snapshot"] == '{"flowfile_name": "nightly v3"}'
    log = api.get(f"/catalog/runs/{record}/log", follow_redirects=False)
    assert log.status_code == 307 and log.headers["location"] == "http://bmax.example:63578/catalog/runs/17/log"
    listed = api.get("/catalog/runs", params={"limit": 500}).json()["items"]
    [row] = [r for r in listed if r["id"] == record]
    assert (row["executed_on"], row["has_log"], row["has_snapshot"], row["flow_version"]) == ("bmax", True, True, 3)

"""Run-log survival across the core lifespan.

Pins the actual bug: shutdown used to wipe every ``*.log`` in the logs dir,
destroying the only record of a scheduled/manual run. Also pins that the
narrowed ``clear_all_flow_logs`` glob (and the ``/clear-logs`` route on top of
it) spares run logs, and that designer logging still works after a lifespan exit.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs.flow_logger import FlowLogger, clear_all_flow_logs, get_flow_log_file
from shared.run_logs import run_log_path
from shared.storage_config import storage


@pytest.fixture(autouse=True)
def logs_dir(tmp_path, monkeypatch):
    """Point the storage singleton at a throwaway root for the whole test."""
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    directory = storage.logs_directory
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _authed_client() -> TestClient:
    with TestClient(main.app) as client:
        token = client.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


def test_shutdown_does_not_delete_logs(logs_dir):
    """A full lifespan enter+exit must leave both log families intact.

    Shutdown used to call ``clear_all_flow_logs()`` over a ``*.log`` glob. Both
    halves are asserted: the flow-log arm catches a re-added shutdown call on its
    own, the run-log arm catches it combined with a re-widened glob.
    """
    run_log = run_log_path(424242)
    run_log.write_text("scheduled run output")
    flow_log = logs_dir / "flow_777.log"
    flow_log.write_text("designer output")

    with TestClient(main.app):
        pass

    assert run_log.exists()
    assert run_log.read_text() == "scheduled run output"
    assert flow_log.exists()
    assert flow_log.read_text() == "designer output"


def test_clear_all_flow_logs_spares_run_logs(logs_dir):
    flow_log = logs_dir / "flow_99.log"
    flow_log.write_text("designer output")
    run_log = run_log_path(99)
    run_log.write_text("scheduled run output")

    clear_all_flow_logs()

    assert not flow_log.exists()
    assert run_log.exists()
    assert run_log.read_text() == "scheduled run output"


def test_clear_logs_endpoint_spares_run_logs(logs_dir):
    flow_log = logs_dir / "flow_98.log"
    flow_log.write_text("designer output")
    run_log = run_log_path(98)
    run_log.write_text("scheduled run output")

    resp = _authed_client().post("/clear-logs")

    assert resp.status_code == 200
    assert not flow_log.exists()
    assert run_log.exists()


def test_designer_logging_survives_lifespan_exit():
    flow_id = 424243
    flow_logger = FlowLogger(flow_id)
    try:
        with TestClient(main.app):
            pass

        flow_logger.info("after lifespan exit")

        assert "after lifespan exit" in get_flow_log_file(flow_id).read_text()
    finally:
        FlowLogger.cleanup_instance(flow_id)

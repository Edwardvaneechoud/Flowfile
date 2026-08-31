"""End-to-end: the real client posting over HTTP to the real collector app.

No monkeypatched ``_post`` here — the whole chain runs for real: emit ->
queue -> httpx -> uvicorn -> collector validation -> events.jsonl. The dead
endpoint tests then prove the client stays silent and never retries when the
collector is unreachable.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import uvicorn
import yaml

from shared import telemetry
from tools.telemetry_collector import app as collector

pytestmark = pytest.mark.timeout(60)

WIRE_KEYS = {"event", "install_id", "app_version", "platform", "mode", "ts", "props"}


@pytest.fixture
def no_background(monkeypatch):
    """Pin delivery to flush() so what got dropped vs delivered is deterministic."""
    monkeypatch.setattr(telemetry, "_ensure_worker", lambda: None)


@pytest.fixture
def dead_endpoint() -> str:
    """A URL on a port nothing listens on (reserved with bind-0 then released)."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}/events"


@pytest.fixture
def live_collector(monkeypatch, tmp_path) -> str:
    """Serve the real collector app on a free local port, writing under tmp_path."""
    monkeypatch.setenv("TELEMETRY_DATA_DIR", str(tmp_path))

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    server = uvicorn.Server(uvicorn.Config(collector.app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started:
        if time.monotonic() > deadline:
            pytest.fail("in-process collector never became ready")
        time.sleep(0.05)

    try:
        yield f"http://127.0.0.1:{port}/events"
    finally:
        server.should_exit = True
        thread.join(timeout=15)


def _collected_events(events_file: Path, count: int, timeout: float = 10.0) -> list[dict[str, Any]]:
    """Poll events.jsonl until at least ``count`` lines landed (delivery is async)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if events_file.exists():
            lines = [json.loads(line) for line in events_file.read_text(encoding="utf-8").splitlines() if line]
            if len(lines) >= count:
                return lines
        time.sleep(0.05)
    pytest.fail(f"collector never received {count} event(s) at {events_file}")


def _stored_install_id(tmp_path: Path) -> str:
    return yaml.safe_load((tmp_path / "telemetry.yaml").read_text(encoding="utf-8"))["install_id"]


def _telemetry_records(caplog) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.name == "flowfile.telemetry"]


def test_real_events_land_in_the_collectors_jsonl(live_collector, tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", live_collector)
    telemetry.set_consent(True)

    telemetry.emit("app_started")
    telemetry.emit(
        "flow_run_succeeded",
        {
            "node_count_bucket": "4-7",
            "node_types": ["write_output", "filter", "read"],
            "duration_bucket": "1-10s",
            "used_sample_data": False,
        },
    )
    telemetry.emit("export_code_used", {"target": "polars"})
    telemetry.flush(timeout=10.0)

    landed = _collected_events(tmp_path / "events.jsonl", 3)
    assert len(landed) == 3
    assert sorted(entry["event"] for entry in landed) == ["app_started", "export_code_used", "flow_run_succeeded"]

    expected_id = _stored_install_id(tmp_path)
    assert expected_id == telemetry.install_id()
    for entry in landed:
        assert set(entry) == WIRE_KEYS | {"received_at"}, "envelope intact plus the collector's received_at"
        assert entry["install_id"] == expected_id
        assert isinstance(entry["app_version"], str) and entry["app_version"]
        assert entry["platform"] in collector.PLATFORMS
        assert entry["mode"] in collector.MODES
        assert entry["ts"].endswith("Z")
        assert collector._parse_ts(entry["ts"])
        assert collector._parse_ts(entry["received_at"])

    by_event = {entry["event"]: entry for entry in landed}
    assert by_event["app_started"]["props"] == {}
    assert by_event["flow_run_succeeded"]["props"] == {
        "node_count_bucket": "4-7",
        "node_types": ["filter", "read", "write_output"],
        "duration_bucket": "1-10s",
        "used_sample_data": False,
    }
    assert by_event["export_code_used"]["props"] == {"target": "polars"}


def test_a_dead_endpoint_is_swallowed_below_debug_noise(dead_endpoint, no_background, monkeypatch, caplog):
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", dead_endpoint)
    telemetry.set_consent(True)

    with caplog.at_level(logging.DEBUG, logger="flowfile.telemetry"):
        telemetry.emit("app_started")
        telemetry.flush(timeout=10.0)

    records = _telemetry_records(caplog)
    assert all(record.levelno <= logging.DEBUG for record in records), [
        (record.levelname, record.message) for record in records
    ]


def test_events_dropped_while_dead_never_resurface(live_collector, dead_endpoint, no_background, tmp_path, monkeypatch):
    """No retry queue by design: a batch that failed to send is gone for good."""
    events_file = tmp_path / "events.jsonl"
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", dead_endpoint)
    telemetry.set_consent(True)

    telemetry.emit("flow_created")
    telemetry.emit("flow_created")
    telemetry.flush(timeout=10.0)
    assert not events_file.exists(), "nothing can land while the endpoint is dead"

    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", live_collector)
    telemetry.emit("app_started")
    telemetry.flush(timeout=10.0)

    landed = _collected_events(events_file, 1)
    assert [entry["event"] for entry in landed] == ["app_started"], "the fresh emit arrives alone"

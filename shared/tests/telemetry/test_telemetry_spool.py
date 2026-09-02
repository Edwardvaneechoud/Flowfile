"""The on-disk spool: appending failed batches, draining them, and the two caps.

Delivery is at-least-once from here on, so the assertions that matter are about
``event_id``: nothing is lost when a drain fails halfway, and nothing that was
already accepted is sent twice.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from shared import telemetry


def _spool_path() -> Path:
    return telemetry._spool_file()


def _spooled() -> list[dict[str, Any]]:
    path = _spool_path()
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _envelope(event: str = "app_started", ts: datetime | None = None) -> dict[str, Any]:
    envelope = telemetry._envelope(event, {}, telemetry.install_id() or "install")
    if ts is not None:
        envelope["ts"] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    return envelope


def _write_spool(lines: list[str]) -> None:
    _spool_path().write_text("".join(line + "\n" for line in lines), encoding="utf-8")


@pytest.fixture
def no_background(monkeypatch):
    """Pin delivery to flush() so what is spooled vs delivered is deterministic."""
    monkeypatch.setattr(telemetry, "_ensure_worker", lambda: None)


def posts_post(recorder):
    """Rebuild the recorder's ``_post`` after a test swapped it out."""

    def _post(url: str, json: dict[str, Any], timeout: float | None = None):
        recorder.sent.append((url, json))
        recorder.timeouts.append(timeout)
        if recorder.raises is not None:
            raise recorder.raises
        return type("R", (), {"status_code": recorder.status})()

    return _post


class TestAppendOnFailure:
    def test_a_transient_failure_parks_the_batch(self, enabled, no_background, posts):
        posts.raises = ConnectionError("collector unreachable")
        telemetry.emit("flow_created")
        telemetry.flush()

        spooled = _spooled()
        assert [entry["event"] for entry in spooled] == ["flow_created"]
        assert spooled[0]["event_id"] == posts.events[0]["event_id"], "the parked copy is the one that failed"

    @pytest.mark.parametrize("status", [500, 502, 503, 429, 404])
    def test_a_failing_status_parks_the_batch(self, status, enabled, no_background, posts):
        posts.status = status
        telemetry.emit("app_started")
        telemetry.flush()
        assert len(_spooled()) == 1

    @pytest.mark.parametrize("status", telemetry.PERMANENT_STATUSES)
    def test_a_permanent_rejection_is_dropped_not_parked(self, status, enabled, no_background, posts):
        posts.status = status
        telemetry.emit("app_started")
        telemetry.flush()
        assert _spooled() == [], "resending bytes the collector refuses would spool forever"

    def test_a_delivered_batch_is_never_parked(self, enabled, no_background, posts):
        telemetry.emit("app_started")
        telemetry.flush()
        assert not _spool_path().exists()

    def test_an_unwritable_spool_degrades_to_the_pre_spool_behaviour(
        self, enabled, no_background, posts, caplog, monkeypatch, tmp_path
    ):
        blocked = tmp_path / "blocked"
        blocked.mkdir()  # a directory where the spool file should be: every write fails
        monkeypatch.setattr(telemetry, "_spool_file", lambda: blocked)
        posts.raises = ConnectionError("collector unreachable")

        with caplog.at_level(logging.DEBUG, logger="flowfile.telemetry"):
            telemetry.emit("app_started")
            telemetry.flush()
            telemetry._drain_spool()

        assert list(blocked.iterdir()) == [], "a failed append must not leave anything behind"
        records = [record for record in caplog.records if record.name == "flowfile.telemetry"]
        assert all(record.levelno <= logging.DEBUG for record in records)


class TestGatesGuardEverySpoolTouch:
    def test_the_kill_switch_short_circuits_before_any_spool_io(self, enabled, no_background, posts, monkeypatch):
        def _explode():
            raise AssertionError("the spool must not be touched once the kill switch is engaged")

        monkeypatch.setattr(telemetry, "_spool_file", _explode)
        monkeypatch.setenv("FLOWFILE_TELEMETRY", "0")
        posts.raises = ConnectionError("collector unreachable")

        telemetry.emit("app_started")
        telemetry.flush()
        telemetry._drain_spool()
        assert posts.sent == []

    def test_a_revoked_install_neither_appends_nor_drains(self, enabled, no_background, posts):
        posts.raises = ConnectionError("collector unreachable")
        telemetry.emit("app_started")
        telemetry.flush()
        assert len(_spooled()) == 1

        telemetry.set_consent(False)
        posts.raises = None
        posts.sent.clear()
        telemetry._drain_spool()
        assert posts.sent == [], "a revoked install must not deliver what it buffered under the old id"

    def test_revoking_consent_purges_the_spool(self, enabled, no_background, posts):
        posts.raises = ConnectionError("collector unreachable")
        telemetry.emit("app_started")
        telemetry.emit("flow_created")
        telemetry.flush()
        assert _spool_path().exists()

        telemetry.set_consent(False)
        assert not _spool_path().exists(), "buffered events carry the id that was just forgotten"


class TestDrain:
    def test_a_clean_drain_delivers_everything_and_removes_the_file(self, enabled, no_background, posts):
        posts.raises = ConnectionError("collector unreachable")
        for _ in range(3):
            telemetry.emit("app_started")
        telemetry.flush()
        parked = [entry["event_id"] for entry in _spooled()]
        assert len(parked) == 3

        posts.raises = None
        posts.sent.clear()
        telemetry._drain_spool()

        assert [event["event_id"] for event in posts.events] == parked, "oldest first, unchanged"
        assert not _spool_path().exists()

    def test_a_transient_failure_mid_drain_keeps_the_survivors(self, enabled, no_background, posts, monkeypatch):
        monkeypatch.setattr(telemetry, "BATCH_SIZE", 2)
        posts.raises = ConnectionError("collector unreachable")
        for _ in range(4):
            telemetry.emit("flow_created")
        telemetry.flush()
        parked = [entry["event_id"] for entry in _spooled()]
        assert len(parked) == 4

        calls: list[list[dict[str, Any]]] = []

        def _post(url: str, json: dict[str, Any], timeout: float | None = None):
            calls.append(json["events"])
            if len(calls) > 1:
                raise ConnectionError("collector died again")
            return type("R", (), {"status_code": 202})()

        monkeypatch.setattr(telemetry, "_post", _post)
        telemetry._drain_spool()

        assert [event["event_id"] for event in calls[0]] == parked[:2]
        assert [entry["event_id"] for entry in _spooled()] == parked[2:], "the failed batch stays, nothing else"

        posts.raises = None
        posts.sent.clear()
        monkeypatch.setattr(telemetry, "_post", posts_post(posts))
        telemetry._drain_spool()

        delivered = [event["event_id"] for event in calls[0]] + [event["event_id"] for event in posts.events]
        assert delivered == parked, "every event exactly once, in order"
        assert not _spool_path().exists()

    def test_batches_respect_both_caps(self, enabled, no_background, posts, monkeypatch):
        monkeypatch.setattr(telemetry, "SPOOL_BATCH_BYTES", 900)
        posts.raises = ConnectionError("collector unreachable")
        for _ in range(12):
            telemetry.emit("app_started")
        telemetry.flush()

        posts.raises = None
        posts.sent.clear()
        telemetry._drain_spool()

        sizes = [len(body["events"]) for _, body in posts.sent]
        assert len(sizes) > 1, f"a 900-byte body cap must split 12 envelopes: {sizes}"
        assert sum(sizes) == 12
        for _, body in posts.sent:
            assert len(json.dumps(body["events"], separators=(",", ":"))) <= 900 + 300

    def test_events_older_than_the_age_cap_are_discarded(self, enabled, no_background, posts):
        stale = _envelope("app_started", datetime.now(timezone.utc) - timedelta(days=31))
        fresh = _envelope("flow_created")
        _write_spool([json.dumps(stale, separators=(",", ":")), json.dumps(fresh, separators=(",", ":"))])

        telemetry._drain_spool()

        assert [event["event_id"] for event in posts.events] == [fresh["event_id"]]
        assert not _spool_path().exists(), "the expired line is dropped with the delivered one"

    def test_corrupt_lines_are_skipped_and_dropped(self, enabled, no_background, posts):
        good = _envelope("flow_created")
        _write_spool(
            [
                "{not json at all",
                "[]",
                json.dumps(good, separators=(",", ":")),
                '{"event": "app_started"',
            ]
        )

        telemetry._drain_spool()

        assert [event["event_id"] for event in posts.events] == [good["event_id"]]
        assert not _spool_path().exists()

    def test_an_unreadable_spool_is_a_silent_no_op(self, enabled, no_background, posts, monkeypatch, tmp_path):
        blocked = tmp_path / "blocked"
        blocked.mkdir()
        monkeypatch.setattr(telemetry, "_spool_file", lambda: blocked)

        telemetry._drain_spool()
        assert posts.sent == []

    def test_the_daemon_drains_before_the_live_queue(self, enabled, posts, monkeypatch):
        posts.raises = ConnectionError("collector unreachable")
        telemetry.emit("app_started")
        telemetry.flush(timeout=5.0)
        parked = [entry["event_id"] for entry in _spooled()]
        assert len(parked) == 1

        telemetry._reset_for_tests()  # a fresh process: new queue, new daemon
        posts.raises = None
        posts.sent.clear()
        telemetry.emit("flow_created")

        deadline = time.monotonic() + 10.0
        while len(posts.events) < 2 and time.monotonic() < deadline:
            time.sleep(0.02)

        assert [event["event"] for event in posts.events] == ["app_started", "flow_created"]
        assert posts.events[0]["event_id"] == parked[0]


class TestCaps:
    def test_the_size_cap_drops_the_oldest_lines(self, enabled, no_background, posts, monkeypatch):
        monkeypatch.setattr(telemetry, "SPOOL_MAX_BYTES", 1500)
        posts.raises = ConnectionError("collector unreachable")
        seen: list[str] = []
        for _ in range(30):
            telemetry.emit("app_started")
            telemetry.flush()
            seen.extend(entry["event_id"] for entry in _spooled()[-1:])

        spooled = [entry["event_id"] for entry in _spooled()]
        assert _spool_path().stat().st_size <= 1500
        assert spooled, "the cap must keep the newest events, not empty the file"
        assert spooled == seen[-len(spooled) :], "FIFO: the oldest went first"

    def test_compaction_leaves_no_temp_file(self, enabled, no_background, posts, monkeypatch):
        monkeypatch.setattr(telemetry, "SPOOL_MAX_BYTES", 800)
        posts.raises = ConnectionError("collector unreachable")
        for _ in range(20):
            telemetry.emit("app_started")
            telemetry.flush()

        leftovers = list(_spool_path().parent.glob("*.tmp"))
        assert leftovers == [], f"the atomic rewrite leaked a temp file: {leftovers}"

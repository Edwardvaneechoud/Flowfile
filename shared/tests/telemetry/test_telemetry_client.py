"""Wire format, payload validation, queue/batching behaviour, and the poison scan."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any

import pytest

from shared import telemetry

ENDPOINT = "https://collector.example.invalid/events"
WIRE_KEYS = {"event", "event_id", "install_id", "app_version", "platform", "mode", "ts", "props"}

MAXIMAL_PROPS: dict[str, dict[str, Any]] = {
    "flow_run_succeeded": {
        "node_count_bucket": "4-7",
        "node_types": ["write_output", "filter", "read"],
        "duration_bucket": "1-10s",
        "used_sample_data": False,
    },
    "flow_run_failed": {"error_class": "ColumnNotFoundError"},
    "export_code_used": {"target": "polars"},
}

POISON = ("/Users/", "C:\\", "SELECT ", "secret", ".csv", "quarterly_revenue_2026")

POISON_PROPS: dict[str, Any] = {
    "flow_name": "quarterly_revenue_2026",
    "node_name": "join customers",
    "file_path": "/Users/someone/data/orders.csv",
    "windows_path": "C:\\data\\orders.csv",
    "sql": "SELECT * FROM customers",
    "api_key": "secret-token-abc",
    "columns": ["customer_email", "iban"],
    "error": "boom: /Users/someone/flow.py",
}


@pytest.fixture
def no_background(monkeypatch):
    """Pin delivery to flush() so batch counts are deterministic."""
    monkeypatch.setattr(telemetry, "_ensure_worker", lambda: None)


def _sent_event(posts, index: int = 0) -> dict[str, Any]:
    return posts.events[index]


def _walk_strings(value: Any):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _walk_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_strings(item)
    elif isinstance(value, str):
        yield value


class TestEnvelope:
    def test_envelope_has_exactly_the_eight_wire_keys(self, enabled, no_background, posts):
        telemetry.emit("flow_run_succeeded", MAXIMAL_PROPS["flow_run_succeeded"])
        telemetry.flush()

        url, body = posts.sent[0]
        assert url == ENDPOINT, "the configured endpoint is posted to verbatim"
        assert set(body) == {"events"}

        event = _sent_event(posts)
        assert set(event) == WIRE_KEYS
        assert event["event"] == "flow_run_succeeded"
        assert uuid.UUID(event["event_id"]).version == 4
        assert uuid.UUID(event["install_id"]) is not None
        assert event["install_id"] == telemetry.install_id()
        assert isinstance(event["app_version"], str) and event["app_version"]
        assert event["platform"] in {"darwin", "linux", "windows", "other"}
        assert event["mode"] in {"electron", "docker", "package", "other"}
        assert event["ts"].endswith("Z") and event["ts"][4] == "-" and event["ts"][10] == "T"

    def test_props_survive_intact_when_valid(self, enabled, no_background, posts):
        telemetry.emit("flow_run_succeeded", MAXIMAL_PROPS["flow_run_succeeded"])
        telemetry.flush()
        assert _sent_event(posts)["props"] == {
            "node_count_bucket": "4-7",
            "node_types": ["filter", "read", "write_output"],
            "duration_bucket": "1-10s",
            "used_sample_data": False,
        }

    @pytest.mark.parametrize(
        ("value", "expected"),
        [("docker", "docker"), ("package", "package"), ("electron", "electron"), ("kubernetes", "other")],
    )
    def test_mode_is_allowlisted(self, value, expected, enabled, no_background, posts, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MODE", value)
        telemetry.emit("app_started")
        telemetry.flush()
        assert _sent_event(posts)["mode"] == expected

    def test_mode_defaults_to_electron(self, enabled, no_background, posts, monkeypatch):
        monkeypatch.delenv("FLOWFILE_MODE", raising=False)
        telemetry.emit("app_started")
        telemetry.flush()
        assert _sent_event(posts)["mode"] == "electron"


class TestValidation:
    @pytest.mark.parametrize("event", ["", "app_launched", "flow_run", "drop_table", "APP_STARTED"])
    def test_unknown_event_names_are_dropped(self, event, enabled, no_background, posts):
        telemetry.emit(event, {})
        telemetry.flush()
        assert posts.sent == []

    def test_every_frozen_event_is_accepted(self, enabled, no_background, posts):
        for event in telemetry.EVENTS:
            telemetry.emit(event, MAXIMAL_PROPS.get(event))
        telemetry.flush()
        assert [event["event"] for event in posts.events] == list(telemetry.EVENTS)

    def test_prop_keys_outside_the_allowlist_are_stripped(self, enabled, no_background, posts):
        telemetry.emit("flow_run_succeeded", {**MAXIMAL_PROPS["flow_run_succeeded"], **POISON_PROPS})
        telemetry.flush()
        assert set(_sent_event(posts)["props"]) == {
            "node_count_bucket",
            "node_types",
            "duration_bucket",
            "used_sample_data",
        }

    def test_events_with_no_allowed_props_send_an_empty_props_object(self, enabled, no_background, posts):
        telemetry.emit("app_started", POISON_PROPS)
        telemetry.flush()
        assert _sent_event(posts)["props"] == {}

    @pytest.mark.parametrize(
        ("event", "props"),
        [
            ("flow_run_failed", {"error_class": "boom: /Users/x/file.py"}),
            ("flow_run_failed", {"error_class": "x" * 70}),
            ("flow_run_failed", {"error_class": 42}),
            ("flow_run_failed", {"error_class": "column 'email' not found"}),
            ("flow_run_succeeded", {"node_types": ["my col"]}),
            ("flow_run_succeeded", {"node_types": ["read", "y" * 70]}),
            ("flow_run_succeeded", {"node_types": "read"}),
            ("flow_run_succeeded", {"node_types": [None]}),
            ("flow_run_succeeded", {"used_sample_data": "yes"}),
            ("flow_run_succeeded", {"used_sample_data": 1}),
            ("flow_run_succeeded", {"node_count_bucket": "4"}),
            ("flow_run_succeeded", {"duration_bucket": "3 seconds"}),
            ("export_code_used", {"target": "excel"}),
            ("export_code_used", {"target": None}),
        ],
    )
    def test_invalid_prop_values_are_dropped(self, event, props, enabled, no_background, posts):
        telemetry.emit(event, props)
        telemetry.flush()
        assert _sent_event(posts)["props"] == {}

    def test_node_types_are_sorted_and_capped(self, enabled, no_background, posts):
        names = [f"node_{index:03d}" for index in range(80)]
        telemetry.emit("flow_run_succeeded", {"node_types": list(reversed(names))})
        telemetry.flush()
        sent = _sent_event(posts)["props"]["node_types"]
        assert sent == names[: telemetry.MAX_NODE_TYPES]
        assert len(sent) == 60

    @pytest.mark.parametrize("target", telemetry.EXPORT_TARGETS)
    def test_every_frozen_export_target_is_accepted(self, target, enabled, no_background, posts):
        telemetry.emit("export_code_used", {"target": target})
        telemetry.flush()
        assert _sent_event(posts)["props"] == {"target": target}


class TestDelivery:
    def test_a_full_queue_drops_without_raising_or_blocking(self, enabled, no_background, posts):
        for _ in range(telemetry.QUEUE_MAXSIZE):
            telemetry.emit("app_started")
        assert telemetry._queue.qsize() == telemetry.QUEUE_MAXSIZE

        started = time.monotonic()
        for _ in range(50):
            telemetry.emit("app_started")
        assert time.monotonic() - started < 1.0, "emit must never block on a full queue"
        assert telemetry._queue.qsize() == telemetry.QUEUE_MAXSIZE, "overflow is dropped, not buffered"

        telemetry.flush(timeout=10.0)
        assert len(posts.events) == telemetry.QUEUE_MAXSIZE

    def test_batches_never_exceed_the_cap(self, enabled, no_background, posts):
        for _ in range(150):
            telemetry.emit("app_started")
        telemetry.flush(timeout=10.0)

        sizes = [len(body["events"]) for _, body in posts.sent]
        assert all(size <= telemetry.BATCH_SIZE for size in sizes), sizes
        assert sizes == [100, 50]
        assert len(posts.events) == 150

    def test_the_background_thread_delivers_without_an_explicit_flush(self, enabled, posts):
        telemetry.emit("app_started")

        deadline = time.monotonic() + 10.0
        while not posts.sent and time.monotonic() < deadline:
            time.sleep(0.02)

        assert posts.sent, "the telemetry-flush thread must drain the queue on its own"
        assert telemetry._thread is not None
        assert telemetry._thread.name == "telemetry-flush"
        assert telemetry._thread.daemon is True

    def test_flush_on_an_untouched_module_is_a_no_op(self, posts):
        telemetry.flush()
        assert posts.sent == []

    def test_a_plain_send_keeps_the_standard_request_timeout(self, enabled, no_background, posts):
        telemetry._send([{"event": "app_started"}])
        assert posts.timeouts == [None], "background delivery has no deadline to squeeze into"

    def test_flush_caps_each_request_by_its_remaining_budget(self, enabled, no_background, posts):
        for _ in range(150):
            telemetry.emit("app_started")
        telemetry.flush(timeout=0.5)

        assert posts.timeouts, "flush must still deliver"
        assert all(sent is not None and 0 < sent <= 0.5 for sent in posts.timeouts), posts.timeouts

    def test_flush_with_no_budget_left_sends_nothing(self, enabled, no_background, posts):
        telemetry.emit("app_started")
        telemetry.flush(timeout=0.0)
        assert posts.sent == [], "an exhausted budget must not start a request"
        assert telemetry._queue.qsize() == 1

    def test_every_event_gets_its_own_event_id(self, enabled, no_background, posts):
        for _ in range(3):
            telemetry.emit("flow_created")
        telemetry.flush()

        ids = [event["event_id"] for event in posts.events]
        assert len(set(ids)) == 3, "same install, same second, same props — only the id separates them"

    def test_emit_once_dedupes_per_process(self, enabled, no_background, posts):
        for _ in range(5):
            telemetry.emit_once("app_started")
        telemetry.emit_once("export_code_used", {"target": "polars"})
        telemetry.emit_once("export_code_used", {"target": "polars"})
        telemetry.emit_once("export_code_used", {"target": "project_zip"})
        telemetry.flush()

        assert [(event["event"], event["props"]) for event in posts.events] == [
            ("app_started", {}),
            ("export_code_used", {"target": "polars"}),
            ("export_code_used", {"target": "project_zip"}),
        ]

    def test_emit_once_stays_armed_while_telemetry_is_disabled(self, no_background, posts, monkeypatch):
        telemetry.emit_once("app_started")
        assert posts.sent == []

        monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", ENDPOINT)
        telemetry.set_consent(True)
        telemetry.emit_once("app_started")
        telemetry.flush()
        assert len(posts.events) == 1, "a dropped emit must not burn the once-per-process slot"


class TestFlushWaitsForTheBackgroundThread:
    """The real daemon thread runs here: ``_ensure_worker`` is deliberately not stubbed.

    ``_enqueue`` wakes the daemon immediately, so by the time a short-lived
    process calls ``flush`` the terminal batch is usually already out of the
    queue and in flight. A flush that only drains the queue returns instantly
    and the interpreter kills the POST on the way out.
    """

    @staticmethod
    def _wait_until_taken(deadline: float = 5.0) -> None:
        limit = time.monotonic() + deadline
        while telemetry._queue is not None and telemetry._queue.qsize() and time.monotonic() < limit:
            time.sleep(0.005)
        assert telemetry._queue is not None and telemetry._queue.qsize() == 0, "the daemon never took the batch"

    def test_flush_waits_for_a_send_the_daemon_already_took(self, enabled, monkeypatch):
        senders: list[str] = []
        completed = threading.Event()

        def _slow_post(url: str, json: dict[str, Any], timeout: float | None = None) -> None:
            senders.append(threading.current_thread().name)
            time.sleep(0.25)
            completed.set()

        monkeypatch.setattr(telemetry, "_post", _slow_post)
        telemetry.emit("app_started")
        time.sleep(0.01)
        self._wait_until_taken()

        started = time.monotonic()
        telemetry.flush(2.0)
        elapsed = time.monotonic() - started

        assert completed.is_set(), "flush returned while the daemon's POST was still in flight"
        assert senders == ["telemetry-flush"], senders
        assert elapsed < 2.0, "flush must not burn its whole budget on a send that finished"

    def test_flush_does_not_wait_past_its_budget_for_a_stalled_send(self, enabled, monkeypatch):
        entered = threading.Event()
        release = threading.Event()

        def _stalled_post(url: str, json: dict[str, Any], timeout: float | None = None) -> None:
            entered.set()
            release.wait(10.0)

        monkeypatch.setattr(telemetry, "_post", _stalled_post)
        telemetry.emit("app_started")
        assert entered.wait(5.0), "the daemon never started the send"

        started = time.monotonic()
        try:
            telemetry.flush(0.3)
            elapsed = time.monotonic() - started
        finally:
            release.set()

        assert elapsed >= 0.2, "flush must wait for the in-flight send"
        assert elapsed < 1.5, f"a stalled collector must not extend flush beyond its budget: {elapsed}"


class TestFailuresAreSilent:
    def _records(self, caplog):
        return [record for record in caplog.records if record.name == "flowfile.telemetry"]

    def test_a_rejecting_collector_is_ignored(self, enabled, no_background, posts, caplog):
        posts.status = 500
        with caplog.at_level(logging.DEBUG, logger="flowfile.telemetry"):
            telemetry.emit("app_started")
            telemetry.flush()

        assert len(posts.sent) == 1
        assert self._records(caplog) == [], "an HTTP failure is not even worth a debug line"

    def test_a_transport_failure_is_swallowed_at_debug(self, enabled, no_background, posts, caplog):
        posts.raises = ConnectionError("collector unreachable")
        with caplog.at_level(logging.DEBUG, logger="flowfile.telemetry"):
            telemetry.emit("app_started")
            telemetry.flush()

        records = self._records(caplog)
        assert records, "the failure should still be debuggable"
        assert all(record.levelno == logging.DEBUG for record in records), [
            (record.levelname, record.message) for record in records
        ]

    def test_a_broken_envelope_never_reaches_the_caller(self, enabled, no_background, posts, caplog, monkeypatch):
        def _boom(*args, **kwargs):
            raise RuntimeError("envelope construction blew up")

        monkeypatch.setattr(telemetry, "_envelope", _boom)
        with caplog.at_level(logging.DEBUG, logger="flowfile.telemetry"):
            telemetry.emit("app_started")
            telemetry.emit_once("flow_created")

        assert posts.sent == []
        assert all(record.levelno == logging.DEBUG for record in self._records(caplog))


class TestPoisonScan:
    def test_no_user_content_can_reach_the_wire(self, enabled, no_background, posts):
        """Emit every frozen event with maximal legit props plus a pile of poison.

        The allowlist is what makes this structurally impossible, so a regression
        that widens it (a passthrough prop, a raw error message, a flow name)
        fails here rather than in production.
        """
        for event in telemetry.EVENTS:
            telemetry.emit(event, {**MAXIMAL_PROPS.get(event, {}), **POISON_PROPS})
        telemetry.flush(timeout=10.0)

        assert len(posts.events) == len(telemetry.EVENTS)
        for _, body in posts.sent:
            for text in _walk_strings(body):
                for needle in POISON:
                    assert needle not in text, f"{needle!r} leaked into the payload via {text!r}"

    def test_only_declared_values_appear_in_props(self, enabled, no_background, posts):
        allowed_values = (
            set(telemetry.NODE_COUNT_BUCKETS)
            | set(telemetry.DURATION_BUCKETS)
            | set(telemetry.EXPORT_TARGETS)
            | {True, False}
        )
        for event in telemetry.EVENTS:
            telemetry.emit(event, {**MAXIMAL_PROPS.get(event, {}), **POISON_PROPS})
        telemetry.flush(timeout=10.0)

        for event in posts.events:
            assert set(event["props"]) <= set(telemetry.EVENTS[event["event"]])
            for key, value in event["props"].items():
                if key == "node_types":
                    assert all(name.isidentifier() for name in value)
                elif key == "error_class":
                    assert value.isidentifier()
                else:
                    assert value in allowed_values, f"{key}={value!r} is not a declared value"


class _Responds:
    """Stand-in for the collector's 2xx response, with whatever body the test needs."""

    status_code = 202
    text = ""

    def __init__(self, body: Any):
        self._body = body

    def json(self) -> Any:
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class TestCollectorResponseBody:
    """Reading the collector's 2xx body must never break delivery, whatever it contains."""

    def _records(self, caplog):
        return [record for record in caplog.records if record.name == "flowfile.telemetry"]

    def _send_against(self, monkeypatch, response: Any) -> str:
        monkeypatch.setattr(telemetry, "_post", lambda url, json, timeout=None: response)
        return telemetry._send([{"event": "app_started"}], spool_on_failure=False)

    def test_a_rejected_count_is_warned_about(self, enabled, no_background, monkeypatch, caplog):
        with caplog.at_level(logging.DEBUG, logger="flowfile.telemetry"):
            outcome = self._send_against(monkeypatch, _Responds({"accepted": 0, "rejected": 1}))

        assert outcome == telemetry.SEND_OK
        warnings = [record for record in self._records(caplog) if record.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "1" in warnings[0].getMessage()

    @pytest.mark.parametrize(
        "make_response",
        [
            pytest.param(lambda: type("_NoJson", (), {"status_code": 202, "text": "ok"})(), id="no_json_method"),
            pytest.param(lambda: _Responds(ValueError("not json")), id="body_is_not_json"),
            pytest.param(lambda: _Responds("<html>"), id="body_is_a_string"),
            pytest.param(lambda: _Responds({"accepted": 1}), id="no_rejected_key"),
            pytest.param(lambda: _Responds({"rejected": "some"}), id="rejected_is_not_a_number"),
            pytest.param(lambda: _Responds({"accepted": 1, "rejected": 0}), id="nothing_rejected"),
        ],
    )
    def test_an_unreadable_body_changes_nothing(self, make_response, enabled, no_background, monkeypatch, caplog):
        with caplog.at_level(logging.DEBUG, logger="flowfile.telemetry"):
            outcome = self._send_against(monkeypatch, make_response())

        assert outcome == telemetry.SEND_OK
        assert [record for record in self._records(caplog) if record.levelno >= logging.WARNING] == []

"""Client/collector schema parity.

The collector deliberately duplicates the frozen event schema so it can deploy
from its own directory without importing the monorepo. That duplication is only
safe while these tests hold: the two sides must agree on event names, prop
allowlists, value sets, and caps — and everything the client can build must
pass the collector's validator.
"""

from __future__ import annotations

import uuid
from typing import Any

from shared import telemetry
from tools.telemetry_collector import app as collector

EXAMPLE_EVENT: dict[str, Any] = {
    "event": "flow_run_succeeded",
    "event_id": "b7a1d9c4-3e52-4f18-9a6b-0c5d2e7f8a13",
    "install_id": "3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1",
    "app_version": "0.12.7",
    "platform": "darwin",
    "mode": "electron",
    "ts": "2026-08-29T12:00:00Z",
    "props": {
        "node_count_bucket": "4-7",
        "node_types": ["filter", "output", "read"],
        "duration_bucket": "1-10s",
        "used_sample_data": False,
    },
}

MAXIMAL_PROPS: dict[str, dict[str, Any]] = {
    "flow_run_succeeded": {
        "node_count_bucket": "4-7",
        "node_types": ["filter", "output", "read"],
        "duration_bucket": "1-10s",
        "used_sample_data": False,
    },
    "flow_run_failed": {"error_class": "ColumnNotFoundError"},
    "export_code_used": {"target": "polars"},
}


def test_maximal_props_cover_every_allowlisted_prop():
    """``MAXIMAL_PROPS`` is what the envelope test exercises, so a gap here silently opts an event out."""
    for event, keys in telemetry.EVENTS.items():
        assert set(MAXIMAL_PROPS.get(event, {})) == set(keys), event


def test_event_name_sets_are_identical():
    assert set(telemetry.EVENTS) == set(collector.ALLOWED_EVENTS)
    assert set(collector.EVENT_PROPS) == set(collector.ALLOWED_EVENTS)


def test_per_event_prop_allowlists_are_identical():
    for event in set(telemetry.EVENTS) | set(collector.EVENT_PROPS):
        assert telemetry.EVENTS[event] == collector.EVENT_PROPS[event], event


def test_collector_value_sets_cover_the_client_frozen_sets():
    assert set(telemetry.NODE_COUNT_BUCKETS) <= collector.NODE_COUNT_BUCKETS
    assert set(telemetry.DURATION_BUCKETS) <= collector.DURATION_BUCKETS
    assert set(telemetry.EXPORT_TARGETS) <= collector.EXPORT_TARGETS


def test_bucket_functions_only_produce_collector_accepted_values():
    for count in [-5, 0, 1, 3, 4, 7, 8, 15, 16, 30, 31, 100, 10_000]:
        assert collector._valid_prop("node_count_bucket", telemetry.bucket_node_count(count))
    for seconds in [0, 0.5, 0.999, 1, 9.999, 10, 59.999, 60, 299.999, 300, 1799.999, 1800, 86_400]:
        assert collector._valid_prop("duration_bucket", telemetry.bucket_duration_seconds(seconds))
    for target in telemetry.EXPORT_TARGETS:
        assert collector._valid_prop("target", target)


def test_client_platform_and_mode_values_are_accepted():
    assert {"darwin", "linux", "windows", "other"} <= collector.PLATFORMS
    assert set(telemetry.KNOWN_MODES) | {"other"} <= collector.MODES


def test_size_caps_agree():
    assert telemetry.MAX_IDENTIFIER_LENGTH == collector.MAX_STRING_LEN
    assert telemetry.MAX_NODE_TYPES == collector.MAX_NODE_TYPES
    assert telemetry.BATCH_SIZE <= collector.MAX_BATCH_SIZE, "a full client batch must fit in one collector request"


def test_canonical_example_event_passes_the_collector_validator():
    cleaned = collector._validate_event(EXAMPLE_EVENT)
    assert cleaned is not None, "the documented example event must be accepted"
    assert cleaned == EXAMPLE_EVENT


def test_event_id_is_optional_and_validated_as_a_uuid():
    without = {key: value for key, value in EXAMPLE_EVENT.items() if key != "event_id"}
    cleaned = collector._validate_event(without)
    assert cleaned == without, "a pre-spool client sends no event_id and must still be accepted"

    for bad in ["not-a-uuid", "", 42, "x" * 70]:
        assert collector._validate_event({**EXAMPLE_EVENT, "event_id": bad}) is None, bad


def test_every_client_built_envelope_validates_on_the_collector():
    identifier = str(uuid.uuid4())
    for event in telemetry.EVENTS:
        envelope = telemetry._envelope(event, MAXIMAL_PROPS.get(event, {}), identifier)
        assert set(envelope["props"]) == telemetry.EVENTS[event], f"{event}: a prop the client cannot build"
        cleaned = collector._validate_event(envelope)
        assert cleaned is not None, f"collector rejected a client-built {event} envelope: {envelope}"
        assert cleaned == envelope

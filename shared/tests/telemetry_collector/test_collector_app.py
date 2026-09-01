import json
import uuid

import pytest


def make_event(event="app_started", **overrides):
    base = {
        "event": event,
        "install_id": str(uuid.uuid4()),
        "app_version": "0.12.7",
        "platform": "darwin",
        "mode": "electron",
        "ts": "2026-08-29T12:00:00Z",
        "props": {},
    }
    base.update(overrides)
    return base


def read_lines(data_dir):
    path = data_dir / "events.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_valid_batch_accepted_and_stored_in_order(client, data_dir):
    events = [
        make_event("app_started"),
        make_event(
            "flow_run_succeeded",
            props={
                "node_count_bucket": "4-7",
                "node_types": ["filter", "read", "write_output"],
                "duration_bucket": "1-10s",
                "used_sample_data": False,
            },
        ),
        make_event("export_code_used", props={"target": "polars"}),
    ]
    response = client.post("/events", json={"events": events})
    assert response.status_code == 202
    assert response.json() == {"accepted": 3, "rejected": 0}

    lines = read_lines(data_dir)
    assert [line["event"] for line in lines] == ["app_started", "flow_run_succeeded", "export_code_used"]
    for sent, stored in zip(events, lines):
        assert stored["install_id"] == sent["install_id"]
        assert stored["props"] == sent["props"]
        assert stored["received_at"]


def test_mixed_batch_skips_invalid_and_stores_valid(client, data_dir):
    events = [
        make_event("flow_created"),
        make_event("not_a_real_event"),
        make_event("app_started", install_id="not-a-uuid"),
        make_event("kernel_used"),
    ]
    response = client.post("/events", json={"events": events})
    assert response.status_code == 202
    assert response.json() == {"accepted": 2, "rejected": 2}
    assert [line["event"] for line in read_lines(data_dir)] == ["flow_created", "kernel_used"]


def test_not_json_body_is_422(client, data_dir):
    response = client.post("/events", content=b"this is not json", headers={"Content-Type": "application/json"})
    assert response.status_code == 422


def test_missing_events_key_is_422(client, data_dir):
    response = client.post("/events", json={"payload": []})
    assert response.status_code == 422


def test_101_events_is_422(client, data_dir):
    response = client.post("/events", json={"events": [make_event() for _ in range(101)]})
    assert response.status_code == 422


def test_oversized_body_is_413(client, data_dir):
    response = client.post("/events", content=b"x" * (256 * 1024 + 1), headers={"Content-Type": "application/json"})
    assert response.status_code == 413


def test_oversized_body_with_understated_content_length_is_413(client, data_dir):
    def chunks():
        for _ in range(9):
            yield b"x" * (32 * 1024)

    response = client.post(
        "/events",
        content=chunks(),
        headers={"Content-Type": "application/json", "Content-Length": "16"},
    )
    assert response.status_code == 413
    assert read_lines(data_dir) == []


def test_unhashable_platform_rejects_only_that_event(client, data_dir):
    events = [make_event("flow_created"), make_event("app_started", platform=[]), make_event("kernel_used")]
    response = client.post("/events", json={"events": events})
    assert response.status_code == 202
    assert response.json() == {"accepted": 2, "rejected": 1}
    assert [line["event"] for line in read_lines(data_dir)] == ["flow_created", "kernel_used"]


def test_unhashable_prop_values_reject_only_those_events(client, data_dir):
    events = [
        make_event("flow_created"),
        make_event("flow_run_succeeded", props={"node_count_bucket": {}}),
        make_event("flow_run_succeeded", props={"duration_bucket": []}),
        make_event("export_code_used", props={"target": {"a": 1}}),
        make_event("kernel_used"),
    ]
    response = client.post("/events", json={"events": events})
    assert response.status_code == 202
    assert response.json() == {"accepted": 2, "rejected": 3}
    assert [line["event"] for line in read_lines(data_dir)] == ["flow_created", "kernel_used"]


def test_unhashable_mode_is_rejected(client, data_dir):
    response = client.post("/events", json={"events": [make_event("app_started", mode={})]})
    assert response.status_code == 202
    assert response.json() == {"accepted": 0, "rejected": 1}


def test_file_path_app_version_is_rejected(client, data_dir):
    event = make_event("app_started", app_version="/Users/someone/projects/flowfile")
    response = client.post("/events", json={"events": [event]})
    assert response.status_code == 202
    assert response.json() == {"accepted": 0, "rejected": 1}
    assert read_lines(data_dir) == []


@pytest.mark.parametrize("app_version", ["unknown", "0.15.4", "1.2.3rc1", "0.12.7.dev3+g1a2b3c4"])
def test_version_shaped_app_versions_are_accepted(client, data_dir, app_version):
    response = client.post("/events", json={"events": [make_event("app_started", app_version=app_version)]})
    assert response.status_code == 202
    assert response.json() == {"accepted": 1, "rejected": 0}
    assert read_lines(data_dir)[0]["app_version"] == app_version


@pytest.mark.parametrize("app_version", ["", " 1.0", "1.0 beta", "a" * 33, "flow file", "../etc"])
def test_non_version_shaped_app_versions_are_rejected(client, data_dir, app_version):
    response = client.post("/events", json={"events": [make_event("app_started", app_version=app_version)]})
    assert response.status_code == 202
    assert response.json() == {"accepted": 0, "rejected": 1}


def test_file_path_in_error_class_is_rejected(client, data_dir):
    event = make_event("flow_run_failed", props={"error_class": "/etc/passwd"})
    response = client.post("/events", json={"events": [event]})
    assert response.status_code == 202
    assert response.json() == {"accepted": 0, "rejected": 1}
    assert read_lines(data_dir) == []


def test_unknown_event_name_is_rejected(client, data_dir):
    response = client.post("/events", json={"events": [make_event("totally_unknown")]})
    assert response.status_code == 202
    assert response.json() == {"accepted": 0, "rejected": 1}
    assert read_lines(data_dir) == []


def test_disallowed_prop_key_is_rejected(client, data_dir):
    event = make_event("app_started", props={"flow_name": "secret"})
    response = client.post("/events", json={"events": [event]})
    assert response.status_code == 202
    assert response.json() == {"accepted": 0, "rejected": 1}


def test_event_id_is_stored_when_present_and_absent_when_not(client, data_dir):
    identifier = str(uuid.uuid4())
    events = [make_event("app_started", event_id=identifier), make_event("flow_created")]
    response = client.post("/events", json={"events": events})
    assert response.status_code == 202
    assert response.json() == {"accepted": 2, "rejected": 0}

    lines = read_lines(data_dir)
    assert lines[0]["event_id"] == identifier
    assert "event_id" not in lines[1], "a pre-spool client sends none, and none is stored"


@pytest.mark.parametrize("event_id", ["not-a-uuid", "", 42, "x" * 70])
def test_a_malformed_event_id_rejects_the_event(client, data_dir, event_id):
    response = client.post("/events", json={"events": [make_event("app_started", event_id=event_id)]})
    assert response.status_code == 202
    assert response.json() == {"accepted": 0, "rejected": 1}
    assert read_lines(data_dir) == []


def test_unknown_top_level_keys_are_still_dropped(client, data_dir):
    response = client.post("/events", json={"events": [make_event("app_started", seq=7, junk="x")]})
    assert response.status_code == 202
    stored = read_lines(data_dir)[0]
    assert set(stored) == {"event", "install_id", "app_version", "platform", "mode", "ts", "props", "received_at"}


def test_a_disk_that_cannot_be_written_answers_500_without_a_traceback(client, data_dir, monkeypatch):
    from tools.telemetry_collector import app as collector

    def _boom(lines):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(collector, "_append_lines", _boom)
    response = client.post("/events", json={"events": [make_event("app_started")]})
    assert response.status_code == 500
    assert response.json() == {"detail": "could not store events"}


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

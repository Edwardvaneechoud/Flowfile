"""The four gates, in order: kill switch, TESTING, endpoint, consent.

Most of these assert that nothing was sent, which is exactly the shape a broken
test can fake, so ``test_all_gates_open_actually_sends`` pins the positive case:
if that one stops sending, every other test here has gone vacuous.
"""

from __future__ import annotations

import pytest

from shared import telemetry

ENDPOINT = "https://collector.example.invalid/events"


@pytest.fixture(autouse=True)
def _no_background_thread(monkeypatch):
    """Delivery is driven by flush() here, so batches land before the assertions."""
    monkeypatch.setattr(telemetry, "_ensure_worker", lambda: None)


def _emit_and_flush() -> None:
    telemetry.emit("app_started")
    telemetry.flush()


def test_all_gates_open_actually_sends(enabled, posts):
    _emit_and_flush()
    assert telemetry.is_enabled() is True
    assert posts.sent, "with every gate open an event must reach the collector"
    assert posts.events[0]["event"] == "app_started"


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "FALSE", " 0 "])
def test_kill_switch_beats_granted_consent(value, enabled, posts, monkeypatch):
    monkeypatch.setenv("FLOWFILE_TELEMETRY", value)
    assert telemetry.is_enabled() is False
    _emit_and_flush()
    assert posts.sent == []


@pytest.mark.parametrize("value", ["", "  "])
def test_empty_kill_switch_is_not_engaged(value, enabled, posts, monkeypatch):
    monkeypatch.setenv("FLOWFILE_TELEMETRY", value)
    assert telemetry.is_enabled() is True
    _emit_and_flush()
    assert posts.sent


@pytest.mark.parametrize("value", ["1", "true", "yes", "on"])
def test_truthy_kill_switch_does_not_grant_consent(value, posts, monkeypatch):
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("FLOWFILE_TELEMETRY", value)
    assert telemetry.consent() is None
    assert telemetry.is_enabled() is False
    _emit_and_flush()
    assert posts.sent == []


def test_kill_switch_short_circuits_before_any_file_io(posts, monkeypatch):
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("FLOWFILE_TELEMETRY", "0")

    def _explode():
        raise AssertionError("the consent file must not be read once the kill switch is engaged")

    monkeypatch.setattr(telemetry, "_settings_file", _explode)
    _emit_and_flush()
    assert posts.sent == []


def test_testing_true_disables(enabled, posts, monkeypatch):
    monkeypatch.setenv("TESTING", "True")
    assert telemetry.is_enabled() is False
    _emit_and_flush()
    assert posts.sent == []


@pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes"])
def test_testing_check_is_the_exact_capital_t_string(value, enabled, posts, monkeypatch):
    """storage_config compares against the literal "True"; anything else is not that marker."""
    monkeypatch.setenv("TESTING", value)
    assert telemetry.is_enabled() is True
    _emit_and_flush()
    assert posts.sent


@pytest.mark.parametrize("value", [None, ""])
def test_missing_endpoint_disables(value, posts, monkeypatch):
    telemetry.set_consent(True)
    monkeypatch.setattr(telemetry, "DEFAULT_ENDPOINT", "")
    if value is None:
        monkeypatch.delenv("FLOWFILE_TELEMETRY_ENDPOINT", raising=False)
    else:
        monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", value)
    assert telemetry.is_enabled() is False
    _emit_and_flush()
    assert posts.sent == []


def test_baked_in_default_endpoint_enables(posts, monkeypatch):
    """A release that ships DEFAULT_ENDPOINT needs no env var; consent still gates."""
    telemetry.set_consent(True)
    monkeypatch.delenv("FLOWFILE_TELEMETRY_ENDPOINT", raising=False)
    monkeypatch.setattr(telemetry, "DEFAULT_ENDPOINT", "http://collector.example/events")
    assert telemetry.is_enabled() is True
    _emit_and_flush()
    assert posts.sent and posts.sent[0][0] == "http://collector.example/events"


def test_env_endpoint_overrides_baked_in_default(posts, monkeypatch):
    telemetry.set_consent(True)
    monkeypatch.setattr(telemetry, "DEFAULT_ENDPOINT", "http://collector.example/events")
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", "http://own.example/events")
    _emit_and_flush()
    assert posts.sent and posts.sent[0][0] == "http://own.example/events"


def test_kill_switch_beats_baked_in_default(posts, monkeypatch):
    telemetry.set_consent(True)
    monkeypatch.setattr(telemetry, "DEFAULT_ENDPOINT", "http://collector.example/events")
    monkeypatch.setenv("FLOWFILE_TELEMETRY", "0")
    assert telemetry.is_enabled() is False
    _emit_and_flush()
    assert posts.sent == []


def test_absent_consent_disables(posts, monkeypatch):
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", ENDPOINT)
    assert telemetry.consent() is None
    assert telemetry.is_enabled() is False
    _emit_and_flush()
    assert posts.sent == []


def test_declined_consent_disables(posts, monkeypatch):
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", ENDPOINT)
    telemetry.set_consent(False)
    assert telemetry.consent() is False
    assert telemetry.is_enabled() is False
    _emit_and_flush()
    assert posts.sent == []


def test_revoking_consent_stops_an_already_sending_install(enabled, posts):
    _emit_and_flush()
    assert len(posts.events) == 1
    telemetry.set_consent(False)
    _emit_and_flush()
    assert len(posts.events) == 1, "no further events may be sent after consent is revoked"


class TestStatus:
    def test_status_reports_every_gate(self, monkeypatch):
        monkeypatch.setattr(telemetry, "DEFAULT_ENDPOINT", "")
        status = telemetry.get_status()
        assert status.as_dict() == {
            "available": False,
            "consent": None,
            "env_kill_switch": False,
            "endpoint_configured": False,
        }

        monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", ENDPOINT)
        telemetry.set_consent(True)
        assert telemetry.get_status().as_dict() == {
            "available": True,
            "consent": True,
            "env_kill_switch": False,
            "endpoint_configured": True,
        }

        monkeypatch.setenv("FLOWFILE_TELEMETRY", "off")
        assert telemetry.get_status().as_dict() == {
            "available": False,
            "consent": True,
            "env_kill_switch": True,
            "endpoint_configured": True,
        }

    def test_status_never_carries_the_install_id(self, enabled):
        assert telemetry.install_id() is not None
        assert "install_id" not in telemetry.get_status().as_dict()

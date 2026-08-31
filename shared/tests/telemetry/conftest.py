"""Isolation fixtures for the telemetry client.

Every test gets its own consent file, a clean environment for the three gate env
vars, and a module reset on both sides of the test so queue/thread/caches never
leak between tests.
"""

from __future__ import annotations

from typing import Any

import pytest

from shared import telemetry

ENDPOINT = "https://collector.example.invalid/events"


class _Response:
    def __init__(self, status_code: int):
        self.status_code = status_code
        self.text = ""


class Posts:
    """Recorder standing in for the outbound request."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.timeouts: list[float | None] = []
        self.status = 200
        self.raises: BaseException | None = None

    @property
    def events(self) -> list[dict[str, Any]]:
        return [event for _, body in self.sent for event in body["events"]]


@pytest.fixture(autouse=True)
def _isolated_telemetry(tmp_path, monkeypatch):
    """Keep consent away from the developer's real ~/.flowfile and CI env."""
    monkeypatch.setattr(telemetry, "_settings_file", lambda: tmp_path / "telemetry.yaml")
    monkeypatch.delenv("TESTING", raising=False)
    monkeypatch.delenv("FLOWFILE_TELEMETRY", raising=False)
    monkeypatch.delenv("FLOWFILE_TELEMETRY_ENDPOINT", raising=False)
    telemetry._reset_for_tests()
    yield
    telemetry._reset_for_tests()


@pytest.fixture
def posts(monkeypatch) -> Posts:
    recorder = Posts()

    def _post(url: str, json: dict[str, Any], timeout: float | None = None) -> _Response:
        recorder.sent.append((url, json))
        recorder.timeouts.append(timeout)
        if recorder.raises is not None:
            raise recorder.raises
        return _Response(recorder.status)

    monkeypatch.setattr(telemetry, "_post", _post)
    return recorder


@pytest.fixture
def enabled(monkeypatch) -> None:
    """All four gates open: endpoint configured and consent granted."""
    monkeypatch.setenv("FLOWFILE_TELEMETRY_ENDPOINT", ENDPOINT)
    telemetry.set_consent(True)

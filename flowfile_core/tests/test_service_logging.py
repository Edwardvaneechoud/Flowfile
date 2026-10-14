"""Core configures root logging at import only in its own process (migration log lines belong in the service log);
a program importing it as a library keeps its logging."""

import sys

import flowfile_core


def test_core_recognises_its_own_entry_points(monkeypatch):
    monkeypatch.setattr(sys, "orig_argv", ["/venv/bin/python", "-m", "flowfile_core.main"])
    assert flowfile_core._runs_as_service()
    monkeypatch.setattr(sys, "orig_argv", ["/venv/bin/python", "/venv/bin/flowfile_core"])
    assert flowfile_core._runs_as_service()
    monkeypatch.setattr(sys, "orig_argv", ["C:/venv/Scripts/flowfile_core.exe"])  # the console-script launcher
    assert flowfile_core._runs_as_service()
    monkeypatch.setattr(sys, "orig_argv", ["/venv/bin/python", "-c", "import flowfile"])
    assert not flowfile_core._runs_as_service()
    monkeypatch.setattr(sys, "orig_argv", ["/venv/bin/python", "-m", "pytest", "flowfile_core/tests"])
    assert not flowfile_core._runs_as_service()
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    assert flowfile_core._runs_as_service()

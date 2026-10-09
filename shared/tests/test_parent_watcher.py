"""The sidecar parent-death watcher, exercised with real processes on every platform."""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import shared
from shared.parent_watcher import start_parent_death_watcher

REPO_ROOT = Path(shared.__file__).resolve().parents[1]

# Runs as the sidecar: starts the watcher and writes a marker file once the parent is gone.
SIDECAR = """
import sys, threading, time
from shared.parent_watcher import start_parent_death_watcher
marker = sys.argv[1]
done = threading.Event()
def on_death():
    open(marker, "w").write("parent died")
    done.set()
thread = start_parent_death_watcher(on_death, poll_interval=0.2)
assert thread is not None
done.wait(20)
"""

# Runs as the shell: spawns the sidecar with the supervisor variable set, then exits at once.
PARENT = """
import os, subprocess, sys
env = {**os.environ, "FLOWFILE_SUPERVISOR_PID": str(os.getpid())}
subprocess.Popen([sys.executable, "-c", sys.argv[1], sys.argv[2]], env=env)
"""


def _child_env(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k != "FLOWFILE_SUPERVISOR_PID"}
    env["PYTHONPATH"] = str(REPO_ROOT)
    env.update(extra)
    return env


def _wait_for(path: Path, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.1)
    return path.exists()


def test_not_a_sidecar_without_supervisor_pid(monkeypatch):
    monkeypatch.delenv("FLOWFILE_SUPERVISOR_PID", raising=False)
    assert start_parent_death_watcher(lambda: None) is None


def test_sidecar_notices_its_parent_dying(tmp_path):
    marker = tmp_path / "died"
    parent = subprocess.run(
        [sys.executable, "-c", PARENT, SIDECAR, str(marker)],
        env=_child_env(),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert parent.returncode == 0, parent.stderr
    assert _wait_for(marker, timeout=10), "the sidecar never noticed its parent exit"


def test_sidecar_stays_while_its_parent_lives(tmp_path):
    marker = tmp_path / "died"
    sidecar = subprocess.Popen(
        [sys.executable, "-c", SIDECAR, str(marker)],
        env=_child_env(FLOWFILE_SUPERVISOR_PID=str(os.getpid())),
    )
    try:
        assert not _wait_for(marker, timeout=1.5)
        assert sidecar.poll() is None
    finally:
        sidecar.kill()
        sidecar.wait(timeout=10)


@pytest.mark.skipif(not sys.platform.startswith("win"), reason="Windows handle path only")
def test_windows_parent_already_gone_fires_at_once(tmp_path):
    # A pid no process holds: OpenProcess fails with ERROR_INVALID_PARAMETER.
    from shared.parent_watcher import _wait_for_parent_windows

    started = time.monotonic()
    _wait_for_parent_windows(0x7FFF_FFF0, poll_interval=0.2)
    assert time.monotonic() - started < 2

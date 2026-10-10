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

# Runs as the sidecar: argv = ready marker, death marker.
SIDECAR = """
import sys, threading
from shared.parent_watcher import start_parent_death_watcher
ready, death = sys.argv[1], sys.argv[2]
done = threading.Event()
def on_death():
    open(death, "w").write("supervisor died")
    done.set()
assert start_parent_death_watcher(on_death, poll_interval=0.2) is not None
open(ready, "w").write("watching")
done.wait(60)
"""

# Runs as the shell: argv = sidecar code, ready marker, death marker, stderr file, "wait" | "exit".
# A venv's python on Windows is a launcher, so the sidecar's real parent is never this process:
# the watcher must key on FLOWFILE_SUPERVISOR_PID, which is what the Tauri shell sets.
PARENT = """
import os, subprocess, sys, time
code, ready, death, errfile, mode = sys.argv[1:6]
env = {**os.environ, "FLOWFILE_SUPERVISOR_PID": str(os.getpid())}
with open(errfile, "w") as err:
    subprocess.Popen([sys.executable, "-c", code, ready, death], env=env, stdout=subprocess.DEVNULL, stderr=err)
if mode == "wait":
    deadline = time.monotonic() + 60
    while not os.path.exists(ready) and time.monotonic() < deadline:
        time.sleep(0.05)
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


def _stderr(path: Path) -> str:
    return path.read_text() if path.exists() else ""


def _run_parent(tmp_path: Path, mode: str) -> tuple[Path, Path, Path]:
    ready, death, err = tmp_path / "ready", tmp_path / "death", tmp_path / "sidecar.err"
    parent = subprocess.run(
        [sys.executable, "-c", PARENT, SIDECAR, str(ready), str(death), str(err), mode],
        env=_child_env(),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert parent.returncode == 0, parent.stderr
    return ready, death, err


def test_not_a_sidecar_without_supervisor_pid(monkeypatch):
    monkeypatch.delenv("FLOWFILE_SUPERVISOR_PID", raising=False)
    assert start_parent_death_watcher(lambda: None) is None


def test_garbage_supervisor_pid_disables_the_watcher(monkeypatch):
    monkeypatch.setenv("FLOWFILE_SUPERVISOR_PID", "not-a-pid")
    assert start_parent_death_watcher(lambda: None) is None


def test_sidecar_notices_its_supervisor_dying(tmp_path):
    ready, death, err = _run_parent(tmp_path, "wait")
    assert ready.exists(), _stderr(err)
    assert _wait_for(death, timeout=30), f"the sidecar never noticed the supervisor exit\n{_stderr(err)}"


def test_sidecar_notices_a_supervisor_gone_before_it_started_watching(tmp_path):
    ready, death, err = _run_parent(tmp_path, "exit")
    assert _wait_for(death, timeout=60), f"the sidecar never noticed the supervisor exit\n{_stderr(err)}"


def test_sidecar_stays_while_its_supervisor_lives(tmp_path):
    ready, death, err = tmp_path / "ready", tmp_path / "death", tmp_path / "sidecar.err"
    with open(err, "w") as err_fh:
        sidecar = subprocess.Popen(
            [sys.executable, "-c", SIDECAR, str(ready), str(death)],
            env=_child_env(FLOWFILE_SUPERVISOR_PID=str(os.getpid())),
            stdout=subprocess.DEVNULL,
            stderr=err_fh,
        )
    try:
        assert _wait_for(ready, timeout=60), _stderr(err)
        assert not _wait_for(death, timeout=1.0)
        assert sidecar.poll() is None
    finally:
        sidecar.kill()
        sidecar.wait(timeout=30)


@pytest.mark.skipif(not sys.platform.startswith("win"), reason="Windows handle path only")
def test_windows_supervisor_already_gone_returns_at_once():
    from shared.parent_watcher import _wait_for_exit_windows

    started = time.monotonic()
    _wait_for_exit_windows(0x7FFF_FFF0, poll_interval=0.2)
    assert time.monotonic() - started < 2

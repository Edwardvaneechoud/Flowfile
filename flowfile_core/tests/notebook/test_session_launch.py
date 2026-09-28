"""How a session is launched and framed: argv, env, Windows flags, the frozen verb, and the wire format."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import flowfile_core
from flowfile_core.notebook import bootstrap, protocol

MAIN_PY = Path(flowfile_core.__file__).resolve().parent / "main.py"


def test_dev_command_swaps_the_protocol_off_stdout_before_any_flowfile_import():
    command = bootstrap.session_command(frozen=False)
    assert command[:2] == [sys.executable, "-c"]
    source = command[2]
    assert source.index("os.dup2(2, 1)") < source.index("flowfile")
    assert source.index("os.dup(1)") < source.index("os.dup2(2, 1)")
    assert "-m" not in command and "start_new_session" not in source


def test_frozen_command_is_the_verb_only():
    assert bootstrap.session_command(frozen=True) == [sys.executable, "--notebook-session"]


def test_frozen_verb_is_checked_before_any_flowfile_core_import():
    source = MAIN_PY.read_text(encoding="utf-8")
    verb = source.index('sys.argv[1:2] == ["--notebook-session"]')
    swap = source.index("os.dup2(2, 1)", verb)
    first_core_import = source.index("from flowfile_core import telemetry")
    assert verb < swap < first_core_import


def test_windows_flags():
    assert bootstrap.creation_flags("win32") == 0x08000000
    assert bootstrap.creation_flags("darwin") == 0
    assert bootstrap.creation_flags("linux") == 0
    assert bootstrap.job_limit_flags() == 0x2000


@pytest.mark.skipif(sys.platform == "win32", reason="the Job Object path is exercised on Windows only")
def test_job_object_is_a_no_op_off_windows():
    assert bootstrap.attach_kill_on_close_job(object()) is None


def test_session_env():
    from flowfile_core.configs import settings

    base = {"FLOWFILE_ADMIN_PASSWORD": "secret", "JWT_SECRET_KEY": "jwt", "FLOWFILE_STORAGE_DIR": "/x", "PATH": "p"}
    env = bootstrap.session_env(7, "/spill", base)
    assert "FLOWFILE_ADMIN_PASSWORD" not in env
    assert env["JWT_SECRET_KEY"] == "jwt" and env["FLOWFILE_STORAGE_DIR"] == "/x" and env["PATH"] == "p"
    assert env["CORE_PORT"] == str(settings.SERVER_PORT)
    assert env["FLOWFILE_WORKER_URL"] == str(settings.WORKER_URL)
    assert env["FLOWFILE_SESSION_USER_ID"] == "7"
    assert {k: env[k] for k in ("FLOWFILE_OFFLOAD_TO_WORKER", "FLOWFILE_TELEMETRY", "FLOWFILE_KERNEL_GC")} == {
        "FLOWFILE_OFFLOAD_TO_WORKER": "0",
        "FLOWFILE_TELEMETRY": "0",
        "FLOWFILE_KERNEL_GC": "0",
    }
    assert env["FLOWFILE_SKIP_STARTUP_MIGRATION"] == env["FLOWFILE_SKIP_INIT_DB"] == "1"


def test_frames_round_trip_and_large_bodies_spill(tmp_path):
    big = {"type": "seed", "snapshot": {"rows": "x" * (protocol.SPILL_THRESHOLD + 10)}}
    small = {"type": "execute", "code": "print('é')"}
    stream = io.BytesIO(protocol.encode(small, tmp_path) + protocol.encode(big, tmp_path))
    assert len(list(tmp_path.iterdir())) == 1
    assert protocol.read_message(stream) == small
    assert protocol.read_message(stream) == big
    assert list(tmp_path.iterdir()) == []
    assert protocol.read_message(stream) is None


def test_unframed_bytes_are_reported_and_skipped():
    junk = b"not json!"
    valid = {"type": "ready"}
    stream = io.BytesIO(
        protocol.HEADER.pack(len(junk)) + junk + protocol.HEADER.pack(2) + b"[]" + protocol.encode(valid)
    )
    seen = []
    assert protocol.read_message(stream, seen.append) == valid
    assert seen == [junk, b"[]"]
    assert json.loads(protocol.encode(valid)[4:]) == valid


def test_the_frozen_verb_runs_a_session_from_main_py():
    """main.py's top-of-file verb, run as ``__main__`` without its package directory on ``sys.path``."""
    runner = (
        "import runpy, sys\n"
        f"sys.argv = [{str(MAIN_PY)!r}, '--notebook-session']\n"
        f"runpy.run_path({str(MAIN_PY)!r}, run_name='__main__')\n"
    )
    env = bootstrap.session_env(1, os.environ.get("TMPDIR", "/tmp"))
    process = subprocess.Popen(
        [sys.executable, "-c", runner],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=env,
        bufsize=0,
    )
    try:
        ready = protocol.read_message(process.stdout)
        # On Windows the venv launcher re-executes the real interpreter, so the reported pid is a child of Popen's.
        assert ready["type"] == "ready" and isinstance(ready["pid"], int) and ready["pid"] > 0
        protocol.write_message(
            process.stdin, {"type": "execute", "ticket": "t", "cell_id": "c", "code": "print(6 * 7)"}
        )
        messages = []
        while not messages or messages[-1]["type"] != "done":
            messages.append(protocol.read_message(process.stdout))
        assert [m["text"] for m in messages if m["type"] == "stream"] == ["42\n"]
        assert messages[-1]["ok"] is True
        process.stdin.close()
        assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.kill()


def test_install_makes_the_registry_the_clean_runner(monkeypatch):
    from flowfile_core.notebook import bridge, registry

    previous = bridge._runner
    try:
        monkeypatch.delenv("FLOWFILE_NOTEBOOK_INPROCESS_CLEAN_RUN", raising=False)
        installed = registry.install()
        assert bridge.get_clean_runner() is installed is registry.get_registry()
        sentinel = object()
        bridge.set_clean_runner(sentinel)
        monkeypatch.setenv("FLOWFILE_NOTEBOOK_INPROCESS_CLEAN_RUN", "1")
        registry.install()
        assert bridge.get_clean_runner() is sentinel
    finally:
        bridge.set_clean_runner(previous)

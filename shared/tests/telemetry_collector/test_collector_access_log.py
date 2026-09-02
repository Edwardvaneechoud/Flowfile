"""The collector must not write an access log: it is the only place a client IP appears.

The stored envelope is anonymous, but uvicorn's default access log prints the
connecting address next to a timestamp, which is enough to correlate an install
id with an IP. Both entry points (``python -m tools.telemetry_collector`` and
the Dockerfile ``CMD``) are pinned here, plus the resulting behaviour of a real
in-process server.
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

import pytest
import uvicorn
from uvicorn.main import main as uvicorn_cli

from tools.telemetry_collector import __main__ as collector_main

COLLECTOR_DIR = Path(__file__).resolve().parents[3] / "tools" / "telemetry_collector"
DOCKERFILE = COLLECTOR_DIR / "Dockerfile"

REQUEST_LINE = re.compile(r'"(?:GET|POST) /\S* HTTP/')

VALID_EVENT = {
    "event": "app_started",
    "install_id": "3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1",
    "app_version": "0.12.7",
    "platform": "darwin",
    "mode": "electron",
    "ts": "2026-08-29T12:00:00Z",
    "props": {},
}


def _main_kwargs() -> dict:
    """The exact arguments the module entry point hands to ``uvicorn.run``."""
    captured: dict = {}

    def fake_run(app, **kwargs):
        captured["app"] = app
        captured.update(kwargs)

    with mock.patch.object(uvicorn, "run", fake_run):
        collector_main.main()
    assert captured, "the entry point no longer calls uvicorn.run"
    return captured


def _dockerfile_cmd_args() -> list[str]:
    """The ``CMD`` argv from the collector Dockerfile, minus the ``uvicorn`` program name."""
    match = re.search(r"^CMD\s+(\[.*\])\s*$", DOCKERFILE.read_text(encoding="utf-8"), re.MULTILINE)
    assert match is not None, f"no JSON-form CMD in {DOCKERFILE}"
    argv = json.loads(match.group(1))
    assert argv and argv[0] == "uvicorn", f"CMD no longer starts uvicorn directly: {argv}"
    return argv[1:]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_module_entry_point_disables_the_access_log() -> None:
    config = uvicorn.Config(**_main_kwargs())
    assert config.access_log is False


def test_dockerfile_cmd_disables_the_access_log() -> None:
    context = uvicorn_cli.make_context("uvicorn", _dockerfile_cmd_args(), resilient_parsing=True)
    assert context.params["access_log"] is False


def test_a_real_request_writes_no_access_log_line(data_dir, capfd) -> None:
    """End-to-end: serving POST /events must print no request line with the client address.

    The server runs with uvicorn's own logging config — the very thing
    ``access_log`` switches — so the assertion is over what the process really
    writes to its output, not over a config attribute.
    """
    kwargs = dict(_main_kwargs())
    app = kwargs.pop("app")
    kwargs.update(host="127.0.0.1", port=_free_port())
    server = uvicorn.Server(uvicorn.Config(app, **kwargs))
    base = f"http://127.0.0.1:{kwargs['port']}"

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/health", timeout=1) as response:
                    assert response.status == 200
                break
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                time.sleep(0.05)
        else:
            pytest.fail("the collector never came up")

        request = urllib.request.Request(
            f"{base}/events",
            data=json.dumps({"events": [VALID_EVENT]}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 202
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        assert not thread.is_alive()

    assert (data_dir / "events.jsonl").exists(), "the request must really have been served"
    captured = capfd.readouterr()
    request_lines = [line for line in (captured.out + captured.err).splitlines() if REQUEST_LINE.search(line)]
    assert request_lines == []


def test_a_rejection_warning_reaches_the_deployed_process_output(tmp_path) -> None:
    """The rejection warning is only useful if a deployed collector actually prints it.

    Nothing configures logging in either entry point, so the record only reaches
    stderr through ``logging.lastResort``. That is invisible under pytest's own
    root handler, hence the subprocess — started from the collector directory
    with the Dockerfile's own argv, which is how the service really runs.
    """
    port = _free_port()
    argv = [arg for arg in _dockerfile_cmd_args() if arg not in {"--host", "0.0.0.0", "--port", "8300"}]
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", *argv, "--host", "127.0.0.1", "--port", str(port)],
        cwd=COLLECTOR_DIR,
        env={**os.environ, "TELEMETRY_DATA_DIR": str(tmp_path)},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                pytest.fail(f"the collector exited early: {process.communicate()[0]}")
            try:
                with urllib.request.urlopen(f"{base}/health", timeout=1) as response:
                    assert response.status == 200
                break
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                time.sleep(0.1)
        else:
            pytest.fail("the collector never came up")

        newer_client = dict(VALID_EVENT, props={"flow_name": "unknown_to_this_schema"})
        request = urllib.request.Request(
            f"{base}/events",
            data=json.dumps({"events": [newer_client]}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            assert json.loads(response.read()) == {"accepted": 0, "rejected": 1}
    finally:
        process.terminate()
        output = process.communicate(timeout=30)[0]

    assert "rejected 1 of 1 event(s)" in output, output
    assert "flow_name" in output
    assert VALID_EVENT["install_id"] not in output

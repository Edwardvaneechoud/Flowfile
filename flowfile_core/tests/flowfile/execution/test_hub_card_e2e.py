"""The worker's command a hub's install line runs joins and serves as the line starts it, beside Flowfile desktop, end
to end.

The hub's worker line is ``flowfile_worker --host 0.0.0.0 --hub <hub> --join <code>``, the code a short one such as
``K7QX-M2PD``; its install line adds ``--port`` with a free port of this machine's, since beside Flowfile desktop the
default one is the desktop's, and ``--url`` with this machine's address on that port, and writes ``node.env`` with
``FLOWFILE_STORAGE_DIR`` and ``FLOWFILE_SECURE_STORAGE_PATH``, folders of the node's own. Here it runs as ``python -m
flowfile_worker`` in the environment of an operator's shell (no other ``FLOWFILE_*`` variable, ``HOME`` a folder of the
test's own holding a desktop's ``~/.flowfile`` with its own ``hub_node.json``), against a fake hub on this machine; the
desktop's own ports are never bound or called. The worker listens on every interface, serves on the port it is given,
tells the hub the address it was given in one join, answers ``GET /health`` there, keeps its files in its storage dir
and leaves the desktop's file as it was; a restart with the same command does not join again and serves on the same
port. A port the operator gives that is taken stops the worker before it spends its token.
"""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import ANY

import pytest
import requests

from shared import hub_node
from shared.tests.hub_join import HEARTBEAT_PATH, JOIN_PATH, FakeJoinHub
from tests.conftest import _free_port
from tests.flowfile.execution import test_hub_join_e2e
from tests.flowfile.execution.test_hub_join_e2e import joins
from tests.flowfile.execution.test_hub_node_tokens_e2e import STARTUP_TIMEOUT, _stop

fake_hub = test_hub_join_e2e.fake_hub

pytestmark = pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")

DESKTOP_NODE = b'{"name": "the desktop\'s node"}\n'
SESSION_VARIABLES = ("TESTING", "CORE_PORT", "CORE_HOST", "WORKER_HOST", "SECURE_STORAGE_PATH", "SKIP_WORKER_TESTS")


def card(hub_url: str, token: str, port: int) -> tuple[str, ...]:
    """The worker's command as the hub's install line runs it, after ``flowfile_worker``: its worker line, with the
    port the line picks and this machine's address on it."""
    return ("--host", "0.0.0.0", "--hub", hub_url, "--join", token, "--port", str(port), "--url", url_of(port))


def url_of(port: int) -> str:
    return f"http://127.0.0.1:{port}"


def storage_of(home: Path) -> Path:
    """The storage dir the install line's ``node.env`` gives the node, beside the home; its secure store is ``secure``
    beside it."""
    return home.parent / "node" / "storage"


def shell_environment(home: Path) -> dict[str, str]:
    """This process's environment as an operator's shell has it, with ``home`` as ``HOME`` and the install line's
    ``FLOWFILE_STORAGE_DIR`` and ``FLOWFILE_SECURE_STORAGE_PATH``.

    The test session's own variables are left out: every ``FLOWFILE_*`` one and those it sets for its own servers.
    """
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("FLOWFILE_") and name not in SESSION_VARIABLES
    }
    secure = storage_of(home).with_name("secure")
    environment.update(
        HOME=str(home), FLOWFILE_STORAGE_DIR=str(storage_of(home)), FLOWFILE_SECURE_STORAGE_PATH=str(secure)
    )
    return environment


def start(home: Path, log: Path, *args: str) -> subprocess.Popen:
    """``python -m flowfile_worker *args`` with ``home`` as its home and its output in ``log``; the process."""
    with log.open("w") as out:
        return subprocess.Popen(
            [sys.executable, "-m", "flowfile_worker", *args],
            stdout=out,
            stderr=subprocess.STDOUT,
            env=shell_environment(home),
            cwd=home.parent,
            preexec_fn=os.setsid,
        )


def healthy(url: str) -> bool:
    try:
        return requests.get(f"{url}/health", timeout=2).status_code == 200
    except requests.RequestException:
        return False


def served_at(proc: subprocess.Popen, hub: FakeJoinHub, log: Path) -> str:
    """The address the worker's join gave the hub, once the worker answers ``GET /health`` there.

    Fails, with the worker's output, when it exits first or does not answer within the startup timeout.
    """
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline and proc.poll() is None:
        sent = joins(hub)
        if sent and healthy(sent[0]["worker_url"]):
            return sent[0]["worker_url"]
        time.sleep(0.5)
    _stop(proc)
    pytest.fail(f"the card's worker did not serve (exit code {proc.returncode}):\n{log.read_text()[-3000:]}")


@pytest.fixture
def home(tmp_path) -> Path:
    """A home holding Flowfile desktop's ``~/.flowfile`` with a ``hub_node.json`` of its own."""
    home = tmp_path / "home"
    (home / ".flowfile").mkdir(parents=True)
    (home / ".flowfile" / hub_node.FILE_NAME).write_bytes(DESKTOP_NODE)
    return home


def test_the_install_lines_command_beside_a_desktop_joins_once_and_serves_on_a_port_of_its_own(fake_hub, home):
    token, name, port = fake_hub.mint(code=True), hub_node.default_name(), _free_port()
    folder = storage_of(home)
    first = start(home, home.parent / "first.log", *card(fake_hub.url, token, port))
    try:
        url = served_at(first, fake_hub, home.parent / "first.log")
    finally:
        _stop(first)

    assert url == url_of(port)
    assert joins(fake_hub) == [{"token": token, "name": name, "worker_url": url, "core_url": None, "keys": ANY}]
    assert f"Starting worker service on 0.0.0.0:{port}" in (home.parent / "first.log").read_text()
    kept = json.loads((folder / hub_node.FILE_NAME).read_text())
    assert (kept["hub_url"], kept["name"]) == (fake_hub.url, name)
    assert (home / ".flowfile" / hub_node.FILE_NAME).read_bytes() == DESKTOP_NODE
    assert [path.name for path in (home / ".flowfile").iterdir()] == [hub_node.FILE_NAME]

    before = len(fake_hub.calls)
    second = start(home, home.parent / "second.log", *card(fake_hub.url, token, port))
    try:
        assert served_at(second, fake_hub, home.parent / "second.log") == url
    finally:
        _stop(second)

    assert [path for path, _ in fake_hub.calls[before:] if path != HEARTBEAT_PATH] == []
    assert (
        f"Already joined {fake_hub.url} as {name}; the join token is not used"
        in (home.parent / "second.log").read_text()
    )
    assert json.loads((folder / hub_node.FILE_NAME).read_text())["name"] == name
    assert (home / ".flowfile" / hub_node.FILE_NAME).read_bytes() == DESKTOP_NODE


def test_a_port_the_operator_gives_that_is_taken_stops_the_worker_before_it_spends_its_token(fake_hub, home):
    token, log = fake_hub.mint(code=True), home.parent / "worker.log"
    with socket.socket() as taken:
        taken.bind(("0.0.0.0", 0))
        taken.listen()
        port = taken.getsockname()[1]
        proc = start(home, log, *card(fake_hub.url, token, port))
        try:
            exit_code = proc.wait(timeout=STARTUP_TIMEOUT)
        except subprocess.TimeoutExpired:
            exit_code = None
        finally:
            _stop(proc)

    printed = log.read_text()
    assert exit_code == 1, printed
    assert f"Cannot serve on 0.0.0.0:{port}: " in printed
    assert fake_hub.calls == []
    assert not (storage_of(home) / hub_node.FILE_NAME).exists()
    after = {"token": token, "name": "after", "worker_url": "http://127.0.0.1:1", "core_url": None}
    assert requests.post(f"{fake_hub.url}{JOIN_PATH}", json=after, timeout=10).status_code == 200

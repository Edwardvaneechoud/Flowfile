"""A core joins a hub with ``--join`` and ``--hub`` for itself and its worker, once, and then knows its own node; it
binds its port before it joins, is refused without a storage dir of its own, and gives the hub ``--url``, else its full
name and port.

The join runs against ``FakeJoinHub`` over real HTTP; the sync through ``/hub_client`` against the fake hub of
``tests/hub_client``. ``hub_node.json`` lives under the test's ``tmp_path``, never in the storage dir.
"""

import errno
import json
import logging
import multiprocessing
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import flowfile_core
from flowfile_core import main
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.hub_client import node_tokens
from flowfile_core.routes import hub_client as hub_routes
from shared import hub_node
from shared.tests import hub_join
from tests.hub_client import conftest as hub_client_fakes
from tests.hub_client.conftest import HUB_URL, compute_node

join_hub = hub_join.join_hub
hub = hub_client_fakes.hub
store = hub_client_fakes.store

VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME")
CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}


@pytest.fixture
def node_file(tmp_path, monkeypatch):
    """Core's ``hub_node.json`` in a storage dir of the test's own, absent until a join or the test writes it."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    return target


@pytest.fixture
def no_variables(monkeypatch):
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)


def joined_as(node_file, name: str, hub_url: str = HUB_URL) -> None:
    """The file a core leaves after joining ``hub_url`` as ``name``."""
    node_file.parent.mkdir(parents=True, exist_ok=True)
    node_file.write_text(
        json.dumps(
            {
                "hub_url": hub_url,
                "issuer": hub_url,
                "name": name,
                "node_id": 1,
                "join_token_hash": "0" * 16,
            }
        ),
        encoding="utf-8",
    )


def parse(monkeypatch, *flags) -> tuple:
    """``--join``, ``--hub``, ``--name``, ``--url`` and ``--worker-url`` as core reads them from ``flags``."""
    monkeypatch.setattr(sys, "argv", ["flowfile_core", *flags])
    monkeypatch.setattr(flowfile_core, "_starts_core", lambda: True)
    args = settings.parse_args()
    return args.join, args.hub, args.name, args.url, args.worker_url


def started_with(
    monkeypatch,
    hub_url,
    token,
    *,
    name="team-1",
    core_url="http://team-1.example:63578",
    worker_url="http://team-1.example:63579",
):
    """The module values core's command line leaves behind, as ``settings`` reads them at import."""
    monkeypatch.setattr(main, "JOIN_TOKEN", token)
    monkeypatch.setattr(main, "JOIN_HUB_URL", hub_url)
    monkeypatch.setattr(main, "JOIN_NODE_NAME", name)
    monkeypatch.setattr(settings, "JOIN_CORE_URL", core_url)
    monkeypatch.setattr(settings, "JOIN_WORKER_URL", worker_url)


def test_the_join_flags_are_read(monkeypatch, no_variables):
    flags = ("--join", "t", "--hub", "http://h", "--name", "n", "--url", "http://c:1", "--worker-url", "http://w:2")

    assert parse(monkeypatch, *flags) == ("t", "http://h", "n", "http://c:1", "http://w:2")


def test_a_program_importing_flowfile_keeps_its_own_join_and_worker_flags(monkeypatch, no_variables):
    monkeypatch.setattr(sys, "argv", ["etl.py", "--join", "x", "--worker", "5"])
    monkeypatch.setattr(flowfile_core, "_starts_core", lambda: False)

    args = settings.parse_args()

    assert (args.join, args.hub, args.worker_port) == (None, None, 5)


def test_core_reads_the_join_flags_by_their_full_names_only(monkeypatch, no_variables):
    assert parse(monkeypatch, "--joi", "t", "--hu", "http://h")[:2] == (None, None)
    assert settings.parse_args().worker_port is None
    assert parse(monkeypatch, "--worker", "5", "--worker-url", "http://w:2")[4] == "http://w:2"
    assert settings.parse_args().worker_port == 5


def test_join_without_a_hub_is_refused_naming_the_flag(monkeypatch, no_variables):
    with pytest.raises(ValueError, match="--join needs --hub"):
        parse(monkeypatch, "--join", "t")


def test_the_join_token_and_hub_come_from_the_variables_alone(monkeypatch, no_variables):
    monkeypatch.setenv("FLOWFILE_HUB_JOIN_TOKEN", "t")
    monkeypatch.setenv("FLOWFILE_HUB_URL", "http://h")

    assert parse(monkeypatch) == ("t", "http://h", None, None, None)


def test_without_flags_or_variables_there_is_nothing_to_join(monkeypatch, no_variables):
    assert parse(monkeypatch) == (None, None, None, None, None)


def test_a_core_joins_for_itself_and_its_worker_and_a_restart_makes_no_request(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, join_hub.mint())

    main._join_hub()

    assert [path for path, _ in join_hub.calls] == ["/hub/nodes/join"]
    body = join_hub.calls[0][1]
    assert (body["name"], body["core_url"], body["worker_url"]) == (
        "team-1",
        "http://team-1.example:63578",
        "http://team-1.example:63579",
    )
    kept = json.loads(node_file.read_text(encoding="utf-8"))
    fields = ["has_core", "hub_url", "issuer", "join_token_hash", "name", "node_id"]
    assert sorted(kept) == fields
    assert (kept["name"], kept["has_core"]) == ("team-1", True)

    main._join_hub()

    assert len(join_hub.calls) == 1


@pytest.mark.parametrize("served_on,core_port", [(None, 64100), (64200, 64200)], ids=["SERVER_PORT", "run's port"])
def test_a_core_given_no_addresses_gives_this_machines_on_its_two_ports(
    monkeypatch, join_hub, node_file, served_on, core_port
):
    on_this_machine(monkeypatch)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), name=None, core_url=None, worker_url=None)

    main._join_hub(served_on)

    body = join_hub.calls[0][1]
    assert (body["name"], body["core_url"], body["worker_url"]) == (
        "team-7",
        f"http://team-7.example.com:{core_port}",
        "http://team-7.example.com:64101",
    )
    assert "ui_url" not in body


def on_this_machine(monkeypatch, host: str = "0.0.0.0") -> None:
    """Core bound to ``host`` on 64100 beside a worker given 64101, on a machine named ``team-7.example.com``."""
    named(monkeypatch)
    monkeypatch.setattr(settings, "SERVER_HOST", host)
    monkeypatch.setattr(settings, "SERVER_PORT", 64100)
    monkeypatch.setattr(settings, "WORKER_PORT", 64101)


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "::1"])
def test_a_core_bound_to_a_loopback_address_warns_that_only_this_machine_reaches_it(
    monkeypatch, join_hub, node_file, caplog, host
):
    on_this_machine(monkeypatch, host)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url=None, worker_url=None)

    with caplog.at_level(logging.WARNING):
        main._join_hub()

    body = join_hub.calls[0][1]
    assert (body["core_url"], body["worker_url"]) == (
        "http://team-7.example.com:64100",
        "http://team-7.example.com:64101",
    )
    warnings = [r.getMessage() for r in caplog.records if r.name == settings.__name__]
    assert len(warnings) == 1
    assert f"Core serves on {host}, which only this machine reaches" in warnings[0]
    assert "--host 0.0.0.0" in warnings[0] and "--url" in warnings[0]


def test_the_port_run_is_given_decides_over_the_settings_and_url_wins_over_both(
    monkeypatch, join_hub, node_file, caplog
):
    on_this_machine(monkeypatch, "0.0.0.0")
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url=None, worker_url=None)

    main._join_hub(64200, "10.1.2.3")

    assert join_hub.calls[0][1]["core_url"] == "http://team-7.example.com:64200"

    on_this_machine(monkeypatch, "127.0.0.1")
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url="http://flowfile.example:8080", worker_url=None)
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        main._join_hub()

    assert join_hub.calls[1][1]["core_url"] == "http://flowfile.example:8080"
    assert [r for r in caplog.records if r.name == settings.__name__] == []


@pytest.fixture
def editor_app(monkeypatch) -> FastAPI:
    """Core's app as the pip server leaves it: the editor's built files served on /ui, the worker mounted on /worker."""
    app = FastAPI()
    app.state.editor_path = "/ui"
    app.state.worker_path = "/worker"
    monkeypatch.setattr(main, "app", app)
    return app


def test_a_core_that_serves_the_editor_gives_its_address_and_its_mounted_worker_s(
    monkeypatch, join_hub, node_file, editor_app
):
    on_this_machine(monkeypatch)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url=None, worker_url=None)

    main._join_hub()

    body = join_hub.calls[0][1]
    core_url = "http://team-7.example.com:64100"
    assert (body["core_url"], body["worker_url"], body["ui_url"]) == (core_url, f"{core_url}/worker", f"{core_url}/ui")


def test_the_addresses_given_win_over_the_mounted_worker_and_name_the_editor(
    monkeypatch, join_hub, node_file, editor_app
):
    on_this_machine(monkeypatch)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url="https://flowfile.example/", worker_url=None)

    main._join_hub()

    body = join_hub.calls[0][1]
    assert (body["core_url"], body["worker_url"], body["ui_url"]) == (
        "https://flowfile.example/",
        "https://flowfile.example/worker",
        "https://flowfile.example/ui",
    )

    started_with(monkeypatch, join_hub.url, join_hub.mint(), worker_url="http://w.example:1")
    main._join_hub()

    body = join_hub.calls[1][1]
    assert (body["worker_url"], body["ui_url"]) == ("http://w.example:1", "http://team-1.example:63578/ui")


def test_a_core_without_the_editor_s_files_says_nothing_about_an_editor(monkeypatch, join_hub, node_file):
    app = FastAPI()
    app.state.worker_path = "/worker"
    monkeypatch.setattr(main, "app", app)
    on_this_machine(monkeypatch)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url=None, worker_url=None)

    main._join_hub()

    body = join_hub.calls[0][1]
    assert "ui_url" not in body
    assert body["worker_url"] == "http://team-7.example.com:64100/worker"


def test_a_core_given_the_token_as_a_variable_starts_no_run_that_reads_it_without_the_hub(
    monkeypatch, no_variables, join_hub, node_file
):
    token = join_hub.mint()
    monkeypatch.setenv("FLOWFILE_HUB_JOIN_TOKEN", token)
    started_with(monkeypatch, join_hub.url, token)
    monkeypatch.setattr(sys, "argv", ["flowfile", "run", "flow", "f.yaml", "--run-id", "1"])
    assert settings.parse_args().join is None

    main._join_hub()

    assert len(join_hub.calls) == 1
    assert "FLOWFILE_HUB_JOIN_TOKEN" not in os.environ
    assert settings.parse_args().join is None


def test_a_core_without_a_join_token_asks_nothing(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, None)

    main._join_hub()

    assert join_hub.calls == []
    assert not node_file.exists()


def test_a_refused_join_stops_core_with_exit_code_2_before_it_serves(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, "join-never-minted")
    served = []
    monkeypatch.setattr(main.compute_registry, "mark_serving", lambda: served.append(True))
    port = free_port()

    with pytest.raises(SystemExit) as exited:
        main.run(host="127.0.0.1", port=port)

    assert exited.value.code == 2
    assert served == []
    assert not node_file.exists()
    with socket.socket() as again:
        again.bind(("127.0.0.1", port))


@pytest.fixture
def api(store) -> TestClient:
    """A client signed in to core; the token is minted after the store is rebuilt, so it verifies against it."""
    c = TestClient(main.app)
    token = c.post("/auth/token").json()["access_token"]
    c.headers["Authorization"] = f"Bearer {token}"
    return c


@pytest.fixture
def no_configured_backends(monkeypatch):
    """No node token or queued node check outlives the test, and every backends row it left is removed."""
    monkeypatch.setattr(node_tokens, "_tokens", {})
    monkeypatch.setattr(hub_routes, "_check_nodes", lambda backend_ids: None)
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            registry.forget(row.id)
            db.delete(row)
        db.commit()
    handshake.reset_handshake()


def _row_ids() -> list[str]:
    with get_db_context() as db:
        return sorted(row.id for row in db.query(ConfiguredBackend).all())


def test_a_joined_core_never_lists_its_own_node_and_drops_a_row_written_before(
    api, hub, no_variables, node_file, no_configured_backends
):
    hub.nodes = [compute_node(1, "team-local"), compute_node(2, "gpu")]
    api.post("/hub_client/connection", json=CONNECT)
    assert _row_ids() == ["worker:gpu", "worker:team-local"]
    joined_as(node_file, "team-local")

    response = api.post("/hub_client/compute_nodes/sync")

    assert response.status_code == 200, response.text
    assert response.json()["skipped"] == []
    assert [row["id"] for row in response.json()["backends"]] == ["worker:gpu"]
    assert _row_ids() == ["worker:gpu"]
    reconnected = api.post("/hub_client/connection", json=CONNECT)
    assert reconnected.status_code == 200, reconnected.text
    assert _row_ids() == ["worker:gpu"]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def held():
    """Ports another server listens on, on 127.0.0.1, for the length of the test."""
    sockets = []

    def hold() -> int:
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        sockets.append(sock)
        return sock.getsockname()[1]

    yield hold
    for sock in sockets:
        sock.close()


def named(monkeypatch) -> None:
    """This machine's name is ``team-7.example.com``, so no lookup is asked of the network."""
    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "team-7.example.com")
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "team-7.example.com")


@pytest.fixture
def serving(monkeypatch):
    """``main.run`` up to the point it serves: the server records the sockets it is handed, and neither the serving
    mark nor a worker backend made for the start-up lines outlives the test."""
    served = []

    class Server:
        def __init__(self, config):
            self.config = config
            self.should_exit = False

        def run(self, sockets=None):
            served.append(sockets)

    monkeypatch.setattr(main, "_Server", Server)
    monkeypatch.setattr(main.signal, "signal", lambda *args: None)
    monkeypatch.setattr(main, "start_parent_death_watcher", lambda stop: None)
    monkeypatch.setattr(main.compute_registry, "mark_serving", lambda: None)
    monkeypatch.setattr(main.compute_registry, "worker_url", lambda: settings.WORKER_URL)
    named(monkeypatch)
    yield served
    for sockets in served:
        for sock in sockets or []:
            sock.close()


def served_port(serving) -> int:
    ((sock,),) = serving
    return sock.getsockname()[1]


@pytest.mark.parametrize("given_by", ["--port", "run"])
def test_a_core_whose_port_is_in_use_exits_1_before_any_request_and_keeps_its_token(
    monkeypatch, join_hub, node_file, held, serving, caplog, given_by
):
    port, token = held(), join_hub.mint()
    started_with(monkeypatch, join_hub.url, token)
    if given_by == "--port":
        monkeypatch.setattr(main, "SERVER_PORT", port)

    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as exited:
        main.run(host="127.0.0.1", port=port if given_by == "run" else None)

    assert exited.value.code == 1
    assert f"Cannot serve on 127.0.0.1:{port}: {os.strerror(errno.EADDRINUSE)}" in caplog.text
    assert join_hub.calls == []
    assert serving == []
    assert hub_node.join(join_hub.url, token, name="team-1", worker_url="http://w:1")[0].name == "team-1"


def test_a_core_joins_with_the_ports_it_was_started_with_and_serves_on_its_own(
    monkeypatch, join_hub, node_file, serving, caplog
):
    core_port, worker_port = free_port(), free_port()
    monkeypatch.setattr(main, "SERVER_PORT", core_port)
    monkeypatch.setattr(settings, "WORKER_PORT", worker_port)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url=None, worker_url=None)

    with caplog.at_level(logging.INFO):
        main.run(host="127.0.0.1")

    assert served_port(serving) == core_port
    body = join_hub.calls[0][1]
    assert (body["core_url"], body["worker_url"]) == (
        f"http://team-7.example.com:{core_port}",
        f"http://team-7.example.com:{worker_port}",
    )
    assert hub_node.read().has_core is True
    assert "is in use" not in caplog.text


def test_without_a_url_core_gives_its_full_name_and_port_whatever_it_binds(monkeypatch, join_hub, node_file):
    named(monkeypatch)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url=None, worker_url=None)
    main._join_hub(64100, "0.0.0.0")
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url=None, worker_url=None)
    main._join_hub(64100, "10.1.2.3")

    assert {body["core_url"] for _, body in join_hub.calls} == {"http://team-7.example.com:64100"}
    assert {body["worker_url"] for _, body in join_hub.calls} == {f"http://team-7.example.com:{settings.WORKER_PORT}"}


def test_a_restart_with_the_same_token_serves_on_its_port_and_makes_no_request(
    monkeypatch, join_hub, node_file, serving
):
    token, port = join_hub.mint(), free_port()
    hub_node.join(join_hub.url, token, name="team-1", worker_url="http://w:1", core_url="http://c:2")
    monkeypatch.setattr(main, "SERVER_PORT", port)
    started_with(monkeypatch, join_hub.url, token, core_url=None, worker_url=None)

    main.run(host="127.0.0.1")

    assert served_port(serving) == port
    assert len(join_hub.calls) == 1


def test_the_worker_port_core_is_started_with_is_the_one_it_gives_the_hub_even_while_a_worker_serves_there(
    monkeypatch, join_hub, node_file, held
):
    named(monkeypatch)
    beside = held()
    monkeypatch.setattr(settings, "WORKER_PORT", beside)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), core_url=None, worker_url=None)

    main._join_hub(free_port(), "127.0.0.1")

    assert join_hub.calls[0][1]["worker_url"] == f"http://team-7.example.com:{beside}"


@pytest.mark.parametrize(
    "argv,command,starts",
    [
        (["/venv/bin/flowfile_core", "--join", "t"], ["/venv/bin/python", "/venv/bin/flowfile_core"], True),
        (["-m", "--join", "t"], ["python", "-m", "flowfile_core.main", "--join", "t"], True),
        (["/venv/bin/flowfile", "run", "ui", "--join", "t"], ["/venv/bin/python", "/venv/bin/flowfile"], True),
        (["/venv/bin/flowfile", "run", "core"], ["/venv/bin/python", "/venv/bin/flowfile"], True),
        (["-m", "run", "ui"], ["python", "-m", "flowfile", "run", "ui"], True),
        (
            ["/venv/bin/flowfile", "--no-browser", "--host", "127.0.0.1", "--join", "t", "--name", "n", "run", "ui"],
            ["/venv/bin/python", "/venv/bin/flowfile"],
            True,
        ),
        (["-m", "--join", "t", "run", "core"], ["python", "-m", "flowfile", "--join", "t", "run", "core"], True),
        (["/venv/bin/flowfile", "run", "flow", "f.yaml"], ["/venv/bin/python", "/venv/bin/flowfile"], False),
        (
            ["/venv/bin/flowfile", "--join", "t", "run", "flow", "core"],
            ["/venv/bin/python", "/venv/bin/flowfile"],
            False,
        ),
        (["/venv/bin/flowfile", "run", "worker"], ["/venv/bin/python", "/venv/bin/flowfile"], False),
        (["/venv/bin/flowfile_worker", "--join", "t"], ["/venv/bin/python", "/venv/bin/flowfile_worker"], False),
        (["/venv/bin/pytest"], ["/venv/bin/python", "/venv/bin/pytest"], False),
    ],
    ids=[
        "console script",
        "python -m",
        "run ui",
        "run core",
        "python -m flowfile",
        "options before run ui",
        "python -m flowfile, options before run core",
        "run flow",
        "run flow of a file named core",
        "run worker",
        "worker",
        "pytest",
    ],
)
def test_only_a_command_that_serves_core_reads_the_join_flags(monkeypatch, argv, command, starts):
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(sys, "orig_argv", command, raising=False)

    assert flowfile_core._starts_core() is starts


def test_a_spawned_child_of_core_never_reads_the_join_flags(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["/venv/bin/flowfile", "run", "ui", "--join", "t"])
    monkeypatch.setattr(multiprocessing.current_process(), "name", "SpawnProcess-1")

    assert flowfile_core._starts_core() is False


STORAGE_VARIABLES = (
    "FLOWFILE_STORAGE_DIR",
    "FLOWFILE_SECURE_STORAGE_PATH",
    "SECURE_STORAGE_PATH",
    "FLOWFILE_MODE",
    "TESTING",
    "CORE_PORT",
    "FLOWFILE_WORKER_PORT",
)


def _wait_for(proc: subprocess.Popen, url: str, log) -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            pytest.fail(f"exited with {proc.returncode}:\n{log.read_text(encoding='utf-8')}")
        try:
            if httpx.get(url, timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.3)
    pytest.fail(f"{url} did not answer:\n{log.read_text(encoding='utf-8')}")


def _stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


DESKTOP_NODE = '{"name": "the desktop\'s node"}\n'


def desktop_home(tmp_path) -> Path:
    """A home directory holding Flowfile desktop's ``~/.flowfile`` with a ``hub_node.json`` of its own."""
    home = tmp_path / "home"
    (home / ".flowfile").mkdir(parents=True)
    (home / ".flowfile" / hub_node.FILE_NAME).write_text(DESKTOP_NODE, encoding="utf-8")
    return home


def shell_environment(home: Path, **variables: str) -> dict[str, str]:
    """This process's environment as an operator's shell has it: ``home`` as ``HOME``, none of the storage or hub
    variables, no scheduler or pool, and ``variables`` on top."""
    environment = {name: value for name, value in os.environ.items() if name not in (*STORAGE_VARIABLES, *VARIABLES)}
    environment.update(HOME=str(home), FLOWFILE_WORKER_POOL_SIZE="0", FLOWFILE_SCHEDULER_ENABLED="0", **variables)
    return environment


def node_env(folder: Path) -> dict[str, str]:
    """The storage variables the hub's install line writes for a node kept in ``folder``."""
    return {
        "FLOWFILE_STORAGE_DIR": str(folder / "storage"),
        "FLOWFILE_SECURE_STORAGE_PATH": str(folder / "secure"),
        "SECURE_STORAGE_PATH": str(folder / "secure"),
    }


def assert_desktop_untouched(home: Path) -> None:
    assert sorted(path.name for path in home.iterdir()) == [".flowfile"]
    assert [path.name for path in (home / ".flowfile").iterdir()] == [hub_node.FILE_NAME]
    assert (home / ".flowfile" / hub_node.FILE_NAME).read_text(encoding="utf-8") == DESKTOP_NODE


BOTH = "FLOWFILE_STORAGE_DIR and FLOWFILE_SECURE_STORAGE_PATH"


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
@pytest.mark.parametrize(
    "command,given,missing",
    [
        (["-m", "flowfile_core.main"], {}, BOTH),
        (["-m", "flowfile", "run", "ui", "--no-browser"], {}, BOTH),
        (["-m", "flowfile", "run", "core"], {}, BOTH),
        (["-m", "flowfile_core.main"], {"FLOWFILE_STORAGE_DIR": "{tmp}/storage"}, "FLOWFILE_SECURE_STORAGE_PATH"),
        (["-m", "flowfile_core.main"], {"FLOWFILE_SECURE_STORAGE_PATH": "{tmp}/secure"}, "FLOWFILE_STORAGE_DIR"),
    ],
    ids=["flowfile_core", "run ui", "run core", "no secure store", "no storage dir"],
)
def test_a_join_without_a_storage_dir_and_secure_store_of_its_own_is_refused_before_any_request(
    tmp_path, join_hub, command, given, missing
):
    home = desktop_home(tmp_path)
    flags = ["--host", "127.0.0.1", "--port", str(free_port()), "--hub", join_hub.url, "--join", join_hub.mint()]
    log = tmp_path / "core.log"
    environment = shell_environment(home, **{name: value.format(tmp=tmp_path) for name, value in given.items()})
    with log.open("wb") as out:
        core = subprocess.Popen(
            [sys.executable, *command, *flags, "--name", "card-node"],
            env=environment,
            cwd=tmp_path,
            stdout=out,
            stderr=out,
        )
    try:
        exit_code = core.wait(timeout=120)
    finally:
        _stop(core)

    output = log.read_text(encoding="utf-8")
    assert exit_code == 2, output
    assert f"Cannot join a hub without a storage dir and a secure store of this node's own: set {missing} to" in output
    assert "~/.flowfile and ~/.config/flowfile" in output
    assert join_hub.calls == []
    assert_desktop_untouched(home)


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
def test_given_a_storage_dir_core_joins_for_itself_and_the_worker_beside_it_from_that_dir(tmp_path, join_hub):
    home, storage = desktop_home(tmp_path), tmp_path / "card-node" / "storage"
    core_port, worker_port = free_port(), free_port()
    environment = shell_environment(home, **node_env(storage.parent))
    core_log, worker_log = tmp_path / "core.log", tmp_path / "worker.log"
    core_flags = ["--host", "127.0.0.1", "--port", str(core_port), "--worker-port", str(worker_port)]
    join_flags = ["--hub", join_hub.url, "--join", join_hub.mint(), "--name", "card-node"]
    join_flags += ["--url", f"http://127.0.0.1:{core_port}", "--worker-url", f"http://127.0.0.1:{worker_port}"]
    with core_log.open("wb") as out:
        core = subprocess.Popen(
            [sys.executable, "-m", "flowfile_core.main", *core_flags, *join_flags],
            env=environment,
            cwd=tmp_path,
            stdout=out,
            stderr=out,
        )
    worker = None
    try:
        _wait_for(core, f"http://127.0.0.1:{core_port}/health/status", core_log)
        with worker_log.open("wb") as out:
            worker = subprocess.Popen(
                [sys.executable, "-m", "flowfile_worker", "--host", "127.0.0.1", "--port", str(worker_port)],
                env=environment,
                cwd=tmp_path,
                stdout=out,
                stderr=out,
            )
        _wait_for(worker, f"http://127.0.0.1:{worker_port}/health", worker_log)
    finally:
        if worker is not None:
            _stop(worker)
        _stop(core)

    joins = [body for path, body in join_hub.calls if path == hub_join.JOIN_PATH]
    assert len(joins) == 1
    assert (joins[0]["core_url"], joins[0]["worker_url"]) == (
        f"http://127.0.0.1:{core_port}",
        f"http://127.0.0.1:{worker_port}",
    )
    kept = json.loads((storage / hub_node.FILE_NAME).read_text(encoding="utf-8"))
    assert (kept["name"], kept["has_core"]) == ("card-node", True)
    assert f"Joined {join_hub.url} as card-node" in core_log.read_text(encoding="utf-8")
    assert_desktop_untouched(home)


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
@pytest.mark.parametrize("component", ["ui", "core"])
def test_the_flowfile_command_given_the_join_flags_first_joins_from_the_storage_dir_it_is_given(
    tmp_path, join_hub, component
):
    home, storage, port = desktop_home(tmp_path), tmp_path / "flags-first" / "storage", free_port()
    environment = shell_environment(
        home, **node_env(storage.parent), FLOWFILE_WORKER_PORT=str(free_port()), FLOWFILE_KERNEL_GC="0"
    )
    flags = ["--no-browser", "--host", "127.0.0.1", "--port", str(port), "--url", f"http://127.0.0.1:{port}"]
    flags += ["--hub", join_hub.url, "--join", join_hub.mint(), "--name", "flags-first"]
    log = tmp_path / "flowfile.log"
    with log.open("wb") as out:
        server = subprocess.Popen(
            [sys.executable, "-m", "flowfile", *flags, "run", component],
            env=environment,
            cwd=tmp_path,
            stdout=out,
            stderr=out,
        )
    try:
        _wait_for(server, f"http://127.0.0.1:{port}/health/status", log)
    finally:
        _stop(server)

    output = log.read_text(encoding="utf-8")
    assert f"Joined {join_hub.url} as flags-first" in output
    joins = [body for path, body in join_hub.calls if path == hub_join.JOIN_PATH]
    assert [body["core_url"] for body in joins] == [f"http://127.0.0.1:{port}"]
    kept = json.loads((storage / hub_node.FILE_NAME).read_text(encoding="utf-8"))
    assert kept["name"] == "flags-first"
    assert_desktop_untouched(home)

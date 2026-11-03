"""A worker joins a hub with ``--join`` and ``--hub`` before it serves, once, and then takes that hub's tokens; it binds
its port before it joins, moves off a default port in use, keeps its files apart from Flowfile desktop's and gives the
hub an address it reaches."""

import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import time

import httpx
import pytest
from fastapi.testclient import TestClient

import flowfile_worker
from flowfile_worker import configs, main
from shared import hub_node
from shared.tests import hub_join, hub_jwks
from shared.tests.hub_join import FakeJoinHub
from tests.test_hub_tokens import v1_headers

pytestmark = pytest.mark.worker

join_hub = hub_join.join_hub
hub_signer = hub_jwks.hub_signer

client = TestClient(main.app)

VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME", "FLOWFILE_HUB_ISSUER")


@pytest.fixture
def node_file(tmp_path, monkeypatch):
    """The worker's ``hub_node.json`` in a storage dir of the test's own, absent until a join writes it."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    return target


@pytest.fixture
def no_variables(monkeypatch):
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)


def parse(monkeypatch, *flags):
    monkeypatch.setattr(sys, "argv", ["flowfile_worker", *flags])
    return configs.parse_args()


def started_with(monkeypatch, hub_url, token, *, name="worker-1", url="http://worker-1.example:63579"):
    """The module values a worker's command line leaves behind, as ``configs`` sets them in the main process."""
    monkeypatch.setattr(main, "JOIN_TOKEN", token)
    monkeypatch.setattr(main, "HUB_URL", hub_url)
    monkeypatch.setattr(main, "NODE_NAME", name)
    monkeypatch.setattr(main, "NODE_URL", url)


def test_join_without_a_hub_is_refused_naming_the_flag(monkeypatch, no_variables):
    with pytest.raises(ValueError, match="--join needs --hub"):
        parse(monkeypatch, "--join", "t")


def test_the_join_flags_are_read(monkeypatch, no_variables):
    args = parse(monkeypatch, "--join", "t", "--hub", "http://h", "--name", "n", "--url", "http://w:1")

    assert (args.join, args.hub, args.name, args.url) == ("t", "http://h", "n", "http://w:1")


def test_the_join_token_and_hub_come_from_the_variables_as_from_the_flags(monkeypatch, no_variables):
    monkeypatch.setenv("FLOWFILE_HUB_JOIN_TOKEN", "t")
    monkeypatch.setenv("FLOWFILE_HUB_URL", "http://h")
    monkeypatch.setenv("FLOWFILE_NODE_NAME", "n")

    args = parse(monkeypatch)

    assert (args.join, args.hub, args.name, args.url) == ("t", "http://h", "n", None)


def test_without_flags_or_variables_there_is_nothing_to_join(monkeypatch, no_variables):
    args = parse(monkeypatch)

    assert (args.join, args.hub, args.name, args.url) == (None, None, None, None)


def test_a_worker_joins_before_it_serves_and_a_restart_makes_no_request(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, join_hub.mint())

    main.join_hub()

    assert [path for path, _ in join_hub.calls] == ["/hub/nodes/join"]
    assert join_hub.calls[0][1]["name"] == "worker-1"
    assert join_hub.calls[0][1]["worker_url"] == "http://worker-1.example:63579"
    assert (hub_node.read().name, hub_node.read().has_core) == ("worker-1", False)

    main.join_hub()

    assert len(join_hub.calls) == 1


def test_a_worker_without_a_join_token_asks_nothing(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, None)

    main.join_hub()

    assert join_hub.calls == []
    assert not node_file.exists()


def test_a_refused_join_stops_the_worker_with_exit_code_2(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, "join-never-minted")

    with pytest.raises(SystemExit) as exited:
        main.join_hub()

    assert exited.value.code == 2
    assert not node_file.exists()


def test_a_worker_given_no_name_or_address_gives_the_machines_and_https_when_it_serves_tls(
    monkeypatch, join_hub, node_file
):
    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "build-7.example.com")
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "build-7.example.com")
    monkeypatch.setattr(hub_node, "_route_address", lambda host, port, family: None)
    monkeypatch.setenv("FLOWFILE_WORKER_TLS_CERT", "/certs/worker.pem")
    started_with(monkeypatch, join_hub.url, join_hub.mint(), name=None, url=None)

    main.join_hub(64000, "0.0.0.0")

    body = join_hub.calls[0][1]
    assert (body["name"], body["worker_url"]) == ("build-7", "https://build-7.example.com:64000")


def test_a_joined_worker_takes_the_hubs_tokens_for_its_name_with_no_variable_set(hub_signer, monkeypatch, node_file):
    monkeypatch.delenv("FLOWFILE_NODE_NAME")
    monkeypatch.delenv("FLOWFILE_HUB_URL")
    hub = FakeJoinHub(signer=hub_signer)
    try:
        started_with(monkeypatch, hub.url, hub.mint(enable_on_join=True), name="joined-worker")
        main.join_hub()
        accepted = client.get("/v1/capabilities", headers=v1_headers(hub_signer.token(aud="joined-worker")))
        refused = client.get("/v1/capabilities", headers=v1_headers(hub_signer.token(aud=hub_jwks.NODE_NAME)))
    finally:
        hub.close()

    assert hub_node.read().issuer == hub_signer.url
    assert accepted.status_code == 200
    assert refused.status_code == 401
    assert "wrong audience" in refused.json()["detail"]


STORAGE_VARIABLES = ("FLOWFILE_STORAGE_DIR", "FLOWFILE_SECURE_STORAGE_PATH", "SECURE_STORAGE_PATH", "FLOWFILE_MODE")


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


@pytest.fixture
def serving(monkeypatch):
    """``main.run`` up to the point it serves: the server records the config and the sockets it is handed instead."""
    served = []

    class Server:
        def __init__(self, config):
            self.config = config
            self.should_exit = False

        def run(self, sockets=None):
            served.append((self.config, sockets))

    monkeypatch.setattr(main.uvicorn, "Server", Server)
    monkeypatch.setattr(main.signal, "signal", lambda *args: None)
    monkeypatch.setattr(main, "start_parent_death_watcher", lambda stop: None)
    monkeypatch.setattr(main, "NODE_STORAGE_DIR", None)
    yield served
    for _, sockets in served:
        for sock in sockets or []:
            sock.close()


def default_port(monkeypatch, port: int, *, given: bool = False) -> None:
    """The port ``configs`` leaves behind: ``--port`` when ``given``, else the default."""
    monkeypatch.setattr(main, "SERVICE_PORT", port)
    monkeypatch.setattr(main, "PORT_GIVEN", given)


def served_port(serving) -> int:
    ((config, sockets),) = serving
    assert config.port == sockets[0].getsockname()[1]
    return config.port


@pytest.mark.parametrize("given_by", ["--port", "run"])
def test_a_given_port_in_use_exits_1_before_any_request_and_the_token_stays_unspent(
    monkeypatch, join_hub, node_file, held, serving, caplog, given_by
):
    port = held()
    token = join_hub.mint()
    started_with(monkeypatch, join_hub.url, token, url=None)
    default_port(monkeypatch, port, given=given_by == "--port")

    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as exited:
        main.run(host="127.0.0.1", port=port if given_by == "run" else None)

    assert exited.value.code == 1
    assert f"Cannot serve on 127.0.0.1:{port}: in use by " in caplog.text
    assert join_hub.calls == []
    assert serving == []
    assert hub_node.join(join_hub.url, token, name="worker-1", worker_url="http://w:1").name == "worker-1"


def test_the_default_port_in_use_gives_way_to_a_free_one_which_the_join_reports(
    monkeypatch, join_hub, node_file, held, serving, caplog
):
    port = held()
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)
    default_port(monkeypatch, port)

    with caplog.at_level(logging.INFO):
        main.run(host="127.0.0.1")

    served = served_port(serving)
    assert served != port
    assert [body["worker_url"] for _, body in join_hub.calls] == [f"http://127.0.0.1:{served}"]
    assert hub_node.read().worker_port == served
    assert f"Port {port} is in use; serving on {served}" in caplog.text
    assert f"Telling the hub this worker is at http://127.0.0.1:{served}" in caplog.text


def test_a_restart_with_the_same_token_serves_on_the_port_the_hub_has(monkeypatch, join_hub, node_file, serving):
    kept, token = free_port(), join_hub.mint()
    hub_node.join(join_hub.url, token, name="worker-1", worker_url=f"http://w:{kept}", worker_port=kept)
    started_with(monkeypatch, join_hub.url, token, url=None)
    default_port(monkeypatch, free_port())

    main.run(host="127.0.0.1")

    assert served_port(serving) == kept
    assert len(join_hub.calls) == 1


def test_a_restart_whose_port_is_in_use_exits_1_saying_the_hub_has_the_node_there(
    monkeypatch, join_hub, node_file, held, serving, caplog
):
    kept, token = held(), join_hub.mint()
    hub_node.join(join_hub.url, token, name="worker-1", worker_url=f"http://w:{kept}", worker_port=kept)
    started_with(monkeypatch, join_hub.url, token, url=None)
    default_port(monkeypatch, free_port())

    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as exited:
        main.run(host="127.0.0.1")

    assert exited.value.code == 1
    assert f"Cannot serve on 127.0.0.1:{kept}: in use by " in caplog.text
    assert "the hub has this node at that port, so free it or join again with a new join token" in caplog.text
    assert serving == []
    assert len(join_hub.calls) == 1


def test_a_new_token_prefers_the_port_the_node_last_joined_with(monkeypatch, join_hub, node_file, held, serving):
    kept = free_port()
    hub_node.join(join_hub.url, join_hub.mint(), name="worker-1", worker_url=f"http://w:{kept}", worker_port=kept)
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)
    default_port(monkeypatch, held())

    main.run(host="127.0.0.1")

    assert served_port(serving) == kept
    assert join_hub.calls[-1][1]["worker_url"] == f"http://127.0.0.1:{kept}"


def test_a_worker_without_a_join_token_serves_on_its_port_or_not_at_all(monkeypatch, join_hub, held, serving):
    port = held()
    started_with(monkeypatch, join_hub.url, None)
    default_port(monkeypatch, port)

    with pytest.raises(SystemExit) as exited:
        main.run(host="127.0.0.1")

    assert exited.value.code == 1
    assert serving == []


def test_a_worker_on_every_address_tells_the_hub_its_address_towards_it(monkeypatch, join_hub, node_file, caplog):
    asked = []
    monkeypatch.setattr(hub_node, "_route_address", lambda host, port, family: asked.append(host) or "10.0.0.7")
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)

    with caplog.at_level(logging.INFO):
        main.join_hub(64000, "0.0.0.0")

    assert join_hub.calls[0][1]["worker_url"] == "http://10.0.0.7:64000"
    assert asked == ["192.0.2.1"]
    assert "Telling the hub this worker is at http://10.0.0.7:64000" in caplog.text


def test_a_worker_on_one_address_gives_that_address(monkeypatch, join_hub, node_file):
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)

    main.join_hub(64000, "10.1.2.3")

    assert join_hub.calls[0][1]["worker_url"] == "http://10.1.2.3:64000"


def test_a_worker_on_loopback_gives_loopback_and_warns(monkeypatch, join_hub, node_file, caplog):
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)

    with caplog.at_level(logging.WARNING):
        main.join_hub(64000, "127.0.0.1")

    assert join_hub.calls[0][1]["worker_url"] == "http://127.0.0.1:64000"
    assert "This worker serves on 127.0.0.1, which only this machine reaches" in caplog.text
    assert "--host 0.0.0.0" in caplog.text


@pytest.mark.parametrize("host", ["0.0.0.0", "127.0.0.1"])
def test_the_url_given_wins_over_the_address_rules(monkeypatch, join_hub, node_file, caplog, host):
    monkeypatch.setattr(hub_node, "_route_address", lambda *args: pytest.fail("no route is asked for"))
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url="http://worker-1.example:63579")

    with caplog.at_level(logging.WARNING):
        main.join_hub(64000, host)

    assert join_hub.calls[0][1]["worker_url"] == "http://worker-1.example:63579"
    assert "only this machine reaches" not in caplog.text


@pytest.mark.parametrize(
    "argv,command,launched",
    [
        (["/venv/bin/flowfile_worker", "--join", "t"], ["/venv/bin/python", "/venv/bin/flowfile_worker"], True),
        (["-m", "--join", "t"], ["python", "-m", "flowfile_worker", "--join", "t"], True),
        (["-m"], ["python", "-I", "-m", "flowfile_worker"], True),
        (["-m"], ["python", "-m", "flowfile_core.main"], False),
        (["/venv/bin/flowfile_core", "--join", "t"], ["/venv/bin/python", "/venv/bin/flowfile_core"], False),
        (["/venv/bin/flowfile", "run", "worker"], ["/venv/bin/python", "/venv/bin/flowfile"], False),
        (["/venv/bin/pytest"], ["/venv/bin/python", "/venv/bin/pytest"], False),
    ],
    ids=["console script", "python -m", "python -I -m", "core -m", "core", "flowfile cli", "pytest"],
)
def test_only_the_workers_own_command_line_chooses_a_storage_dir(monkeypatch, argv, command, launched):
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(sys, "orig_argv", command, raising=False)

    assert flowfile_worker._launched_as_worker() is launched


def test_a_spawned_child_never_chooses_a_storage_dir(monkeypatch):
    import multiprocessing

    monkeypatch.setattr(sys, "argv", ["/venv/bin/flowfile_worker", "--join", "t"])
    monkeypatch.setattr(multiprocessing.current_process(), "name", "SpawnProcess-1")

    assert flowfile_worker._launched_as_worker() is False


def _wait_for_health(worker: subprocess.Popen, port: int, log) -> dict:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            pytest.fail(f"the worker exited with {worker.returncode}:\n{log.read_text(encoding='utf-8')}")
        try:
            answer = httpx.get(f"http://127.0.0.1:{port}/health", timeout=2)
            if answer.status_code == 200:
                return answer.json()
        except httpx.HTTPError:
            pass
        time.sleep(0.3)
    pytest.fail(f"the worker did not answer on {port}:\n{log.read_text(encoding='utf-8')}")


def _stop(worker: subprocess.Popen) -> None:
    worker.terminate()
    try:
        worker.wait(timeout=30)
    except subprocess.TimeoutExpired:
        worker.kill()
        worker.wait(timeout=10)


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
@pytest.mark.parametrize("launch", ["python -m flowfile_worker", "console script"])
def test_a_worker_started_with_a_join_token_keeps_its_files_apart_from_the_desktops(tmp_path, join_hub, launch):
    command = (
        [shutil.which("flowfile_worker")] if launch == "console script" else [sys.executable, "-m", "flowfile_worker"]
    )
    if command[0] is None:
        pytest.skip("the flowfile_worker console script is not installed")
    home = tmp_path / "home"
    desktop = home / ".flowfile"
    desktop.mkdir(parents=True)
    (desktop / hub_node.FILE_NAME).write_text('{"name": "the desktop\'s node"}\n', encoding="utf-8")
    port = free_port()
    environment = {name: value for name, value in os.environ.items() if name not in (*STORAGE_VARIABLES, *VARIABLES)}
    environment.update(HOME=str(home), FLOWFILE_WORKER_POOL_SIZE="0")
    log = tmp_path / "worker.log"
    flags = ["--host", "127.0.0.1", "--port", str(port), "--hub", join_hub.url, "--join", join_hub.mint()]
    with log.open("wb") as out:
        worker = subprocess.Popen(
            [*command, *flags, "--name", "card-node"], env=environment, cwd=tmp_path, stdout=out, stderr=out
        )
    try:
        health = _wait_for_health(worker, port, log)
    finally:
        _stop(worker)

    folder = home / ".flowfile-node" / "card-node"
    output = log.read_text(encoding="utf-8")
    assert health["status"] == "ok"
    assert "Keeping this node's files in ~/.flowfile-node/card-node, apart from Flowfile desktop's" in output
    joins = [body for path, body in join_hub.calls if path == hub_join.JOIN_PATH]
    assert [body["worker_url"] for body in joins] == [f"http://127.0.0.1:{port}"]
    kept = json.loads((folder / hub_node.FILE_NAME).read_text(encoding="utf-8"))
    assert (kept["name"], kept["worker_port"]) == ("card-node", port)
    assert (folder / "hub_node_key.json").is_file()
    assert sorted(path.name for path in desktop.iterdir()) == [hub_node.FILE_NAME]
    assert (desktop / hub_node.FILE_NAME).read_text(encoding="utf-8") == '{"name": "the desktop\'s node"}\n'

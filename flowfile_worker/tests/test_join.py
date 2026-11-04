"""A worker joins a hub with ``--join`` and ``--hub`` before it serves, once, and then takes that hub's tokens; it binds
its port before it joins, keeps its files apart from Flowfile desktop's and gives the hub an address it reaches."""

import errno
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit

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


def test_a_program_importing_flowfile_keeps_its_own_join_flags(monkeypatch, no_variables):
    monkeypatch.setattr(sys, "argv", ["etl.py", "--join", "t", "--name", "n"])

    args = configs.parse_args()

    assert (args.join, args.hub, args.name, args.host) == (None, None, None, "127.0.0.1")


def test_the_worker_reads_the_join_flags_by_their_full_names_only(monkeypatch, no_variables):
    args = parse(monkeypatch, "--joi", "t", "--hu", "http://h")

    assert (args.join, args.hub) == (None, None)


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


@pytest.mark.parametrize(
    "flags,variable,host",
    [
        ((), None, "127.0.0.1"),
        (("--hub", "http://h", "--join", "K7QX-M2PD"), None, "0.0.0.0"),
        (("--hub", "http://h"), "K7QX-M2PD", "0.0.0.0"),
        (("--hub", "http://h", "--join", "K7QX-M2PD", "--host", "127.0.0.1"), None, "127.0.0.1"),
        (("--hub", "http://h", "--join", "K7QX-M2PD", "--host", "10.1.2.3"), None, "10.1.2.3"),
        (("--name", "team-1"), None, "127.0.0.1"),
    ],
    ids=["no join token", "--join", "the variable", "--host wins", "an address given", "a name only"],
)
def test_a_join_token_binds_every_interface_unless_a_host_is_given(monkeypatch, no_variables, flags, variable, host):
    if variable is not None:
        monkeypatch.setenv("FLOWFILE_HUB_JOIN_TOKEN", variable)

    assert parse(monkeypatch, *flags).host == host


def test_an_empty_host_is_still_refused(monkeypatch, no_variables):
    with pytest.raises(ValueError, match="Worker host cannot be empty"):
        parse(monkeypatch, "--hub", "http://h", "--join", "K7QX-M2PD", "--host", "")


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


def default_port(monkeypatch, port: int) -> None:
    """The port ``configs`` leaves behind: ``--port``, else the default."""
    monkeypatch.setattr(main, "SERVICE_PORT", port)


def served_port(serving) -> int:
    ((config, sockets),) = serving
    assert config.port == sockets[0].getsockname()[1]
    return config.port


@pytest.mark.parametrize("given_by", ["--port", "run"])
def test_a_port_in_use_exits_1_before_any_request_and_the_token_stays_unspent(
    monkeypatch, join_hub, node_file, held, serving, caplog, given_by
):
    port = held()
    token = join_hub.mint()
    started_with(monkeypatch, join_hub.url, token, url=None)
    default_port(monkeypatch, port if given_by == "--port" else free_port())

    with caplog.at_level(logging.ERROR), pytest.raises(SystemExit) as exited:
        main.run(host="127.0.0.1", port=port if given_by == "run" else None)

    assert exited.value.code == 1
    assert f"Cannot serve on 127.0.0.1:{port}: {os.strerror(errno.EADDRINUSE)}" in caplog.text
    assert join_hub.calls == []
    assert serving == []
    assert hub_node.join(join_hub.url, token, name="worker-1", worker_url="http://w:1").name == "worker-1"


def test_the_port_the_worker_is_started_with_is_the_one_it_joins_with_and_serves_on(
    monkeypatch, join_hub, node_file, serving, caplog
):
    port = free_port()
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)
    default_port(monkeypatch, port)

    with caplog.at_level(logging.INFO):
        main.run(host="127.0.0.1")

    assert served_port(serving) == port
    assert [body["worker_url"] for _, body in join_hub.calls] == [f"http://127.0.0.1:{port}"]
    assert f"Telling the hub this worker is at http://127.0.0.1:{port}" in caplog.text
    assert "is in use" not in caplog.text


@pytest.mark.parametrize("host", ["127.0.0.1", "::"])
def test_a_worker_without_a_join_token_leaves_binding_its_port_to_uvicorn(monkeypatch, join_hub, held, serving, host):
    port = held()
    started_with(monkeypatch, join_hub.url, None)
    default_port(monkeypatch, port)

    main.run(host=host)

    ((config, sockets),) = serving
    assert (config.host, config.port, sockets) == (host, port, None)
    assert join_hub.calls == []


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
def test_a_worker_with_a_folder_of_its_own_says_so_at_start(
    monkeypatch, tmp_path, join_hub, node_file, serving, caplog
):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(main, "NODE_STORAGE_DIR", tmp_path / ".flowfile-node" / "card-node")
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)
    default_port(monkeypatch, free_port())

    with caplog.at_level(logging.INFO):
        main.run(host="127.0.0.1")

    assert "Keeping this node's files in ~/.flowfile-node/card-node, apart from Flowfile desktop's" in caplog.text


def found_beside_a_core(monkeypatch, node_file) -> None:
    """The worker started with ``--name team-1`` and no join token in the folder of core ``team-1`` that joined."""
    node_file.parent.mkdir(parents=True, exist_ok=True)
    kept = {"hub_url": "http://hub.example", "issuer": "http://hub.example", "name": "team-1", "node_id": 4}
    kept.update(joined_at="2026-11-03T09:00:00+00:00", has_core=True)
    node_file.write_text(json.dumps(kept), encoding="utf-8")
    monkeypatch.setattr(main, "NODE_STORAGE_DIR", node_file.parent)
    monkeypatch.setattr(main, "JOIN_TOKEN", None)
    monkeypatch.setattr(main, "HUB_URL", None)


def test_the_worker_beside_a_core_says_which_folder_it_found_and_leaves_its_port_to_uvicorn(
    monkeypatch, node_file, serving, caplog
):
    found_beside_a_core(monkeypatch, node_file)
    port = free_port()
    default_port(monkeypatch, port)

    with caplog.at_level(logging.INFO):
        main.run(host="127.0.0.1")

    ((config, sockets),) = serving
    assert (config.host, config.port, sockets) == ("127.0.0.1", port, None)
    assert f"Found node {node_file.parent.name} in {hub_node.home_relative(node_file.parent)}" in caplog.text
    assert "Keeping this node's files" not in caplog.text


def test_a_worker_whose_storage_dir_is_set_says_nothing_about_it(monkeypatch, serving, caplog):
    monkeypatch.setattr(main, "JOIN_TOKEN", None)
    default_port(monkeypatch, free_port())

    with caplog.at_level(logging.INFO):
        main.run(host="127.0.0.1")

    assert "Keeping this node's files" not in caplog.text


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
    flags = ["--port", str(port), "--hub", join_hub.url, "--join", join_hub.mint(code=True)]
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
    (joined,) = [urlsplit(body["worker_url"]) for path, body in join_hub.calls if path == hub_join.JOIN_PATH]
    assert f"Starting worker service on 0.0.0.0:{port}" in output
    assert (joined.port, hub_node.is_loopback_host(joined.hostname)) == (port, False)
    kept = json.loads((folder / hub_node.FILE_NAME).read_text(encoding="utf-8"))
    assert kept["name"] == "card-node"
    assert (folder / "hub_node_key.json").is_file()
    assert sorted(path.name for path in home.iterdir()) == [".flowfile", ".flowfile-node"]
    assert sorted(path.name for path in desktop.iterdir()) == [hub_node.FILE_NAME]
    assert (desktop / hub_node.FILE_NAME).read_text(encoding="utf-8") == '{"name": "the desktop\'s node"}\n'


def _start(command, flags, environment, cwd, log) -> subprocess.Popen:
    with log.open("wb") as out:
        return subprocess.Popen([*command, *flags], env=environment, cwd=cwd, stdout=out, stderr=out)


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
@pytest.mark.parametrize("joined", [True, False], ids=["a core joined as that name", "no node of that name"])
def test_a_worker_named_after_a_node_that_joined_here_finds_its_folder_and_else_stays_as_today(tmp_path, joined):
    home = tmp_path / "home"
    desktop = home / ".flowfile"
    desktop.mkdir(parents=True)
    (desktop / hub_node.FILE_NAME).write_text('{"name": "the desktop\'s node"}\n', encoding="utf-8")
    port = free_port()
    flags = ["--host", "127.0.0.1", "--port", str(port), "--name", "card-node"]
    folder = home / ".flowfile-node" / "card-node"
    if joined:
        folder.mkdir(parents=True)
        kept = {"hub_url": "http://127.0.0.1:9", "issuer": "http://127.0.0.1:9", "name": "card-node", "node_id": 4}
        kept.update(joined_at="2026-11-03T09:00:00+00:00", has_core=True)
        (folder / hub_node.FILE_NAME).write_text(json.dumps(kept), encoding="utf-8")
    environment = {name: value for name, value in os.environ.items() if name not in (*STORAGE_VARIABLES, *VARIABLES)}
    environment.update(HOME=str(home), FLOWFILE_WORKER_POOL_SIZE="0")
    log = tmp_path / "worker.log"
    worker = _start([sys.executable, "-m", "flowfile_worker"], flags, environment, tmp_path, log)
    try:
        health = _wait_for_health(worker, port, log)
    finally:
        _stop(worker)

    output = log.read_text(encoding="utf-8")
    found = "Found node card-node in ~/.flowfile-node/card-node"
    assert health["status"] == "ok"
    assert (found in output) is joined
    assert "Keeping this node's files" not in output
    if joined:
        assert sorted(path.name for path in home.iterdir()) == [".flowfile", ".flowfile-node"]
        assert sorted(path.name for path in desktop.iterdir()) == [hub_node.FILE_NAME]
        assert (desktop / hub_node.FILE_NAME).read_text(encoding="utf-8") == '{"name": "the desktop\'s node"}\n'
    else:
        assert not (home / ".flowfile-node").exists()

"""A worker joins a hub with ``--join`` and ``--hub`` before it serves, once, and then takes that hub's tokens; it binds
its port before it joins, is refused without a storage dir of its own and gives the hub ``--url``, else its full name
and port."""

import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

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

VARIABLES = ("FLOWFILE_HUB_JOIN_TOKEN", "FLOWFILE_HUB_URL", "FLOWFILE_NODE_NAME")


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
        (("--hub", "http://h", "--join", "K7QX-M2PD"), None, "127.0.0.1"),
        (("--hub", "http://h"), "K7QX-M2PD", "127.0.0.1"),
        (("--hub", "http://h", "--join", "K7QX-M2PD", "--host", "0.0.0.0"), None, "0.0.0.0"),
        (("--hub", "http://h", "--join", "K7QX-M2PD", "--host", "10.1.2.3"), None, "10.1.2.3"),
    ],
    ids=["no join token", "--join", "the variable", "--host 0.0.0.0", "an address given"],
)
def test_the_worker_binds_this_machine_alone_unless_given_a_host_join_or_not(
    monkeypatch, no_variables, flags, variable, host
):
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


def in_use_reason(port: int) -> str:
    """The OS's own reason a second bind of the held ``port`` on 127.0.0.1 is refused (its wording differs per OS)."""
    with socket.socket() as sock, pytest.raises(OSError) as refused:
        sock.bind(("127.0.0.1", port))
    return refused.value.strerror


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
    assert f"Cannot serve on 127.0.0.1:{port}: {in_use_reason(port)}" in caplog.text
    assert join_hub.calls == []
    assert serving == []
    assert hub_node.join(join_hub.url, token, name="worker-1", worker_url="http://w:1")[0].name == "worker-1"


def test_the_port_the_worker_is_started_with_is_the_one_it_joins_with_and_serves_on(
    monkeypatch, join_hub, node_file, serving, named, caplog
):
    port = free_port()
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)
    default_port(monkeypatch, port)

    with caplog.at_level(logging.INFO):
        main.run(host="127.0.0.1")

    assert served_port(serving) == port
    assert [body["worker_url"] for _, body in join_hub.calls] == [f"http://build-7.example.com:{port}"]
    assert f"Telling the hub this worker is at http://build-7.example.com:{port}" in caplog.text
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


@pytest.fixture
def named(monkeypatch):
    """This machine's name is ``build-7.example.com``, so no lookup is asked of the network."""
    monkeypatch.setattr(hub_node.socket, "gethostname", lambda: "build-7.example.com")
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: "build-7.example.com")


@pytest.mark.parametrize("host", ["0.0.0.0", "10.1.2.3"])
def test_without_a_url_the_worker_gives_its_full_name_and_port_whatever_it_binds(
    monkeypatch, join_hub, node_file, named, caplog, host
):
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)

    with caplog.at_level(logging.INFO):
        main.join_hub(64000, host)

    assert join_hub.calls[0][1]["worker_url"] == "http://build-7.example.com:64000"
    assert "Telling the hub this worker is at http://build-7.example.com:64000" in caplog.text
    assert "only this machine reaches" not in caplog.text


def test_a_worker_on_loopback_warns_that_only_this_machine_reaches_it(monkeypatch, join_hub, node_file, named, caplog):
    started_with(monkeypatch, join_hub.url, join_hub.mint(), url=None)

    with caplog.at_level(logging.WARNING):
        main.join_hub(64000, "127.0.0.1")

    assert join_hub.calls[0][1]["worker_url"] == "http://build-7.example.com:64000"
    assert "This worker serves on 127.0.0.1, which only this machine reaches" in caplog.text
    assert "--host 0.0.0.0" in caplog.text


@pytest.mark.parametrize("host", ["0.0.0.0", "127.0.0.1"])
def test_the_url_given_is_what_the_hub_gets_with_no_warning(monkeypatch, join_hub, node_file, caplog, host):
    monkeypatch.setattr(hub_node.socket, "getfqdn", lambda: pytest.fail("the machine's name is not asked for"))
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
def test_only_the_workers_own_command_line_reads_the_join_flags(monkeypatch, argv, command, launched):
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(sys, "orig_argv", command, raising=False)

    assert flowfile_worker._launched_as_worker() is launched


def test_a_spawned_child_never_reads_the_join_flags(monkeypatch):
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


def _start(command, flags, environment, cwd, log) -> subprocess.Popen:
    with log.open("wb") as out:
        return subprocess.Popen([*command, *flags], env=environment, cwd=cwd, stdout=out, stderr=out)


DESKTOP_NODE = '{"name": "the desktop\'s node"}\n'


def desktop_home(tmp_path) -> Path:
    """A home directory holding Flowfile desktop's ``~/.flowfile`` with a ``hub_node.json`` of its own."""
    home = tmp_path / "home"
    (home / ".flowfile").mkdir(parents=True)
    (home / ".flowfile" / hub_node.FILE_NAME).write_text(DESKTOP_NODE, encoding="utf-8")
    return home


def shell_environment(home: Path, **variables: str) -> dict[str, str]:
    """This process's environment as an operator's shell has it: ``home`` as ``HOME``, none of the storage or hub
    variables, and ``variables`` on top."""
    environment = {name: value for name, value in os.environ.items() if name not in (*STORAGE_VARIABLES, *VARIABLES)}
    environment.update(HOME=str(home), FLOWFILE_WORKER_POOL_SIZE="0", **variables)
    return environment


def node_env(folder: Path, **more: str) -> dict[str, str]:
    """The storage variables the hub's install line writes for a node kept in ``folder``."""
    return {
        "FLOWFILE_STORAGE_DIR": str(folder / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(folder / "user"),
        "FLOWFILE_SHARED_DIR": str(folder / "shared"),
        "FLOWFILE_SECURE_STORAGE_PATH": str(folder / "secure"),
        "SECURE_STORAGE_PATH": str(folder / "secure"),
        **more,
    }


def assert_desktop_untouched(home: Path) -> None:
    assert sorted(path.name for path in home.iterdir()) == [".flowfile"]
    assert [path.name for path in (home / ".flowfile").iterdir()] == [hub_node.FILE_NAME]
    assert (home / ".flowfile" / hub_node.FILE_NAME).read_text(encoding="utf-8") == DESKTOP_NODE


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
@pytest.mark.parametrize(
    "launch,given,missing",
    [
        ("python -m flowfile_worker", {}, "FLOWFILE_STORAGE_DIR and FLOWFILE_SECURE_STORAGE_PATH"),
        ("console script", {}, "FLOWFILE_STORAGE_DIR and FLOWFILE_SECURE_STORAGE_PATH"),
        ("python -m flowfile_worker", {"FLOWFILE_STORAGE_DIR": "{tmp}/storage"}, "FLOWFILE_SECURE_STORAGE_PATH"),
        ("python -m flowfile_worker", {"FLOWFILE_SECURE_STORAGE_PATH": "{tmp}/secure"}, "FLOWFILE_STORAGE_DIR"),
    ],
    ids=["neither", "neither, console script", "no secure store", "no storage dir"],
)
def test_a_join_without_a_storage_dir_and_secure_store_of_its_own_is_refused_before_any_request(
    tmp_path, join_hub, launch, given, missing
):
    command = (
        [shutil.which("flowfile_worker")] if launch == "console script" else [sys.executable, "-m", "flowfile_worker"]
    )
    if command[0] is None:
        pytest.skip("the flowfile_worker console script is not installed")
    home = desktop_home(tmp_path)
    log = tmp_path / "worker.log"
    flags = ["--port", str(free_port()), "--hub", join_hub.url, "--join", join_hub.mint(code=True)]
    environment = shell_environment(home, **{name: value.format(tmp=tmp_path) for name, value in given.items()})
    worker = _start(command, [*flags, "--name", "card-node"], environment, tmp_path, log)
    try:
        exit_code = worker.wait(timeout=90)
    finally:
        _stop(worker)

    output = log.read_text(encoding="utf-8")
    assert exit_code == 2, output
    assert f"Cannot join a hub without a storage dir and a secure store of this node's own: set {missing} to" in output
    assert "~/.flowfile and ~/.config/flowfile" in output
    assert join_hub.calls == []
    assert_desktop_untouched(home)


@pytest.mark.skipif(os.name == "nt", reason="the home directory comes from HOME on POSIX only")
@pytest.mark.parametrize("mode", [{}, {"FLOWFILE_MODE": "docker"}], ids=["a storage dir given", "docker mode"])
def test_started_as_the_install_line_starts_it_the_worker_joins_from_its_own_storage_dir(tmp_path, join_hub, mode):
    home, folder, port = desktop_home(tmp_path), tmp_path / "card-node", free_port()
    flags = ["--host", "0.0.0.0", "--hub", join_hub.url, "--join", join_hub.mint(code=True), "--name", "card-node"]
    flags += ["--port", str(port), "--url", f"http://127.0.0.1:{port}"]
    log = tmp_path / "worker.log"
    environment = shell_environment(home, **node_env(folder, **mode))
    worker = _start([sys.executable, "-m", "flowfile_worker"], flags, environment, tmp_path, log)
    try:
        health = _wait_for_health(worker, port, log)
    finally:
        _stop(worker)

    output = log.read_text(encoding="utf-8")
    assert health["status"] == "ok"
    assert f"Starting worker service on 0.0.0.0:{port}" in output
    (joined,) = [body for path, body in join_hub.calls if path == hub_join.JOIN_PATH]
    assert (joined["name"], joined["worker_url"]) == ("card-node", f"http://127.0.0.1:{port}")
    kept = json.loads((folder / "storage" / hub_node.FILE_NAME).read_text(encoding="utf-8"))
    assert kept["name"] == "card-node"
    assert (folder / "storage" / "hub_node_key.json").is_file()
    assert_desktop_untouched(home)

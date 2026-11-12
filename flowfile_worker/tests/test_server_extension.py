"""The worker asks its server's extension at five points and nowhere else: who else may call it, what runs while it
serves, the socket it serves on, the last step before it serves, and what opens a sealed credential. A spawned child
loads the extension only when it opens one."""

import multiprocessing
import socket
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main, viz_sessions
from shared import server_extension
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER
from shared.tests import extension_child
from shared.tests import toy_server_extension as toy

pytestmark = pytest.mark.worker

client = TestClient(main.app)
TOY = "shared.tests.toy_server_extension"


def signed(token: str) -> dict:
    return {"X-Flowfile-Internal": token, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}


def test_a_token_the_extension_vouches_for_calls_the_worker(monkeypatch):
    asked = []
    toy.use(monkeypatch, SimpleNamespace(caller_claims=lambda token: asked.append(token) or object()))

    assert client.get("/v1/capabilities", headers=signed("a.signed.token")).status_code == 200
    assert asked == ["a.signed.token"]


def test_a_token_the_extension_refuses_is_a_401_naming_its_issuer_and_the_reason(monkeypatch):
    def refuse(token):
        raise server_extension.Refused("wrong audience")

    toy.use(monkeypatch, SimpleNamespace(caller_claims=refuse, caller_issuer="Toy"))

    refused = client.get("/v1/capabilities", headers=signed("a.signed.token"))

    assert refused.status_code == 401
    assert refused.json()["detail"] == "Toy token refused: wrong audience"


@pytest.mark.parametrize("extension", [None, SimpleNamespace(caller_claims=lambda token: None)], ids=["none", "no"])
def test_a_token_nobody_vouches_for_is_refused_as_any_wrong_token_is(monkeypatch, extension):
    toy.use(monkeypatch, extension)

    refused = client.get("/v1/capabilities", headers=signed("a.signed.token"))

    assert refused.status_code == 401
    assert refused.json()["detail"] == "Invalid or missing internal token"


@pytest.fixture
def stubbed_server(monkeypatch):
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
    return served


def test_the_worker_serves_on_the_socket_its_extension_bound_after_its_last_step(monkeypatch, stubbed_server):
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))
    port = bound.getsockname()[1]
    steps = []
    extension = SimpleNamespace(
        bind=lambda service, host, port: steps.append(("bind", service, host, port)) or bound,
        before_serving=lambda service, host, port, app: steps.append(("before_serving", service, host, port)),
    )
    toy.use(monkeypatch, extension)

    with bound:
        main.run(host="127.0.0.1", port=port)

        ((config, sockets),) = stubbed_server
        assert sockets == [bound]
    assert steps == [("bind", "worker", "127.0.0.1", port), ("before_serving", "worker", "127.0.0.1", port)]


def test_a_last_step_that_stops_the_worker_closes_the_socket_and_serves_nothing(monkeypatch, stubbed_server):
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))

    def refuse(service, host, port, app):
        raise SystemExit(2)

    toy.use(monkeypatch, SimpleNamespace(bind=lambda service, host, port: bound, before_serving=refuse))

    with pytest.raises(SystemExit) as stopped:
        main.run(host="127.0.0.1", port=bound.getsockname()[1])

    assert stopped.value.code == 2
    assert stubbed_server == []
    assert bound.fileno() == -1


def test_without_an_extension_uvicorn_binds_the_port_as_it_always_has(monkeypatch, stubbed_server):
    toy.use(monkeypatch, None)

    main.run(host="127.0.0.1", port=64001)

    ((config, sockets),) = stubbed_server
    assert (config.host, config.port, sockets) == ("127.0.0.1", 64001, None)


def test_a_storage_dir_marked_for_another_extension_is_not_served(monkeypatch, stubbed_server, tmp_path):
    marker = tmp_path / server_extension.MARKER_FILE
    marker.write_text("an_extension_not_loaded\n", encoding="utf-8")
    monkeypatch.setattr(server_extension, "_marker", lambda: marker)
    toy.use(monkeypatch, None)

    with pytest.raises(SystemExit) as stopped:
        main.run(host="127.0.0.1", port=64001)

    assert stopped.value.code == 2
    assert stubbed_server == []


def test_what_the_extension_starts_runs_while_the_worker_serves_and_is_stopped_after(monkeypatch):
    monkeypatch.setattr(main, "task_pool", SimpleNamespace(prewarm=lambda: None, shutdown=lambda: None))
    monkeypatch.setattr(main, "mp_context", SimpleNamespace(active_children=lambda: []))
    monkeypatch.setattr(main, "storage", SimpleNamespace(cleanup_directories=lambda: None))
    monkeypatch.setattr(viz_sessions, "viz_session_registry", SimpleNamespace(shutdown=lambda: None))
    monkeypatch.setattr(main, "serving_port", 64001)
    seen = []
    extension = SimpleNamespace(
        serving=lambda service, port, app: seen.append(("serving", service, port)) or (lambda: seen.append("stopped"))
    )
    toy.use(monkeypatch, extension)

    with TestClient(main.app):
        assert seen == [("serving", "worker", 64001)]

    assert seen == [("serving", "worker", 64001), "stopped"]


def test_a_spawned_child_loads_the_extension_only_to_open_a_sealed_credential(monkeypatch):
    monkeypatch.setenv(server_extension.VARIABLE, TOY)
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    child = context.Process(target=extension_child.report, args=(queue,))

    child.start()
    try:
        loaded_at_import, loaded, opened = queue.get(timeout=120)
    finally:
        child.join(timeout=30)

    assert loaded_at_import is False
    assert (loaded, opened) == (TOY, "opened by the toy")

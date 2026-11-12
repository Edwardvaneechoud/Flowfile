"""Core asks its server's extension how people sign in, who it is to a controller, which routes to add and what to do
around serving; with none it answers as a Flowfile on its own does."""

import asyncio
import os
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import flowfile_core.main as core_main
from flowfile_core.routes import auth as auth_routes
from flowfile_core.routes import public as public_routes
from shared import server_extension
from shared.server_extension import Identity
from shared.tests import toy_server_extension as toy

TOY = "shared.tests.toy_server_extension"


def test_people_sign_in_with_a_password_unless_the_extension_says_how(monkeypatch):
    toy.use(monkeypatch, None)
    assert auth_routes.sign_in_method() == {"method": "password"}
    auth_routes._refuse_passwords()

    elsewhere = {"method": "hub", "url": "http://hub.example/open/3", "hub_url": "http://hub.example"}
    extension = SimpleNamespace(
        sign_in_method=lambda: elsewhere, password_refusal=lambda: "This server signs people in through hub.example"
    )
    toy.use(monkeypatch, extension)

    assert auth_routes.sign_in_method() == elsewhere
    with pytest.raises(HTTPException) as refused:
        auth_routes._refuse_passwords()
    assert (refused.value.status_code, refused.value.detail) == (403, "This server signs people in through hub.example")


def test_the_setup_status_names_the_server_its_storage_dir_keeps(monkeypatch):
    toy.use(monkeypatch, None)
    assert asyncio.run(public_routes.get_setup_status()).node is None

    kept = Identity(name="team-1", url="http://hub.example", issuer="http://hub.example", recorded_name="team-1")
    toy.use(monkeypatch, SimpleNamespace(identity=lambda: kept))
    assert asyncio.run(public_routes.get_setup_status()).node == "team-1"

    told = Identity(name="team-1", url="http://hub.example", issuer="http://hub.example")
    toy.use(monkeypatch, SimpleNamespace(identity=lambda: told))
    assert asyncio.run(public_routes.get_setup_status()).node is None


@pytest.fixture
def stubbed_server(monkeypatch):
    """``run`` up to the point it serves: the server records the sockets it is handed, and the process is not marked
    as serving."""
    served = []

    class Server:
        def __init__(self, config):
            self.config = config

        def run(self, sockets=None):
            served.append((self.config, sockets))

    monkeypatch.setattr(core_main, "_Server", Server)
    monkeypatch.setattr(core_main.signal, "signal", lambda *args: None)
    monkeypatch.setattr(core_main, "start_parent_death_watcher", lambda stop: None)
    monkeypatch.setattr(core_main, "install_access_log_redaction", lambda: None)
    monkeypatch.setattr(core_main.compute_registry, "mark_serving", lambda: None)
    return served


def test_core_serves_on_the_socket_its_extension_bound_after_its_last_step(monkeypatch, stubbed_server):
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))
    port = bound.getsockname()[1]
    steps = []
    extension = SimpleNamespace(
        bind=lambda service, host, port: steps.append(("bind", service, host, port)) or bound,
        before_serving=lambda service, host, port, app: steps.append(("before_serving", service, host, port, app)),
    )
    toy.use(monkeypatch, extension)

    with bound:
        core_main.run(host="127.0.0.1", port=port)

        ((config, sockets),) = stubbed_server
        assert sockets == [bound]
    assert steps == [
        ("bind", "core", "127.0.0.1", port),
        ("before_serving", "core", "127.0.0.1", port, core_main.app),
    ]
    assert core_main.serving_port is None


def test_a_socket_the_caller_bound_is_not_bound_again(monkeypatch, stubbed_server):
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))
    asked = []
    toy.use(monkeypatch, SimpleNamespace(bind=lambda *args: asked.append(args)))

    with bound:
        core_main.run(host="127.0.0.1", port=bound.getsockname()[1], sock=bound)

    assert asked == []
    assert stubbed_server[0][1] == [bound]


def test_a_last_step_that_stops_core_closes_the_socket_and_serves_nothing(monkeypatch, stubbed_server):
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))

    def refuse(service, host, port, app):
        raise SystemExit(2)

    toy.use(monkeypatch, SimpleNamespace(bind=lambda service, host, port: bound, before_serving=refuse))

    with pytest.raises(SystemExit) as stopped:
        core_main.run(host="127.0.0.1", port=bound.getsockname()[1])

    assert stopped.value.code == 2
    assert stubbed_server == []
    assert bound.fileno() == -1


def test_a_storage_dir_marked_for_another_extension_is_not_served(monkeypatch, stubbed_server, tmp_path):
    marker = tmp_path / server_extension.MARKER_FILE
    marker.write_text("an_extension_not_loaded\n", encoding="utf-8")
    monkeypatch.setattr(server_extension, "_marker", lambda: marker)
    toy.use(monkeypatch, None)

    with pytest.raises(SystemExit) as stopped:
        core_main.run(host="127.0.0.1", port=64002)

    assert stopped.value.code == 2
    assert stubbed_server == []


@pytest.mark.parametrize("named,mounted", [(TOY, True), (None, False)], ids=["named", "not named"])
def test_core_mounts_the_routes_of_the_extension_its_configuration_names_and_of_no_other(tmp_path, named, mounted):
    code = (
        "import logging\n"
        "logging.getLogger('passlib').setLevel(logging.ERROR)\n"
        "import flowfile_core.main as core_main\n"
        "print('/toy/hello' in core_main.app.openapi()['paths'])\n"
    )
    env = {
        **os.environ,
        "FLOWFILE_STORAGE_DIR": str(tmp_path / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(tmp_path / "user"),
        "FLOWFILE_DB_PATH": str(tmp_path / "catalog.db"),
        "FLOWFILE_SECURE_STORAGE_PATH": str(tmp_path / "secure"),
        "FLOWFILE_TELEMETRY": "0",
        "TESTING": "True",
    }
    env.pop(server_extension.VARIABLE, None)
    if named:
        env[server_extension.VARIABLE] = named

    done = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=600)

    assert done.returncode == 0, done.stderr
    assert done.stdout.split()[-1] == str(mounted)

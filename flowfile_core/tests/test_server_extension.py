"""Core asks its server's extension how people sign in, who it is to a controller, which routes to add and what to do
around serving; with none it answers as a Flowfile on its own does."""

import asyncio
import importlib
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
RELIED_ON = {
    "flowfile_core.auth.jwt": ("create_access_token", "create_refresh_token", "invalidate_user_cache"),
    "flowfile_core.auth.models": ("Token",),
    "flowfile_core.auth.password": ("get_password_hash",),
    "flowfile_core.auth.sharing": ("INTERNAL_SERVICE_USERNAME",),
    "flowfile_core.catalog": ("SQLAlchemyCatalogRepository",),
    "flowfile_core.catalog.services.runs": ("FlowRunService",),
    "flowfile_core.configs": ("logger",),
    "flowfile_core.configs.flow_logger": ("get_flow_log_file",),
    "flowfile_core.configs.settings": ("SERVER_HOST", "SERVER_PORT", "WORKER_PORT"),
    "flowfile_core.database.connection": ("engine", "get_db", "get_db_context"),
    "flowfile_core.database.models": ("Base", "FlowRun", "User"),
    "flowfile_core.flowfile.manage.ephemeral_graph": ("ephemeral_flow_graph",),
    "flowfile_core.hub_client.client": (
        "HubRefused",
        "HubUnreachable",
        "HubVersionRefused",
        "checkout",
        "http_client",
    ),
    "flowfile_core.hub_client.exchange_store": ("store_for_run",),
    "flowfile_core.hub_client.node_addresses": ("sync_for",),
    "flowfile_core.hub_client.node_tokens": ("acting_for",),
    "flowfile_core.hub_client.report": ("build_run_report", "hub_flow_uuid", "hub_target", "post_run_report"),
    "flowfile_core.hub_client.session": (
        "access_token",
        "clear",
        "load",
        "refresh_delegation",
        "save_delegation",
    ),
    "flowfile_core.schemas.schemas": ("FlowfileData", "HubOrigin"),
    "flowfile_worker.configs": ("SERVICE_HOST", "SERVICE_PORT"),
    "shared._version": ("get_version",),
    "shared.net": ("is_loopback_host",),
    "shared.run_logs": ("HUB_RUN_LOG_PREFIX", "run_log_path"),
    "shared.sealed": ("SealedCredentialRefused", "b64url", "unb64url"),
    "shared.server_extension": ("Identity", "MARKER_FILE", "Refused", "mark_storage_dir"),
    "shared.storage_config": ("storage",),
}
"""The names a server's extension is known to use, by module. Renaming one stops that server, so it fails here first;
an extension's own tests keep this list complete."""


@pytest.mark.parametrize("module", sorted(RELIED_ON))
def test_the_names_a_server_s_extension_relies_on_are_there(module):
    loaded = importlib.import_module(module)

    assert [name for name in RELIED_ON[module] if not hasattr(loaded, name)] == []


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


def test_core_serves_on_the_socket_its_extension_s_last_step_answers(monkeypatch, stubbed_server):
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))
    port = bound.getsockname()[1]
    steps = []
    extension = SimpleNamespace(
        before_serving=lambda service, host, port, app: steps.append((service, host, port, app)) or bound
    )
    toy.use(monkeypatch, extension)

    with bound:
        core_main.run(host="127.0.0.1", port=port)

        ((config, sockets),) = stubbed_server
        assert sockets == [bound]
    assert steps == [("core", "127.0.0.1", port, core_main.app)]
    assert core_main.serving_port is None


def test_without_a_socket_from_its_extension_uvicorn_binds_core_s_port_as_it_always_has(monkeypatch, stubbed_server):
    toy.use(monkeypatch, SimpleNamespace(before_serving=lambda service, host, port, app: None))

    core_main.run(host="127.0.0.1", port=64002)

    ((config, sockets),) = stubbed_server
    assert (config.host, config.port, sockets) == ("127.0.0.1", 64002, None)


def test_a_last_step_that_stops_core_serves_nothing(monkeypatch, stubbed_server):
    def refuse(service, host, port, app):
        raise SystemExit(2)

    toy.use(monkeypatch, SimpleNamespace(before_serving=refuse))

    with pytest.raises(SystemExit) as stopped:
        core_main.run(host="127.0.0.1", port=64002)

    assert stopped.value.code == 2
    assert stubbed_server == []


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

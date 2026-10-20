"""The /compute_backends admin routes: list, add, remove and handshake the workers core reaches by id.

A configured worker here is a fake: a small FastAPI app on a local uvicorn that answers ``GET /capabilities`` and
``GET /v1/capabilities`` only to the token it was started with, so a verdict that passes proves the row's own token was
sent, not the process's. The handshake cases, and the refusal of a token that is no header value, run under both
states of the protocol flag. Auth fixtures mirror ``test_system_worker.py``.
"""

import socket
import threading
import time
from collections.abc import Callable, Iterator

import polars as pl
import pytest
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import TOKEN_RULE
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared.compute_protocol import local_capabilities

ROUTE = "/compute_backends"
NO_HEADER_VALUES = pytest.mark.parametrize(
    "token",
    ["row-token\n", "row-token\r\n", "row-token\x00", "row-token\u20ac"],
    ids=["line-feed", "crlf", "nul", "beyond-latin-1"],
)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def client_as(user: PydanticUser) -> Iterator[TestClient]:
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
        main.app.dependency_overrides.pop(get_current_user, None)


@pytest.fixture
def admin_client() -> Iterator[TestClient]:
    yield from client_as(
        PydanticUser(username="local_user", id=1, disabled=False, is_admin=True, must_change_password=False)
    )


@pytest.fixture
def non_admin_client() -> Iterator[TestClient]:
    yield from client_as(
        PydanticUser(username="not_admin", id=2, disabled=False, is_admin=False, must_change_password=False)
    )


@pytest.fixture(params=[False, True], ids=["flag-off", "flag-on"])
def flag(request, monkeypatch) -> bool:
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", request.param)
    return request.param


@pytest.fixture(autouse=True)
def no_configured_backends() -> Iterator[None]:
    """Every row a test configured is removed when it ends, and the registry forgets the backend made from it."""
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            registry.forget(row.id)
            db.delete(row)
        db.commit()


@pytest.fixture
def fake_worker() -> Iterator[Callable[..., str]]:
    """Start a worker that answers its capabilities only to ``token``, as this process's with ``changes``; its URL."""
    servers: list[tuple[uvicorn.Server, threading.Thread]] = []

    def start(token: str, **changes) -> str:
        reported = local_capabilities().model_copy(update=changes)
        app = FastAPI()

        @app.get("/capabilities")
        @app.get("/v1/capabilities")
        def capabilities(request: Request) -> dict:
            if request.headers.get("X-Flowfile-Internal") != token:
                raise HTTPException(status_code=401, detail="Invalid or missing internal token")
            return reported.model_dump(mode="json")

        port = free_port()
        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        servers.append((server, thread))
        deadline = time.monotonic() + 30
        while not server.started:
            if time.monotonic() > deadline:
                pytest.fail("the fake worker never became ready")
            time.sleep(0.05)
        return f"http://127.0.0.1:{port}"

    yield start
    for server, thread in servers:
        server.should_exit = True
        thread.join(timeout=15)


def add(client: TestClient, url: str, name: str = "team", token: str = "row-token"):
    return client.post(ROUTE, json={"name": name, "url": url, "token": token})


def handshake(client: TestClient, backend_id: str = "worker:team") -> dict:
    response = client.post(f"{ROUTE}/{backend_id}/handshake")
    assert response.status_code == 200, response.text
    return response.json()


def test_no_backend_is_configured_at_first(admin_client):
    response = admin_client.get(ROUTE)

    assert response.status_code == 200, response.text
    assert response.json() == []


def test_an_added_worker_is_listed_by_id_and_url_and_its_token_is_stored_encrypted(admin_client):
    added = add(admin_client, "http://team-worker.test:63579/")

    assert added.status_code == 201, added.text
    assert added.json() == {"id": "worker:team", "url": "http://team-worker.test:63579"}
    with get_db_context() as db:
        stored = db.get(ConfiguredBackend, "worker:team").token
    assert stored.startswith("$ffsec$")
    assert "row-token" not in stored
    listed = admin_client.get(ROUTE).json()
    assert listed == [{"id": "worker:team", "url": "http://team-worker.test:63579"}]
    assert "row-token" not in admin_client.get(ROUTE).text


def test_adding_an_id_that_exists_is_a_conflict(admin_client):
    assert add(admin_client, "http://team-worker.test:63579").status_code == 201

    again = add(admin_client, "http://other-worker.test:63579", token="other-token")

    assert again.status_code == 409, again.text
    assert admin_client.get(ROUTE).json() == [{"id": "worker:team", "url": "http://team-worker.test:63579"}]


@pytest.mark.parametrize(
    ("name", "url"),
    [("my team", "http://team-worker.test:63579"), ("team", "ftp://team-worker.test")],
    ids=["name-with-a-space", "ftp-url"],
)
def test_a_bad_name_or_url_is_refused(admin_client, name, url):
    response = add(admin_client, url, name=name)

    assert response.status_code == 422, response.text
    assert admin_client.get(ROUTE).json() == []


@NO_HEADER_VALUES
def test_a_token_that_is_no_header_value_is_refused_naming_the_rule_never_the_token(admin_client, flag, token):
    response = add(admin_client, "http://team-worker.test:63579", token=token)

    assert response.status_code == 422, response.text
    assert response.json() == {"detail": f"The token was not stored: {TOKEN_RULE}"}
    assert "row-token" not in response.text
    assert admin_client.get(ROUTE).json() == []


@NO_HEADER_VALUES
def test_a_row_whose_token_is_no_header_value_fails_the_handshake_naming_the_rule_never_the_token(
    admin_client, flag, token
):
    """A row filled in by hand skips the add route's check; the backend refuses to send its token instead."""
    with get_db_context() as db:
        db.add(
            ConfiguredBackend(id="worker:team", url=f"http://127.0.0.1:{free_port()}", token=encrypt_secret(token, 1))
        )
        db.commit()

    response = admin_client.post(f"{ROUTE}/worker:team/handshake")

    assert response.status_code == 200, response.text
    verdict = response.json()
    assert verdict["ok"] is False
    assert verdict["worker"] is None
    assert verdict["error"] == f"The token configured for the worker:team backend cannot be sent: {TOKEN_RULE}"
    assert "row-token" not in response.text


def test_a_worker_on_this_process_versions_passes_the_handshake_with_the_rows_token(admin_client, fake_worker, flag):
    add(admin_client, fake_worker("row-token"))

    verdict = handshake(admin_client)

    assert verdict["ok"] is True, verdict
    assert verdict["differences"] == []
    assert verdict["error"] is None
    assert verdict["worker"]["polars_version"] == verdict["core"]["polars_version"] == pl.__version__


def test_a_worker_on_other_versions_fails_the_handshake_naming_both_sides(admin_client, fake_worker, flag):
    add(admin_client, fake_worker("row-token", polars_version="0.0.1"))

    verdict = handshake(admin_client)

    assert verdict["ok"] is False
    assert verdict["differences"] == [f"polars_version: core {pl.__version__}, worker 0.0.1"]
    assert verdict["worker"]["polars_version"] == "0.0.1"
    assert verdict["error"] is None


def test_a_worker_that_refuses_the_rows_token_fails_the_handshake_naming_401(admin_client, fake_worker, flag):
    add(admin_client, fake_worker("another-token"))

    verdict = handshake(admin_client)

    assert verdict["ok"] is False
    assert verdict["worker"] is None
    assert "401" in verdict["error"]
    assert "row-token" not in verdict["error"]


def test_a_worker_out_of_reach_fails_the_handshake_naming_it(admin_client, flag):
    url = f"http://127.0.0.1:{free_port()}"
    add(admin_client, url)

    verdict = handshake(admin_client)

    assert verdict["ok"] is False
    assert verdict["worker"] is None
    assert verdict["error"].startswith(f"Could not reach the worker at {url}")
    assert verdict["error"].endswith('Is backend "worker:team" running? Start it, or check the Backends page.')


def test_a_removed_worker_is_gone_from_the_list_and_the_registry(admin_client, fake_worker):
    add(admin_client, fake_worker("row-token"))
    assert handshake(admin_client)["ok"] is True

    removed = admin_client.delete(f"{ROUTE}/worker:team")

    assert removed.status_code == 204, removed.text
    assert admin_client.get(ROUTE).json() == []
    with pytest.raises(LookupError, match="worker:team"):
        registry.backend("worker:team")
    assert admin_client.delete(f"{ROUTE}/worker:team").status_code == 404
    assert admin_client.post(f"{ROUTE}/worker:team/handshake").status_code == 404


def test_a_non_admin_is_refused_every_route(non_admin_client):
    assert non_admin_client.get(ROUTE).status_code == 403
    assert add(non_admin_client, "http://team-worker.test:63579").status_code == 403
    assert non_admin_client.delete(f"{ROUTE}/worker:team").status_code == 403
    assert non_admin_client.post(f"{ROUTE}/worker:team/handshake").status_code == 403


def test_an_unauthenticated_caller_gets_401():
    assert TestClient(main.app).get(ROUTE).status_code == 401

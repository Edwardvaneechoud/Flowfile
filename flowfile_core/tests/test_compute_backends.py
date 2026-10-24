"""The /compute_backends routes: list, add, remove and handshake the workers core reaches by id.

Every user lists the ids and the last verdicts; the URL, adding, removing and the handshake are an administrator's. A
configured worker here is a fake: a small FastAPI app on a local uvicorn that answers ``GET /capabilities`` and
``GET /v1/capabilities`` only to the token it was started with, so a verdict that passes proves the row's own token was
sent, not the process's; it answers nothing else, so no probe plan runs there. A row at the session worker's URL is a
worker every probe runs on. The handshake cases, and the refusal of a token that is no header value, run under both
states of the protocol flag. Auth fixtures mirror ``test_system_worker.py``.
"""

import socket
import ssl
import threading
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user, get_internal_token
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import TOKEN_RULE
from flowfile_core.flowfile.execution.placement import PYTHON_FUNCTIONS
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake as checks
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import subprocess_operations as subops
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared.compute_protocol import EXPRESSION_PLUGINS, local_capabilities

ROUTE = "/compute_backends"
NO_HEADER_VALUES = pytest.mark.parametrize(
    "token",
    ["row-token\n", "row-token\r\n", "row-token\x00", "row-token\u20ac"],
    ids=["line-feed", "crlf", "nul", "beyond-latin-1"],
)
OLDER_ANSWER = {
    **local_capabilities().model_dump(
        mode="json", exclude={"operations", "read_schemes", "write_schemes", "credential_resolution"}
    ),
    "app_version": "0.23.0",
}
"""The capabilities of a worker built before it declared its operations, schemes and credential resolution."""


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
    """Every row a test configured is removed when it ends, the registry forgets the backend made from it and no
    backend's verdict is kept."""
    yield
    with get_db_context() as db:
        for row in db.query(ConfiguredBackend).all():
            registry.forget(row.id)
            db.delete(row)
        db.commit()
    checks.reset_handshake()


@pytest.fixture
def fake_worker() -> Iterator[Callable[..., str]]:
    """Start a worker that answers its capabilities only to ``token``, as this process's with ``changes``; its URL.

    Given ``answer``, the worker answers that instead, as it is.
    """
    servers: list[tuple[uvicorn.Server, threading.Thread]] = []

    def start(token: str, *, answer: dict | None = None, **changes) -> str:
        reported = local_capabilities().model_copy(update=changes)
        app = FastAPI()

        @app.get("/capabilities")
        @app.get("/v1/capabilities")
        def capabilities(request: Request) -> dict:
            if request.headers.get("X-Flowfile-Internal") != token:
                raise HTTPException(status_code=401, detail="Invalid or missing internal token")
            return reported.model_dump(mode="json") if answer is None else answer

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


def add(
    client: TestClient,
    url: str,
    name: str = "team",
    token: str = "row-token",
    ca_cert: str | None = None,
    shares_core_disk: bool | None = None,
):
    body = {"name": name, "url": url, "token": token}
    if ca_cert is not None:
        body["ca_cert"] = ca_cert
    if shares_core_disk is not None:
        body["shares_core_disk"] = shares_core_disk
    return client.post(ROUTE, json=body)


def self_signed_pem() -> str:
    """A self-signed P-256 certificate, PEM: the CA of a worker that serves it."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "team-worker.test")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=2))
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


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
    assert added.json() == {"id": "worker:team", "url": "http://team-worker.test:63579", "shares_core_disk": False, "verdict": None}
    with get_db_context() as db:
        stored = db.get(ConfiguredBackend, "worker:team").token
    assert stored.startswith("$ffsec$")
    assert "row-token" not in stored
    listed = admin_client.get(ROUTE).json()
    assert listed == [{"id": "worker:team", "url": "http://team-worker.test:63579", "shares_core_disk": False, "verdict": None}]
    assert "row-token" not in admin_client.get(ROUTE).text


def test_adding_an_id_that_exists_is_a_conflict(admin_client):
    assert add(admin_client, "http://team-worker.test:63579").status_code == 201

    again = add(admin_client, "http://other-worker.test:63579", token="other-token")

    assert again.status_code == 409, again.text
    assert admin_client.get(ROUTE).json() == [
        {"id": "worker:team", "url": "http://team-worker.test:63579", "shares_core_disk": False, "verdict": None}
    ]


@pytest.mark.parametrize(
    ("name", "url"),
    [("my team", "http://team-worker.test:63579"), ("team", "ftp://team-worker.test")],
    ids=["name-with-a-space", "ftp-url"],
)
def test_a_bad_name_or_url_is_refused(admin_client, name, url):
    response = add(admin_client, url, name=name)

    assert response.status_code == 422, response.text
    assert admin_client.get(ROUTE).json() == []


def test_a_body_that_fails_validation_is_answered_without_its_input(admin_client):
    response = admin_client.post(ROUTE, json={"name": "team", "token": "row-token"})

    assert response.status_code == 422, response.text
    assert response.json() == {"detail": [{"type": "missing", "loc": ["body", "url"], "msg": "Field required"}]}
    assert "row-token" not in response.text
    assert admin_client.get(ROUTE).json() == []


def test_an_added_worker_keeps_its_ca_certificate_and_is_listed_without_it(admin_client):
    pem = self_signed_pem()

    added = add(admin_client, "https://team-worker.test:63579", ca_cert=pem)

    assert added.status_code == 201, added.text
    assert added.json() == {"id": "worker:team", "url": "https://team-worker.test:63579", "shares_core_disk": False, "verdict": None}
    with get_db_context() as db:
        assert db.get(ConfiguredBackend, "worker:team").ca_cert == pem
    listed = admin_client.get(ROUTE)
    assert listed.json() == [{"id": "worker:team", "url": "https://team-worker.test:63579", "shares_core_disk": False, "verdict": None}]
    assert "BEGIN CERTIFICATE" not in listed.text


def test_an_empty_ca_certificate_is_stored_as_none(admin_client):
    added = add(admin_client, "http://team-worker.test:63579", ca_cert="")

    assert added.status_code == 201, added.text
    with get_db_context() as db:
        assert db.get(ConfiguredBackend, "worker:team").ca_cert is None


def test_a_worker_marked_as_seeing_cores_files_is_stored_and_listed_so(admin_client):
    added = add(admin_client, "http://team-worker.test:63579", shares_core_disk=True)

    assert added.status_code == 201, added.text
    assert added.json()["shares_core_disk"] is True
    with get_db_context() as db:
        assert db.get(ConfiguredBackend, "worker:team").shares_core_disk is True
    assert admin_client.get(ROUTE).json() == [
        {"id": "worker:team", "url": "http://team-worker.test:63579", "shares_core_disk": True, "verdict": None}
    ]


@pytest.mark.parametrize("shares_core_disk", [False, True], ids=["unmarked", "marked"])
def test_a_marked_worker_is_offered_local_files_it_does_not_declare(admin_client, fake_worker, flag, shares_core_disk):
    url = fake_worker("row-token", read_schemes=frozenset({"s3"}), write_schemes=frozenset({"s3"}))
    add(admin_client, url, shares_core_disk=shares_core_disk)

    verdict = handshake(admin_client)

    assert verdict["ok"] is True, verdict
    assert ("file" in verdict["offered"]["write_schemes"]) is shares_core_disk
    assert ("file" in verdict["offered"]["read_schemes"]) is shares_core_disk


def test_a_ca_certificate_that_is_no_pem_is_refused_and_nothing_is_stored(admin_client):
    response = add(admin_client, "https://team-worker.test:63579", ca_cert="not a certificate")

    assert response.status_code == 422, response.text
    assert response.json() == {
        "detail": "The CA certificate was not stored: it must be PEM, -----BEGIN CERTIFICATE----- blocks"
    }
    assert admin_client.get(ROUTE).json() == []


def test_a_ca_certificate_with_an_http_url_is_refused_and_nothing_is_stored(admin_client):
    response = add(admin_client, "http://team-worker.test:63579", ca_cert=self_signed_pem())

    assert response.status_code == 422, response.text
    assert response.json() == {"detail": "The CA certificate was not stored: it applies to an https:// worker URL only"}
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
    assert verdict["backend"] is None
    assert verdict["error"] == f"The token configured for the worker:team backend cannot be sent: {TOKEN_RULE}"
    assert "row-token" not in response.text


def test_a_worker_on_this_process_versions_passes_the_handshake_with_the_rows_token_and_runs_no_probe(
    admin_client, fake_worker, flag
):
    add(admin_client, fake_worker("row-token"))

    verdict = handshake(admin_client)

    assert verdict["ok"] is True, verdict
    assert verdict["differences"] == []
    assert verdict["error"] is None
    assert verdict["backend"]["polars_version"] == verdict["core"]["polars_version"] == pl.__version__
    assert sorted(verdict["absent"]) == sorted([*EXPRESSION_PLUGINS, PYTHON_FUNCTIONS])
    for plugin in EXPRESSION_PLUGINS:
        assert verdict["absent"][plugin].startswith("this backend could not load from the path core's plans carry")
        assert plugin not in verdict["offered"]["plugins"]
    assert verdict["absent"][PYTHON_FUNCTIONS].startswith("core's Python functions do not run on this backend")


def test_a_row_at_the_session_worker_passes_the_handshake_with_every_probe(admin_client, flag):
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")
    add(admin_client, registry.worker_url(), token=get_internal_token())

    verdict = handshake(admin_client)

    assert verdict["ok"] is True, verdict
    assert verdict["absent"] == {}
    assert set(EXPRESSION_PLUGINS) <= verdict["offered"]["plugins"].keys()
    listed = admin_client.get(ROUTE).json()
    assert listed == [{"id": "worker:team", "url": registry.worker_url(), "shares_core_disk": False, "verdict": verdict}]


def test_a_worker_on_other_versions_fails_the_handshake_naming_both_sides(admin_client, fake_worker, flag):
    add(admin_client, fake_worker("row-token", polars_version="0.0.1"))

    verdict = handshake(admin_client)

    assert verdict["ok"] is False
    assert verdict["differences"] == [f"polars_version: core {pl.__version__}, backend 0.0.1"]
    assert verdict["backend"]["polars_version"] == "0.0.1"
    assert verdict["error"] == (
        f'Backend "worker:team" does not match core (polars_version: core {pl.__version__}, backend 0.0.1)'
    )


def test_a_worker_of_an_earlier_release_fails_the_handshake_naming_both_app_versions(admin_client, fake_worker, flag):
    url = fake_worker("row-token", answer=OLDER_ANSWER)
    add(admin_client, url)

    verdict = handshake(admin_client)

    assert verdict["ok"] is False
    assert verdict["backend"] is None
    assert verdict["differences"] == []
    assert verdict["error"] == (
        f"The worker at {url} does not match core (app_version: core {local_capabilities().app_version}, "
        "worker 0.23.0): its capabilities answer is not one this release reads (credential_resolution: Field required)"
    )


def test_a_url_that_is_no_worker_fails_the_handshake_as_unavailable(admin_client, fake_worker, flag):
    url = fake_worker("row-token", answer={"status": "ok"})
    add(admin_client, url)

    verdict = handshake(admin_client)

    assert verdict["ok"] is False
    assert verdict["backend"] is None
    assert verdict["differences"] == []
    assert verdict["error"] == (
        f"The server at {url} is not a Flowfile worker: its capabilities answer carries no app_version"
    )


def test_a_worker_that_refuses_the_rows_token_fails_the_handshake_naming_401(admin_client, fake_worker, flag):
    add(admin_client, fake_worker("another-token"))

    verdict = handshake(admin_client)

    assert verdict["ok"] is False
    assert verdict["backend"] is None
    assert "401" in verdict["error"]
    assert "row-token" not in verdict["error"]


def test_a_worker_out_of_reach_fails_the_handshake_naming_it(admin_client, flag):
    url = f"http://127.0.0.1:{free_port()}"
    add(admin_client, url)

    verdict = handshake(admin_client)

    assert verdict["ok"] is False
    assert verdict["backend"] is None
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


def test_a_worker_removed_and_added_again_is_trusted_as_its_new_row_says(admin_client):
    url = "https://team-worker.test:63579"
    add(admin_client, url, ca_cert=self_signed_pem())
    registry.backend("worker:team")
    first = subops.ssl_context_for(url)
    assert isinstance(first, ssl.SSLContext)

    admin_client.delete(f"{ROUTE}/worker:team")
    add(admin_client, url, ca_cert=self_signed_pem())
    registry.backend("worker:team")
    second = subops.ssl_context_for(url)
    assert isinstance(second, ssl.SSLContext)
    assert second is not first

    admin_client.delete(f"{ROUTE}/worker:team")
    add(admin_client, url)
    registry.backend("worker:team")
    assert subops.ssl_context_for(url) is None
    assert not isinstance(subops._worker_session.get_adapter(f"{url}/capabilities"), subops._TrustedCa)


def test_a_non_admin_lists_the_ids_and_last_verdicts_without_the_urls_and_is_refused_every_other_route(
    non_admin_client, fake_worker
):
    url = fake_worker("row-token")
    with get_db_context() as db:
        db.add(ConfiguredBackend(id="worker:team", url=url, token=encrypt_secret("row-token", 1)))
        db.add(ConfiguredBackend(id="worker:unchecked", url=url, token=encrypt_secret("row-token", 1)))
        db.commit()
    verdict = checks.verify(registry.backend("worker:team"))

    listed = non_admin_client.get(ROUTE)

    assert listed.status_code == 200, listed.text
    assert listed.json() == [
        {"id": "worker:team", "url": None, "shares_core_disk": False, "verdict": verdict.model_dump(mode="json")},
        {"id": "worker:unchecked", "url": None, "shares_core_disk": False, "verdict": None},
    ]
    assert add(non_admin_client, "http://team-worker.test:63579").status_code == 403
    assert non_admin_client.delete(f"{ROUTE}/worker:team").status_code == 403
    assert non_admin_client.post(f"{ROUTE}/worker:team/handshake").status_code == 403


def test_an_unauthenticated_caller_gets_401():
    assert TestClient(main.app).get(ROUTE).status_code == 401

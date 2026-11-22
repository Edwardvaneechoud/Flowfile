"""``GET /health`` says the worker is up and which Flowfile release it runs, to any caller, and the name its storage
dir keeps as a controller's server when its extension says it keeps one; nothing more."""

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main
from shared._version import __version__
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER
from shared.server_extension import Identity
from shared.tests import toy_server_extension
from tests.conftest import INTERNAL_AUTH_HEADERS

client = TestClient(main.app)


def known_as(monkeypatch, recorded_name: str | None) -> None:
    """The server's extension says this storage dir keeps ``recorded_name``; ``None`` for a name it only was told."""
    identity = Identity(name="build-7", url="http://hub.example", issuer="http://hub.example", recorded_name=recorded_name)
    toy_server_extension.use(monkeypatch, SimpleNamespace(identity=lambda: identity))


@pytest.mark.worker
def test_health_names_the_node_its_storage_dir_keeps_and_nothing_without_one(monkeypatch):
    toy_server_extension.use(monkeypatch, None)
    before = client.get("/health").json()
    known_as(monkeypatch, None)
    told_only = client.get("/health").json()
    known_as(monkeypatch, "build-7")
    after = client.get("/health").json()

    assert before == told_only == {"status": "ok", "version": __version__}
    assert after == {"status": "ok", "version": __version__, "node": "build-7"}


@pytest.mark.worker
@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer not-a-token"},
        {"Authorization": "Basic dXNlcjpwYXNz"},
        {"X-Flowfile-Internal": "not-the-token"},
        INTERNAL_AUTH_HEADERS,
    ],
    ids=["no-header", "bearer", "basic", "wrong-internal-token", "internal-token"],
)
def test_health_names_the_version_with_or_without_a_token(headers):
    response = client.get("/health", headers=headers)

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": __version__}


@pytest.mark.worker
def test_the_capabilities_stay_behind_the_token():
    assert client.get("/health").status_code == 200
    assert client.get("/v1/capabilities", headers={PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}).status_code == 401
    assert client.get("/capabilities").status_code == 401

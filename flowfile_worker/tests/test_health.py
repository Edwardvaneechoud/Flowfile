"""``GET /health`` says the worker is up and which Flowfile release it runs, to any caller, and the name it joined a
hub under once it did; nothing more."""

import json

import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main
from shared import hub_node
from shared._version import __version__
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER
from tests.conftest import INTERNAL_AUTH_HEADERS

client = TestClient(main.app)


@pytest.fixture
def node_file(tmp_path, monkeypatch):
    """The worker's ``hub_node.json`` in a storage dir of the test's own, absent until the test writes it."""
    target = tmp_path / "storage" / hub_node.FILE_NAME
    monkeypatch.setattr(hub_node, "path", lambda: target)
    return target


def joined_as(node_file, name: str) -> None:
    node_file.parent.mkdir(parents=True)
    node = {"hub_url": "http://hub.example", "issuer": "http://hub.example", "name": name, "node_id": 3}
    node_file.write_text(json.dumps({**node, "join_token_hash": "0" * 16}), encoding="utf-8")


@pytest.mark.worker
def test_health_names_the_node_once_this_storage_dir_joined_a_hub_and_nothing_before(node_file):
    before = client.get("/health").json()
    joined_as(node_file, "build-7")
    after = client.get("/health").json()

    assert before == {"status": "ok", "version": __version__}
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

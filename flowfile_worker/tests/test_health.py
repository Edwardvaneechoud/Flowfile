"""``GET /health`` says the worker is up and which Flowfile release it runs, to any caller, and nothing more."""

import pytest
from fastapi.testclient import TestClient

from flowfile_worker import main
from shared._version import __version__
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER
from tests.conftest import INTERNAL_AUTH_HEADERS

client = TestClient(main.app)


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

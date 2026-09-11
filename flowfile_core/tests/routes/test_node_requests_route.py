"""GET /node_requests proxies the open node-request issues; GitHub is stubbed at the httpx client boundary."""

import httpx
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.flowfile import node_requests

ENDPOINT = "/node_requests"


def _get_authed_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _get_authed_client()
unauthed_client = TestClient(main.app)


@pytest.fixture(autouse=True)
def _fresh_cache():
    node_requests._reset_cache()
    yield
    node_requests._reset_cache()


def _github_answers(monkeypatch, by_label, status=200):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.github.com"
        return httpx.Response(status, json=by_label.get(request.url.params["labels"], []))

    real_client = httpx.Client
    monkeypatch.setattr(
        node_requests.httpx, "Client", lambda **kwargs: real_client(transport=httpx.MockTransport(handler))
    )


def test_lists_every_open_request(monkeypatch):
    _github_answers(
        monkeypatch,
        {
            node_requests.LABELS["general"]: [
                {"number": 5, "title": "A pivot-longer node", "html_url": "https://x/5", "reactions": {"+1": 2}}
            ],
            node_requests.LABELS["alteryx"]: [
                {"number": 3, "title": "[Alteryx node] DateTime", "html_url": "https://x/3"}
            ],
        },
    )
    response = client.get(ENDPOINT)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "requests": [
            {"number": 3, "title": "[Alteryx node] DateTime", "url": "https://x/3", "kind": "alteryx", "tool_key": "DateTime", "upvotes": 0},
            {"number": 5, "title": "A pivot-longer node", "url": "https://x/5", "kind": "general", "tool_key": None, "upvotes": 2},
        ]
    }


def test_a_github_failure_answers_empty_not_an_error(monkeypatch):
    _github_answers(monkeypatch, {}, status=403)
    response = client.get(ENDPOINT)
    assert response.status_code == 200
    assert response.json() == {"requests": []}


def test_requires_auth():
    assert unauthed_client.get(ENDPOINT).status_code == 401

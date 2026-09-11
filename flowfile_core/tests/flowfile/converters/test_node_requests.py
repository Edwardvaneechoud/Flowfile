"""Open node-request lookup — no network, everything through the ``http=`` MockTransport seam."""

import httpx
import pytest

from flowfile_core.flowfile.converters.alteryx import node_requests

ISSUE_URL = "https://github.com/edwardvaneechoud/Flowfile/issues/{n}"


def _issue(n: int, title: str, **extra) -> dict:
    return {"number": n, "title": title, "html_url": ISSUE_URL.format(n=n), **extra}


def _client(payload, status: int = 200, calls: list | None = None) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(request)
        return httpx.Response(status, json=payload)

    return httpx.Client(transport=httpx.MockTransport(handler))


@pytest.fixture(autouse=True)
def _fresh_cache():
    node_requests._reset_cache()
    yield
    node_requests._reset_cache()


def test_fetch_maps_titled_issues_to_their_urls():
    calls: list[httpx.Request] = []
    payload = [
        _issue(1, "[Alteryx node] DateTime"),
        _issue(2, "[Alteryx node]   Download  "),
        _issue(3, "[Alteryx node] DateTime"),  # a later duplicate never shadows the original request
        _issue(4, "Please support the Tile tool"),  # not the convention, so not matchable
        _issue(5, "[Alteryx node] Tile", pull_request={"url": "x"}),  # a PR carrying the label is not a request
        _issue(6, "[Alteryx node] ../../etc"),  # a key must be an identifier
        {"title": "[Alteryx node] Sort"},  # no html_url
    ]
    result = node_requests.fetch_open_requests(http=_client(payload, calls=calls))

    assert result == {"DateTime": ISSUE_URL.format(n=1), "Download": ISSUE_URL.format(n=2)}
    request = calls[0]
    assert request.url.host == "api.github.com"
    assert request.url.params["labels"] == node_requests.ISSUE_LABEL
    assert request.url.params["state"] == "open"
    assert request.url.params["direction"] == "asc"
    assert "Authorization" not in request.headers


def test_fetch_raises_on_a_github_error():
    with pytest.raises(httpx.HTTPStatusError):
        node_requests.fetch_open_requests(http=_client({"message": "rate limited"}, status=403))


def test_open_requests_is_cached_for_the_community_ttl(monkeypatch):
    calls: list[httpx.Request] = []
    http = _client([_issue(1, "[Alteryx node] DateTime")], calls=calls)
    monkeypatch.setenv("FLOWFILE_COMMUNITY_CACHE_TTL", "3600")

    first = node_requests.open_requests(http=http)
    second = node_requests.open_requests(http=http)

    assert first == second == {"DateTime": ISSUE_URL.format(n=1)}
    assert len(calls) == 1
    second["DateTime"] = "mutated"
    assert node_requests.open_requests(http=http)["DateTime"] == ISSUE_URL.format(n=1)


def test_a_failure_reads_as_no_requests_and_is_retried_after_the_short_ttl(monkeypatch):
    calls: list[httpx.Request] = []
    http = _client({"message": "boom"}, status=500, calls=calls)
    clock = [1000.0]
    monkeypatch.setattr(node_requests.time, "monotonic", lambda: clock[0])

    assert node_requests.open_requests(http=http) == {}
    assert node_requests.open_requests(http=http) == {}
    assert len(calls) == 1, "a failure is cached so the dialog cannot hammer GitHub"

    clock[0] += node_requests.FAILURE_TTL + 1
    assert node_requests.open_requests(http=http) == {}
    assert len(calls) == 2


def test_a_dead_socket_never_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    http = httpx.Client(transport=httpx.MockTransport(handler))
    assert node_requests.open_requests(http=http) == {}

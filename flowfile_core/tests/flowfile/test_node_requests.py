"""Open node-request lookup — no network, everything through the ``http=`` MockTransport seam."""

import httpx
import pytest

from flowfile_core.flowfile import node_requests

ISSUE_URL = "https://github.com/edwardvaneechoud/Flowfile/issues/{n}"
ALTERYX, GENERAL = node_requests.LABELS["alteryx"], node_requests.LABELS["general"]


def _issue(n: int, title: str, **extra) -> dict:
    return {"number": n, "title": title, "html_url": ISSUE_URL.format(n=n), **extra}


def _client(by_label: dict[str, object], status: int = 200, calls: list | None = None) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(request)
        return httpx.Response(status, json=by_label.get(request.url.params["labels"], []))

    return httpx.Client(transport=httpx.MockTransport(handler))


@pytest.fixture(autouse=True)
def _fresh_cache():
    node_requests._reset_cache()
    yield
    node_requests._reset_cache()


def test_fetch_merges_both_labels_oldest_first():
    calls: list[httpx.Request] = []
    by_label = {
        ALTERYX: [
            _issue(7, "[Alteryx node] DateTime", reactions={"+1": 3}),
            _issue(2, "[Alteryx node]   Download  "),
            _issue(9, "[Alteryx node] DateTime"),  # a later duplicate key stays listed but never wins the key
            _issue(4, "Please support the Tile tool"),  # not the convention, so not an Alteryx request
            _issue(5, "[Alteryx node] Tile", pull_request={"url": "x"}),  # a PR carrying the label is not a request
            _issue(6, "[Alteryx node] ../../etc"),  # a key must be an identifier
            {"title": "[Alteryx node] Sort"},  # no number / url
        ],
        GENERAL: [
            _issue(3, "A pivot-longer node with multiple value columns", reactions={"+1": 12}),
            _issue(7, "[Alteryx node] DateTime"),  # carries both labels: counted once, as Alteryx
        ],
    }
    result = node_requests.fetch_open_requests(http=_client(by_label, calls=calls))

    assert [(r.number, r.kind, r.tool_key, r.upvotes) for r in result] == [
        (2, "alteryx", "Download", 0),
        (3, "general", None, 12),
        (7, "alteryx", "DateTime", 3),
        (9, "alteryx", "DateTime", 0),
    ]
    assert node_requests.alteryx_request_urls(result) == {
        "Download": ISSUE_URL.format(n=2),
        "DateTime": ISSUE_URL.format(n=7),
    }
    assert {request.url.params["labels"] for request in calls} == {ALTERYX, GENERAL}
    for request in calls:
        assert request.url.host == "api.github.com"
        assert request.url.params["state"] == "open"
        assert request.url.params["direction"] == "asc"
        assert "Authorization" not in request.headers


def test_fetch_raises_on_a_github_error():
    with pytest.raises(httpx.HTTPStatusError):
        node_requests.fetch_open_requests(http=_client({}, status=403))


def test_open_requests_is_cached_for_the_community_ttl(monkeypatch):
    calls: list[httpx.Request] = []
    http = _client({ALTERYX: [_issue(1, "[Alteryx node] DateTime")]}, calls=calls)
    monkeypatch.setenv("FLOWFILE_COMMUNITY_CACHE_TTL", "3600")

    first = node_requests.open_requests(http=http)
    second = node_requests.open_requests(http=http)

    assert first == second and [r.tool_key for r in first] == ["DateTime"]
    assert len(calls) == len(node_requests.LABELS)
    second.clear()
    assert len(node_requests.open_requests(http=http)) == 1


def test_a_failure_reads_as_no_requests_and_is_retried_after_the_short_ttl(monkeypatch):
    calls: list[httpx.Request] = []
    http = _client({}, status=500, calls=calls)
    clock = [1000.0]
    monkeypatch.setattr(node_requests.time, "monotonic", lambda: clock[0])

    assert node_requests.open_requests(http=http) == []
    assert node_requests.open_requests(http=http) == []
    assert len(calls) == 1, "a failure is cached so the dialogs cannot hammer GitHub"

    clock[0] += node_requests.FAILURE_TTL + 1
    assert node_requests.open_requests(http=http) == []
    assert len(calls) == 2


def test_a_dead_socket_never_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    assert node_requests.open_requests(http=httpx.Client(transport=httpx.MockTransport(handler))) == []

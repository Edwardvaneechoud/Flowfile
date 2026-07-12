"""GitHub client tests — no network, everything through the ``http=`` MockTransport seam.

Covers the device-flow state machine, the token lifecycle, and every ``GithubPublisher``
step (fork/sync/branch/atomic-commit/PR), plus the invariant that tokens and device
codes never reach the logs.
"""

import base64
import json
import logging

import httpx
import pytest

from flowfile_core.configs import logger as core_logger
from flowfile_core.flowfile.community_nodes import github_client

UP = github_client.DEFAULT_UPSTREAM
REPO = UP.split("/")[-1]
DEVICE_CODE_URL = github_client.DEVICE_CODE_URL
ACCESS_TOKEN_URL = github_client.ACCESS_TOKEN_URL


def _mk(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _configured(monkeypatch) -> None:
    monkeypatch.setenv("FLOWFILE_COMMUNITY_GITHUB_CLIENT_ID", "cid_test")


# ---- device flow ----------------------------------------------------------


def test_device_start_success(monkeypatch):
    _configured(monkeypatch)

    def handler(request):
        assert str(request.url) == DEVICE_CODE_URL
        assert request.headers["Accept"] == "application/json"
        return httpx.Response(
            200,
            json={
                "device_code": "DC123",
                "user_code": "WXYZ-1234",
                "verification_uri": "https://github.com/login/device",
                "interval": 5,
                "expires_in": 900,
            },
        )

    start = github_client.device_start(http=_mk(handler))
    assert start.device_code == "DC123"
    assert start.user_code == "WXYZ-1234"
    assert start.verification_uri == "https://github.com/login/device"
    assert start.interval == 5


def test_device_start_unconfigured(monkeypatch):
    monkeypatch.delenv("FLOWFILE_COMMUNITY_GITHUB_CLIENT_ID", raising=False)
    monkeypatch.setattr(github_client, "get_community_github_client_id", lambda: "")
    with pytest.raises(github_client.GithubNotConfiguredError) as exc:
        github_client.device_start(http=_mk(lambda r: httpx.Response(200, json={})))
    assert exc.value.reason == "not_configured"


def _poll(body, monkeypatch, status=200):
    _configured(monkeypatch)

    def handler(request):
        assert str(request.url) == ACCESS_TOKEN_URL
        return httpx.Response(status, json=body)

    return github_client.device_poll("DEVCODE", http=_mk(handler))


def test_device_poll_pending(monkeypatch):
    poll = _poll({"error": "authorization_pending"}, monkeypatch)
    assert poll.status == "pending"
    assert poll.access_token == ""


def test_device_poll_slow_down_uses_response_interval(monkeypatch):
    poll = _poll({"error": "slow_down", "interval": 12}, monkeypatch)
    assert poll.status == "slow_down"
    assert poll.interval == 12


def test_device_poll_slow_down_defaults_when_absent(monkeypatch):
    poll = _poll({"error": "slow_down"}, monkeypatch)
    assert poll.status == "slow_down"
    assert poll.interval == github_client._DEVICE_POLL_INTERVAL + 5


def test_device_poll_success(monkeypatch):
    poll = _poll({"access_token": "ghp_token", "token_type": "bearer"}, monkeypatch)
    assert poll.status == "success"
    assert poll.access_token == "ghp_token"


def test_device_poll_expired(monkeypatch):
    with pytest.raises(github_client.GithubAuthError) as exc:
        _poll({"error": "expired_token"}, monkeypatch)
    assert exc.value.reason == "device_expired"


def test_device_poll_denied(monkeypatch):
    with pytest.raises(github_client.GithubAuthError) as exc:
        _poll({"error": "access_denied"}, monkeypatch)
    assert exc.value.reason == "device_denied"


# ---- token lifecycle ------------------------------------------------------


def test_fetch_login_401():
    def handler(request):
        assert request.url.path == "/user"
        return httpx.Response(401, json={"message": "Bad credentials"})

    with pytest.raises(github_client.GithubAuthError) as exc:
        github_client.fetch_login("tok", http=_mk(handler))
    assert exc.value.reason == "token_invalid"


def test_rate_limit_mapping_includes_reset():
    def handler(request):
        return httpx.Response(
            403,
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1700000000"},
            json={"message": "API rate limit exceeded"},
        )

    with pytest.raises(github_client.GithubApiError) as exc:
        github_client.fetch_login("tok", http=_mk(handler))
    assert exc.value.reason == "rate_limited"
    assert exc.value.status == 403
    assert "after" in str(exc.value).lower()


# ---- fork -----------------------------------------------------------------


def _user(login: str):
    return httpx.Response(200, json={"login": login})


def test_ensure_fork_pending():
    def handler(request):
        if request.method == "GET" and request.url.path == "/user":
            return _user("octo")
        if request.method == "POST" and request.url.path == f"/repos/{UP}/forks":
            return httpx.Response(202, json={"full_name": f"octo/{REPO}"})
        if request.method == "GET" and request.url.path == f"/repos/octo/{REPO}":
            return httpx.Response(404, json={"message": "Not Found"})
        raise AssertionError(request.url.path)

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    with pytest.raises(github_client.ForkPendingError):
        pub.ensure_fork()


def test_ensure_fork_renamed_full_name_passthrough():
    renamed = "octo/my-renamed-nodes"

    def handler(request):
        if request.method == "GET" and request.url.path == "/user":
            return _user("octo")
        if request.method == "POST" and request.url.path == f"/repos/{UP}/forks":
            return httpx.Response(202, json={"full_name": renamed})
        if request.method == "GET" and request.url.path == f"/repos/{renamed}":
            return httpx.Response(
                200, json={"fork": True, "default_branch": "main", "parent": {"full_name": UP}}
            )
        raise AssertionError(request.url.path)

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    assert pub.ensure_fork() == renamed


def test_ensure_fork_non_fork_mismatch():
    def handler(request):
        if request.method == "GET" and request.url.path == "/user":
            return _user("octo")
        if request.method == "POST" and request.url.path == f"/repos/{UP}/forks":
            return httpx.Response(202, json={"full_name": f"octo/{REPO}"})
        if request.method == "GET" and request.url.path == f"/repos/octo/{REPO}":
            return httpx.Response(200, json={"fork": False, "default_branch": "main", "parent": {}})
        raise AssertionError(request.url.path)

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    with pytest.raises(github_client.GithubApiError) as exc:
        pub.ensure_fork()
    assert exc.value.reason == "fork_unusable"


def test_ensure_fork_owner_uses_upstream_directly():
    # The registry owner can't fork their own repo; publishing pushes a branch to
    # the upstream itself and opens a same-repo PR. No POST /forks must be issued.
    owner = UP.split("/")[0]

    def handler(request):
        if request.method == "GET" and request.url.path == "/user":
            return _user(owner)
        if request.url.path == f"/repos/{UP}/forks":
            raise AssertionError("owner must not fork their own repo")
        raise AssertionError(request.url.path)

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    assert pub.ensure_fork() == UP
    pub.sync_fork(UP, "main")  # no-op for the owner, must not call merge-upstream


# ---- branch ---------------------------------------------------------------


def test_ensure_branch_preexisting_true():
    fork = f"octo/{REPO}"

    def handler(request):
        assert request.method == "POST"
        assert request.url.path == f"/repos/{fork}/git/refs"
        return httpx.Response(422, json={"message": "Reference already exists"})

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    assert pub.ensure_branch(fork, "node/x-v1.0.0", "basesha") is True


def test_ensure_branch_created_false():
    fork = f"octo/{REPO}"

    def handler(request):
        return httpx.Response(201, json={"ref": "refs/heads/node/x-v1.0.0"})

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    assert pub.ensure_branch(fork, "node/x-v1.0.0", "basesha") is False


# ---- atomic commit --------------------------------------------------------


def test_commit_files_blob_tree_commit_patch_sequence_and_base64():
    fork = f"octo/{REPO}"
    branch = "node/x-v1.0.0"
    node_py = b"import polars as pl\n"
    png = b"\x89PNG\r\n\x1a\n" + b"\x00\x01\x02\x03rawpngbytes"
    files = [("nodes/x/node.py", node_py), ("nodes/x/icon.png", png)]

    calls: list[tuple[str, str]] = []
    blob_contents: list[str] = []

    def handler(request):
        p = request.url.path
        calls.append((request.method, p))
        if request.method == "GET" and p == f"/repos/{fork}/git/commits/BASESHA":
            return httpx.Response(200, json={"sha": "BASESHA", "tree": {"sha": "BASETREE"}})
        if request.method == "POST" and p == f"/repos/{fork}/git/blobs":
            body = json.loads(request.content)
            assert body["encoding"] == "base64"
            blob_contents.append(body["content"])
            return httpx.Response(201, json={"sha": f"blob{len(blob_contents)}"})
        if request.method == "POST" and p == f"/repos/{fork}/git/trees":
            body = json.loads(request.content)
            assert body["base_tree"] == "BASETREE"
            assert [e["path"] for e in body["tree"]] == ["nodes/x/node.py", "nodes/x/icon.png"]
            assert all(e["mode"] == "100644" and e["type"] == "blob" for e in body["tree"])
            return httpx.Response(201, json={"sha": "NEWTREE"})
        if request.method == "POST" and p == f"/repos/{fork}/git/commits":
            body = json.loads(request.content)
            assert body["parents"] == ["BASESHA"]
            assert body["tree"] == "NEWTREE"
            return httpx.Response(201, json={"sha": "NEWCOMMIT"})
        if request.method == "PATCH" and p == f"/repos/{fork}/git/refs/heads/{branch}":
            body = json.loads(request.content)
            assert body["force"] is True
            assert body["sha"] == "NEWCOMMIT"
            return httpx.Response(200, json={"object": {"sha": "NEWCOMMIT"}})
        raise AssertionError(f"unexpected {request.method} {p}")

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    sha = pub.commit_files(fork, branch, "BASESHA", files, "publish x v1.0.0")
    assert sha == "NEWCOMMIT"

    def _kind(method, path):
        if path.endswith("/git/blobs"):
            return "blob"
        if path.endswith("/git/trees"):
            return "tree"
        if method == "POST" and path.endswith("/git/commits"):
            return "commit"
        if method == "PATCH" and "/git/refs/heads/" in path:
            return "patch"
        return None

    write_seq = [k for k in (_kind(m, p) for m, p in calls) if k]
    assert write_seq == ["blob", "blob", "tree", "commit", "patch"]
    assert base64.b64decode(blob_contents[0]) == node_py
    assert base64.b64decode(blob_contents[1]) == png


# ---- pull request ---------------------------------------------------------


def test_open_or_get_pr_existing_returns_created_false():
    branch = "node/x-v1.0.0"

    def handler(request):
        p = request.url.path
        if request.method == "GET" and p == "/user":
            return httpx.Response(200, json={"login": "octo"})
        if request.method == "GET" and p == f"/repos/{UP}/pulls":
            assert request.url.params.get("state") == "open"
            assert request.url.params.get("head") == f"octo:{branch}"
            return httpx.Response(200, json=[{"html_url": "https://github.com/x/pull/7", "number": 7}])
        raise AssertionError(f"unexpected {request.method} {p}")

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    result = pub.open_or_get_pr(branch, "title", "body")
    assert result.created is False
    assert result.number == 7
    assert result.url == "https://github.com/x/pull/7"


def test_open_or_get_pr_422_race_regets():
    branch = "node/x-v1.0.0"
    state = {"posted": False}

    def handler(request):
        p = request.url.path
        if request.method == "GET" and p == "/user":
            return httpx.Response(200, json={"login": "octo"})
        if request.method == "GET" and p == f"/repos/{UP}/pulls":
            if state["posted"]:
                return httpx.Response(200, json=[{"html_url": "https://github.com/x/pull/9", "number": 9}])
            return httpx.Response(200, json=[])
        if request.method == "POST" and p == f"/repos/{UP}/pulls":
            state["posted"] = True
            return httpx.Response(422, json={"message": "A pull request already exists for octo:..."})
        raise AssertionError(f"unexpected {request.method} {p}")

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    result = pub.open_or_get_pr(branch, "title", "body")
    assert result.created is False
    assert result.number == 9


# ---- full publisher composition ------------------------------------------


def test_full_publish_flow_composes():
    fork = f"octo/{REPO}"
    branch = "node/x-v1.0.0"

    def handler(request):
        m, p = request.method, request.url.path
        if m == "GET" and p == "/user":
            return httpx.Response(200, json={"login": "octo"})
        if m == "POST" and p == f"/repos/{UP}/forks":
            return httpx.Response(202, json={"full_name": fork})
        if m == "GET" and p == f"/repos/{fork}":
            return httpx.Response(
                200, json={"fork": True, "default_branch": "main", "parent": {"full_name": UP}}
            )
        if m == "POST" and p == f"/repos/{fork}/merge-upstream":
            return httpx.Response(200, json={"merge_type": "fast-forward"})
        if m == "GET" and p == f"/repos/{fork}/git/ref/heads/main":
            return httpx.Response(200, json={"object": {"sha": "BASESHA"}})
        if m == "POST" and p == f"/repos/{fork}/git/refs":
            return httpx.Response(201, json={"ref": f"refs/heads/{branch}"})
        if m == "GET" and p == f"/repos/{fork}/git/commits/BASESHA":
            return httpx.Response(200, json={"tree": {"sha": "BASETREE"}})
        if m == "POST" and p == f"/repos/{fork}/git/blobs":
            return httpx.Response(201, json={"sha": "BLOB"})
        if m == "POST" and p == f"/repos/{fork}/git/trees":
            return httpx.Response(201, json={"sha": "NEWTREE"})
        if m == "POST" and p == f"/repos/{fork}/git/commits":
            return httpx.Response(201, json={"sha": "NEWCOMMIT"})
        if m == "PATCH" and p == f"/repos/{fork}/git/refs/heads/{branch}":
            return httpx.Response(200, json={"object": {"sha": "NEWCOMMIT"}})
        if m == "GET" and p == f"/repos/{UP}/pulls":
            return httpx.Response(200, json=[])
        if m == "POST" and p == f"/repos/{UP}/pulls":
            return httpx.Response(201, json={"html_url": "https://github.com/x/pull/1", "number": 1})
        raise AssertionError(f"unexpected {m} {p}")

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    assert pub.login() == "octo"
    assert pub.ensure_fork() == fork
    pub.sync_fork(fork, "main")
    default_branch, head = pub.base_sha(fork)
    assert (default_branch, head) == ("main", "BASESHA")
    assert pub.ensure_branch(fork, branch, head) is False
    sha = pub.commit_files(fork, branch, head, [("nodes/x/node.py", b"x\n")], "msg")
    assert sha == "NEWCOMMIT"
    pr = pub.open_or_get_pr(branch, "title", "body")
    assert pr.created is True
    assert pr.number == 1


def test_sync_fork_swallows_conflict():
    fork = f"octo/{REPO}"

    def handler(request):
        assert request.url.path == f"/repos/{fork}/merge-upstream"
        return httpx.Response(409, json={"message": "merge conflict"})

    pub = github_client.GithubPublisher("tok", http=_mk(handler))
    pub.sync_fork(fork, "main")  # no raise


# ---- logging hygiene ------------------------------------------------------


def test_token_and_device_code_never_logged(monkeypatch):
    _configured(monkeypatch)
    token = "ghp_SUPERSECRET_TOKEN_abcdef1234567890"
    device_code = "DEVICE_SECRET_CODE_abcdef9999"

    def handler(request):
        url = str(request.url)
        if url == DEVICE_CODE_URL:
            return httpx.Response(
                200,
                json={
                    "device_code": device_code,
                    "user_code": "WXYZ-1234",
                    "verification_uri": "https://github.com/login/device",
                    "interval": 5,
                    "expires_in": 900,
                },
            )
        if url == ACCESS_TOKEN_URL:
            return httpx.Response(200, json={"access_token": token, "token_type": "bearer"})
        return httpx.Response(200, json={"login": "octo"})

    # Capture directly off the module logger — PipelineHandler sets propagate=False, so
    # caplog's root-attached handler sees nothing; a self-contained handler is order-proof.
    records: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    cap = _Capture()
    prev_level = core_logger.level
    core_logger.addHandler(cap)
    core_logger.setLevel(logging.INFO)
    try:
        start = github_client.device_start(http=_mk(handler))
        assert start.device_code == device_code
        poll = github_client.device_poll(device_code, http=_mk(handler))
        assert poll.access_token == token
        assert github_client.fetch_login(token, http=_mk(handler)) == "octo"
    finally:
        core_logger.removeHandler(cap)
        core_logger.setLevel(prev_level)

    text = "\n".join(records)
    assert "github" in text.lower()  # sanity: requests were logged at all
    assert token not in text
    assert device_code not in text

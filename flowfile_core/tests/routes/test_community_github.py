"""Auth + behavior tests for the GitHub publishing routes.

Covers the token-lifecycle router (/community_nodes/github/*) and the publish-pr
route: every route rejects unauthenticated requests with 401 (never for a
GitHub-domain state — the frontend treats 401 as JWT expiry), device-flow config
gating, PAT validation, and the publish-pr ladder (412/422/409/fork_creating/happy
path) against the on-disk fixture registry with a monkeypatched publisher.
"""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.flowfile.community_nodes import github_client
from flowfile_core.flowfile.community_nodes.github_client import GithubAuthError, PrResult
from flowfile_core.routes import community_github

# Shared node-writing helpers + storage isolation live in the community_nodes conftest.
from tests.flowfile.community_nodes.conftest import _write_node, isolated_storage  # noqa: F401


def get_authed_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


unauthed_client = TestClient(main.app)
authed_client = get_authed_client()


@pytest.fixture(autouse=True)
def clean_github_token():
    """Guarantee a disconnected starting state and clean up token rows afterwards."""
    authed_client.delete("/community_nodes/github/token")
    yield
    authed_client.delete("/community_nodes/github/token")


UNAUTHENTICATED_REQUESTS = [
    ("GET", "/community_nodes/github/status", {}),
    ("POST", "/community_nodes/github/device/start", {}),
    ("POST", "/community_nodes/github/device/poll", {"json": {"device_code": "abc"}}),
    ("POST", "/community_nodes/github/pat", {"json": {"token": "ghp_x"}}),
    ("DELETE", "/community_nodes/github/token", {}),
    ("POST", "/community_nodes/publish-pr", {"json": {"file_name": "x.py", "license": "MIT"}}),
]


@pytest.mark.parametrize(
    "method,url,kwargs", UNAUTHENTICATED_REQUESTS, ids=[f"{m} {u}" for m, u, _ in UNAUTHENTICATED_REQUESTS]
)
def test_every_route_requires_auth(method, url, kwargs):
    response = unauthed_client.request(method, url, **kwargs)
    assert response.status_code == 401, f"{method} {url} returned {response.status_code}, expected 401"


def _connect_github(monkeypatch, login: str = "octocat") -> None:
    """Store a token for the test user via the PAT route with a fake login lookup."""
    monkeypatch.setattr(github_client, "fetch_login", lambda token, **kw: login)
    monkeypatch.setattr(community_github, "_scopes_warning", lambda token: "")
    resp = authed_client.post("/community_nodes/github/pat", json={"token": "ghp_test"})
    assert resp.status_code == 200, resp.text


# --------------------------------------------------------------------- status / device


def test_status_unconfigured_and_disconnected(monkeypatch):
    # Explicit empty force-disables; delenv would fall through to the dev's .env value.
    monkeypatch.setenv("FLOWFILE_COMMUNITY_GITHUB_CLIENT_ID", "")
    resp = authed_client.get("/community_nodes/github/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {"configured": False, "connected": False, "login": ""}


def test_status_connected_after_pat_save(monkeypatch):
    _connect_github(monkeypatch, login="mona")
    resp = authed_client.get("/community_nodes/github/status")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["connected"] is True
    assert body["login"] == "mona"


def test_device_start_unconfigured_503(monkeypatch):
    monkeypatch.setenv("FLOWFILE_COMMUNITY_GITHUB_CLIENT_ID", "")
    resp = authed_client.post("/community_nodes/github/device/start")
    assert resp.status_code == 503, resp.text
    assert resp.json()["detail"]["error_code"] == "GITHUB_NOT_CONFIGURED"


# --------------------------------------------------------------------- PAT validation


def test_pat_valid_token_stored(monkeypatch):
    monkeypatch.setattr(github_client, "fetch_login", lambda token, **kw: "octocat")
    monkeypatch.setattr(community_github, "_scopes_warning", lambda token: "")
    resp = authed_client.post("/community_nodes/github/pat", json={"token": "ghp_valid"})
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"login": "octocat", "scopes_warning": ""}
    assert authed_client.get("/community_nodes/github/status").json()["connected"] is True


def test_pat_invalid_token_400_nothing_stored(monkeypatch):
    def _raise(token, **kw):
        raise GithubAuthError("token_invalid", "Your GitHub token is not valid or was revoked.")

    monkeypatch.setattr(github_client, "fetch_login", _raise)
    resp = authed_client.post("/community_nodes/github/pat", json={"token": "ghp_bad"})
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"]["error_code"] == "GITHUB_TOKEN_INVALID"
    assert authed_client.get("/community_nodes/github/status").json()["connected"] is False


# --------------------------------------------------------------------- publish-pr

FAKE_SHA = "0" * 64


def _write_index(tmp_path: Path, entries: list[dict]) -> Path:
    index = {
        "schema_version": 1,
        "registry": {"repo": "test/community", "commit": "commit0"},
        "categories": [],
        "nodes": entries,
    }
    path = tmp_path / "index.json"
    path.write_text(json.dumps(index), encoding="utf-8")
    return path


def _index_entry(node_id: str, version: str) -> dict:
    return {
        "id": node_id,
        "node_name": "Mood Emoji",
        "version": version,
        "author": {"github": "octocat"},
        "min_flowfile_version": "0.0.1",
        "artifacts": {
            "node": {"path": f"nodes/{node_id}/node.py", "sha256": FAKE_SHA, "size": 1},
            "manifest": {"path": f"nodes/{node_id}/manifest.json", "sha256": FAKE_SHA, "size": 1},
        },
    }


def test_publish_pr_not_connected_412(isolated_storage, monkeypatch):
    _write_node()
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(_write_index(isolated_storage, [])))
    resp = authed_client.post(
        "/community_nodes/publish-pr", json={"file_name": "mood_emoji.py", "license": "MIT"}
    )
    assert resp.status_code == 412, resp.text
    assert resp.json()["detail"]["error_code"] == "GITHUB_NOT_CONNECTED"


def test_publish_pr_incomplete_422(isolated_storage, monkeypatch):
    _write_node(with_examples=False)
    _connect_github(monkeypatch)
    resp = authed_client.post(
        "/community_nodes/publish-pr", json={"file_name": "mood_emoji.py", "license": "MIT"}
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert detail["error_code"] == "INCOMPLETE"
    assert any(issue["code"] == "EXAMPLES_MISSING" for issue in detail["issues"])


def test_publish_pr_version_not_incremented_409(isolated_storage, monkeypatch):
    _write_node(version="1.0.0")
    _connect_github(monkeypatch)
    index_path = _write_index(isolated_storage, [_index_entry("mood_emoji", "1.0.0")])
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(index_path))
    resp = authed_client.post(
        "/community_nodes/publish-pr", json={"file_name": "mood_emoji.py", "license": "MIT"}
    )
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["error_code"] == "VERSION_NOT_INCREMENTED"
    assert detail["published_version"] == "1.0.0"


def test_publish_pr_fork_creating_passthrough(isolated_storage, monkeypatch):
    _write_node(version="1.0.0")
    _connect_github(monkeypatch)
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(_write_index(isolated_storage, [])))

    class _ForkPendingPublisher:
        def __init__(self, token, **kw):
            pass

        def ensure_fork(self):
            raise github_client.ForkPendingError()

    monkeypatch.setattr(github_client, "GithubPublisher", _ForkPendingPublisher)
    resp = authed_client.post(
        "/community_nodes/publish-pr", json={"file_name": "mood_emoji.py", "license": "MIT"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "fork_creating"
    assert body["node_id"] == "mood_emoji"
    assert body["version"] == "1.0.0"
    assert body["branch"] == "node/mood_emoji-v1.0.0"


def test_publish_pr_happy_path(isolated_storage, monkeypatch):
    _write_node(version="1.0.0", author="octocat")
    _connect_github(monkeypatch, login="octocat")
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(_write_index(isolated_storage, [])))

    created: list = []

    class _FakePublisher:
        def __init__(self, token, **kw):
            self.commit_calls: list[dict] = []
            created.append(self)

        def ensure_fork(self):
            return "octocat/flowfile-community-nodes"

        def base_sha(self, fork):
            return "main", "basesha"

        def sync_fork(self, fork, branch):
            pass

        def ensure_branch(self, fork, branch, base_sha):
            return False

        def commit_files(self, fork, branch, base_sha, files, message):
            self.commit_calls.append({"branch": branch, "files": files, "message": message})
            return "commitsha"

        def open_or_get_pr(self, branch, title, body):
            return PrResult(
                url="https://github.com/edwardvaneechoud/flowfile-community-nodes/pull/7",
                number=7,
                created=True,
            )

    monkeypatch.setattr(github_client, "GithubPublisher", _FakePublisher)
    resp = authed_client.post(
        "/community_nodes/publish-pr",
        json={"file_name": "mood_emoji.py", "license": "MIT", "description": "Adds a mood emoji to every row."},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "created"
    assert body["pr_url"].endswith("/pull/7")
    assert body["pr_number"] == 7
    assert body["branch"] == "node/mood_emoji-v1.0.0"
    assert body["is_update"] is False

    assert len(created) == 1
    commit = created[0].commit_calls[0]
    assert commit["branch"] == "node/mood_emoji-v1.0.0"
    assert commit["message"] == "Add mood_emoji v1.0.0"
    paths = [path for path, _ in commit["files"]]
    assert paths, "expected committed files"
    assert all(path.startswith("nodes/mood_emoji/") for path in paths)

    manifest_bytes = next(data for path, data in commit["files"] if path.endswith("manifest.json"))
    manifest = json.loads(manifest_bytes)
    assert manifest["author"]["github"] == "octocat"


class _OpenPrPublisher:
    """Fake publisher whose branch/PR already exist: open_or_get_pr returns created=False."""

    instances: list = []

    def __init__(self, token, **kw):
        self.commit_calls: list[dict] = []
        self.update_calls: list[tuple] = []
        type(self).instances.append(self)

    def ensure_fork(self):
        return "octocat/flowfile-community-nodes"

    def base_sha(self, fork):
        return "main", "basesha"

    def sync_fork(self, fork, branch):
        pass

    def ensure_branch(self, fork, branch, base_sha):
        return True

    def commit_files(self, fork, branch, base_sha, files, message):
        self.commit_calls.append({"branch": branch, "files": files, "message": message})
        return "commitsha"

    def open_or_get_pr(self, branch, title, body):
        return PrResult(
            url="https://github.com/edwardvaneechoud/flowfile-community-nodes/pull/7",
            number=7,
            created=False,
        )

    def update_pr(self, number, title, body):
        self.update_calls.append((number, title, body))


def test_publish_pr_refreshes_existing_open_pr(isolated_storage, monkeypatch):
    _write_node(version="1.0.0", author="octocat")
    _connect_github(monkeypatch, login="octocat")
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(_write_index(isolated_storage, [])))
    _OpenPrPublisher.instances = []
    monkeypatch.setattr(github_client, "GithubPublisher", _OpenPrPublisher)

    resp = authed_client.post(
        "/community_nodes/publish-pr",
        json={"file_name": "mood_emoji.py", "license": "MIT", "description": "Adds a mood emoji to every row."},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "updated"

    pub = _OpenPrPublisher.instances[0]
    assert pub.commit_calls, "branch must be force-refreshed before the PR lookup"
    assert len(pub.update_calls) == 1
    number, title, body = pub.update_calls[0]
    assert number == 7
    assert "v1.0.0" in title
    assert "nodes/mood_emoji/" in body


def test_publish_pr_update_pr_failure_still_updated(isolated_storage, monkeypatch):
    _write_node(version="1.0.0", author="octocat")
    _connect_github(monkeypatch, login="octocat")
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(_write_index(isolated_storage, [])))

    class _PatchFailsPublisher(_OpenPrPublisher):
        def update_pr(self, number, title, body):
            raise github_client.GithubApiError("api", "PATCH failed", status=500)

    _PatchFailsPublisher.instances = []
    monkeypatch.setattr(github_client, "GithubPublisher", _PatchFailsPublisher)

    resp = authed_client.post(
        "/community_nodes/publish-pr",
        json={"file_name": "mood_emoji.py", "license": "MIT", "description": "Adds a mood emoji to every row."},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "updated"  # force-push landed; title/body refresh is cosmetic


def test_publish_pr_commits_readme_and_changelog(isolated_storage, monkeypatch):
    _write_node(version="1.0.0", author="octocat")
    _connect_github(monkeypatch, login="octocat")
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(_write_index(isolated_storage, [])))
    _OpenPrPublisher.instances = []
    monkeypatch.setattr(github_client, "GithubPublisher", _OpenPrPublisher)

    resp = authed_client.post(
        "/community_nodes/publish-pr",
        json={
            "file_name": "mood_emoji.py",
            "license": "MIT",
            "description": "Adds a mood emoji to every row.",
            "readme": "# Hello",
            "changelog": "1.0.0 - first release.",
        },
    )
    assert resp.status_code == 200, resp.text
    files = dict(_OpenPrPublisher.instances[0].commit_calls[0]["files"])
    assert files["nodes/mood_emoji/README.md"] == b"# Hello\n"
    manifest = json.loads(files["nodes/mood_emoji/manifest.json"])
    assert manifest["changelog"] == "1.0.0 - first release."


def test_publish_pr_oversized_readme_422(isolated_storage, monkeypatch):
    from flowfile_core.flowfile.community_nodes import validation

    _write_node(version="1.0.0", author="octocat")
    _connect_github(monkeypatch, login="octocat")

    resp = authed_client.post(
        "/community_nodes/publish-pr",
        json={
            "file_name": "mood_emoji.py",
            "license": "MIT",
            "readme": "x" * (validation.README_MAX + 1),
        },
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert detail["error_code"] == "INCOMPLETE"
    assert any(i["code"] == "README_TOO_LARGE" for i in detail["issues"])

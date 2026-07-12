"""In-app GitHub client for publishing community nodes via a pull request.

All GitHub traffic runs from core (the renderer CSP forbids it). Two surfaces:

- **Device flow** (``device_start`` / ``device_poll``) + ``fetch_login`` — the token
  lifecycle used by the ``/community_nodes/github`` router. Requires an OAuth App
  client id (``settings.get_community_github_client_id``); empty ⇒ device flow
  disabled and ``device_start`` raises ``GithubNotConfiguredError``.
- **``GithubPublisher``** — fork → sync → branch → one atomic commit (Git Data API,
  base64 blobs so PNG media survives) → PR against the community registry. Every
  step is idempotent so the publish route is safe to re-invoke after any failure.

Errors carry a machine ``reason`` (for the route's typed status codes) and a
user-readable ``str``. Requests log method + path + status only — never headers,
bodies, tokens, or device codes.
"""

import base64
import threading
from datetime import datetime, timezone

import httpx
from pydantic import BaseModel

from flowfile_core.configs import logger
from flowfile_core.configs.settings import get_community_github_client_id

GITHUB_API = "https://api.github.com"
DEVICE_CODE_URL = "https://github.com/login/device/code"
ACCESS_TOKEN_URL = "https://github.com/login/oauth/access_token"
API_VERSION = "2022-11-28"
DEVICE_SCOPE = "public_repo"

DEFAULT_UPSTREAM = "edwardvaneechoud/flowfile-community-nodes"
_UPSTREAM_BASE_BRANCH = "main"
_DEVICE_POLL_INTERVAL = 5
_HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0)


class GithubError(Exception):
    """Base for GitHub-domain failures. ``str`` is user-readable, ``.reason`` is a machine code."""

    def __init__(self, message: str, *, reason: str = "error"):
        super().__init__(message)
        self.reason = reason


class GithubNotConfiguredError(GithubError):
    """Device flow is disabled because no OAuth App client id is configured (route → 503)."""

    def __init__(self, message: str = "GitHub publishing is not configured for this build."):
        super().__init__(message, reason="not_configured")


class GithubAuthError(GithubError):
    """Token/device auth failure (reasons: token_invalid | device_expired | device_denied)."""

    def __init__(self, reason: str, message: str):
        super().__init__(message, reason=reason)


class GithubApiError(GithubError):
    """A GitHub API call failed (reasons: rate_limited | fork_unusable | network | api)."""

    def __init__(self, reason: str, message: str, *, status: int | None = None):
        super().__init__(message, reason=reason)
        self.status = status


class ForkPendingError(GithubError):
    """The user's fork is still materializing; the caller should retry shortly."""

    def __init__(self, message: str = "Your fork is still being created."):
        super().__init__(message, reason="fork_pending")


class DeviceStart(BaseModel):
    device_code: str
    user_code: str
    verification_uri: str
    interval: int = _DEVICE_POLL_INTERVAL
    expires_in: int = 900


class DevicePoll(BaseModel):
    status: str  # pending | slow_down | success
    interval: int = _DEVICE_POLL_INTERVAL
    access_token: str = ""


class PrResult(BaseModel):
    url: str
    number: int
    created: bool


_module_http: httpx.Client | None = None
_module_http_lock = threading.Lock()


def _http(http: httpx.Client | None) -> httpx.Client:
    """Injected client for tests, else a lazily-created module singleton (mirrors client.py)."""
    if http is not None:
        return http
    global _module_http
    with _module_http_lock:
        if _module_http is None:
            _module_http = httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=False)
        return _module_http


def _auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
    }


def _reset_hint(resp: httpx.Response) -> str:
    reset = resp.headers.get("X-RateLimit-Reset", "")
    try:
        when = datetime.fromtimestamp(int(reset), timezone.utc).strftime("%H:%M UTC")
    except (TypeError, ValueError):
        return ""
    return f" Try again after {when}."


def _raise_if_rate_limited(resp: httpx.Response) -> None:
    if resp.status_code in (403, 429) and resp.headers.get("X-RateLimit-Remaining") == "0":
        raise GithubApiError(
            "rate_limited",
            f"GitHub's rate limit was reached.{_reset_hint(resp)}",
            status=resp.status_code,
        )


def device_start(*, http: httpx.Client | None = None) -> DeviceStart:
    """Begin the device flow. Raises ``GithubNotConfiguredError`` when no client id is set."""
    client_id = get_community_github_client_id()
    if not client_id:
        raise GithubNotConfiguredError()
    try:
        resp = _http(http).post(
            DEVICE_CODE_URL,
            data={"client_id": client_id, "scope": DEVICE_SCOPE},
            headers={"Accept": "application/json"},
        )
    except httpx.TransportError as e:
        raise GithubApiError("network", "Could not reach GitHub to start sign-in.") from e
    logger.info("github POST /login/device/code -> %s", resp.status_code)
    if resp.status_code >= 400:
        raise GithubApiError("api", "GitHub rejected the sign-in request.", status=resp.status_code)
    return DeviceStart.model_validate(resp.json())


def device_poll(device_code: str, *, http: httpx.Client | None = None) -> DevicePoll:
    """One upstream poll for the device flow token; the caller owns the cadence."""
    client_id = get_community_github_client_id()
    if not client_id:
        raise GithubNotConfiguredError()
    try:
        resp = _http(http).post(
            ACCESS_TOKEN_URL,
            data={
                "client_id": client_id,
                "device_code": device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            },
            headers={"Accept": "application/json"},
        )
    except httpx.TransportError as e:
        raise GithubApiError("network", "Could not reach GitHub to check sign-in.") from e
    logger.info("github POST /login/oauth/access_token -> %s", resp.status_code)
    data = resp.json()

    token = data.get("access_token")
    if token:
        return DevicePoll(status="success", access_token=token)
    error = data.get("error")
    if error == "authorization_pending":
        return DevicePoll(status="pending")
    if error == "slow_down":
        return DevicePoll(status="slow_down", interval=int(data.get("interval") or _DEVICE_POLL_INTERVAL + 5))
    if error == "expired_token":
        raise GithubAuthError("device_expired", "The sign-in code expired. Start again.")
    if error == "access_denied":
        raise GithubAuthError("device_denied", "The sign-in request was denied.")
    raise GithubApiError("api", f"GitHub sign-in failed: {error or 'unknown error'}.", status=resp.status_code)


def fetch_login(token: str, *, http: httpx.Client | None = None) -> str:
    """Resolve a token to its GitHub login (revocation surfaces here as ``token_invalid``)."""
    return GithubPublisher(token, http=http).login()


class GithubPublisher:
    def __init__(self, token: str, *, upstream: str = DEFAULT_UPSTREAM, http: httpx.Client | None = None):
        self._token = token
        self._upstream = upstream
        self._http_client = http
        self._login: str | None = None

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        params: dict | None = None,
        ok: tuple[int, ...] = (200,),
    ) -> httpx.Response:
        client = _http(self._http_client)
        headers = _auth_headers(self._token)
        url = f"{GITHUB_API}{path}"
        attempts = 2 if method == "GET" else 1
        resp: httpx.Response | None = None
        last_exc: httpx.TransportError | None = None
        for _ in range(attempts):
            try:
                resp = client.request(method, url, json=json, params=params, headers=headers)
                break
            except httpx.TransportError as e:
                last_exc = e
                resp = None
        if resp is None:
            raise GithubApiError("network", "Could not reach GitHub.") from last_exc
        logger.info("github %s %s -> %s", method, path, resp.status_code)
        if resp.status_code == 401:
            raise GithubAuthError("token_invalid", "Your GitHub token is not valid or was revoked.")
        _raise_if_rate_limited(resp)
        if resp.status_code not in ok:
            raise GithubApiError("api", f"GitHub {method} {path} failed ({resp.status_code}).", status=resp.status_code)
        return resp

    def login(self) -> str:
        if self._login is None:
            self._login = self._request("GET", "/user", ok=(200,)).json().get("login") or ""
        return self._login

    def _upstream_owner(self) -> str:
        return self._upstream.split("/")[0]

    def ensure_fork(self) -> str:
        """The repo to push the node branch to: the upstream itself when the connected
        account owns it (GitHub can't fork your own repo — a same-repo PR is used),
        otherwise an idempotent fork. Returns the working repo's ``full_name``."""
        if self.login().lower() == self._upstream_owner().lower():
            return self._upstream

        created = self._request("POST", f"/repos/{self._upstream}/forks", ok=(200, 202)).json()
        full_name = created.get("full_name") or f"{self.login()}/{self._upstream.split('/')[-1]}"

        repo = self._request("GET", f"/repos/{full_name}", ok=(200, 404))
        if repo.status_code == 404:
            raise ForkPendingError()
        data = repo.json()
        if not data.get("default_branch"):
            raise ForkPendingError()
        if not data.get("fork") or (data.get("parent") or {}).get("full_name") != self._upstream:
            raise GithubApiError("fork_unusable", "That repository is not a fork of the community registry.")
        return full_name

    def sync_fork(self, fork: str, branch: str) -> None:
        """Best-effort merge-upstream so the fork's base branch matches the registry; never fatal."""
        if fork == self._upstream:
            return  # owner pushes straight to upstream; nothing to sync
        try:
            resp = self._request(
                "POST",
                f"/repos/{fork}/merge-upstream",
                json={"branch": branch},
                ok=(200, 404, 409, 422),
            )
        except GithubApiError as e:
            logger.info("github merge-upstream skipped (%s)", e.reason)
            return
        if resp.status_code != 200:
            logger.info("github merge-upstream non-200 (%s)", resp.status_code)

    def base_sha(self, fork: str) -> tuple[str, str]:
        """(default_branch, head_sha) of the fork's default branch."""
        default_branch = self._request("GET", f"/repos/{fork}", ok=(200,)).json().get("default_branch") or "main"
        ref = self._request("GET", f"/repos/{fork}/git/ref/heads/{default_branch}", ok=(200,)).json()
        return default_branch, ref["object"]["sha"]

    def ensure_branch(self, fork: str, branch: str, base_sha: str) -> bool:
        """Create ``branch`` at ``base_sha``. Returns True when it already existed (422)."""
        resp = self._request(
            "POST",
            f"/repos/{fork}/git/refs",
            json={"ref": f"refs/heads/{branch}", "sha": base_sha},
            ok=(201, 422),
        )
        return resp.status_code == 422

    def commit_files(self, fork: str, branch: str, base_sha: str, files: list[tuple[str, bytes]], message: str) -> str:
        """One atomic commit of ``files`` (base64 blobs, PNG-safe), force-updating ``branch``. Returns its sha."""
        base_tree = self._request("GET", f"/repos/{fork}/git/commits/{base_sha}", ok=(200,)).json()["tree"]["sha"]

        tree_entries = []
        for path, data in files:
            content = base64.b64encode(data).decode("ascii")
            blob = self._request(
                "POST",
                f"/repos/{fork}/git/blobs",
                json={"content": content, "encoding": "base64"},
                ok=(201,),
            ).json()
            tree_entries.append({"path": path, "mode": "100644", "type": "blob", "sha": blob["sha"]})

        tree = self._request(
            "POST",
            f"/repos/{fork}/git/trees",
            json={"base_tree": base_tree, "tree": tree_entries},
            ok=(201,),
        ).json()
        commit = self._request(
            "POST",
            f"/repos/{fork}/git/commits",
            json={"message": message, "tree": tree["sha"], "parents": [base_sha]},
            ok=(201,),
        ).json()
        self._request(
            "PATCH",
            f"/repos/{fork}/git/refs/heads/{branch}",
            json={"sha": commit["sha"], "force": True},
            ok=(200,),
        )
        return commit["sha"]

    def open_or_get_pr(self, branch: str, title: str, body: str) -> PrResult:
        """Open a PR from ``login:branch`` into the registry, or return the existing open one."""
        head = f"{self.login()}:{branch}"
        existing = self._pulls_for_head(head)
        if existing is not None:
            return existing

        resp = self._request(
            "POST",
            f"/repos/{self._upstream}/pulls",
            json={
                "title": title,
                "body": body,
                "head": head,
                "base": _UPSTREAM_BASE_BRANCH,
                "maintainer_can_modify": True,
            },
            ok=(201, 422),
        )
        if resp.status_code == 422:
            existing = self._pulls_for_head(head)
            if existing is not None:
                return existing
            raise GithubApiError("api", "GitHub rejected the pull request.", status=422)
        pr = resp.json()
        return PrResult(url=pr["html_url"], number=pr["number"], created=True)

    def _pulls_for_head(self, head: str) -> PrResult | None:
        pulls = self._request(
            "GET",
            f"/repos/{self._upstream}/pulls",
            params={"state": "open", "head": head},
            ok=(200,),
        ).json()
        if not pulls:
            return None
        pr = pulls[0]
        return PrResult(url=pr["html_url"], number=pr["number"], created=False)

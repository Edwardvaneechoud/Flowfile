"""Open GitHub issues asking for nodes Flowfile does not have yet.

The renderer's CSP forbids GitHub, so core fetches the list: every open issue in the
Flowfile repo carrying one of the :data:`LABELS`. An Alteryx request's title follows
``[Alteryx node] <key>`` (``<key>`` from ``converters/alteryx/tool_identity.tool_key``)
so the import dialog can match a placeholder row to it; general requests are free-form
and the help dialog searches their titles. The result is cached in-process for the
community cache TTL (a couple of unauthenticated calls per hour keeps well inside
GitHub's per-IP budget); a failed fetch is cached briefly as empty so the dialogs fall
back to "file a new request" instead of hammering GitHub.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Literal

import httpx
from pydantic import BaseModel

from flowfile_core.configs.settings import get_community_cache_ttl

logger = logging.getLogger(__name__)

FLOWFILE_REPO = "edwardvaneechoud/Flowfile"
LABELS: dict[str, str] = {"alteryx": "alteryx-node-request", "general": "node-request"}
TITLE_PREFIX = "[Alteryx node]"
API_URL = f"https://api.github.com/repos/{FLOWFILE_REPO}/issues"
API_VERSION = "2022-11-28"
HTTP_TIMEOUT = 10.0
FAILURE_TTL = 60.0

RequestKind = Literal["alteryx", "general"]

_TITLE_RE = re.compile(r"^\[Alteryx node\]\s*([A-Za-z_][A-Za-z0-9_]*)\s*$")

_cache: tuple[float, list[NodeRequest]] | None = None
_cache_lock = threading.Lock()


class NodeRequest(BaseModel):
    number: int
    title: str
    url: str
    kind: RequestKind
    tool_key: str | None = None
    upvotes: int = 0


def _get(http: httpx.Client | None, label: str) -> httpx.Response:
    params = {"labels": label, "state": "open", "sort": "created", "direction": "asc", "per_page": 100}
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": API_VERSION}
    if http is not None:
        return http.get(API_URL, params=params, headers=headers)
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        return client.get(API_URL, params=params, headers=headers)


def _parse(issue: object, kind: RequestKind) -> NodeRequest | None:
    if not isinstance(issue, dict) or "pull_request" in issue:
        return None
    number, title, url = issue.get("number"), issue.get("title"), issue.get("html_url")
    if not isinstance(number, int) or not isinstance(title, str) or not isinstance(url, str):
        return None
    tool_key = None
    if kind == "alteryx":
        match = _TITLE_RE.match(title)
        if match is None:
            return None
        tool_key = match.group(1)
    reactions = issue.get("reactions")
    upvotes = reactions.get("+1") if isinstance(reactions, dict) else 0
    if not isinstance(upvotes, int):
        upvotes = 0
    return NodeRequest(number=number, title=title, url=url, kind=kind, tool_key=tool_key, upvotes=upvotes)


def fetch_open_requests(*, http: httpx.Client | None = None) -> list[NodeRequest]:
    """Every open request, oldest first; an issue carrying both labels counts once, as Alteryx."""
    seen: set[int] = set()
    requests: list[NodeRequest] = []
    for kind, label in LABELS.items():
        response = _get(http, label)
        response.raise_for_status()
        for issue in response.json():
            parsed = _parse(issue, kind)  # type: ignore[arg-type]
            if parsed is not None and parsed.number not in seen:
                seen.add(parsed.number)
                requests.append(parsed)
    return sorted(requests, key=lambda request: request.number)


def open_requests(*, http: httpx.Client | None = None) -> list[NodeRequest]:
    """Cached :func:`fetch_open_requests`; never raises, a failure reads as no open requests."""
    global _cache
    with _cache_lock:
        now = time.monotonic()
        if _cache is not None and _cache[0] > now:
            return list(_cache[1])
        try:
            requests = fetch_open_requests(http=http)
            ttl = float(get_community_cache_ttl())
        except Exception as exc:
            logger.warning("Fetching open node requests failed: %s", type(exc).__name__)
            requests, ttl = [], FAILURE_TTL
        _cache = (now + ttl, requests)
        return list(requests)


def alteryx_request_urls(requests: list[NodeRequest]) -> dict[str, str]:
    """Tool key → issue URL, the oldest open request winning a duplicate key."""
    urls: dict[str, str] = {}
    for request in requests:
        if request.tool_key is not None:
            urls.setdefault(request.tool_key, request.url)
    return urls


def _reset_cache() -> None:
    global _cache
    with _cache_lock:
        _cache = None

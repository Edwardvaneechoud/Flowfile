"""Open GitHub issues asking for Alteryx tools the importer cannot convert yet.

The renderer's CSP forbids GitHub, so core fetches the list: every open issue in the
Flowfile repo carrying :data:`ISSUE_LABEL` whose title is ``[Alteryx node] <key>``,
where ``<key>`` is a :func:`tool_identity.tool_key`. The result is cached in-process
for the community cache TTL (one unauthenticated call per hour keeps well inside
GitHub's per-IP budget); a failed fetch is cached briefly as empty so the import
dialog falls back to "create a request" instead of hammering GitHub.
"""

from __future__ import annotations

import logging
import re
import threading
import time

import httpx

from flowfile_core.configs.settings import get_community_cache_ttl

logger = logging.getLogger(__name__)

FLOWFILE_REPO = "edwardvaneechoud/Flowfile"
ISSUE_LABEL = "alteryx-node-request"
TITLE_PREFIX = "[Alteryx node]"
API_URL = f"https://api.github.com/repos/{FLOWFILE_REPO}/issues"
API_VERSION = "2022-11-28"
HTTP_TIMEOUT = 10.0
FAILURE_TTL = 60.0

_TITLE_RE = re.compile(r"^\[Alteryx node\]\s*([A-Za-z_][A-Za-z0-9_]*)\s*$")

_cache: tuple[float, dict[str, str]] | None = None
_cache_lock = threading.Lock()


def _get(http: httpx.Client | None) -> httpx.Response:
    params = {"labels": ISSUE_LABEL, "state": "open", "sort": "created", "direction": "asc", "per_page": 100}
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": API_VERSION}
    if http is not None:
        return http.get(API_URL, params=params, headers=headers)
    with httpx.Client(timeout=HTTP_TIMEOUT) as client:
        return client.get(API_URL, params=params, headers=headers)


def fetch_open_requests(*, http: httpx.Client | None = None) -> dict[str, str]:
    """Tool key → issue URL for every open request, oldest issue winning a duplicate key."""
    response = _get(http)
    response.raise_for_status()
    requests: dict[str, str] = {}
    for issue in response.json():
        if not isinstance(issue, dict) or "pull_request" in issue:
            continue
        match = _TITLE_RE.match(issue.get("title") or "")
        url = issue.get("html_url")
        if match and isinstance(url, str):
            requests.setdefault(match.group(1), url)
    return requests


def open_requests(*, http: httpx.Client | None = None) -> dict[str, str]:
    """Cached :func:`fetch_open_requests`; never raises, a failure reads as no open requests."""
    global _cache
    with _cache_lock:
        now = time.monotonic()
        if _cache is not None and _cache[0] > now:
            return dict(_cache[1])
        try:
            requests = fetch_open_requests(http=http)
            ttl = float(get_community_cache_ttl())
        except Exception as exc:
            logger.warning("Fetching open Alteryx node requests failed: %s", type(exc).__name__)
            requests, ttl = {}, FAILURE_TTL
        _cache = (now + ttl, requests)
        return dict(requests)


def _reset_cache() -> None:
    global _cache
    with _cache_lock:
        _cache = None

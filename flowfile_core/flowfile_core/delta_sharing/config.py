"""Delta Sharing settings, read from the environment on every call so they can change without a restart."""

from __future__ import annotations

import os

PROTOCOL_PREFIX = "/delta-sharing"
DEFAULT_URL_TTL_SECONDS = 3600
MIN_URL_TTL_SECONDS = 60
MAX_URL_TTL_SECONDS = 7 * 24 * 3600

_FALSY = {"0", "false", "no", "off"}


def enabled() -> bool:
    """Whether Delta Sharing is served: ``FLOWFILE_DELTA_SHARING`` unset or anything but a falsy word."""
    return os.environ.get("FLOWFILE_DELTA_SHARING", "1").strip().lower() not in _FALSY


def url_ttl_seconds() -> int:
    """How long a file URL handed to a recipient stays valid (``FLOWFILE_DELTA_SHARING_URL_TTL``, default 1 h)."""
    raw = os.environ.get("FLOWFILE_DELTA_SHARING_URL_TTL", "").strip()
    try:
        value = int(raw) if raw else DEFAULT_URL_TTL_SECONDS
    except ValueError:
        value = DEFAULT_URL_TTL_SECONDS
    return max(MIN_URL_TTL_SECONDS, min(MAX_URL_TTL_SECONDS, value))


def configured_endpoint() -> str | None:
    """The public base URL of the protocol (``FLOWFILE_DELTA_SHARING_ENDPOINT``), without a trailing slash."""
    raw = os.environ.get("FLOWFILE_DELTA_SHARING_ENDPOINT", "").strip()
    return raw.rstrip("/") or None

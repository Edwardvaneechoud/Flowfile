"""Anonymous, opt-in usage telemetry.

Nothing leaves the machine unless the user granted consent in the UI *and* an
endpoint is configured. Payloads are structurally incapable of carrying user
content: every event name is on a frozen list, every prop key is on that event's
allowlist, and every prop value must match a frozen bucket / enum / identifier
rule before it is enqueued. Delivery is a bounded queue drained by one daemon
thread, so a slow or dead collector can never block or crash the caller.

Import stays light and side-effect free (no httpx, no yaml at module level):
flowfile_worker's spawned children pay for every module this drags in.
"""

from __future__ import annotations

import atexit
import logging
import os
import queue
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shared.storage_config import storage

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger("flowfile.telemetry")

ENV_KILL_SWITCH = "FLOWFILE_TELEMETRY"
ENV_ENDPOINT = "FLOWFILE_TELEMETRY_ENDPOINT"
DEFAULT_ENDPOINT = ""  # project collector URL baked in at release; "" = none ships, telemetry disabled unless env-set
FALSY = ("false", "0", "no", "off")
KNOWN_MODES = ("electron", "docker", "package")

QUEUE_MAXSIZE = 256
BATCH_SIZE = 100
FLUSH_INTERVAL_SECONDS = 5.0
REQUEST_TIMEOUT_SECONDS = 5.0
CONNECT_TIMEOUT_SECONDS = 2.0
FLUSH_MIN_REMAINING_SECONDS = 0.05

NODE_COUNT_BUCKETS = ("1-3", "4-7", "8-15", "16-30", "31+")
DURATION_BUCKETS = ("<1s", "1-10s", "10-60s", "1-5m", "5-30m", "30m+")
ROW_BUCKETS = ("0", "1-100", "101-10k", "10k-1M", "1M+")
EXPORT_TARGETS = ("polars", "flowframe", "project", "project_zip", "project_save")

MAX_IDENTIFIER_LENGTH = 64
MAX_NODE_TYPES = 60

EVENTS: dict[str, frozenset[str]] = {
    "app_started": frozenset(),
    "flow_created": frozenset(),
    "flow_run_started": frozenset(),
    "flow_run_succeeded": frozenset({"node_count_bucket", "node_types", "duration_bucket", "used_sample_data"}),
    "flow_run_failed": frozenset({"error_class"}),
    "activation": frozenset(),
    "ai_diff_accepted": frozenset(),
    "ai_diff_rejected": frozenset(),
    "catalog_used": frozenset(),
    "schedule_created": frozenset(),
    "kernel_used": frozenset(),
    "export_code_used": frozenset({"target"}),
}

SETTINGS_HEADER = (
    "# Anonymous usage telemetry — managed from the Flowfile UI (Compute -> Privacy).\n"
    "# FLOWFILE_TELEMETRY=0 disables telemetry regardless of this file.\n"
    "# Docs: https://edwardvaneechoud.github.io/Flowfile/users/telemetry.html\n"
)

_UNSET = object()

_lock = threading.RLock()
_queue: queue.Queue | None = None
_wake: threading.Event | None = None
_stop: threading.Event | None = None
_thread: threading.Thread | None = None
_emitted_once: set[str] = set()
_state_cache: Any = _UNSET
_version_cache: str | None = None
_atexit_registered = False


def bucket_node_count(count: int) -> str:
    """Coarse node-count band; anything below 1 reports as the smallest band."""
    if count <= 3:
        return "1-3"
    if count <= 7:
        return "4-7"
    if count <= 15:
        return "8-15"
    if count <= 30:
        return "16-30"
    return "31+"


def bucket_duration_seconds(seconds: float) -> str:
    """Coarse wall-clock band for a run duration."""
    if seconds < 1:
        return "<1s"
    if seconds < 10:
        return "1-10s"
    if seconds < 60:
        return "10-60s"
    if seconds < 300:
        return "1-5m"
    if seconds < 1800:
        return "5-30m"
    return "30m+"


def bucket_rows(count: int) -> str:
    """Coarse row-count band. Not emitted by any event yet; kept for future ones."""
    if count <= 0:
        return "0"
    if count <= 100:
        return "1-100"
    if count <= 10_000:
        return "101-10k"
    if count <= 1_000_000:
        return "10k-1M"
    return "1M+"


@dataclass(frozen=True)
class TelemetryState:
    """What the consent file holds. ``install_id`` is absent unless consent was granted."""

    consent: bool
    install_id: str | None


@dataclass(frozen=True)
class TelemetryStatus:
    """What the UI needs to render the privacy toggle. Never carries the install id."""

    available: bool
    consent: bool | None
    env_kill_switch: bool
    endpoint_configured: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "consent": self.consent,
            "env_kill_switch": self.env_kill_switch,
            "endpoint_configured": self.endpoint_configured,
        }


@dataclass(frozen=True)
class ConsentResult:
    """Outcome of a consent change: the stored state, and whether the write stuck."""

    status: TelemetryStatus
    persisted: bool


def _settings_file() -> Path:
    return storage.base_directory / "telemetry.yaml"


def load_state() -> TelemetryState | None:
    """Read the consent file, or ``None`` when it is missing, unreadable or malformed."""
    import yaml

    try:
        data = yaml.safe_load(_settings_file().read_text(encoding="utf-8"))
        consent = bool(data["consent"])
        identifier = data.get("install_id")
    except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError):
        return None
    return TelemetryState(consent=consent, install_id=identifier if isinstance(identifier, str) else None)


def persist_state(consent: bool, install_id: str | None) -> bool:
    """Save consent so it survives restarts; atomic, never raises.

    Commented YAML for the same reason as ``worker_pool.yaml``: the file is
    hand-editable and diffs cleanly. Declining drops the install id entirely —
    turning telemetry off forgets the identifier rather than parking it.
    """
    target = _settings_file()
    content = SETTINGS_HEADER + f"consent: {'true' if consent else 'false'}\n"
    if consent and install_id:
        content += f"install_id: {install_id}\n"
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".yaml.tmp")
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, target)
        return True
    except OSError as exc:
        logger.debug("Could not persist telemetry consent to %s: %s", target, exc)
        return False


def _cached_state() -> TelemetryState | None:
    global _state_cache
    with _lock:
        if _state_cache is _UNSET:
            _state_cache = load_state()
        return _state_cache


def _invalidate_state_cache() -> None:
    global _state_cache
    with _lock:
        _state_cache = _UNSET


def _kill_switch_engaged() -> bool:
    """Gate 1: an explicitly falsy ``FLOWFILE_TELEMETRY`` kills telemetry outright.

    A truthy value does not grant consent; it only declines to kill.
    """
    raw = os.environ.get(ENV_KILL_SWITCH)
    if raw is None:
        return False
    return raw.strip().lower() in FALSY


def _testing_disabled() -> bool:
    """Gate 2: the exact ``TESTING=True`` marker used by shared/storage_config."""
    return os.environ.get("TESTING") == "True"


def _endpoint() -> str | None:
    """Gate 3: no collector resolves means nothing can be sent.

    Resolution: env override > baked-in DEFAULT_ENDPOINT > None. Shipping a
    hosted collector is a one-constant change; consent and the kill switch
    gate it regardless.
    """
    return os.environ.get(ENV_ENDPOINT) or DEFAULT_ENDPOINT or None


def is_available() -> bool:
    """Gates 1-3: telemetry *could* run here, whatever the user chose."""
    return not _kill_switch_engaged() and not _testing_disabled() and _endpoint() is not None


def consent() -> bool | None:
    """Stored consent, or ``None`` when the user has never been asked."""
    state = _cached_state()
    return None if state is None else state.consent


def is_enabled() -> bool:
    """All four gates, in order. The single question every emit path asks."""
    return is_available() and consent() is True


def install_id() -> str | None:
    """The random per-install identifier; only meaningful once consent was granted."""
    state = _cached_state()
    return None if state is None else state.install_id


def get_status() -> TelemetryStatus:
    return TelemetryStatus(
        available=is_available(),
        consent=consent(),
        env_kill_switch=_kill_switch_engaged(),
        endpoint_configured=_endpoint() is not None,
    )


def set_consent(enabled: bool) -> ConsentResult:
    """Grant or revoke consent, minting a fresh install id on each fresh grant.

    ``persisted`` is False when the consent file could not be written (read-only
    storage), in which case the returned status still describes what is stored —
    callers must not report the requested choice as taken.
    """
    state = load_state()
    identifier = None
    if enabled:
        identifier = state.install_id if state else None
        if not identifier:
            identifier = str(uuid.uuid4())
    persisted = persist_state(enabled, identifier)
    _invalidate_state_cache()
    return ConsentResult(status=get_status(), persisted=persisted and consent() is enabled)


def _app_version() -> str:
    global _version_cache
    if _version_cache is None:
        try:
            from importlib.metadata import version

            _version_cache = version("flowfile")
        except Exception:
            _version_cache = "unknown"
    return _version_cache


def _platform() -> str:
    if sys.platform == "darwin":
        return "darwin"
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform in ("win32", "cygwin"):
        return "windows"
    return "other"


def _mode() -> str:
    mode = os.environ.get("FLOWFILE_MODE", "electron")
    return mode if mode in KNOWN_MODES else "other"


def _clean_node_types(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    for name in value:
        if not isinstance(name, str) or not name.isidentifier() or len(name) > MAX_IDENTIFIER_LENGTH:
            return None
    return sorted(value)[:MAX_NODE_TYPES]


def _is_valid_prop(key: str, value: Any) -> bool:
    if key == "node_count_bucket":
        return value in NODE_COUNT_BUCKETS
    if key == "duration_bucket":
        return value in DURATION_BUCKETS
    if key == "used_sample_data":
        return isinstance(value, bool)
    if key == "target":
        return value in EXPORT_TARGETS
    if key == "error_class":
        return isinstance(value, str) and value.isidentifier() and len(value) <= MAX_IDENTIFIER_LENGTH
    return False


def _sanitize_props(event: str, props: dict[str, Any]) -> dict[str, Any]:
    """Keep only allowlisted keys carrying values that match the frozen rules."""
    clean: dict[str, Any] = {}
    for key in EVENTS[event]:
        if key not in props:
            continue
        value = props[key]
        if key == "node_types":
            value = _clean_node_types(value)
            if value is None:
                continue
        elif not _is_valid_prop(key, value):
            continue
        clean[key] = value
    return clean


def _envelope(event: str, props: dict[str, Any], identifier: str) -> dict[str, Any]:
    return {
        "event": event,
        "install_id": identifier,
        "app_version": _app_version(),
        "platform": _platform(),
        "mode": _mode(),
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "props": _sanitize_props(event, props),
    }


def _post(url: str, json: dict[str, Any], timeout: float | None = None) -> httpx.Response:
    """Single seam for the outbound request — tests monkeypatch this.

    ``timeout`` is the whole-request budget; ``None`` means the standard one.
    """
    import httpx

    budget = REQUEST_TIMEOUT_SECONDS if timeout is None else timeout
    connect = min(CONNECT_TIMEOUT_SECONDS, budget)
    return httpx.post(url, json=json, timeout=httpx.Timeout(budget, connect=connect), follow_redirects=False)


def _send(batch: list[dict[str, Any]], timeout: float | None = None) -> None:
    url = _endpoint()
    if not url:
        return
    try:
        if timeout is None:
            _post(url, {"events": batch})
        else:
            _post(url, {"events": batch}, timeout=timeout)
    except Exception:
        logger.debug("telemetry delivery failed", exc_info=True)


def _take_batch(q: queue.Queue) -> list[dict[str, Any]]:
    batch: list[dict[str, Any]] = []
    while len(batch) < BATCH_SIZE:
        try:
            batch.append(q.get_nowait())
        except queue.Empty:
            break
    return batch


def _loop(q: queue.Queue, wake: threading.Event, stop: threading.Event) -> None:
    """Drain on a ~5s tick or whenever an emit wakes us, until this generation is reset."""
    while not stop.is_set():
        wake.wait(FLUSH_INTERVAL_SECONDS)
        wake.clear()
        while not stop.is_set():
            batch = _take_batch(q)
            if not batch:
                break
            _send(batch)


def _ensure_worker() -> None:
    global _wake, _stop, _thread, _atexit_registered
    if _thread is not None and _thread.is_alive():
        return
    _wake = threading.Event()
    _stop = threading.Event()
    _thread = threading.Thread(target=_loop, args=(_get_queue(), _wake, _stop), name="telemetry-flush", daemon=True)
    _thread.start()
    if not _atexit_registered:
        atexit.register(flush)
        _atexit_registered = True


def _get_queue() -> queue.Queue:
    global _queue
    if _queue is None:
        _queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
    return _queue


def _enqueue(event: str, props: dict[str, Any] | None) -> bool:
    if not is_enabled():
        return False
    if event not in EVENTS:
        return False
    identifier = install_id()
    if not identifier:
        return False
    envelope = _envelope(event, props or {}, identifier)
    with _lock:
        q = _get_queue()
        try:
            q.put_nowait(envelope)
        except queue.Full:
            return False
        _ensure_worker()
        if _wake is not None:
            _wake.set()
    return True


def emit(event: str, props: dict[str, Any] | None = None) -> None:
    """Queue one event. Never blocks, never raises, drops silently when disabled or full."""
    try:
        _enqueue(event, props)
    except Exception:
        logger.debug("telemetry emit failed", exc_info=True)


def emit_once(event: str, props: dict[str, Any] | None = None) -> None:
    """Like ``emit``, but at most once per process for this event/props combination."""
    try:
        key = f"{event}:{sorted((k, repr(v)) for k, v in (props or {}).items())}"
        with _lock:
            if key in _emitted_once:
                return
            if _enqueue(event, props):
                _emitted_once.add(key)
    except Exception:
        logger.debug("telemetry emit_once failed", exc_info=True)


def flush(timeout: float = 2.0) -> None:
    """Best-effort synchronous drain for short-lived processes. Returns silently on timeout.

    Each request is capped by what is left of *timeout*, so a CLI exit or a
    desktop quit never waits out the full per-request budget on a dead collector.
    """
    try:
        q = _queue
        if q is None:
            return
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= FLUSH_MIN_REMAINING_SECONDS:
                return
            batch = _take_batch(q)
            if not batch:
                return
            _send(batch, timeout=min(remaining, REQUEST_TIMEOUT_SECONDS))
    except Exception:
        logger.debug("telemetry flush failed", exc_info=True)


def _reset_for_tests() -> None:
    """Drop every scrap of module state so each test starts from a known point."""
    global _queue, _wake, _stop, _thread, _state_cache, _version_cache
    with _lock:
        if _stop is not None:
            _stop.set()
        if _wake is not None:
            _wake.set()
        _queue = None
        _wake = None
        _stop = None
        _thread = None
        _emitted_once.clear()
        _state_cache = _UNSET
        _version_cache = None

"""Anonymous, opt-in usage telemetry.

Nothing leaves the machine unless the user granted consent in the UI *and* an
endpoint is configured. Payloads are structurally incapable of carrying user
content: every event name is on a frozen list, every prop key is on that event's
allowlist, and every prop value must match a frozen bucket / enum / identifier
rule before it is enqueued. Delivery is a bounded queue drained by one daemon
thread, so a slow or dead collector can never block or crash the caller. A batch
that fails to send is parked on a capped on-disk spool and re-sent when a later
process starts, which makes delivery at-least-once — hence the per-event
``event_id`` in the envelope.

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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shared import telemetry_spool as spool
from shared._version import get_version
from shared.storage_config import storage

if TYPE_CHECKING:
    import httpx

logger = logging.getLogger("flowfile.telemetry")

ENV_KILL_SWITCH = "FLOWFILE_TELEMETRY"
ENV_ENDPOINT = "FLOWFILE_TELEMETRY_ENDPOINT"
DEFAULT_ENDPOINT = "https://events.flowfile.app/events"
FALSY = ("false", "0", "no", "off")
KNOWN_MODES = ("electron", "docker", "package")

QUEUE_MAXSIZE = 256
BATCH_SIZE = 100
FLUSH_INTERVAL_SECONDS = 5.0
REQUEST_TIMEOUT_SECONDS = 5.0
CONNECT_TIMEOUT_SECONDS = 2.0
FLUSH_MIN_REMAINING_SECONDS = 0.05

SPOOL_MAX_BYTES = 16 * 1024 * 1024
SPOOL_MAX_AGE_SECONDS = 30 * 24 * 60 * 60
SPOOL_BATCH_BYTES = 250 * 1024
PERMANENT_STATUSES = (413, 422)

SEND_OK = "ok"
SEND_TRANSIENT = "transient"
SEND_PERMANENT = "permanent"

NODE_COUNT_BUCKETS = ("1-3", "4-7", "8-15", "16-30", "31+")
DURATION_BUCKETS = ("<1s", "1-10s", "10-60s", "1-5m", "5-30m", "30m+")
ROW_BUCKETS = ("0", "1-100", "101-10k", "10k-1M", "1M+")
EXPORT_TARGETS = ("polars", "flowframe", "project_zip", "project_save")

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
    "# Anonymous usage telemetry — managed from the Flowfile UI (Settings -> Preferences -> Privacy).\n"
    "# FLOWFILE_TELEMETRY=0 disables telemetry regardless of this file.\n"
    "# Docs: https://edwardvaneechoud.github.io/Flowfile/users/telemetry.html\n"
)

_UNSET = object()

_lock = threading.RLock()
_send_guard = threading.Lock()  # held across the daemon's take-and-send; flush waits on it to catch an in-flight POST
_spool_lock = threading.Lock()
_spool_generation = 0  # bumped by each compaction so a drain can spot a rewrite under its feet
_queue: queue.Queue | None = None
_wake: threading.Event | None = None
_stop: threading.Event | None = None
_thread: threading.Thread | None = None
_emitted_once: set[str] = set()
_state_cache: Any = _UNSET
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


def _spool_file() -> Path:
    """Undelivered events, beside the consent file. Never under temp/ or cache/ — both are swept."""
    return storage.base_directory / "telemetry_spool.jsonl"


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
    tmp = target.with_suffix(".yaml.tmp")
    content = SETTINGS_HEADER + f"consent: {'true' if consent else 'false'}\n"
    if consent and install_id:
        content += f"install_id: {install_id}\n"
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, target)
        return True
    except OSError as exc:
        logger.debug("Could not persist telemetry consent to %s: %s", target, exc)
        return False
    finally:
        try:
            tmp.unlink(missing_ok=True)  # a failed replace must not leave the install id behind
        except OSError:
            pass


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
    if not enabled:
        # buffered events carry the id that was just forgotten
        _purge_queue()
        _purge_spool()
    return ConsentResult(status=get_status(), persisted=persisted and consent() is enabled)


def _app_version() -> str:
    """The in-source version, not importlib.metadata: the Docker images run
    ``poetry install --no-root``, so no dist-info exists to look up."""
    return get_version()


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
        "event_id": str(uuid.uuid4()),
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


def _warn_on_rejected(response: Any, sent: int) -> None:
    """Surface a collector that accepted the request but dropped events from it.

    A 2xx with a non-zero ``rejected`` count means the collector's schema is
    older than this client's — the events are gone and no retry will help, so
    the log line is the only signal an operator gets. The body is untrusted:
    anything unreadable is ignored.
    """
    try:
        body = response.json()
        rejected = body["rejected"]
        if not isinstance(rejected, int) or isinstance(rejected, bool) or rejected <= 0:
            return
    except Exception:
        return
    logger.warning(
        "telemetry collector rejected %d of %d event(s); redeploy the collector to match this client's schema",
        rejected,
        sent,
    )


def _send(batch: list[dict[str, Any]], timeout: float | None = None, spool_on_failure: bool = True) -> str:
    """Post one batch and report how it went.

    A transient failure — dead socket, timeout, or any status that is not a 2xx
    and not a permanent rejection — parks the batch on the spool for a later
    drain. 413 and 422 are permanent: the same bytes would be refused again, so
    they are dropped exactly as they were before the spool existed. The drain
    itself passes ``spool_on_failure=False``; its lines are already on disk.

    The gate is re-read here rather than trusted from the caller: a revoke can
    land between an enqueue and this post, and those bytes must never leave.
    """
    if not is_enabled():
        return SEND_PERMANENT
    url = _endpoint()
    if not url:
        return SEND_PERMANENT
    try:
        if timeout is None:
            response = _post(url, {"events": batch})
        else:
            response = _post(url, {"events": batch}, timeout=timeout)
        status = getattr(response, "status_code", 200)
        if 200 <= status < 300:
            _warn_on_rejected(response, len(batch))
            return SEND_OK
        if status in PERMANENT_STATUSES:
            return SEND_PERMANENT
    except Exception:
        logger.debug("telemetry delivery failed", exc_info=True)
    if spool_on_failure:
        _spool_append(batch)
    return SEND_TRANSIENT


def _purge_queue() -> None:
    """Discard whatever is queued in memory. Called when consent is revoked, never otherwise."""
    with _lock:
        q = _queue
    if q is None:
        return
    while True:
        try:
            q.get_nowait()
        except queue.Empty:
            return


def _purge_spool() -> None:
    """Delete everything buffered. Called when consent is revoked, never otherwise.

    The generation bump is what stops a drain that already read these lines: it
    is holding offsets into a file that no longer exists.
    """
    global _spool_generation
    with _spool_lock:
        spool.purge(_spool_file())
        _spool_generation += 1


def _spool_append(batch: list[dict[str, Any]]) -> None:
    """Park a failed batch on disk. Behind every gate, and silent on any I/O failure."""
    global _spool_generation
    if not is_enabled():
        return
    try:
        with _spool_lock:
            if spool.append(_spool_file(), batch, SPOOL_MAX_BYTES):
                _spool_generation += 1
    except Exception:
        logger.debug("telemetry spool append failed", exc_info=True)


def _spool_batches(entries: list[str], cutoff: datetime):
    """Yield ``(batch, entries_consumed)`` pairs, skipping corrupt and expired lines.

    A batch is cut at whichever cap trips first: ``BATCH_SIZE`` events or
    ``SPOOL_BATCH_BYTES`` of serialized body, since the collector's body cap
    binds before its count cap on large envelopes.
    """
    batch: list[dict[str, Any]] = []
    size = 0
    consumed = 0
    for index, line in enumerate(entries):
        envelope = spool.decode(line)
        stamped = None if envelope is None else _spooled_at(envelope)
        if envelope is None or stamped is None or stamped < cutoff:
            continue
        length = len(line) + 1
        if batch and (len(batch) >= BATCH_SIZE or size + length > SPOOL_BATCH_BYTES):
            yield batch, consumed
            batch, size = [], 0
        batch.append(envelope)
        size += length
        consumed = index + 1
    if batch:
        yield batch, consumed


def _spooled_at(envelope: dict[str, Any]) -> datetime | None:
    try:
        return datetime.strptime(envelope["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (KeyError, TypeError, ValueError):
        return None


def _drain_spool() -> None:
    """Deliver spooled events oldest-first. Daemon thread only, never the caller's.

    A transient failure stops the pass with the survivors still on disk; the next
    process start picks them up. Lines that were delivered, permanently rejected,
    corrupt or older than ``SPOOL_MAX_AGE_SECONDS`` are dropped in one rewrite at
    the end — unless a compaction or a revoke rewrote the file underneath us, in
    which case the pass gives up its offsets and lets the next drain re-send
    (at-least-once is why the envelope carries ``event_id``). Consent and the
    generation are re-read at every batch boundary, so a revoke stops the pass
    within one batch instead of delivering the rest under an erased id.
    """
    if not is_enabled():
        return
    path = _spool_file()
    with _spool_lock:
        entries = spool.load(path)
        generation = _spool_generation
    if not entries:
        return

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=SPOOL_MAX_AGE_SECONDS)
    resolved = 0
    completed = True
    for batch, consumed in _spool_batches(entries, cutoff):
        with _spool_lock:
            stale = _spool_generation != generation
        if stale or not is_enabled():
            return
        with _send_guard:
            outcome = _send(batch, spool_on_failure=False)
        if outcome == SEND_TRANSIENT:
            completed = False
            break
        resolved = consumed
    if completed:
        resolved = len(entries)
    if resolved == 0:
        return
    with _spool_lock:
        if _spool_generation != generation:
            return
        spool.rewrite(path, spool.load(path)[resolved:])


def _take_batch(q: queue.Queue) -> list[dict[str, Any]]:
    batch: list[dict[str, Any]] = []
    while len(batch) < BATCH_SIZE:
        try:
            batch.append(q.get_nowait())
        except queue.Empty:
            break
    return batch


def _loop(q: queue.Queue, wake: threading.Event, stop: threading.Event) -> None:
    """Drain on a ~5s tick or whenever an emit wakes us, until this generation is reset.

    Whatever a previous process left spooled goes first, so a cron box delivers
    last run's events during this one. The guard is taken *before* the batch, so
    an empty queue plus a free guard is proof to ``flush`` that nothing is in
    flight.
    """
    try:
        _drain_spool()
    except Exception:
        logger.debug("telemetry spool drain failed", exc_info=True)
    while not stop.is_set():
        wake.wait(FLUSH_INTERVAL_SECONDS)
        wake.clear()
        while not stop.is_set():
            with _send_guard:
                if not is_enabled():
                    break
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
        _ensure_worker()  # also revives a dead worker when the queue is full
        try:
            q.put_nowait(envelope)
        except queue.Full:
            return False
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

    Drains the queue, then waits out whatever the daemon thread already took and
    is still posting — otherwise the terminal event of a CLI run is killed in
    flight by interpreter shutdown. Each request and that final wait are capped
    by what is left of *timeout*, so a CLI exit or a desktop quit never waits out
    the full per-request budget on a dead collector. Whatever the budget did not
    cover is spooled by ``_send`` rather than lost, so the budget can stay small.
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
                break
            _send(batch, timeout=min(remaining, REQUEST_TIMEOUT_SECONDS))
        _wait_for_in_flight(deadline)
    except Exception:
        logger.debug("telemetry flush failed", exc_info=True)


def _wait_for_in_flight(deadline: float) -> None:
    """Block until the daemon holds no batch, or the flush budget runs out."""
    remaining = deadline - time.monotonic()
    if remaining <= FLUSH_MIN_REMAINING_SECONDS:
        return
    if _send_guard.acquire(timeout=remaining):
        _send_guard.release()


def _reset_for_tests() -> None:
    """Drop every scrap of module state so each test starts from a known point.

    The old thread is stopped and briefly joined: a stale generation that is
    already past its ``stop`` check would otherwise resolve ``_post`` and
    ``_endpoint`` after monkeypatch teardown and post to the real collector.
    """
    global _queue, _wake, _stop, _thread, _state_cache, _spool_generation
    with _lock:
        stale = _thread
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
        _spool_generation = 0  # the spool file itself belongs to the fixture that redirected it
    if stale is not None and stale is not threading.current_thread():
        stale.join(0.5)

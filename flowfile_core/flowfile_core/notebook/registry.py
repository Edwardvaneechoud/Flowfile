"""Core's notebook sessions: one Python subprocess per ``(user_id, flow_id)``, driven over a framed pipe.

A :class:`NotebookSession` owns one child process at a time (a *generation*; restarts bump it). Per
generation core runs one reader thread that drains the child's protocol stream and one writer thread that
owns the child's stdin; neither ever runs on the event loop (V3 F3). ``run_cell``, ``clean_run`` and
``schemas`` block the calling (threadpool) thread on a ticket; a cell's ``stream`` and ``display`` messages
are collected on that ticket until its ``done``.

``reset`` on a session busy with a cell kills the child (terminate, 1 s, kill), starts it again and re-seeds
it: a blocked ``sleep``, socket or collect cannot be stopped from inside (V3 F2), and a queued reset would
wait behind it.

The :class:`NotebookSessionRegistry` starts sessions lazily, closes them after ``idle_ttl`` seconds without
use, evicts the least recently used past ``max_sessions``, closes a flow's sessions when the flow closes,
and at shutdown closes every stdin in parallel before a bounded parallel terminate/kill. It is the
bridge's ``CleanRunner`` (:func:`install`). ``notebook/kernel_adapter.py`` puts it behind the kernel routes.
"""

from __future__ import annotations

import logging
import os
import queue
import subprocess
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from flowfile_core.notebook import bootstrap, protocol
from shared.storage_config import storage

logger = logging.getLogger("flowfile.notebook.sessions")

DEFAULT_IDLE_TTL = 15 * 60
DEFAULT_MAX_SESSIONS = 3
REPLY_TIMEOUT = 600.0
RESTARTED_MESSAGE = "session restarted, variables lost"
_STOP = object()


def seed_snapshot(flow: Any) -> dict[str, Any]:
    """The seed payload of an open flow: its FlowfileData, parameters, node references and per-handle schemas."""
    names = {}
    schemas = {}
    for node in flow.nodes:
        reference = getattr(node.setting_input, "node_reference", None)
        if reference:
            names[node.node_id] = reference
        named = getattr(node, "_named_schemas", None) or {}
        if named:
            schemas[node.node_id] = {
                handle: [{"name": c.column_name, "data_type": c.data_type} for c in columns]
                for handle, columns in named.items()
            }
    return {
        "flowfile_data": flow.get_flowfile_data().model_dump(mode="json"),
        "parameters": [p.model_dump(mode="json") for p in flow.flow_settings.parameters or []],
        "names": names,
        "schemas": schemas,
    }


def _env_number(name: str, default: float) -> float:
    raw = os.environ.get(name)
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _spill_dir() -> str:
    path = storage.temp_directory / "notebook_sessions"
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def _log_path(flow_id: int):
    directory = storage.logs_directory
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"notebook_session_{flow_id}.log"


def kill_process(process: subprocess.Popen, grace: float = 1.0) -> None:
    """Terminate, wait ``grace`` seconds, then kill; never raises."""
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
            process.wait(timeout=grace)
        except Exception:
            logger.warning("notebook session %s did not die", process.pid)
    except Exception:
        logger.debug("terminate failed", exc_info=True)


class _Waiter:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.reply: dict[str, Any] | None = None
        self.events: list[dict[str, Any]] = []


class NotebookSession:
    """One user's Python session for one flow; see the module docstring."""

    def __init__(self, user_id: int, flow_id: int, snapshot: dict[str, Any] | None = None) -> None:
        self.session_id = uuid.uuid4().hex
        self.user_id = user_id
        self.flow_id = flow_id
        self.snapshot = snapshot
        self.state = "starting"
        self.last_used = time.monotonic()
        self.startup_seconds: float | None = None
        self.generation = 0
        self.epoch = 0
        self.revision = 0
        self._process: subprocess.Popen | None = None
        self._job: object | None = None
        self._writer: queue.Queue | None = None
        self._log = None
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._waiters: dict[str, _Waiter] = {}
        self._inflight: list[str] = []
        self._lifecycle = threading.RLock()
        self._closed = False
        self._spawned_at = 0.0

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process is not None else None

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def busy(self) -> bool:
        with self._lock:
            return bool(self._inflight)

    @property
    def namespace_generation(self) -> str:
        """Changes whenever the namespace is dropped (reset or restart), like a kernel's namespace generation."""
        return f"{self.session_id}.{self.epoch}"

    def touch(self) -> None:
        self.last_used = time.monotonic()

    def alive(self) -> bool:
        return not self._closed and self._process is not None and self._process.poll() is None

    def wait_ready(self, timeout: float) -> bool:
        return self._ready.wait(timeout)

    def start(self) -> None:
        """Spawn the child and queue the seed; returns without waiting for ``ready``."""
        with self._lifecycle:
            self._spawn()

    def _spawn(self) -> None:
        self.generation += 1
        self.epoch += 1
        generation = self.generation
        self.state = "starting"
        self._ready.clear()
        if self._log is None:
            self._log = open(_log_path(self.flow_id), "ab")
        self._spawned_at = time.monotonic()
        process = subprocess.Popen(
            bootstrap.session_command(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._log,
            env=bootstrap.session_env(self.user_id, _spill_dir()),
            bufsize=0,
            creationflags=bootstrap.creation_flags(),
        )
        self._process = process
        # TODO(windows): verify on a real Windows run that closing the Job kills the interpreter the venv launcher spawns.
        self._job = bootstrap.attach_kill_on_close_job(process)
        writer: queue.Queue = queue.Queue()
        self._writer = writer
        threading.Thread(
            target=self._write_loop, args=(process, writer), name=f"nb-writer-{self.flow_id}", daemon=True
        ).start()
        threading.Thread(
            target=self._read_loop, args=(process, generation), name=f"nb-reader-{self.flow_id}", daemon=True
        ).start()
        if self.snapshot:
            self._send({"type": "seed", "ticket": uuid.uuid4().hex, "snapshot": self.snapshot})

    def _write_loop(self, process: subprocess.Popen, writer: queue.Queue) -> None:
        spill_dir = _spill_dir()
        while True:
            message = writer.get()
            if message is _STOP:
                break
            try:
                protocol.write_message(process.stdin, message, spill_dir=spill_dir)
            except (BrokenPipeError, OSError, ValueError):
                break
        try:
            process.stdin.close()
        except Exception:
            pass

    def _read_loop(self, process: subprocess.Popen, generation: int) -> None:
        while True:
            try:
                message = protocol.read_message(process.stdout)
            except Exception:
                logger.warning("notebook session reader failed", exc_info=True)
                message = None
            if message is None:
                break
            self._on_message(message, generation)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        if generation == self.generation and not self._closed and self.state != "restarting":
            self.state = "dead"
            self._fail_waiters("the notebook session exited")

    def _on_message(self, message: dict[str, Any], generation: int) -> None:
        if generation != self.generation:
            return
        kind = message.get("type")
        ticket = message.get("ticket")
        if kind == "ready":
            self.startup_seconds = round(time.monotonic() - self._spawned_at, 3)
            self.state = "busy" if self.busy else "ready"
            self._ready.set()
            return
        if kind in ("stream", "display"):
            waiter = self._waiters.get(ticket) if ticket else None
            if waiter is not None:
                waiter.events.append(message)
            return
        if kind == "done":
            with self._lock:
                if ticket in self._inflight:
                    self._inflight.remove(ticket)
                    self.revision += 1
                if self.state == "busy" and not self._inflight:
                    self.state = "ready"
        self._resolve(ticket, message)

    def _resolve(self, ticket: str | None, message: dict[str, Any]) -> None:
        waiter = self._waiters.pop(ticket, None) if ticket else None
        if waiter is not None:
            waiter.reply = message
            waiter.event.set()

    def _fail_waiters(self, error: str) -> None:
        for ticket in list(self._waiters):
            self._resolve(ticket, {"type": "error", "ticket": ticket, "error": error})
        with self._lock:
            self._inflight.clear()

    def _send(self, message: dict[str, Any]) -> None:
        if self._writer is not None:
            self._writer.put(message)

    def _request(self, message: dict[str, Any], timeout: float, inflight: bool = False) -> _Waiter:
        """Send ``message`` under a new ticket and wait for its reply (an ``error`` reply on timeout)."""
        ticket = uuid.uuid4().hex
        waiter = _Waiter()
        self._waiters[ticket] = waiter
        if inflight:
            with self._lock:
                self._inflight.append(ticket)
                if self.state == "ready":
                    self.state = "busy"
        self._send({**message, "ticket": ticket})
        if not waiter.event.wait(timeout):
            self._waiters.pop(ticket, None)
            waiter.reply = {
                "type": "error",
                "ticket": ticket,
                "error": f"no reply from the notebook session in {timeout:.0f}s",
            }
        return waiter

    def run_cell(
        self, cell_id: str, code: str, provenance: Any = None, timeout: float = REPLY_TIMEOUT
    ) -> dict[str, Any]:
        """Execute a cell and wait for its ``done``: ``{ok, error, traceback, stdout, stderr, displays}``."""
        self.touch()
        message = {"type": "execute", "cell_id": cell_id, "code": code, "provenance": provenance}
        waiter = self._request(message, timeout, inflight=True)
        reply = waiter.reply or {}
        streams = {"stdout": "", "stderr": ""}
        for event in waiter.events:
            if event["type"] == "stream":
                streams[event.get("name", "stdout")] += event.get("text", "")
        return {
            "ok": reply.get("type") == "done" and bool(reply.get("ok")),
            "error": reply.get("error"),
            "traceback": reply.get("traceback"),
            **streams,
            "displays": [event["payload"] for event in waiter.events if event["type"] == "display"],
        }

    def restart(self) -> None:
        """Kill the child, start a new one and re-seed it; running and queued cells fail with ``RESTARTED_MESSAGE``."""
        with self._lifecycle:
            if self._closed:
                return
            self.state = "restarting"
            process, writer = self._process, self._writer
            self.generation += 1
            if writer is not None:
                writer.put(_STOP)
            if process is not None:
                kill_process(process)
            self._fail_waiters(RESTARTED_MESSAGE)
            self._spawn()

    def reset(self, snapshot: dict[str, Any] | None = None) -> None:
        """Drop every variable and re-seed (from ``snapshot`` when given, else the last seed); restarts when busy."""
        self.touch()
        if snapshot is not None:
            self.snapshot = snapshot
        if self.busy:
            self.restart()
            return
        self.epoch += 1
        self._send({"type": "reset", "ticket": uuid.uuid4().hex, "snapshot": snapshot})

    def schemas(self, timeout: float = 30.0) -> dict[str, Any]:
        """``{name: [{"name", "data_type"}]}`` for every frame bound in the session namespace."""
        self.touch()
        return self._request({"type": "schemas"}, timeout).reply.get("frames") or {}

    def clean_run(self, request: Any, timeout: float = REPLY_TIMEOUT) -> dict[str, Any]:
        """Run the clean run of ``request`` (a ``CleanRunRequest``) in the child; the raw ``graph`` reply."""
        self.touch()
        fields = request.model_dump(mode="json") if hasattr(request, "model_dump") else dict(request)
        if fields.get("snapshot"):
            self.snapshot = fields["snapshot"]
        return self._request({"type": "clean_run", **fields}, timeout).reply

    def close_input(self) -> None:
        """Close the child's stdin (it exits on EOF) without waiting."""
        self._closed = True
        self.state = "closed"
        if self._writer is not None:
            self._writer.put(_STOP)

    def wait_or_kill(self, timeout: float = 2.0) -> None:
        process = self._process
        if process is None:
            return
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            kill_process(process)
        self._fail_waiters("the notebook session was closed")
        if self._log is not None:
            try:
                self._log.close()
            except Exception:
                pass
        self._job = None

    def close(self, timeout: float = 2.0) -> None:
        with self._lifecycle:
            self.close_input()
            self.wait_or_kill(timeout)


def _close_in_background(sessions: list[NotebookSession]) -> None:
    """Close ``sessions`` off the caller's thread: stdin at once, the bounded wait/kill in a daemon thread."""
    for session in sessions:
        session.close_input()
        threading.Thread(target=session.wait_or_kill, name="nb-session-close", daemon=True).start()


class NotebookSessionRegistry:
    """Sessions keyed by ``(user_id, flow_id)`` with idle TTL, LRU eviction and parallel shutdown."""

    def __init__(
        self,
        idle_ttl: float | None = None,
        max_sessions: int | None = None,
        session_factory: Callable[..., NotebookSession] = NotebookSession,
    ) -> None:
        if idle_ttl is None:
            idle_ttl = _env_number("FLOWFILE_NOTEBOOK_IDLE_TTL", DEFAULT_IDLE_TTL)
        if max_sessions is None:
            max_sessions = int(_env_number("FLOWFILE_NOTEBOOK_MAX_SESSIONS", DEFAULT_MAX_SESSIONS))
        self.idle_ttl = idle_ttl
        self.max_sessions = max_sessions
        self._factory = session_factory
        self._sessions: dict[str, NotebookSession] = {}
        self._lock = threading.Lock()
        self._janitor: threading.Thread | None = None
        self._stopping = threading.Event()

    def sessions(self) -> list[NotebookSession]:
        with self._lock:
            return list(self._sessions.values())

    def open(self, user_id: int, flow_id: int, snapshot: dict[str, Any] | None = None) -> NotebookSession:
        """The live session of ``(user_id, flow_id)``, else a new one (evicting idle and LRU sessions first)."""
        self.sweep_idle()
        evicted: list[NotebookSession] = []
        with self._lock:
            for session in self._sessions.values():
                if session.user_id == user_id and session.flow_id == flow_id and not session.closed:
                    if session.state != "dead":
                        session.touch()
                        return session
            for sid in [sid for sid, s in self._sessions.items() if s.closed or s.state == "dead"]:
                evicted.append(self._sessions.pop(sid))
            while len(self._sessions) >= max(self.max_sessions, 1):
                oldest = min(self._sessions.values(), key=lambda s: s.last_used)
                evicted.append(self._sessions.pop(oldest.session_id))
            session = self._factory(user_id, flow_id, snapshot)
            self._sessions[session.session_id] = session
        _close_in_background(evicted)
        session.start()
        self._ensure_janitor()
        return session

    def find(self, user_id: int, flow_id: int) -> NotebookSession | None:
        """The live session of ``(user_id, flow_id)`` without starting one."""
        with self._lock:
            sessions = list(self._sessions.values())
        return next(
            (
                s
                for s in sessions
                if s.user_id == user_id and s.flow_id == flow_id and not s.closed and s.state != "dead"
            ),
            None,
        )

    def _pop(self, predicate: Callable[[NotebookSession], bool]) -> list[NotebookSession]:
        with self._lock:
            victims = [s for s in self._sessions.values() if predicate(s)]
            for session in victims:
                self._sessions.pop(session.session_id, None)
        return victims

    def close(self, flow_id: int, user_id: int | None = None) -> int:
        """Close the sessions of ``flow_id`` (of ``user_id`` only, when given); how many were closed."""
        victims = self._pop(lambda s: s.flow_id == flow_id and (user_id is None or s.user_id == user_id))
        for session in victims:
            session.close()
        return len(victims)

    def sweep_idle(self) -> int:
        """Close sessions unused for longer than ``idle_ttl`` and not running a cell; how many were closed."""
        now = time.monotonic()
        victims = self._pop(lambda s: not s.busy and now - s.last_used > self.idle_ttl)
        _close_in_background(victims)
        return len(victims)

    def _ensure_janitor(self) -> None:
        if self._janitor is not None and self._janitor.is_alive():
            return
        interval = max(0.05, min(self.idle_ttl / 2, 30.0))
        stopping = self._stopping = threading.Event()

        def sweep() -> None:
            while not stopping.wait(interval):
                try:
                    self.sweep_idle()
                except Exception:
                    logger.warning("notebook session sweep failed", exc_info=True)

        self._janitor = threading.Thread(target=sweep, name="nb-session-janitor", daemon=True)
        self._janitor.start()

    def shutdown(self, timeout: float = 2.0) -> None:
        """Close every stdin in parallel, then a bounded parallel terminate/kill."""
        self._stopping.set()
        victims = self._pop(lambda s: True)
        for session in victims:
            session.close_input()
        threads = [threading.Thread(target=s.wait_or_kill, args=(timeout,), daemon=True) for s in victims]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout + 3.0)

    def clean_run(self, user_id: int, flow_id: int, request: Any) -> Any:
        """The bridge's ``CleanRunner``: run ``request`` in the user's session for the flow, starting it if needed."""
        from flowfile_core.notebook.bridge import CleanRunResult

        snapshot = getattr(request, "snapshot", None) or None
        reply = self.open(user_id, flow_id, snapshot).clean_run(request)
        if reply.get("type") != "graph":
            return CleanRunResult(
                flowfile_data={}, node_ids_by_cell={}, names={}, refusals=[], warnings=[], error=reply.get("error")
            )
        return CleanRunResult(
            flowfile_data=reply.get("flowfile_data") or {},
            node_ids_by_cell=reply.get("node_ids_by_cell") or {},
            names={int(k): v for k, v in (reply.get("names") or {}).items()},
            refusals=list(reply.get("refusals") or []),
            warnings=list(reply.get("warnings") or []),
            error=reply.get("error"),
        )


_registry: NotebookSessionRegistry | None = None
_registry_lock = threading.Lock()


def get_registry() -> NotebookSessionRegistry:
    global _registry
    with _registry_lock:
        if _registry is None:
            _registry = NotebookSessionRegistry()
        return _registry


def install() -> NotebookSessionRegistry:
    """Make the process registry the bridge's clean runner (core startup).

    Left alone when ``FLOWFILE_NOTEBOOK_INPROCESS_CLEAN_RUN=1``: the test-only in-process runner that
    ``routes/notebook.py`` installed at import stays in place.
    """
    from flowfile_core.notebook.bridge import set_clean_runner

    registry = get_registry()
    if os.environ.get("FLOWFILE_NOTEBOOK_INPROCESS_CLEAN_RUN") != "1":
        set_clean_runner(registry)
    return registry


def shutdown_sessions(timeout: float = 2.0) -> None:
    """Close every session (core's lifespan shutdown, before the kernels)."""
    registry = _registry
    if registry is not None:
        registry.shutdown(timeout)


def close_flow_sessions(flow_id: int, user_id: int | None = None) -> None:
    """Close the sessions of a closed flow; a no-op when no session was ever started."""
    registry = _registry
    if registry is not None:
        registry.close(flow_id, user_id)

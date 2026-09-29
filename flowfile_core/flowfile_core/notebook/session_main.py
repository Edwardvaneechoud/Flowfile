"""The notebook session process: a plain-Python REPL plus graph builder that core drives over a pipe.

Entered from the bootstrap (dev / pip) or the ``--notebook-session`` verb (frozen), both of which have
already moved the protocol off fd 1 and handed over the protocol streams. The main thread runs the
protocol loop; every message but ``interrupt`` and ``shutdown`` runs in order on one cell thread.
``interrupt`` (not sent by core today) raises ``KeyboardInterrupt`` in the cell thread via
``PyThreadState_SetAsyncExc``; a reset of a busy session is a kill-and-restart in the registry, never an
interrupt. Writes from both threads go through one lock, and ``sys.stdout`` / ``sys.stderr`` become
``stream`` messages. Cells run inside ``flowfile_frame`` notebook mode (:mod:`flowfile_frame.notebook_cells`).
No signal handler is installed; stdin EOF is ``os._exit(0)``, so a cell thread stuck in native code cannot
keep the process alive.
"""

from __future__ import annotations

import ctypes
import io
import os
import queue
import sys
import threading
import time
import traceback
from typing import IO, Any

from flowfile_core.notebook import protocol

_STREAM_FLUSH_BYTES = 4096


class _Channel:
    """The protocol writer shared by the protocol and cell threads."""

    def __init__(self, stream: IO[bytes], spill_dir: str | None) -> None:
        self._stream = stream
        self._lock = threading.Lock()
        self._spill_dir = spill_dir

    def send(self, message: dict[str, Any]) -> None:
        try:
            protocol.write_message(self._stream, message, self._lock, self._spill_dir)
        except (BrokenPipeError, ValueError, OSError):
            os._exit(0)


class _StreamProxy(io.TextIOBase):
    """``sys.stdout`` / ``sys.stderr`` in the session: text becomes ``stream`` messages, flushed per line or 4 KB."""

    def __init__(self, channel: _Channel, name: str, context: _Context) -> None:
        self._channel = channel
        self._name = name
        self._context = context
        self._buffer: list[str] = []
        self._size = 0
        self._lock = threading.Lock()

    @property
    def encoding(self) -> str:
        return "utf-8"

    def writable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def write(self, text: str) -> int:
        if not isinstance(text, str):
            text = str(text)
        with self._lock:
            self._buffer.append(text)
            self._size += len(text)
            ready = "\n" in text or self._size >= _STREAM_FLUSH_BYTES
        if ready:
            self.flush()
        return len(text)

    def flush(self) -> None:
        with self._lock:
            text, self._buffer, self._size = "".join(self._buffer), [], 0
        if text:
            self._channel.send({"type": "stream", "name": self._name, "text": text, **self._context.current()})


class _Context:
    """The ticket and cell of the job running on the cell thread, stamped onto its stream and display messages."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._job: dict[str, Any] | None = None

    def set(self, job: dict[str, Any] | None) -> None:
        with self._lock:
            self._job = job

    def current(self) -> dict[str, Any]:
        with self._lock:
            job = self._job
        if job is None:
            return {"ticket": None, "cell_id": None}
        return {"ticket": job.get("ticket"), "cell_id": job.get("cell_id")}

    def running(self) -> bool:
        with self._lock:
            return self._job is not None


def _error_line(traceback_text: str | None) -> str | None:
    if not traceback_text:
        return None
    lines = [line for line in traceback_text.strip().splitlines() if line.strip()]
    return lines[-1] if lines else traceback_text


class Session:
    """The session state the cell thread owns: the namespace, the last seed snapshot and provenance."""

    def __init__(self, channel: _Channel, user_id: int | None) -> None:
        self.channel = channel
        self.user_id = user_id
        self.context = _Context()
        self.jobs: queue.Queue[dict[str, Any]] = queue.Queue()
        self.namespace: dict[str, Any] | None = None
        self.snapshot: dict[str, Any] | None = None
        self.provenance: dict[str, list[list[Any]]] = {}
        self.thread = threading.Thread(target=self._run, name="notebook-cell", daemon=True)

    def _seed(self) -> None:
        from flowfile_frame import notebook
        from flowfile_frame.notebook_cells import new_namespace, seed_session

        snapshot = self.snapshot
        if snapshot:
            bound = seed_session(
                snapshot.get("flowfile_data") or {},
                snapshot.get("parameters") or [],
                snapshot.get("names") or {},
                snapshot.get("schemas") or {},
                user_id=self.user_id,
            )
        else:
            notebook.exit()
            notebook.enter(user_id=self.user_id).graph.unique_subflow_port_names = False
            bound = {}
        namespace = new_namespace()
        namespace.update(bound)
        self.namespace = namespace

    def _namespace(self) -> dict[str, Any]:
        if self.namespace is None:
            self._seed()
        return self.namespace

    def _flush_streams(self) -> None:
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception:
                pass

    def _done(self, job: dict[str, Any], ok: bool, error: str | None = None, **extra: Any) -> None:
        self._flush_streams()
        self.channel.send(
            {
                "type": "done",
                "ticket": job.get("ticket"),
                "op": job["type"],
                "cell_id": job.get("cell_id"),
                "ok": ok,
                "error": _error_line(error),
                "traceback": error,
                **extra,
            }
        )

    def on_seed(self, job: dict[str, Any]) -> None:
        self.snapshot = job.get("snapshot") or None
        self.namespace = None
        self._seed()
        self._done(job, True)

    def on_reset(self, job: dict[str, Any]) -> None:
        if job.get("snapshot"):
            self.snapshot = job["snapshot"]
        self.namespace = None
        self.provenance = {}
        self._seed()
        self._done(job, True)

    def on_execute(self, job: dict[str, Any]) -> None:
        from flowfile_frame.notebook_cells import execute_cell

        if job.get("provenance") is not None:
            self.provenance[job["cell_id"]] = job["provenance"]
        result = execute_cell(job["cell_id"], job["code"], self._namespace())
        self._flush_streams()
        context = {"ticket": job.get("ticket"), "cell_id": job["cell_id"]}
        for payload in result.outputs:
            self.channel.send({"type": "display", "payload": payload, **context})
        if result.display is not None:
            self.channel.send({"type": "display", "payload": result.display, "auto": True, **context})
        self._done(
            job,
            result.ok,
            result.error,
            nodes_created=[list(entry) for entry in result.created],
            names_bound=list(result.names),
            references={str(k): v for k, v in result.references.items()},
        )

    def on_clean_run(self, job: dict[str, Any]) -> None:
        from flowfile_core.notebook.bridge import CleanRunResult, result_from_payload
        from flowfile_frame import notebook
        from flowfile_frame.notebook_cells import clean_run, seed_session

        self._namespace()
        previous = notebook.current()
        snapshot = job.get("snapshot") or None
        try:
            if snapshot:
                seed_session(
                    snapshot.get("flowfile_data") or {},
                    snapshot.get("parameters") or [],
                    snapshot.get("names") or {},
                    snapshot.get("schemas") or {},
                    user_id=self.user_id,
                )
            cells = [tuple(cell) for cell in job.get("cells") or []]
            provenance = {k: [tuple(e) for e in v] for k, v in (job.get("provenance") or {}).items()}
            outcome = result_from_payload(clean_run(cells, int(job.get("ceiling") or 0), provenance))
        except BaseException as exc:
            outcome = CleanRunResult(error="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
        finally:
            if snapshot and notebook.current() is not previous:
                notebook.exit()
                if previous is not None:
                    notebook._activate(previous)
        self._flush_streams()
        self.channel.send({"type": "graph", "ticket": job.get("ticket"), **outcome.model_dump(mode="json")})

    def on_schemas(self, job: dict[str, Any]) -> None:
        from flowfile_frame.flow_frame import FlowFrame
        from flowfile_frame.notebook_cells import _schema_entries

        frames: dict[str, list[dict[str, str]]] = {}
        for name, value in list((self.namespace or {}).items()):
            if name.startswith("_") or not isinstance(value, FlowFrame):
                continue
            try:
                frames[name] = _schema_entries(value)
            except Exception:
                continue
        self.channel.send({"type": "schemas", "ticket": job.get("ticket"), "frames": frames})

    def _handle(self, job: dict[str, Any]) -> None:
        handler = getattr(self, f"on_{job['type']}", None)
        if handler is None:
            self._done(job, False, f"unknown message type {job['type']!r}")
            return
        try:
            handler(job)
        except BaseException as exc:
            text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            if job["type"] == "clean_run":
                self.channel.send({"type": "graph", "ticket": job.get("ticket"), "error": text})
            elif job["type"] == "schemas":
                self.channel.send({"type": "schemas", "ticket": job.get("ticket"), "frames": {}, "error": text})
            else:
                self._done(job, False, text)

    def _run(self) -> None:
        while True:
            try:
                job = self.jobs.get()
                self.context.set(job)
                try:
                    self._handle(job)
                finally:
                    self.context.set(None)
            except KeyboardInterrupt:
                continue
            except BaseException:
                traceback.print_exc(file=sys.__stderr__)

    def interrupt(self) -> bool:
        """Raise ``KeyboardInterrupt`` in the cell thread when it runs a job; whether one was injected."""
        if not self.context.running() or self.thread.ident is None:
            return False
        count = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_ulong(self.thread.ident), ctypes.py_object(KeyboardInterrupt)
        )
        return count == 1


def _user_id() -> int | None:
    raw = os.environ.get("FLOWFILE_SESSION_USER_ID")
    return int(raw) if raw else None


def main(proto_out: IO[bytes] | int, proto_in: IO[bytes] | int | None = None) -> int:
    """Run the session until stdin EOF or ``shutdown``; never returns normally (``os._exit``)."""
    started = time.monotonic()
    out = proto_out if hasattr(proto_out, "write") else os.fdopen(proto_out, "wb", 0)
    if proto_in is None:
        inp: IO[bytes] = sys.stdin.buffer
    else:
        inp = proto_in if hasattr(proto_in, "read") else os.fdopen(proto_in, "rb", 0)
    channel = _Channel(out, os.environ.get("FLOWFILE_NOTEBOOK_SPILL_DIR") or None)
    session = Session(channel, _user_id())
    sys.stdout = _StreamProxy(channel, "stdout", session.context)
    sys.stderr = _StreamProxy(channel, "stderr", session.context)

    import flowfile  # noqa: F401
    import flowfile_frame.notebook_cells  # noqa: F401

    session.thread.start()
    channel.send({"type": "ready", "pid": os.getpid(), "startup_seconds": round(time.monotonic() - started, 3)})
    while True:
        try:
            message = protocol.read_message(inp)
        except Exception:
            traceback.print_exc(file=sys.__stderr__)
            message = None
        if message is None or message.get("type") == "shutdown":
            session._flush_streams()
            os._exit(0)
        if message["type"] == "interrupt":
            session.interrupt()
        else:
            session.jobs.put(message)

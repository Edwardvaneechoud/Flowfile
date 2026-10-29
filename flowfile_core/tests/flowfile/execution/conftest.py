"""Fixtures and helpers the execution tests share: the two backends, results held on them, a loopback HTTP server."""

import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.protocol import ComputeBackend, ResultRef
from flowfile_core.schemas import input_schema

Held = list[tuple[ComputeBackend, ResultRef]]


@pytest.fixture
def local() -> EmbeddedWorkerBackend:
    """The worker embedded in this process, under its own id ``local``."""
    return EmbeddedWorkerBackend()


@pytest.fixture
def worker(running_worker) -> WorkerBackend:
    """A fresh worker backend on the session worker."""
    return WorkerBackend(url=registry.backend(registry.WORKER_BACKEND_ID).url)


@pytest.fixture
def kept() -> Iterator[Held]:
    """Every result a test made, beside the backend holding it, released when the test ends."""
    held: Held = []
    yield held
    for backend, ref in held:
        backend.release(ref)


@pytest.fixture
def held():
    """The result ids the worker embedded in this process holds that it did not hold when the test began."""
    from flowfile_worker.results import results

    registry.reset()
    before = set(results._records)
    yield lambda: set(results._records) - before
    registry.reset()


@pytest.fixture
def clean_import_aliases():
    """Undo the SDK import aliases and finder a custom node run from its source in this process installs."""
    from shared.node_designer.loading import _ForbiddenCoreImportFinder

    modules_before = set(sys.modules)
    yield
    sys.meta_path[:] = [finder for finder in sys.meta_path if not isinstance(finder, _ForbiddenCoreImportFinder)]
    for name in set(sys.modules) - modules_before:
        root = name.split(".", 1)[0]
        if root in {"flowfile_core", "flowfile"} and not getattr(sys.modules[name], "__file__", None):
            del sys.modules[name]


def fuzzy_settings() -> input_schema.NodeFuzzyMatch:
    """A fuzzy match of ``name`` on ``name`` (levenshtein, 75), an inner join keeping each side's ``name``."""
    return input_schema.NodeFuzzyMatch(
        flow_id=1,
        node_id=2,
        join_input={
            "join_mapping": [
                {"left_col": "name", "right_col": "name", "threshold_score": 75, "fuzzy_type": "levenshtein"}
            ],
            "left_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
            "right_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
            "how": "inner",
        },
    )


@contextmanager
def serve(body: bytes, content_type: str | None = None, *, ranges: bool = False) -> Iterator[str]:
    """``body`` served on a free loopback port for every GET and HEAD; yields ``http://127.0.0.1:<port>``.

    With ``ranges`` a request's ``Range`` header picks the bytes it gets, answered 206, as an object store answers.
    """

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _answer(self) -> bytes:
            part = body
            if ranges:
                start, _, end = self.headers.get("Range", "bytes=0-").removeprefix("bytes=").partition("-")
                part = body[int(start) : int(end) + 1 if end else None]
                self.send_response(206 if "Range" in self.headers else 200)
                self.send_header("Content-Range", f"bytes {start}-{int(start) + len(part) - 1}/{len(body)}")
                self.send_header("Accept-Ranges", "bytes")
            else:
                self.send_response(200)
            if content_type:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(part)))
            self.end_headers()
            return part

        def do_HEAD(self):  # noqa: N802
            self._answer()

        def do_GET(self):  # noqa: N802
            self.wfile.write(self._answer())

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        thread.join(timeout=5)

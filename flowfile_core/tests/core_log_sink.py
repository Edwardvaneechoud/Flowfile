"""A loopback stand-in for core's ``POST /raw_logs`` for the duration of the suite.

Every worker child ships its per-node log lines to core by POSTing
``http://$CORE_HOST:$CORE_PORT/raw_logs`` (``flowfile_worker/flow_logger.py``:
``FlowfileLogHandler.emit``, connect timeout 2s). The core suite runs core
in-process via ``TestClient``, so nothing listens on that port, and on Windows a
refused loopback connect burns the full ~2s connect timeout instead of failing
instantly — roughly 7 log lines x 2s of dead time on every worker-backed test.
Serving the port keeps the real delivery path running end to end, at loopback
speed.

Stdlib only at import time, and imported by conftest before anything reaches
flowfile_core, so it must stay free of import-time side effects.
"""

from __future__ import annotations

import importlib
import os
import platform
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Match how a spawned worker child resolves core (flowfile_worker/configs.py).
CORE_PORT = int(os.environ.get("CORE_PORT", 63578))
# Loopback only — binding 0.0.0.0 would raise a Windows firewall prompt.
SINK_HOST = "127.0.0.1"

_ACCEPTED_PATHS = frozenset({"/raw_logs"})

# Tests that bind the core port themselves; the sink must stay out of their way —
# but only when they will actually run. Each entry points at the claimer's own skip
# predicate (True = it runs) so the claim and the skip can never drift apart.
# conftest's kernel_manager_with_core skips on exactly this function.
_PORT_CLAIMING_FIXTURES = {
    "kernel_manager_with_core": ("tests.flowfile_core_test_utils", "is_docker_available"),
}
# Read off the collected item's own module, so there is nothing to keep in sync.
_PORT_CLAIMING_MODULES = {"test_auth_e2e.py": "is_docker_available"}


class _LogSinkHandler(BaseHTTPRequestHandler):
    # HTTP/1.0: respond and close, no keep-alive bookkeeping.
    protocol_version = "HTTP/1.0"

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        path = self.path.split("?", 1)[0].rstrip("/")
        if path in _ACCEPTED_PATHS:
            self._respond(200, b'{"message":"Log added successfully"}')
        else:
            self._respond(404, b'{"detail":"Not Found"}')

    def do_GET(self) -> None:
        self._respond(404, b'{"detail":"Not Found"}')

    def _respond(self, status: int, body: bytes) -> None:
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except OSError:
            pass

    def log_message(self, *args) -> None:
        pass


class _SinkServer(ThreadingHTTPServer):
    daemon_threads = True
    # On Windows SO_REUSEADDR lets a bind steal a port another process is
    # actively listening on, which would hijack a real core; a failed bind is
    # exactly the guard we want there. On POSIX it only clears TIME_WAIT.
    allow_reuse_address = platform.system() != "Windows"


class CoreLogSink:
    """Serves ``/raw_logs`` on the core port; a no-op if the port is taken."""

    def __init__(self, host: str = SINK_HOST, port: int = CORE_PORT):
        self.host = host
        self.port = port
        self._server: _SinkServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> bool:
        """Bind and serve. Returns False when something else already listens."""
        try:
            self._server = _SinkServer((self.host, self.port), _LogSinkHandler)
        except OSError:
            self._server = None
            return False
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="core-log-sink", daemon=True
        )
        self._thread.start()
        return True

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None


def _imported_predicate(module_name: str, attr: str):
    try:
        return getattr(importlib.import_module(module_name), attr, None)
    except Exception:
        return None


def _claimer_runs(predicate) -> bool:
    """Fail closed: an unresolvable or raising predicate counts as "will run"."""
    if predicate is None:
        return True
    try:
        return bool(predicate())
    except Exception:
        return True


def session_claims_core_port(items) -> bool:
    """True when a collected test that will really run binds the core port.

    ``items`` must be the post-deselection list — conftest calls this from a
    ``trylast`` hook, because pytest's own ``-m``/``-k`` deselection runs after a
    plain conftest hook and would otherwise leave deselected claimers visible.
    """
    predicates = {}
    for item in items:
        fixturenames = getattr(item, "fixturenames", ())
        for name, (module_name, attr) in _PORT_CLAIMING_FIXTURES.items():
            if name in fixturenames and name not in predicates:
                predicates[name] = _imported_predicate(module_name, attr)
        basename = os.path.basename(str(getattr(item, "fspath", "")))
        module_attr = _PORT_CLAIMING_MODULES.get(basename)
        if module_attr is not None and basename not in predicates:
            predicates[basename] = getattr(getattr(item, "module", None), module_attr, None)
    return any(_claimer_runs(predicate) for predicate in predicates.values())

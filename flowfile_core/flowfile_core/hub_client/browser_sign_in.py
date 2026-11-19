"""A desktop's sign-in to a hub through the system browser: OAuth 2.0 authorization code with PKCE and a loopback
redirect (RFC 8252).

``start`` binds a one-shot HTTP listener to ``127.0.0.1`` on a free port, makes the PKCE pair and a random ``state``,
and keeps the pending sign-in in memory; the person signs in on the hub's own page, which redirects the browser to
``http://127.0.0.1:<port>/callback``. The listener answers only that path with a ``state`` and a ``code`` or an
``error``: it checks the ``state``, exchanges the code with the verifier and hands the tokens to the ``complete``
callback the caller gave (register the device, keep the session, sync the nodes), then answers the browser with a
small page. Flowfile never sees the password, a second factor or a company sign-in.

The listener never logs a request line (it carries the code), closes once its sign-in ended, and a sign-in that
nobody finished ends by itself after ``TTL_SECONDS``. Nothing of a pending sign-in outlives the process.
"""

from __future__ import annotations

import hmac
import html
import secrets
import socketserver
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from flowfile_core.configs import logger
from flowfile_core.hub_client import client
from shared.snowflake_auth import new_pkce_pair

TTL_SECONDS = 300
_KEEP_ENDED_SECONDS = 600
_LOOPBACK = "127.0.0.1"
_CALLBACK_PATH = "/callback"
_MAX_PATH = 8192
_POLL_SECONDS = 0.25
SIGNED_IN = "You are signed in. You can close this tab and go back to Flowfile."
EXPIRED = "The sign-in took too long and was stopped. Start it again in Flowfile."
CANCELLED = "The sign-in was cancelled in Flowfile."
DECLINED = "You declined the sign-in on the hub. Start it again in Flowfile to connect."


class SignInUnavailable(Exception):
    """A browser sign-in could not be started here."""


@dataclass
class _SignIn:
    flow_id: str
    user_id: int
    hub_url: str
    state: str
    verifier: str
    redirect_uri: str
    complete: Callable[[dict], dict]
    expires_at: float
    listener: _Listener
    status: str = "waiting"
    detail: str | None = None
    connection: dict | None = None
    ended_at: float | None = None
    _taken: bool = field(default=False, repr=False)


_flows: dict[str, _SignIn] = {}
_lock = threading.Lock()


class _Handler(BaseHTTPRequestHandler):
    server: _Server
    timeout = 10

    def do_GET(self) -> None:
        if len(self.path) > _MAX_PATH:
            self._answer(414, "text/plain; charset=utf-8", "Request too long")
            return
        parts = urlsplit(self.path)
        query = parse_qs(parts.query, max_num_fields=8)
        first = {name: (query.get(name) or [None])[0] for name in ("state", "code", "error")}
        if parts.path != _CALLBACK_PATH or not first["state"] or not (first["code"] or first["error"]):
            self._answer(404, "text/plain; charset=utf-8", "Not Found")
            return
        ok, status, message, finished = _callback(self.server.flow_id, first["state"], first["code"], first["error"])
        self._answer(status, "text/html; charset=utf-8", _page(ok, message))
        if finished:
            self.server.stop.set()

    def _answer(self, status: int, content_type: str, body: str) -> None:
        data = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format: str, *args: Any) -> None:
        # The request line carries the authorization code: never log it.
        pass


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    flow_id: str
    stop: threading.Event

    def server_bind(self) -> None:
        # HTTPServer would resolve the host's name, a lookup a loopback listener does not need.
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]

    def handle_error(self, request, client_address) -> None:
        pass


class _Listener:
    """One sign-in's listener on ``127.0.0.1`` and a free port; it serves until stopped or until ``alive()`` fails."""

    def __init__(self) -> None:
        try:
            self._server = _Server((_LOOPBACK, 0), _Handler)
        except OSError as e:
            raise SignInUnavailable(
                f"Flowfile could not open a local port for the sign-in ({type(e).__name__}). Try again."
            ) from None
        self._server.timeout = _POLL_SECONDS
        self._server.stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def redirect_uri(self) -> str:
        return f"http://{_LOOPBACK}:{self._server.server_address[1]}{_CALLBACK_PATH}"

    def serve(self, flow_id: str, alive: Callable[[], bool]) -> None:
        self._server.flow_id = flow_id
        self._thread = threading.Thread(target=self._loop, args=(alive,), daemon=True, name="hub-sign-in")
        self._thread.start()

    def close(self) -> None:
        self._server.stop.set()
        thread = self._thread
        if thread is None:
            self._server.server_close()
        elif thread is not threading.current_thread():
            thread.join(timeout=2)

    def _loop(self, alive: Callable[[], bool]) -> None:
        try:
            while not self._server.stop.is_set() and alive():
                self._server.handle_request()
        finally:
            self._server.stop.set()
            self._server.server_close()


def _page(ok: bool, message: str) -> str:
    title = "Signed in to the hub" if ok else "Hub sign-in did not complete"
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{html.escape(title)}</title></head>
<body style="font-family: system-ui, sans-serif; padding: 2rem; max-width: 40rem; margin: 0 auto;">
<h3>{html.escape(title)}</h3>
<p>{html.escape(message)}</p>
</body></html>"""


def _end(flow: _SignIn, status: str, detail: str | None, connection: dict | None = None) -> None:
    with _lock:
        if flow.status != "waiting":
            return
        flow.status, flow.detail, flow.connection, flow.ended_at = status, detail, connection, time.time()
    flow.listener.close()


def _alive(flow: _SignIn) -> bool:
    if flow.status != "waiting":
        return False
    if time.time() >= flow.expires_at and not flow._taken:
        _end(flow, "failed", EXPIRED)
        return False
    return True


def _callback(flow_id: str, state: str, code: str | None, error: str | None) -> tuple[bool, int, str, bool]:
    """``(ok, HTTP status, page text, finished)`` for a redirect to the listener of ``flow_id``."""
    with _lock:
        flow = _flows.get(flow_id)
        if flow is None or flow.status != "waiting":
            return False, 400, "This sign-in has ended. Start it again in Flowfile.", True
        if not hmac.compare_digest(state.encode(), flow.state.encode()):
            return False, 400, "This sign-in link does not belong to the sign-in Flowfile started.", False
        if flow._taken:
            return False, 409, "This sign-in is already being completed.", False
        flow._taken = True
    if error:
        message = DECLINED if error == "access_denied" else f"The hub did not complete the sign-in ({error})."
        _end(flow, "failed", message)
        return False, 200, message, True
    try:
        tokens = client.exchange_code(flow.hub_url, code or "", flow.verifier, flow.redirect_uri)
        connection = flow.complete(tokens)
    except client.HubRefused as e:
        message = f"The hub refused the sign-in: {e.detail}"
    except client.HubVersionRefused as e:
        message = e.detail
    except client.HubUnreachable as e:
        message = str(e)
    except Exception as e:
        logger.warning("A sign-in through the browser to %s failed: %s", flow.hub_url, type(e).__name__)
        message = f"Flowfile could not complete the sign-in ({type(e).__name__}). Start it again in Flowfile."
    else:
        _end(flow, "connected", None, connection)
        return True, 200, SIGNED_IN, True
    _end(flow, "failed", message)
    return False, 200, message, True


def _prune(now: float) -> None:
    for flow_id, flow in list(_flows.items()):
        if flow.ended_at is not None and now - flow.ended_at > _KEEP_ENDED_SECONDS:
            del _flows[flow_id]


def start(*, user_id: int, hub_url: str, device_name: str, complete: Callable[[dict], dict]) -> tuple[str, str]:
    """Open a listener, keep the pending sign-in and answer ``(flow id, the hub's authorize URL)``.

    ``complete`` gets the hub's token answer on the listener's thread and answers the connection to report; whatever
    it raises fails the sign-in with a sentence. Raises ``SignInUnavailable`` when no local port can be opened.
    """
    listener = _Listener()
    pkce = new_pkce_pair()
    flow = _SignIn(
        flow_id=uuid.uuid4().hex,
        user_id=user_id,
        hub_url=hub_url.rstrip("/"),
        state=secrets.token_urlsafe(32),
        verifier=pkce.verifier,
        redirect_uri=listener.redirect_uri,
        complete=complete,
        expires_at=time.time() + TTL_SECONDS,
        listener=listener,
    )
    url = client.authorize_url(
        flow.hub_url,
        redirect_uri=flow.redirect_uri,
        code_challenge=pkce.challenge,
        state=flow.state,
        device_name=device_name,
    )
    with _lock:
        _prune(time.time())
        _flows[flow.flow_id] = flow
    try:
        listener.serve(flow.flow_id, alive=lambda: _alive(flow))
    except BaseException:
        with _lock:
            _flows.pop(flow.flow_id, None)
        listener.close()
        raise
    return flow.flow_id, url


def status(flow_id: str, user_id: int) -> dict | None:
    """``{status, detail, connection}`` of ``user_id``'s sign-in ``flow_id``; ``None`` for one that is not theirs."""
    flow = _flows.get(flow_id)
    if flow is None or flow.user_id != user_id:
        return None
    _alive(flow)
    return {"status": flow.status, "detail": flow.detail, "connection": flow.connection}


def cancel(flow_id: str, user_id: int) -> bool:
    """End ``user_id``'s sign-in ``flow_id`` that is still waiting and close its port; ``False`` when not theirs."""
    flow = _flows.get(flow_id)
    if flow is None or flow.user_id != user_id:
        return False
    if not flow._taken:
        _end(flow, "failed", CANCELLED)
    return True

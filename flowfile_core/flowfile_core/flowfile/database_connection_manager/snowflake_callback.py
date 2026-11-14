"""The browser side of a Snowflake sign-in: a one-shot loopback listener and what it does with the callback.

Snowflake's built-in OAuth client for local applications accepts only ``http://127.0.0.1:<any port>`` with an
empty path as its redirect (a path, or ``localhost``, is refused as a mismatch with the registered client), so the
redirect cannot be a route on core's app. ``begin_sign_in`` therefore binds a small HTTP server to
``127.0.0.1`` on a free port, the way snowflake-connector-python's own browser sign-in does, records the pending
sign-in (``snowflake_signin``) with ``http://127.0.0.1:<port>`` as its redirect and answers the Snowflake page to
open. The listener answers a request whose query carries ``state`` and ``code`` or ``error`` through
``complete_sign_in`` with a small HTML page; any other request gets a 404 and leaves the sign-in untouched. It shuts
itself down once its sign-in has finished, and the sign-in table closes it whenever the sign-in expires, is claimed
or is forgotten, so no listener outlives its sign-in.

The callback has no Flowfile session: it is trusted only through ``state``, a JWT signed with core's JWT secret
whose ``type`` is ``snowflake_oauth_state`` and which names the sign-in. The listener binds ``127.0.0.1`` only,
never logs a request line (it carries the authorization code) and reads no more than a capped request line and
headers.
"""

from __future__ import annotations

import html
import socketserver
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import jwt
from jwt import PyJWTError

from flowfile_core.auth.jwt import get_jwt_secret
from flowfile_core.configs.settings import ALGORITHM
from flowfile_core.flowfile.database_connection_manager import snowflake_signin
from flowfile_core.flowfile.database_connection_manager.db_connections import SNOWFLAKE, snowflake_unavailable_message
from shared.db_dialects import get_dialect
from shared.snowflake_auth import AUTH_OAUTH, SnowflakeAuthError, authorize_url, exchange_code, new_pkce_pair

STATE_TYPE = "snowflake_oauth_state"
_LOOPBACK = "127.0.0.1"
_MAX_PATH = 8192
_REQUEST_TIMEOUT_SECONDS = 10
_POLL_SECONDS = 0.25
_SIGNED_IN = "Signed in to Snowflake. You can close this tab and return to Flowfile."
_NO_REFRESH_TOKEN = (
    "This Snowflake account does not issue refresh tokens for the built-in sign-in, so Flowfile cannot keep the "
    "connection. Use an access token or a key-pair."
)


class SignInListenerError(SnowflakeAuthError):
    """The local listener a browser sign-in needs could not be opened."""


@dataclass(frozen=True)
class CallbackOutcome:
    ok: bool
    message: str
    status_code: int
    # The sign-in is over (or gone), so its listener can close.
    finished: bool


def sign_state(flow_id: str) -> str:
    payload = {"type": STATE_TYPE, "flow_id": flow_id, "exp": int(time.time()) + snowflake_signin.TTL_SECONDS}
    return jwt.encode(payload, get_jwt_secret(), algorithm=ALGORITHM)


def verify_state(state: str) -> dict:
    try:
        payload = jwt.decode(state, get_jwt_secret(), algorithms=[ALGORITHM])
    except PyJWTError as e:
        raise ValueError(
            f"This sign-in link is not valid any more ({type(e).__name__}). Start again in Flowfile."
        ) from e
    if payload.get("type") != STATE_TYPE or not payload.get("flow_id"):
        raise ValueError("This sign-in link was not made by Flowfile. Start again in Flowfile.")
    return payload


def render_page(ok: bool, message: str) -> str:
    title = "Signed in to Snowflake" if ok else "Snowflake sign-in failed"
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{html.escape(title)}</title></head>
<body style="font-family: system-ui, sans-serif; padding: 2rem; max-width: 40rem; margin: 0 auto;">
<h3>{html.escape(title)}</h3>
<p>{html.escape(message)}</p>
</body></html>"""


def session_out(host: str, info: dict[str, Any]) -> dict[str, Any]:
    """A discover result as the routes answer it."""
    return {
        "account_host": host,
        "username": info.get("user") or None,
        "role": info.get("role") or None,
        "warehouse": info.get("warehouse") or None,
        "database": info.get("database") or None,
        "warehouses": list(info.get("warehouses") or []),
        "databases": list(info.get("databases") or []),
        "roles": list(info.get("roles") or []),
    }


def complete_sign_in(
    code: str | None, state: str | None, error: str | None, *, flow_id: str | None = None
) -> CallbackOutcome:
    """Finish the sign-in ``state`` names with what Snowflake sent the browser back with.

    Exchanges the code for the tokens, then describes the session (a failure there does not fail the sign-in). A
    sign-in that already finished is never overwritten. ``flow_id``, when given, is the only sign-in this call may
    finish (the listener's own); a state naming another is refused without touching either.
    """
    if not state:
        return CallbackOutcome(False, "The sign-in link is incomplete. Start again in Flowfile.", 400, False)
    try:
        named = verify_state(state)["flow_id"]
    except ValueError as e:
        return CallbackOutcome(False, str(e), 400, False)
    if flow_id is not None and named != flow_id:
        return CallbackOutcome(
            False, "This sign-in link belongs to another sign-in. Start again in Flowfile.", 400, False
        )
    flow = snowflake_signin.begin_exchange(named)
    if flow is None:
        current = snowflake_signin.get(named)
        if current is None:
            return CallbackOutcome(False, "This sign-in has expired. Start again in Flowfile.", 400, True)
        if current.status == "ok":
            return CallbackOutcome(True, _SIGNED_IN, 200, True)
        if current.status == "error":
            return CallbackOutcome(False, current.message or "The sign-in failed. Start again in Flowfile.", 200, True)
        return CallbackOutcome(False, "This sign-in is already being completed.", 200, False)
    if error:
        message = (
            "The sign-in was cancelled in Snowflake. Start again in Flowfile."
            if error == "access_denied"
            else f"Snowflake did not complete the sign-in ({error})."
        )
        return _failed(named, message)
    if not code:
        return _failed(named, "Snowflake returned no sign-in code. Start again in Flowfile.")
    try:
        tokens = exchange_code(flow.host, code=code, verifier=flow.verifier, redirect_uri=flow.redirect_uri)
    except SnowflakeAuthError as e:
        return _failed(named, str(e))
    if not tokens.refresh_token:
        return _failed(named, _NO_REFRESH_TOKEN)

    session: dict[str, Any] = {}
    message = None
    try:
        uri = get_dialect(SNOWFLAKE).build_uri(
            host=flow.host,
            username=tokens.username or None,
            password=tokens.refresh_token,
            auth=AUTH_OAUTH,
            role=flow.role,
        )
        session = session_out(flow.host, get_dialect(SNOWFLAKE).discover(uri))
    except ImportError:
        message = snowflake_unavailable_message()
    except ValueError as e:
        message = f"Signed in, but the warehouses and databases could not be listed: {e}"
    except Exception as e:
        message = f"Signed in, but the warehouses and databases could not be listed ({type(e).__name__})."
    snowflake_signin.finish(
        named,
        status="ok",
        message=message,
        refresh_token=tokens.refresh_token,
        access_token=tokens.access_token,
        username=session.get("username") or tokens.username,
        session=session,
    )
    return CallbackOutcome(True, _SIGNED_IN, 200, True)


def _failed(flow_id: str, message: str) -> CallbackOutcome:
    snowflake_signin.finish(flow_id, status="error", message=message)
    return CallbackOutcome(False, message, 200, True)


class _Handler(BaseHTTPRequestHandler):
    server: _ListenerServer
    timeout = _REQUEST_TIMEOUT_SECONDS

    def do_GET(self) -> None:
        if len(self.path) > _MAX_PATH:
            self._answer(414, "text/plain; charset=utf-8", "Request too long")
            return
        query = parse_qs(urlsplit(self.path).query, max_num_fields=8)
        state = (query.get("state") or [None])[0]
        code = (query.get("code") or [None])[0]
        error = (query.get("error") or [None])[0]
        if not state or not (code or error):
            self._answer(404, "text/plain; charset=utf-8", "Not Found")
            return
        listener = self.server.listener
        outcome = listener.handle(code, state, error)
        self._answer(outcome.status_code, "text/html; charset=utf-8", render_page(outcome.ok, outcome.message))
        if outcome.finished:
            listener.close()

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


class _ListenerServer(ThreadingHTTPServer):
    """One thread per connection, so a browser's idle preconnect cannot hold up the real callback."""

    daemon_threads = True
    block_on_close = False
    listener: CallbackListener

    def server_bind(self) -> None:
        # HTTPServer would resolve the host's name, a DNS lookup a loopback listener does not need.
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]

    def handle_error(self, request, client_address) -> None:
        pass


class CallbackListener:
    """A one-shot HTTP listener on ``127.0.0.1`` and a free port, serving one sign-in's redirect."""

    def __init__(self) -> None:
        try:
            self._server = _ListenerServer((_LOOPBACK, 0), _Handler)
        except OSError as e:
            raise SignInListenerError(
                f"Flowfile could not open a local port for the Snowflake sign-in ({type(e).__name__}). Try again."
            ) from None
        self._server.listener = self
        self._server.timeout = _POLL_SECONDS
        self._stop = threading.Event()
        self._flow_id: str | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    @property
    def redirect_uri(self) -> str:
        return f"http://{_LOOPBACK}:{self.port}"

    @property
    def closed(self) -> bool:
        return self._stop.is_set() and (self._thread is None or not self._thread.is_alive())

    def serve(self, flow_id: str, *, alive: Callable[[], bool]) -> None:
        """Start answering for ``flow_id`` in a daemon thread until closed or until ``alive()`` turns false."""
        self._flow_id = flow_id
        self._thread = threading.Thread(target=self._loop, args=(alive,), daemon=True, name="snowflake-sign-in")
        self._thread.start()

    def handle(self, code: str | None, state: str, error: str | None) -> CallbackOutcome:
        return complete_sign_in(code, state, error, flow_id=self._flow_id)

    def close(self) -> None:
        """Stop listening; the port is released within a poll interval (at once when no request is being served)."""
        self._stop.set()
        thread = self._thread
        if thread is None:
            self._server.server_close()
        elif thread is not threading.current_thread():
            thread.join(timeout=2)

    def _loop(self, alive: Callable[[], bool]) -> None:
        try:
            while not self._stop.is_set() and alive():
                self._server.handle_request()
        finally:
            self._stop.set()
            self._server.server_close()


def begin_sign_in(*, user_id: int, host: str, role: str | None) -> tuple[str, str]:
    """Open a listener, record the pending sign-in and answer ``(authorize URL, flow id)``.

    Raises ``SignInListenerError`` when no local port can be opened.
    """
    listener = CallbackListener()
    pkce = new_pkce_pair()
    flow = snowflake_signin.start(
        user_id=user_id,
        host=host,
        role=role,
        verifier=pkce.verifier,
        redirect_uri=listener.redirect_uri,
        closer=listener.close,
    )
    listener.serve(flow.flow_id, alive=lambda: snowflake_signin.get(flow.flow_id) is not None)
    try:
        state = sign_state(flow.flow_id)
    except Exception:
        snowflake_signin.forget(flow.flow_id)
        raise
    url = authorize_url(host, redirect_uri=listener.redirect_uri, state=state, code_challenge=pkce.challenge, role=role)
    return url, flow.flow_id

"""Resilient SQL reads shared by flowfile_core (local execution) and flowfile_worker.

connectorx (pl.read_database_uri) is the fast path, but against transaction-mode
poolers (e.g. Supabase Supavisor :6543, pgbouncer) individual reads intermittently
hang at the protocol level instead of erroring. SQLAlchemy reads (simple query
protocol) are reliable on those endpoints. ``read_sql_with_fallback`` hedges: it
starts connectorx, and if that neither finishes nor fails within ``hedge_delay``
seconds it races a SQLAlchemy read in parallel, returning the first success.
"""

from __future__ import annotations

import hashlib
import logging
import os
import threading
import time
from collections.abc import Callable

import polars as pl

from shared.sql_utils import get_sqlalchemy_uri

HEDGE_DELAY_SECONDS: float = float(os.environ.get("FLOWFILE_DB_READ_HEDGE_DELAY", "8"))
_POLL_INTERVAL_SECONDS = 0.25

# URIs where a hedged SQLAlchemy read won (connectorx hung or lost): skip
# connectorx for the rest of this process. Worker children exit per task, so this
# mainly helps the long-lived core process in local execution mode. Stored as
# hashes — never the raw URI, which embeds the plaintext password.
_sqlalchemy_first: set[str] = set()
_sqlalchemy_first_lock = threading.Lock()


class DatabaseReadCancelledError(Exception):
    """Raised when a database read is abandoned because cancel_check fired."""


class _Attempt:
    """A read attempt running in a daemon thread (abandonable on hang/cancel)."""

    def __init__(self, name: str, fn: Callable[[], pl.DataFrame]):
        self.name = name
        self.done = threading.Event()
        self.result: pl.DataFrame | None = None
        self.error: BaseException | None = None
        self._fn = fn

    def start(self) -> _Attempt:
        threading.Thread(target=self._run, daemon=True, name=f"db-read-{self.name}").start()
        return self

    def _run(self) -> None:
        try:
            self.result = self._fn()
        except BaseException as e:  # noqa: BLE001 - surfaced to the caller thread
            self.error = e
        finally:
            self.done.set()


def _read_connectorx(query: str, uri: str) -> pl.DataFrame:
    return pl.read_database_uri(query, uri)


def _read_sqlalchemy(query: str, uri: str) -> pl.DataFrame:
    from sqlalchemy import create_engine

    engine = create_engine(get_sqlalchemy_uri(uri))
    try:
        return pl.read_database(query, connection=engine)
    finally:
        engine.dispose()


def _uri_key(uri: str) -> str:
    return hashlib.sha256(uri.encode()).hexdigest()


def _mark_sqlalchemy_first(uri: str) -> None:
    with _sqlalchemy_first_lock:
        _sqlalchemy_first.add(_uri_key(uri))


def read_sql_with_fallback(
    query: str,
    uri: str,
    logger: logging.Logger,
    hedge_delay: float | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> pl.DataFrame:
    """Read ``query`` into a DataFrame, resilient to connectorx-hostile servers.

    - connectorx result returned as soon as it completes (fast path, unchanged).
    - If connectorx neither finishes nor errors within ``hedge_delay`` seconds, a
      SQLAlchemy read starts in parallel; the first success wins and a SQLAlchemy
      win flags the URI so later reads in this process skip connectorx entirely.
    - ``cancel_check`` (polled ~4x/s) aborts the wait with DatabaseReadCancelledError;
      attempt threads are daemons, so they never block shutdown.
    """
    delay = HEDGE_DELAY_SECONDS if hedge_delay is None else hedge_delay
    with _sqlalchemy_first_lock:
        use_connectorx = _uri_key(uri) not in _sqlalchemy_first

    cx = _Attempt("connectorx", lambda: _read_connectorx(query, uri)).start() if use_connectorx else None
    sa: _Attempt | None = None
    if cx is None:
        sa = _Attempt("sqlalchemy", lambda: _read_sqlalchemy(query, uri)).start()
    hedge_at = time.monotonic() + delay
    give_up_at: float | None = None

    def _start_sqlalchemy(reason: str) -> _Attempt:
        logger.warning(
            "%s - starting SQLAlchemy fallback read in parallel. Transaction-mode pooler endpoints "
            "(e.g. Supabase port 6543) intermittently hang connectorx; for best performance use a "
            "session pooler (e.g. Supabase port 5432) or a direct connection.",
            reason,
        )
        return _Attempt("sqlalchemy", lambda: _read_sqlalchemy(query, uri)).start()

    while True:
        if cancel_check is not None and cancel_check():
            raise DatabaseReadCancelledError("Database read cancelled")

        if cx is not None and cx.done.is_set():
            if cx.error is None:
                return cx.result
            cx_error, cx = cx.error, None
            if sa is None:
                sa = _start_sqlalchemy(f"connectorx read failed ({cx_error})")
            else:
                logger.warning("connectorx read failed (%s); awaiting SQLAlchemy result", cx_error)

        if sa is not None and sa.done.is_set():
            if sa.error is None:
                if cx is not None:
                    _mark_sqlalchemy_first(uri)
                    logger.info("SQLAlchemy read won the hedge; skipping connectorx for this connection")
                return sa.result
            if cx is None:
                raise RuntimeError(f"Database read failed via both connectorx and SQLAlchemy: {sa.error}") from sa.error
            if give_up_at is None:
                give_up_at = time.monotonic() + max(delay, 30.0)
                logger.warning("SQLAlchemy fallback failed (%s); awaiting connectorx", sa.error)
            elif time.monotonic() >= give_up_at:
                raise RuntimeError(
                    f"Database read failed: SQLAlchemy error ({sa.error}); connectorx unresponsive"
                ) from sa.error

        if sa is None and time.monotonic() >= hedge_at:
            sa = _start_sqlalchemy(f"connectorx read still running after {delay:.0f}s")

        waiters = [a.done for a in (cx, sa) if a is not None and not a.done.is_set()]
        if waiters:
            waiters[0].wait(_POLL_INTERVAL_SECONDS)
        else:
            time.sleep(_POLL_INTERVAL_SECONDS)

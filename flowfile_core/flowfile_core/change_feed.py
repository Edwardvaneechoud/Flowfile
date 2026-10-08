"""Per-flow change feed: the revision events every client of a flow can subscribe to.

A ``FlowGraph`` publishes ``flow_revision`` on the in-process bus for every change its
``revision`` counts (mutations, undo/redo, run start and end, saves), and the handler publishes
``flow_closed`` when a flow leaves memory. This module fans those out to per-flow asyncio
queues that ``GET /editor/events`` streams as server-sent events, so a second browser tab,
another user on the same server or a pop-out window learns that the flow moved under it.

Core runs as one process with the flows in memory, so an in-process registry sees every
change. The bus handlers only hand the event to each subscriber's loop and never block the
mutating thread. A request names its origin in the ``X-Flowfile-Client`` header, read into
:data:`CLIENT_ORIGIN` by :class:`ClientOriginMiddleware`; every event echoes it so a client
can tell its own changes from foreign ones.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator
from contextvars import ContextVar
from dataclasses import dataclass

from flowfile_core import events

CLIENT_HEADER = "X-Flowfile-Client"
_CLIENT_HEADER_WIRE = CLIENT_HEADER.lower().encode("latin-1")
_ORIGIN_MAX_LENGTH = 128

CLIENT_ORIGIN: ContextVar[str | None] = ContextVar("flowfile_client_origin", default=None)

CLOSED = "closed"
REKEYED = "rekeyed"
# The events that end a flow's stream: the flow left memory, or lives on under another id.
_FINAL_KINDS = frozenset({CLOSED, REKEYED})


@dataclass(frozen=True, slots=True)
class FlowEvent:
    """One change of a flow; ``revision`` is ``None`` only for ``closed`` and ``rekeyed``."""

    flow_id: int
    revision: int | None
    kind: str
    origin: str | None = None
    new_flow_id: int | None = None

    def payload(self) -> dict:
        payload = {"kind": self.kind, "flow_id": self.flow_id, "revision": self.revision, "origin": self.origin}
        if self.new_flow_id is not None:
            payload["new_flow_id"] = self.new_flow_id
        return payload


_SHUTDOWN = object()


class _Subscriber:
    __slots__ = ("loop", "queue")

    def __init__(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue) -> None:
        self.loop = loop
        self.queue = queue

    def deliver(self, item: object) -> None:
        try:
            self.loop.call_soon_threadsafe(self.queue.put_nowait, item)
        except RuntimeError:
            pass  # the loop is closed: the stream it served is already gone


_lock = threading.Lock()
_subscribers: dict[int, set[_Subscriber]] = {}


def _deliver(flow_id: int, item: object) -> None:
    with _lock:
        targets = list(_subscribers.get(flow_id, ()))
    for subscriber in targets:
        subscriber.deliver(item)


def _on_revision(graph, revision: int, kind: str) -> None:
    _deliver(graph.flow_id, FlowEvent(graph.flow_id, revision, kind, CLIENT_ORIGIN.get()))


def _on_closed(flow_id: int) -> None:
    _deliver(flow_id, FlowEvent(flow_id, None, CLOSED))


def _on_rekeyed(old_flow_id: int, new_flow_id: int) -> None:
    _deliver(old_flow_id, FlowEvent(old_flow_id, None, REKEYED, new_flow_id=new_flow_id))


def install() -> None:
    """Subscribe to the bus. Idempotent, also after ``events._reset_for_tests``."""
    for event, handler in (
        ("flow_revision", _on_revision),
        ("flow_closed", _on_closed),
        ("flow_rekeyed", _on_rekeyed),
    ):
        if handler not in events._handlers.get(event, ()):
            events.subscribe(event, handler)


def subscriber_count(flow_id: int) -> int:
    with _lock:
        return len(_subscribers.get(flow_id, ()))


async def stream(flow_id: int, *, keepalive: float | None = None) -> AsyncIterator[FlowEvent | None]:
    """Yield the flow's events as they happen, ``None`` after ``keepalive`` idle seconds.

    Ends after the flow's ``closed`` or ``rekeyed`` event and when :func:`close_all` runs.
    """
    install()  # a bus reset (tests) must not leave a later stream deaf
    subscriber = _Subscriber(asyncio.get_running_loop(), asyncio.Queue())
    with _lock:
        _subscribers.setdefault(flow_id, set()).add(subscriber)
    try:
        while True:
            if keepalive is None:
                item = await subscriber.queue.get()
            else:
                try:
                    item = await asyncio.wait_for(subscriber.queue.get(), keepalive)
                except asyncio.TimeoutError:
                    yield None
                    continue
            if item is _SHUTDOWN:
                return
            yield item
            if item.kind in _FINAL_KINDS:
                return
    finally:
        with _lock:
            group = _subscribers.get(flow_id)
            if group is not None:
                group.discard(subscriber)
                if not group:
                    del _subscribers[flow_id]


def close_all() -> None:
    """End every open stream, from any thread: an open stream would hold up uvicorn's shutdown."""
    with _lock:
        targets = [subscriber for group in _subscribers.values() for subscriber in group]
    for subscriber in targets:
        subscriber.deliver(_SHUTDOWN)


class ClientOriginMiddleware:
    """Read ``X-Flowfile-Client`` into :data:`CLIENT_ORIGIN` for the request.

    Pure ASGI, like the telemetry middleware: ``BaseHTTPMiddleware`` would buffer the
    streaming responses. The variable is set on the request's task, so a sync endpoint on
    the threadpool and the response's background tasks see it too.
    """

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        origin = None
        for name, value in scope.get("headers") or ():
            if name == _CLIENT_HEADER_WIRE:
                origin = value.decode("latin-1")[:_ORIGIN_MAX_LENGTH] or None
                break
        token = CLIENT_ORIGIN.set(origin)
        try:
            await self.app(scope, receive, send)
        finally:
            CLIENT_ORIGIN.reset(token)


def _reset_for_tests() -> None:
    with _lock:
        _subscribers.clear()


install()

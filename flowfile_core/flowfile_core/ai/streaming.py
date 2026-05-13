"""SSE primitives for AI endpoints — keepalive, resumption tokens, checkpoint hook.

Consumes the ``Provider.stream()`` contract
(``AsyncIterator[StreamChunk]``) and serialises chunks into the SSE
wire format with three hardenings:

* **Keepalive comments** every ``KEEPALIVE_INTERVAL_SECONDS`` (15s
  default) so upstream proxies don't idle-time-out a slow generation.
* **Resumption tokens** at every tool-call boundary — each
  ``tool_call`` event carries an ``id:`` line, which EventSource
  clients echo back as ``Last-Event-ID`` on reconnect.
  ``resumable_sse_stream`` does the cursor-skip; the actual replay
  buffer lives in :mod:`flowfile_core.ai.replay_buffer`.
* **Server-side checkpoint hook** (``on_checkpoint``) called once per
  complete tool call so the session sidecar can persist state for
  crash-recovery.

Public surface:

* :class:`SSEEvent` — frozen slots dataclass with a ``format()`` method.
* :func:`format_sse_chunk` / :func:`format_sse_tool_call` /
  :func:`format_sse_done` / :func:`format_sse_error` /
  :func:`format_sse_keepalive` — sync wire helpers.
* :func:`sse_stream` — the core async generator (StreamChunk → wire string).
* :func:`resumable_sse_stream` — thin skip-cursor wrapper around ``sse_stream``.
* :func:`make_streaming_response` — ``StreamingResponse`` with the
  ``text/event-stream`` headers.

The litellm import stays out of this module by construction (tests verify).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from fastapi.responses import StreamingResponse

from flowfile_core.ai.providers.base import StreamChunk, ToolCall

if TYPE_CHECKING:
    from flowfile_core.ai.replay_buffer import ReplayBuffer as ReplayBufferProtocol

logger = logging.getLogger(__name__)

KEEPALIVE_INTERVAL_SECONDS: float = 15.0
"""Seconds between keepalive comments emitted to defeat proxy idle-timeouts.

15s default. Configurable per call via ``sse_stream(...,
keepalive_interval=...)`` so tests can run in <1s.
"""

_TOOL_CALL_ID_LINE = re.compile(r"^id: (?P<id>[^\r\n]+)$", re.MULTILINE)


@dataclass(slots=True, frozen=True)
class SSEEvent:
    """A single SSE wire event.

    ``event=None`` produces a comment line (``": <data>\\n\\n"``) — used for
    keepalives. Otherwise produces an optional ``id:`` line, an
    ``event:`` line, and a ``data:`` line, terminated by a blank line.
    """

    event: str | None
    data: str
    id: str | None = None

    def format(self) -> str:
        if self.event is None:
            return f": {self.data}\n\n"
        parts: list[str] = []
        if self.id is not None:
            parts.append(f"id: {self.id}\n")
        parts.append(f"event: {self.event}\n")
        parts.append(f"data: {self.data}\n\n")
        return "".join(parts)


def format_sse_chunk(chunk: StreamChunk) -> str:
    """Serialise a content-delta ``StreamChunk`` as an ``event: chunk`` event."""
    payload = json.dumps({"content_delta": chunk.content_delta})
    return SSEEvent(event="chunk", data=payload).format()


def format_sse_tool_call(tool_call: ToolCall) -> str:
    """Serialise a complete ``ToolCall`` as an ``event: tool_call`` event.

    The ``id:`` line carries ``tool_call.id`` so the EventSource
    client can echo it back as ``Last-Event-ID`` on reconnect.
    """
    payload = json.dumps(
        {
            "id": tool_call.id,
            "name": tool_call.name,
            "arguments": tool_call.arguments,
        }
    )
    return SSEEvent(event="tool_call", data=payload, id=tool_call.id).format()


def format_sse_done(finish_reason: str) -> str:
    """Final marker — ``event: done`` with the provider's ``finish_reason``."""
    payload = json.dumps({"finish_reason": finish_reason})
    return SSEEvent(event="done", data=payload).format()


def format_sse_error(message: str) -> str:
    """Surface a generation-time error to the client before re-raising."""
    payload = json.dumps({"message": message})
    return SSEEvent(event="error", data=payload).format()


def format_sse_keepalive() -> str:
    """A comment line — EventSource clients ignore it; proxies see traffic."""
    return SSEEvent(event=None, data="keepalive").format()


async def sse_stream(
    provider_stream: AsyncIterator[StreamChunk],
    *,
    keepalive_interval: float = KEEPALIVE_INTERVAL_SECONDS,
    on_checkpoint: Callable[[ToolCall], Awaitable[None]] | None = None,
) -> AsyncIterator[str]:
    """Translate a ``Provider.stream()`` iterator into SSE wire strings.

    Races the upstream iterator against a per-step timeout so we always
    emit something at least every ``keepalive_interval`` seconds: either a
    real chunk or a keepalive comment.

    The pending ``__anext__()`` is wrapped in a task and shielded so a
    timeout doesn't cancel the underlying async generator — cancelling the
    raw awaitable would destroy the generator's state and we'd never see
    the chunk that was about to arrive.

    On a complete tool-call boundary
    (``StreamChunk.tool_call_delta``), invokes
    ``on_checkpoint(tool_call)`` if provided — this is the seam for
    sidecar session persistence.

    On exception, emits an ``event: error`` payload so the client sees the
    failure, then closes the response cleanly. Re-raising would tear the
    connection down before the error frame is guaranteed to flush, leaving
    the browser to surface the abort as a generic ``TypeError: network
    error`` instead of the structured message we just yielded.
    """
    ait = provider_stream.__aiter__()
    next_task: asyncio.Task[StreamChunk] | None = None
    try:
        while True:
            if next_task is None:
                next_task = asyncio.ensure_future(ait.__anext__())

            try:
                chunk = await asyncio.wait_for(asyncio.shield(next_task), timeout=keepalive_interval)
            except asyncio.TimeoutError:
                yield format_sse_keepalive()
                continue
            except StopAsyncIteration:
                next_task = None
                return

            next_task = None

            if chunk.content_delta is not None:
                yield format_sse_chunk(chunk)

            if chunk.tool_call_delta is not None:
                yield format_sse_tool_call(chunk.tool_call_delta)
                if on_checkpoint is not None:
                    await on_checkpoint(chunk.tool_call_delta)

            if chunk.finish_reason is not None:
                yield format_sse_done(chunk.finish_reason)
                return
    except asyncio.CancelledError:
        # Client disconnect — let it propagate so the generator unwinds cleanly.
        raise
    except Exception as exc:
        logger.exception("sse_stream errored mid-generation")
        yield format_sse_error(str(exc))
        return
    finally:
        if next_task is not None and not next_task.done():
            next_task.cancel()
            try:
                await next_task
            except (asyncio.CancelledError, StopAsyncIteration, Exception):
                # Drain whatever the cancelled task surfaces; we're already
                # exiting so there's no client to deliver it to.
                pass
        aclose = getattr(ait, "aclose", None)
        if aclose is not None:
            try:
                await aclose()
            except Exception:
                logger.debug("provider stream aclose() raised; ignoring", exc_info=True)


async def resumable_sse_stream(
    provider_stream: AsyncIterator[StreamChunk],
    *,
    last_event_id: str | None = None,
    keepalive_interval: float = KEEPALIVE_INTERVAL_SECONDS,
    on_checkpoint: Callable[[ToolCall], Awaitable[None]] | None = None,
) -> AsyncIterator[str]:
    """Skip-cursor wrapper around :func:`sse_stream` for client reconnection.

    When ``last_event_id`` is provided, suppresses every emitted block until a
    ``tool_call`` event with ``id == last_event_id`` is seen; that block is
    *also* dropped (the client already has it), and everything after is
    forwarded.

    If the cursor never matches (provider regenerates a different
    plan, or the stream ends first), emits an ``event: error`` and
    stops — the client should restart fresh. This is the cheapest
    correct behaviour at the streaming layer; the disk replay buffer
    is what enables true replay.

    With ``last_event_id=None`` the wrapper is identical to :func:`sse_stream`.
    """
    inner = sse_stream(
        provider_stream,
        keepalive_interval=keepalive_interval,
        on_checkpoint=on_checkpoint,
    )

    if last_event_id is None:
        async for line in inner:
            yield line
        return

    found = False
    async for line in inner:
        if found:
            yield line
            continue
        if _matches_tool_call_id(line, last_event_id):
            found = True
            # Drop this block — the client already has it.
            continue
        # Drop everything before the cursor. (Includes any keepalives or
        # earlier tool calls — the client already saw them.)

    if not found:
        logger.warning(
            "resumable_sse_stream: last_event_id %r never matched a tool_call boundary; "
            "stream ended without resumption point",
            last_event_id,
        )
        yield format_sse_error(f"resumption cursor {last_event_id!r} not found in stream; restart fresh")


def _matches_tool_call_id(sse_block: str, target_id: str) -> bool:
    """True iff ``sse_block`` is a ``tool_call`` event with the matching ``id:``."""
    if "event: tool_call" not in sse_block:
        return False
    match = _TOOL_CALL_ID_LINE.search(sse_block)
    return match is not None and match.group("id") == target_id


def format_sse_planner_event(
    event_name: str,
    payload: dict,
    *,
    session_id: str,
    step_count: int,
) -> str:
    """Serialise a ``PlannerEvent`` as an SSE wire string.

    The ``id:`` line carries ``f"{session_id}.{step_count}"`` so an
    EventSource client can echo it back via ``Last-Event-ID``.
    ``event:`` matches the Python ``PlannerEvent.event`` Literal —
    ``tool_call_proposed`` / ``tool_call_staged`` /
    ``tool_call_warned`` / ``tool_call_rejected`` / ``thinking`` /
    ``drift_detected`` / ``paused`` / ``retry`` / ``abort`` /
    ``complete`` / ``error`` / ``info``. ``data:`` is JSON of the
    payload dict.
    """
    data = json.dumps(payload)
    return SSEEvent(event=event_name, data=data, id=f"{session_id}.{step_count}").format()


async def planner_events_sse(
    events: AsyncIterator,
    *,
    session_id: str,
    step_count_getter: Callable[[], int],
    keepalive_interval: float = KEEPALIVE_INTERVAL_SECONDS,
    replay_buffer: ReplayBufferProtocol | None = None,
    flow_id: int | None = None,
    replay_after_event_id: str | None = None,
) -> AsyncIterator[str]:
    """Translate a ``PlannerEvent`` iterator into SSE wire strings.

    Mirrors :func:`sse_stream`'s race-against-keepalive pattern so the
    connection stays alive across slow LLM calls.
    ``step_count_getter`` is a closure over the live
    :class:`AgentSession` — each event picks up the *current* step
    counter so resume cursors are step-aligned, not wall-clock.

    Replay-buffer plumbing:

    * ``replay_buffer`` / ``flow_id`` enable the post-emit append:
      every live frame is captured into the
      per-(flow_id, session_id) ring so a future ``Last-Event-ID``
      reconnect can replay it. Best-effort — buffer write failures
      never crash the stream.
    * ``replay_after_event_id`` flushes buffered frames newer than the
      cursor *before* live streaming resumes. Used by the resume
      route when the client supplies ``Last-Event-ID`` on reconnect.

    Like :func:`sse_stream`, errors mid-stream become an
    ``event: error`` frame; cancellations propagate. The pending
    ``__anext__()`` is shielded so a keepalive timeout doesn't tear
    the underlying generator.
    """
    # Flush buffered frames past the cursor before live streaming.
    if replay_buffer is not None and flow_id is not None and replay_after_event_id is not None:
        try:
            for _eid, payload in replay_buffer.read_after(
                flow_id=flow_id,
                session_id=session_id,
                event_id=replay_after_event_id,
            ):
                yield payload.decode("utf-8", errors="replace")
        except Exception:
            logger.exception("planner_events_sse: replay-buffer drain failed")

    ait = events.__aiter__()
    next_task: asyncio.Task | None = None
    try:
        while True:
            if next_task is None:
                next_task = asyncio.ensure_future(ait.__anext__())
            try:
                event = await asyncio.wait_for(asyncio.shield(next_task), timeout=keepalive_interval)
            except asyncio.TimeoutError:
                yield format_sse_keepalive()
                continue
            except StopAsyncIteration:
                next_task = None
                return
            next_task = None
            step = step_count_getter()
            line = format_sse_planner_event(
                event.event,
                event.payload,
                session_id=session_id,
                step_count=step,
            )
            yield line
            if replay_buffer is not None and flow_id is not None:
                try:
                    replay_buffer.append(
                        flow_id=flow_id,
                        session_id=session_id,
                        event_id=f"{session_id}.{step}",
                        payload=line.encode("utf-8"),
                    )
                except Exception:
                    logger.exception("planner_events_sse: replay-buffer append failed")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("planner_events_sse errored mid-generation")
        yield format_sse_error(str(exc))
        return
    finally:
        if next_task is not None and not next_task.done():
            next_task.cancel()
            try:
                await next_task
            except (asyncio.CancelledError, StopAsyncIteration, Exception):
                pass
        aclose = getattr(ait, "aclose", None)
        if aclose is not None:
            try:
                await aclose()
            except Exception:
                logger.debug("planner events aclose() raised; ignoring", exc_info=True)


def make_streaming_response(generator: AsyncIterator[str]) -> StreamingResponse:
    """Wrap an SSE generator in a ``StreamingResponse`` with proxy-friendly headers.

    Mirrors ``routes/logs.py`` — the existing in-tree SSE pattern — so any
    reverse-proxy / Starlette config that already serves the logs endpoint
    works for AI streaming too.
    """
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
            # nginx defaults to proxy_buffering on, which holds SSE chunks
            # until the buffer fills — fatal for token-by-token streaming.
            "X-Accel-Buffering": "no",
        },
    )

"""W13 — SSE streaming primitives tests.

Cases:

* ``test_format_sse_chunk`` — content delta serialises as ``event: chunk``.
* ``test_format_sse_tool_call_includes_id`` — tool_call event carries the
  ``id:`` line so EventSource clients can echo it back as ``Last-Event-ID``.
* ``test_format_sse_keepalive_is_comment`` — keepalive is a comment line,
  not a data line.
* ``test_sse_stream_emits_keepalive_when_provider_idle`` — proves the
  §5.4 keepalive contract under a deliberately-slow upstream.
* ``test_sse_stream_emits_chunk_then_done`` — happy path: content chunks
  followed by ``finish_reason`` → ``event: done``.
* ``test_sse_stream_emits_tool_call_with_id`` — tool_call_delta surfaces
  the ``id:`` line that drives resumption.
* ``test_sse_stream_invokes_on_checkpoint_at_tool_boundary`` — W42 seam:
  the async hook fires once per complete ``ToolCall``.
* ``test_sse_stream_propagates_provider_exception_as_error_event`` —
  upstream raises mid-stream → ``event: error`` emitted, exception re-raised.
* ``test_resumable_sse_stream_skips_until_last_event_id`` — cursor-skip
  drops the matching tool_call and everything before, forwards everything
  after.
* ``test_resumable_sse_stream_no_last_event_id_emits_everything`` — the
  wrapper degrades to a passthrough when no cursor is supplied.
* ``test_resumable_sse_stream_unmatched_cursor_emits_error`` — cursor
  never matches → ``event: error`` so the client restarts fresh.
* ``test_make_streaming_response_headers`` — proxy-friendly headers match
  the existing in-tree pattern at ``routes/logs.py``.
* ``test_lazy_litellm_import`` — importing ``flowfile_core.ai.streaming``
  does not pull in litellm.
* ``test_streaming_response_via_test_client`` — end-to-end smoke: SSE
  endpoint mounted on a fresh FastAPI app, hit via ``TestClient``,
  returns the right content type and wire format.
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from flowfile_core.ai.providers.base import StreamChunk, ToolCall
from flowfile_core.ai.streaming import (
    KEEPALIVE_INTERVAL_SECONDS,
    SSEEvent,
    format_sse_chunk,
    format_sse_done,
    format_sse_error,
    format_sse_keepalive,
    format_sse_tool_call,
    make_streaming_response,
    resumable_sse_stream,
    sse_stream,
)

# ---------- helpers ----------


async def fake_stream(*chunks: StreamChunk) -> AsyncIterator[StreamChunk]:
    """Yield each chunk in order. No sleeps — for synchronous-ish tests."""
    for chunk in chunks:
        yield chunk


async def slow_stream(delay: float, *chunks: StreamChunk) -> AsyncIterator[StreamChunk]:
    """Sleep ``delay`` before *every* chunk — exercises the keepalive timer."""
    for chunk in chunks:
        await asyncio.sleep(delay)
        yield chunk


async def raising_stream(*chunks: StreamChunk, exc: Exception) -> AsyncIterator[StreamChunk]:
    """Yield chunks then raise ``exc`` — exercises the error path."""
    for chunk in chunks:
        yield chunk
    raise exc


async def collect(gen: AsyncIterator[str]) -> list[str]:
    out: list[str] = []
    async for line in gen:
        out.append(line)
    return out


def _data_line(sse_block: str) -> str:
    """Return the ``data: ...`` line from a multi-line SSE block.

    ``str.splitlines()`` followed by ``[-1]`` returns an empty string when
    the block ends in ``\\n\\n`` (which every well-formed SSE event does),
    so callers need a more forgiving extractor for the test assertions.
    """
    for line in sse_block.splitlines():
        if line.startswith("data: "):
            return line
    raise AssertionError(f"No data: line in SSE block: {sse_block!r}")


# ---------- 1. format_sse_chunk ----------


def test_format_sse_chunk() -> None:
    out = format_sse_chunk(StreamChunk(content_delta="hello"))
    assert out == 'event: chunk\ndata: {"content_delta": "hello"}\n\n'


# ---------- 2. format_sse_tool_call carries id: line ----------


def test_format_sse_tool_call_includes_id() -> None:
    tc = ToolCall(id="tc_abc", name="flowfile.graph.add_filter", arguments={"col": "x"})
    out = format_sse_tool_call(tc)

    assert out.startswith("id: tc_abc\n"), (
        "tool_call event must lead with id: line so EventSource clients " f"echo it back as Last-Event-ID; got: {out!r}"
    )
    assert "event: tool_call\n" in out
    payload_line = next(line for line in out.splitlines() if line.startswith("data: "))
    payload = json.loads(payload_line.removeprefix("data: "))
    assert payload == {
        "id": "tc_abc",
        "name": "flowfile.graph.add_filter",
        "arguments": {"col": "x"},
    }


# ---------- 3. keepalive is a comment, not a data line ----------


def test_format_sse_keepalive_is_comment() -> None:
    out = format_sse_keepalive()
    assert out.startswith(":"), "keepalive must be an SSE comment (`: ...`)"
    assert "data:" not in out
    assert out.endswith("\n\n")


# ---------- 4. keepalive emits when provider is idle ----------


@pytest.mark.asyncio
async def test_sse_stream_emits_keepalive_when_provider_idle() -> None:
    """A 0.2s-per-chunk upstream with a 0.05s keepalive must produce ≥1 keepalive."""

    async def gen() -> AsyncIterator[StreamChunk]:
        async for c in slow_stream(
            0.2,
            StreamChunk(content_delta="late"),
            StreamChunk(finish_reason="stop"),
        ):
            yield c

    out = await collect(sse_stream(gen(), keepalive_interval=0.05))

    keepalive_count = sum(1 for line in out if line.startswith(":"))
    assert keepalive_count >= 1, (
        "Expected at least one keepalive comment when the upstream is slow; " f"got {keepalive_count} in output {out!r}"
    )
    # ...and the real chunk + done are still emitted in order.
    chunks = [line for line in out if line.startswith("event: chunk")]
    dones = [line for line in out if line.startswith("event: done")]
    assert len(chunks) == 1
    assert len(dones) == 1
    assert out.index(chunks[0]) < out.index(dones[0])


# ---------- 5. happy path: chunk → chunk → done ----------


@pytest.mark.asyncio
async def test_sse_stream_emits_chunk_then_done() -> None:
    out = await collect(
        sse_stream(
            fake_stream(
                StreamChunk(content_delta="Hello "),
                StreamChunk(content_delta="world"),
                StreamChunk(finish_reason="stop"),
            ),
            keepalive_interval=10.0,  # avoid stray keepalives in this short test
        )
    )

    assert out == [
        'event: chunk\ndata: {"content_delta": "Hello "}\n\n',
        'event: chunk\ndata: {"content_delta": "world"}\n\n',
        'event: done\ndata: {"finish_reason": "stop"}\n\n',
    ]


# ---------- 6. tool_call surfaces id: line ----------


@pytest.mark.asyncio
async def test_sse_stream_emits_tool_call_with_id() -> None:
    tc = ToolCall(id="tc_xyz", name="flowfile.graph.add_select", arguments={})
    out = await collect(
        sse_stream(
            fake_stream(StreamChunk(tool_call_delta=tc)),
            keepalive_interval=10.0,
        )
    )
    assert len(out) == 1
    assert out[0].startswith("id: tc_xyz\n")
    assert "event: tool_call\n" in out[0]


# ---------- 7. on_checkpoint hook (W42 seam) ----------


@pytest.mark.asyncio
async def test_sse_stream_invokes_on_checkpoint_at_tool_boundary() -> None:
    seen: list[ToolCall] = []

    async def checkpoint(tc: ToolCall) -> None:
        seen.append(tc)

    tc1 = ToolCall(id="tc_1", name="flowfile.graph.add_filter", arguments={"k": 1})
    tc2 = ToolCall(id="tc_2", name="flowfile.graph.add_select", arguments={"k": 2})

    await collect(
        sse_stream(
            fake_stream(
                StreamChunk(content_delta="thinking..."),
                StreamChunk(tool_call_delta=tc1),
                StreamChunk(content_delta="more thinking..."),
                StreamChunk(tool_call_delta=tc2),
                StreamChunk(finish_reason="tool_calls"),
            ),
            keepalive_interval=10.0,
            on_checkpoint=checkpoint,
        )
    )

    assert [tc.id for tc in seen] == ["tc_1", "tc_2"]
    assert seen[0] is tc1 and seen[1] is tc2


# ---------- 8. error path: provider raises mid-stream ----------


@pytest.mark.asyncio
async def test_sse_stream_propagates_provider_exception_as_error_event() -> None:
    out: list[str] = []
    boom = RuntimeError("provider exploded")

    with pytest.raises(RuntimeError, match="provider exploded"):
        async for line in sse_stream(
            raising_stream(StreamChunk(content_delta="ok"), exc=boom),
            keepalive_interval=10.0,
        ):
            out.append(line)

    # The chunk made it out before the error fired.
    assert out[0].startswith("event: chunk")
    # The last emitted line is the error event so the client sees the failure.
    assert out[-1].startswith("event: error")
    payload = json.loads(_data_line(out[-1]).removeprefix("data: "))
    assert "provider exploded" in payload["message"]


# ---------- 9. resumable: skip until last_event_id ----------


@pytest.mark.asyncio
async def test_resumable_sse_stream_skips_until_last_event_id() -> None:
    tc_a = ToolCall(id="tc_a", name="flowfile.graph.add_filter", arguments={})
    tc_b = ToolCall(id="tc_b", name="flowfile.graph.add_select", arguments={})

    out = await collect(
        resumable_sse_stream(
            fake_stream(
                StreamChunk(content_delta="planning"),
                StreamChunk(tool_call_delta=tc_a),
                StreamChunk(content_delta="midway"),
                StreamChunk(tool_call_delta=tc_b),
                StreamChunk(finish_reason="stop"),
            ),
            last_event_id="tc_a",
            keepalive_interval=10.0,
        )
    )

    # tc_a (the cursor) is dropped; everything before it is also dropped;
    # everything after IS forwarded.
    joined = "".join(out)
    assert "tc_a" not in joined, "The cursor's own block must not be re-emitted (the client already has it)"
    assert "planning" not in joined, "Pre-cursor content is dropped — the client already received it"
    assert "midway" in joined, "Post-cursor content must pass through"
    assert "tc_b" in joined, "Post-cursor tool calls must pass through"
    assert any(line.startswith("event: done") for line in out)


# ---------- 10. resumable with no cursor degrades to passthrough ----------


@pytest.mark.asyncio
async def test_resumable_sse_stream_no_last_event_id_emits_everything() -> None:
    chunks = [
        StreamChunk(content_delta="a"),
        StreamChunk(tool_call_delta=ToolCall(id="tc_1", name="x", arguments={})),
        StreamChunk(content_delta="b"),
        StreamChunk(finish_reason="stop"),
    ]

    plain = await collect(sse_stream(fake_stream(*chunks), keepalive_interval=10.0))
    via_resumable = await collect(
        resumable_sse_stream(
            fake_stream(*chunks),
            last_event_id=None,
            keepalive_interval=10.0,
        )
    )

    assert plain == via_resumable, "resumable_sse_stream(last_event_id=None) must be identical to sse_stream"


# ---------- 11. unmatched cursor emits an error event ----------


@pytest.mark.asyncio
async def test_resumable_sse_stream_unmatched_cursor_emits_error() -> None:
    out = await collect(
        resumable_sse_stream(
            fake_stream(
                StreamChunk(content_delta="something"),
                StreamChunk(finish_reason="stop"),
            ),
            last_event_id="tc_never_seen",
            keepalive_interval=10.0,
        )
    )

    assert len(out) == 1, f"Expected single error event when cursor mismatches; got {out!r}"
    assert out[0].startswith("event: error")
    payload = json.loads(_data_line(out[0]).removeprefix("data: "))
    assert "tc_never_seen" in payload["message"]


# ---------- 12. headers on make_streaming_response ----------


def test_make_streaming_response_headers() -> None:
    async def gen() -> AsyncIterator[str]:
        yield "data: x\n\n"

    response = make_streaming_response(gen())

    assert response.media_type == "text/event-stream"
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["connection"] == "keep-alive"
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["x-accel-buffering"] == "no"


# ---------- 13. lazy litellm import ----------


def test_lazy_litellm_import() -> None:
    """Importing ``flowfile_core.ai.streaming`` must not pull in litellm.

    Mirrors W11/W15's lazy-import contract. Restore is unconditional (per
    the W15 review note) so cross-test class identities stay consistent.
    """
    cleared: dict[str, Any] = {}
    for mod_name in list(sys.modules):
        if mod_name == "litellm" or mod_name.startswith("litellm."):
            cleared[mod_name] = sys.modules.pop(mod_name)
        elif mod_name == "flowfile_core.ai.streaming":
            cleared[mod_name] = sys.modules.pop(mod_name)
    try:
        import flowfile_core.ai.streaming  # noqa: F401

        assert "litellm" not in sys.modules, "Importing flowfile_core.ai.streaming must not eagerly import litellm"
    finally:
        for mod_name, mod in cleared.items():
            sys.modules[mod_name] = mod


# ---------- 14. end-to-end smoke via TestClient ----------


def test_streaming_response_via_test_client() -> None:
    """Full path: SSE endpoint mounted on a fresh FastAPI app + TestClient.

    Proves the wire format actually survives Starlette/Uvicorn encoding —
    no mocking. Mounted on a throwaway ``FastAPI()`` so the test doesn't
    depend on the real app's router setup or auth.
    """
    app = FastAPI()

    @app.get("/_test_stream")
    async def _stream():  # pragma: no cover - exercised via TestClient
        return make_streaming_response(
            sse_stream(
                fake_stream(
                    StreamChunk(content_delta="hi"),
                    StreamChunk(finish_reason="stop"),
                ),
                keepalive_interval=10.0,
            )
        )

    with TestClient(app) as client:
        response = client.get("/_test_stream")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    body = response.text
    assert "event: chunk" in body
    assert '"content_delta": "hi"' in body
    assert "event: done" in body


# ---------- 15. bonus: SSEEvent.format covers comment + named forms ----------


def test_sse_event_format_comment_and_named_forms() -> None:
    """Pin the ``SSEEvent.format()`` contract used by every helper above."""

    assert SSEEvent(event=None, data="ping").format() == ": ping\n\n"
    assert SSEEvent(event="chunk", data='{"k":1}').format() == 'event: chunk\ndata: {"k":1}\n\n'
    assert (
        SSEEvent(event="tool_call", data='{"id":"x"}', id="x").format()
        == 'id: x\nevent: tool_call\ndata: {"id":"x"}\n\n'
    )


# ---------- 16. constant exposed for downstream tuning ----------


def test_keepalive_interval_constant() -> None:
    """W22 / W42 lean on this constant; pinning the value protects them."""
    assert KEEPALIVE_INTERVAL_SECONDS == 15.0


# ---------- 17. format_sse_done / format_sse_error wire format ----------


def test_format_sse_done_and_error() -> None:
    assert format_sse_done("stop") == 'event: done\ndata: {"finish_reason": "stop"}\n\n'
    assert format_sse_error("nope") == 'event: error\ndata: {"message": "nope"}\n\n'

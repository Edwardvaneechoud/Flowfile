"""The per-flow change feed: bus events reach asyncio subscribers with the request's origin, until the flow closes."""

import asyncio
from contextlib import aclosing

import pytest

from flowfile_core import change_feed, events
from tests.flowfile.history_graphs import make_graph, promise


@pytest.fixture(autouse=True)
def clean_feed():
    change_feed.install()
    change_feed._reset_for_tests()
    yield
    change_feed._reset_for_tests()


async def _subscribed(flow_id: int) -> None:
    for _ in range(200):
        if change_feed.subscriber_count(flow_id):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("the stream never subscribed")


async def _collect(flow_id: int, stop=None, keepalive: float | None = None) -> list:
    received = []
    async with aclosing(change_feed.stream(flow_id, keepalive=keepalive)) as feed:
        async for event in feed:
            received.append(event)
            if stop is not None and stop(received):
                break
    return received


@pytest.mark.asyncio
async def test_changes_made_on_another_thread_reach_the_subscriber_with_their_origin():
    graph = make_graph(9401)
    task = asyncio.create_task(_collect(graph.flow_id, stop=lambda r: len(r) == 2))
    await _subscribed(graph.flow_id)

    def mutate_as_tab_a():
        token = change_feed.CLIENT_ORIGIN.set("tab-a")
        try:
            promise(graph, "manual_input", 1)
            graph.mark_as_saved()
        finally:
            change_feed.CLIENT_ORIGIN.reset(token)

    await asyncio.to_thread(mutate_as_tab_a)
    received = await asyncio.wait_for(task, 5)
    assert [(e.kind, e.origin, e.revision) for e in received] == [
        ("graph", "tab-a", graph.revision - 1),
        ("saved", "tab-a", graph.revision),
    ]
    assert received[0].payload() == {
        "kind": "graph",
        "flow_id": graph.flow_id,
        "revision": graph.revision - 1,
        "origin": "tab-a",
    }
    assert change_feed.subscriber_count(graph.flow_id) == 0


@pytest.mark.asyncio
async def test_a_change_made_after_subscribing_and_before_reading_is_not_lost():
    graph = make_graph(9409)
    with change_feed.subscribe(graph.flow_id) as subscriber:
        await asyncio.to_thread(promise, graph, "manual_input", 1)
        async with aclosing(subscriber.events()) as feed:
            received = await asyncio.wait_for(feed.__anext__(), 5)
    assert (received.kind, received.revision) == ("graph", graph.revision)
    assert change_feed.subscriber_count(graph.flow_id) == 0


@pytest.mark.asyncio
async def test_a_change_without_a_client_header_has_no_origin():
    graph = make_graph(9402)
    task = asyncio.create_task(_collect(graph.flow_id, stop=lambda r: True))
    await _subscribed(graph.flow_id)
    await asyncio.to_thread(promise, graph, "manual_input", 1)
    assert (await asyncio.wait_for(task, 5))[0].origin is None


@pytest.mark.asyncio
async def test_an_idle_stream_yields_keepalives():
    graph = make_graph(9403)
    received = await asyncio.wait_for(_collect(graph.flow_id, stop=lambda r: True, keepalive=0.01), 5)
    assert received == [None]


@pytest.mark.asyncio
async def test_closing_the_flow_ends_the_stream():
    graph = make_graph(9404)
    task = asyncio.create_task(_collect(graph.flow_id))
    await _subscribed(graph.flow_id)
    events.publish("flow_closed", flow_id=graph.flow_id)
    received = await asyncio.wait_for(task, 5)
    assert [(e.kind, e.revision) for e in received] == [("closed", None)]
    assert change_feed.subscriber_count(graph.flow_id) == 0


@pytest.mark.asyncio
async def test_close_all_ends_every_stream_from_any_thread():
    a, b = make_graph(9405), make_graph(9406)
    tasks = [asyncio.create_task(_collect(a.flow_id)), asyncio.create_task(_collect(b.flow_id))]
    await _subscribed(a.flow_id)
    await _subscribed(b.flow_id)
    await asyncio.to_thread(change_feed.close_all)
    assert await asyncio.wait_for(asyncio.gather(*tasks), 5) == [[], []]
    assert change_feed.subscriber_count(a.flow_id) == change_feed.subscriber_count(b.flow_id) == 0


@pytest.mark.asyncio
async def test_a_cancelled_stream_unsubscribes():
    graph = make_graph(9407)
    task = asyncio.create_task(_collect(graph.flow_id))
    await _subscribed(graph.flow_id)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert change_feed.subscriber_count(graph.flow_id) == 0


def test_the_middleware_reads_the_client_header_for_the_request_only():
    seen = []

    async def app(scope, receive, send):
        seen.append(change_feed.CLIENT_ORIGIN.get())

    middleware = change_feed.ClientOriginMiddleware(app)

    async def run():
        await middleware({"type": "http", "headers": [(b"x-flowfile-client", b"tab-a")]}, None, None)
        await middleware({"type": "http", "headers": []}, None, None)
        await middleware({"type": "http", "headers": [(b"x-flowfile-client", b"")]}, None, None)
        await middleware({"type": "http", "headers": [(b"x-flowfile-client", b"x" * 500)]}, None, None)
        await middleware({"type": "lifespan"}, None, None)
        return change_feed.CLIENT_ORIGIN.get()

    assert asyncio.run(run()) is None
    assert seen == ["tab-a", None, None, "x" * 128, None]


@pytest.mark.asyncio
async def test_rekeying_the_flow_ends_the_stream_with_the_new_id():
    graph = make_graph(9407)
    task = asyncio.create_task(_collect(graph.flow_id))
    await _subscribed(graph.flow_id)
    events.publish("flow_rekeyed", old_flow_id=graph.flow_id, new_flow_id=9408)
    received = await asyncio.wait_for(task, 5)
    assert [(e.kind, e.revision, e.new_flow_id) for e in received] == [("rekeyed", None, 9408)]
    assert received[0].payload() == {
        "kind": "rekeyed",
        "flow_id": graph.flow_id,
        "revision": None,
        "origin": None,
        "new_flow_id": 9408,
    }
    assert change_feed.subscriber_count(graph.flow_id) == 0

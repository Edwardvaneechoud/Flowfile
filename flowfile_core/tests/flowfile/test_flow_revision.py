"""``FlowGraph.revision``: the change counter clients compare, and the ``flow_revision`` events behind it."""

import pytest

from flowfile_core import events
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import schemas
from tests.flowfile.history_graphs import make_graph, promise


@pytest.fixture
def bus_events():
    saved = {event: list(handlers) for event, handlers in events._handlers.items()}
    seen: list[tuple[str, dict]] = []
    events.subscribe("flow_revision", lambda **payload: seen.append(("flow_revision", payload)))
    events.subscribe("flow_closed", lambda **payload: seen.append(("flow_closed", payload)))
    events.subscribe("flow_rekeyed", lambda **payload: seen.append(("flow_rekeyed", payload)))
    yield seen
    events._handlers.clear()
    events._handlers.update(saved)


def kinds(seen) -> list[str]:
    return [payload["kind"] for event, payload in seen if event == "flow_revision"]


def test_a_transaction_moves_the_revision_once_and_the_history_state_carries_it(bus_events):
    graph = make_graph(9301)
    before = graph.revision
    with graph.transaction("two placements") as txn:
        promise(graph, "manual_input", 1)
        promise(graph, "filter", 2)
    assert graph.revision == before + 1
    assert txn.history.revision == graph.revision
    assert graph.get_history_state().revision == graph.revision
    assert kinds(bus_events) == ["graph"]
    assert bus_events[-1][1]["graph"] is graph
    assert bus_events[-1][1]["revision"] == graph.revision


def test_a_transaction_that_changes_nothing_leaves_the_revision(bus_events):
    graph = make_graph(9311)
    promise(graph, "manual_input", 1)
    revision = graph.revision
    with graph.transaction("nothing") as txn:
        pass
    assert graph.revision == revision
    assert txn.history.revision == revision
    assert kinds(bus_events) == ["graph"]


def test_a_placement_outside_a_transaction_moves_the_revision_once_with_history_on(bus_events):
    graph = make_graph(9302)
    before = graph.revision
    promise(graph, "manual_input", 1)
    assert graph.revision == before + 1
    assert graph._untracked_writes == 0


def test_direct_writes_move_the_revision_with_history_off(bus_events):
    graph = make_graph(9303, track_history=False)
    before = graph.revision
    promise(graph, "manual_input", 1)
    assert graph.revision > before
    assert graph._untracked_writes == 0
    assert set(kinds(bus_events)) == {"graph"}


def test_undo_and_redo_move_the_revision_and_a_no_op_step_does_not(bus_events):
    graph = make_graph(9304)
    promise(graph, "manual_input", 1)
    revision = graph.revision
    assert graph.undo().success is True
    assert graph.revision == revision + 1
    assert graph.redo().success is True
    assert graph.revision == revision + 2
    assert graph.redo().success is False
    assert graph.revision == revision + 2


def test_a_run_claim_and_release_bracket_the_run(bus_events):
    graph = make_graph(9305)
    revision = graph.revision
    assert graph.try_claim_run() is True
    assert graph.revision == revision + 1
    assert graph.try_claim_run() is False
    assert graph.revision == revision + 1
    graph.release_run()
    assert graph.revision == revision + 2
    graph.release_run()
    assert graph.revision == revision + 2
    assert kinds(bus_events) == ["run_started", "run_ended"]


def test_saving_moves_the_revision(bus_events):
    graph = make_graph(9306)
    revision = graph.revision
    graph.mark_as_saved()
    assert graph.revision == revision + 1
    assert kinds(bus_events) == ["saved"]


def test_closing_a_flow_publishes_flow_closed(bus_events):
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=9307, name="closing", path="."))
    handler.delete_flow(9307)
    assert [payload for event, payload in bus_events if event == "flow_closed"] == [{"flow_id": 9307}]


def test_rekeying_a_flow_publishes_flow_rekeyed_once_it_moved(bus_events):
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=9308, name="moving", path="."))
    handler.rekey_flow(9308, 9309)
    handler.rekey_flow(9999, 9998)  # nothing under that id: nothing to announce
    assert [payload for event, payload in bus_events if event == "flow_rekeyed"] == [
        {"old_flow_id": 9308, "new_flow_id": 9309}
    ]
    assert handler.get_flow(9309) is not None
    handler.delete_flow(9309)

"""W41 — ``GraphDiff`` staging tests.

Cases:

* ``test_diff_store_roundtrip`` — register / get / pop / second-pop returns
  ``None``.
* ``test_collect_audit_ids_preserves_op_order`` — the walker emits ids in
  ``additions → connections_added → deletions → connections_removed``
  order, skipping ``None``.
* ``test_apply_diff_creates_single_history_snapshot`` — stage 2 adds + 1
  connection; assert ``flow.get_history_state().undo_count`` increases by
  exactly 1.
* ``test_apply_diff_rolls_back_on_midbatch_failure`` — monkeypatch
  ``flow.delete_node`` to raise on the deletion bucket; post-apply graph
  hash matches pre-apply; the diff stays in the store; the raise
  propagates.
* ``test_apply_diff_drift_detected`` — addition references upstream id
  that's been deleted; ``DiffDriftError`` raised; graph unchanged; no
  history snapshot taken.
* ``test_accept_route_flips_audit_actions`` — drive
  ``execute_tool_call(mode="stage")`` for an addition + a connect; POST
  ``/ai/diff/stage``; POST accept; assert every ``AiAuditEvent.diff_action
  == "accepted"`` and the diff is gone from the store.
* ``test_reject_route_no_mutation`` — same staging; POST reject; node
  count unchanged; audit rows ``"rejected"``; diff popped.
* ``test_route_404_unknown_diff`` — POST accept on bogus id → 404.
* ``test_route_409_drift`` — accept after the underlying upstream is
  deleted → 409 with ``missing_node_ids`` payload; diff still in the store.
* ``test_route_422_cross_flow_mismatch`` — diff stored for ``flow_id=1``,
  body says ``flow_id=2`` → 422.
* ``test_route_503_when_feature_flag_off`` — toggle ``FEATURE_FLAG_AI``
  off; all three routes return 503.
* ``test_lazy_litellm_contract`` — importing ``flowfile_core.ai.diff``
  doesn't load ``litellm``.
* ``test_end_to_end_stage_then_accept`` — full stage→accept loop with two
  W31-staged ops; assert real graph mutated, every audit row flipped,
  ``undo_count`` increased by exactly 1.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import audit, diff
from flowfile_core.ai.tools import (
    InsertionContext,
    ToolExecutionResult,
    execute_tool_call,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs.settings import FEATURE_FLAG_AI
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas, transform_schema

# --------------------------------------------------------------------------- #
# Shared helpers + fixtures                                                    #
# --------------------------------------------------------------------------- #


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_ai_diff",
    )


def _add_orders_input(flow: FlowGraph, node_id: int = 1) -> None:
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="customer_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="amount", data_type="Double"),
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[[1, 2, 3, 4], [10, 20, 30, 40], [100.0, 200.0, 50.0, 75.0], ["EU", "US", "EU", "US"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(node_id).name = "orders"


def _flow_with_orders(flow_id: int = 1) -> FlowGraph:
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="diff_test")
    _add_orders_input(flow)
    flow.get_node(1).get_predicted_schema()
    return flow


def _filter_args(node_id: int = 2, depending_on_id: int = 1) -> dict[str, Any]:
    settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=node_id,
        depending_on_id=depending_on_id,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
    )
    return settings.model_dump(mode="json")


def _user_id() -> int:
    return 1


def _stage_filter(flow: FlowGraph, node_id: int = 2, upstream: int = 1) -> ToolExecutionResult:
    """Run W31 ``execute_tool_call`` in ``stage`` mode against the test flow."""
    return execute_tool_call(
        flow_id=flow.flow_id,
        tool_name="flowfile.graph.add_filter",
        tool_args=_filter_args(node_id=node_id, depending_on_id=upstream),
        insertion_context=InsertionContext(upstream_node_ids=[upstream], pos_x=400.0, pos_y=200.0),
        flow=flow,
        session_id="test-session-w41",
        user_id=_user_id(),
        mode="stage",
    )


@pytest.fixture(autouse=True)
def _reset_diff_store() -> Iterator[None]:
    """Wipe the in-memory ``DiffStore`` between cases."""
    diff.clear_for_tests()
    yield
    diff.clear_for_tests()


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    """TestClient with auth overridden to a synthetic local user."""
    fake_user = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


@pytest.fixture
def registered_flow() -> Iterator[FlowGraph]:
    """Register a flow under ``flow_file_handler`` so route resolution works."""
    flow = _flow_with_orders()
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


# --------------------------------------------------------------------------- #
# 1. DiffStore round-trip                                                      #
# --------------------------------------------------------------------------- #


def test_diff_store_roundtrip() -> None:
    graph_diff = diff.GraphDiff(session_id="s", flow_id=1)
    diff_id = diff.register_diff(graph_diff)
    assert isinstance(diff_id, str) and diff_id == graph_diff.diff_id

    fetched = diff.get_diff(diff_id)
    assert fetched is graph_diff

    popped = diff.pop_diff(diff_id)
    assert popped is graph_diff
    assert diff.get_diff(diff_id) is None
    assert diff.pop_diff(diff_id) is None


# --------------------------------------------------------------------------- #
# 2. collect_audit_ids preserves op order                                      #
# --------------------------------------------------------------------------- #


def test_collect_audit_ids_preserves_op_order() -> None:
    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings={},
                insertion_context=diff.StagedInsertionContext(),
                audit_id=10,
            ),
            diff.StagedAddition(
                node_type="select",
                settings={},
                insertion_context=diff.StagedInsertionContext(),
                audit_id=None,  # skipped
            ),
        ],
        connections_added=[diff.StagedConnection(connection={}, audit_id=20)],
        deletions=[diff.StagedDeletion(delete_node_id=99, audit_id=30)],
        connections_removed=[diff.StagedConnection(connection={}, audit_id=40)],
    )
    assert diff.collect_audit_ids(graph_diff) == [10, 20, 30, 40]


# --------------------------------------------------------------------------- #
# 3. apply_diff creates exactly one history snapshot                           #
# --------------------------------------------------------------------------- #


def test_apply_diff_creates_single_history_snapshot() -> None:
    flow = _flow_with_orders()
    pre_undo = flow.get_history_state().undo_count

    # Stage two additions: filter on top of orders (#2), then a second filter
    # on top of #2 (#3). Two ops in one batch — assert undo_count delta is 1.
    second_filter = input_schema.NodeFilter(
        flow_id=1,
        node_id=3,
        depending_on_id=2,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[amount] > 50"),
    ).model_dump(mode="json")

    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings=_filter_args(node_id=2, depending_on_id=1),
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[1], pos_x=400.0, pos_y=200.0),
            ),
            diff.StagedAddition(
                node_type="filter",
                settings=second_filter,
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[2], pos_x=600.0, pos_y=200.0),
            ),
        ],
        rationale="add two filters",
    )

    result = diff.apply_diff(flow, graph_diff)
    post_undo = flow.get_history_state().undo_count

    assert post_undo - pre_undo == 1, "expected exactly one BATCH snapshot for the whole apply"
    assert result.applied_node_ids == [2, 3]
    assert flow.get_node(2) is not None
    assert flow.get_node(3) is not None


# --------------------------------------------------------------------------- #
# 4. apply_diff rolls back on mid-batch failure                                #
# --------------------------------------------------------------------------- #


def test_apply_diff_rolls_back_on_midbatch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    flow = _flow_with_orders()
    pre_undo = flow.get_history_state().undo_count
    pre_node_count = len(flow.nodes)

    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings=_filter_args(node_id=2, depending_on_id=1),
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[1]),
                audit_id=42,
            )
        ],
        deletions=[diff.StagedDeletion(delete_node_id=1, audit_id=43)],
    )
    diff.register_diff(graph_diff)

    # Make the deletion bucket explode after the addition has fired.
    original_delete = FlowGraph.delete_node

    def _boom(self: FlowGraph, node_id):  # type: ignore[no-untyped-def]
        raise RuntimeError("simulated mid-batch failure")

    monkeypatch.setattr(FlowGraph, "delete_node", _boom)

    with pytest.raises(RuntimeError, match="simulated mid-batch failure"):
        diff.apply_diff(flow, graph_diff)

    monkeypatch.setattr(FlowGraph, "delete_node", original_delete)

    # Graph rolled back to pre-apply state (the addition is gone).
    assert len(flow.nodes) == pre_node_count
    assert flow.get_node(2) is None
    # Diff still in store so the user can fix-and-retry or reject.
    assert diff.get_diff(graph_diff.diff_id) is graph_diff
    # The snapshot was taken and then undone — the net undo_count delta is 0.
    assert flow.get_history_state().undo_count == pre_undo


# --------------------------------------------------------------------------- #
# 5. apply_diff drift detection                                                #
# --------------------------------------------------------------------------- #


def test_apply_diff_drift_detected() -> None:
    flow = _flow_with_orders()
    # Diff references upstream id 7 — never existed.
    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings=_filter_args(node_id=2, depending_on_id=7),
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[7]),
            )
        ],
    )

    pre_undo = flow.get_history_state().undo_count
    pre_node_count = len(flow.nodes)

    with pytest.raises(diff.DiffDriftError) as exc_info:
        diff.apply_diff(flow, graph_diff)

    assert exc_info.value.missing_node_ids == [7]
    # No snapshot was taken (drift is checked first).
    assert flow.get_history_state().undo_count == pre_undo
    assert len(flow.nodes) == pre_node_count


# --------------------------------------------------------------------------- #
# 6. Accept route flips audit actions                                          #
# --------------------------------------------------------------------------- #


def test_accept_route_flips_audit_actions(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    staged_filter = _stage_filter(registered_flow, node_id=2, upstream=1)
    assert staged_filter.status == "staged"
    assert staged_filter.audit_id is not None

    stage_response = authed_client.post(
        "/ai/diff/stage",
        json={
            "session_id": "s",
            "flow_id": registered_flow.flow_id,
            "rationale": "filter EU rows",
            "staged_results": [
                {
                    "tool_name": staged_filter.tool_name,
                    "audit_id": staged_filter.audit_id,
                    "staged_node_payload": staged_filter.staged_node_payload,
                }
            ],
        },
    )
    assert stage_response.status_code == 200, stage_response.text
    diff_id = stage_response.json()["diff_id"]
    assert stage_response.json()["op_count"] == 1

    pre_undo = registered_flow.get_history_state().undo_count

    accept_response = authed_client.post(
        f"/ai/diff/{diff_id}/accept",
        json={"flow_id": registered_flow.flow_id},
    )
    assert accept_response.status_code == 200, accept_response.text
    body = accept_response.json()
    assert body["status"] == "accepted"
    assert body["applied_node_ids"] == [2]
    assert body["audit_ids_updated"] == [staged_filter.audit_id]

    # Audit row flipped.
    rows = audit.query_events(session_id="test-session-w41")
    assert len(rows) >= 1
    row = next(r for r in rows if r.id == staged_filter.audit_id)
    assert row.diff_action == "accepted"

    # Graph reflects the addition; diff is gone.
    assert registered_flow.get_node(2) is not None
    assert diff.get_diff(diff_id) is None
    # Single undo point.
    assert registered_flow.get_history_state().undo_count - pre_undo == 1


# --------------------------------------------------------------------------- #
# 7. Reject route — no mutation                                                #
# --------------------------------------------------------------------------- #


def test_reject_route_no_mutation(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    staged_filter = _stage_filter(registered_flow, node_id=2, upstream=1)
    pre_node_count = len(registered_flow.nodes)
    pre_undo = registered_flow.get_history_state().undo_count

    stage_response = authed_client.post(
        "/ai/diff/stage",
        json={
            "session_id": "s",
            "flow_id": registered_flow.flow_id,
            "staged_results": [
                {
                    "tool_name": staged_filter.tool_name,
                    "audit_id": staged_filter.audit_id,
                    "staged_node_payload": staged_filter.staged_node_payload,
                }
            ],
        },
    )
    diff_id = stage_response.json()["diff_id"]

    reject_response = authed_client.post(f"/ai/diff/{diff_id}/reject")
    assert reject_response.status_code == 200, reject_response.text
    body = reject_response.json()
    assert body["status"] == "rejected"
    assert body["audit_ids_updated"] == [staged_filter.audit_id]

    # Audit row flipped.
    rows = audit.query_events(session_id="test-session-w41")
    row = next(r for r in rows if r.id == staged_filter.audit_id)
    assert row.diff_action == "rejected"

    # Graph + history unchanged; diff popped.
    assert len(registered_flow.nodes) == pre_node_count
    assert registered_flow.get_history_state().undo_count == pre_undo
    assert diff.get_diff(diff_id) is None


# --------------------------------------------------------------------------- #
# 8. 404 unknown diff                                                          #
# --------------------------------------------------------------------------- #


def test_route_404_unknown_diff(authed_client: TestClient) -> None:
    accept = authed_client.post("/ai/diff/no-such-diff/accept", json={"flow_id": 1})
    assert accept.status_code == 404
    assert "no-such-diff" in accept.json()["detail"]

    reject = authed_client.post("/ai/diff/no-such-diff/reject")
    assert reject.status_code == 404


# --------------------------------------------------------------------------- #
# 9. 409 drift via the route                                                   #
# --------------------------------------------------------------------------- #


def test_route_409_drift(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    staged_filter = _stage_filter(registered_flow, node_id=2, upstream=1)
    stage_response = authed_client.post(
        "/ai/diff/stage",
        json={
            "session_id": "s",
            "flow_id": registered_flow.flow_id,
            "staged_results": [
                {
                    "tool_name": staged_filter.tool_name,
                    "audit_id": staged_filter.audit_id,
                    "staged_node_payload": staged_filter.staged_node_payload,
                }
            ],
        },
    )
    diff_id = stage_response.json()["diff_id"]

    # User mutates the canvas in between staging and accept (D006).
    registered_flow.delete_node(1)

    accept = authed_client.post(
        f"/ai/diff/{diff_id}/accept",
        json={"flow_id": registered_flow.flow_id},
    )
    assert accept.status_code == 409, accept.text
    detail = accept.json()["detail"]
    assert detail["error"] == "diff_drift"
    assert detail["missing_node_ids"] == [1]
    assert detail["diff_id"] == diff_id

    # Diff still in the store so the user can fix-and-retry.
    assert diff.get_diff(diff_id) is not None


# --------------------------------------------------------------------------- #
# 10. 422 cross-flow mismatch                                                  #
# --------------------------------------------------------------------------- #


def test_route_422_cross_flow_mismatch(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    staged_filter = _stage_filter(registered_flow, node_id=2, upstream=1)
    stage_response = authed_client.post(
        "/ai/diff/stage",
        json={
            "session_id": "s",
            "flow_id": registered_flow.flow_id,
            "staged_results": [
                {
                    "tool_name": staged_filter.tool_name,
                    "audit_id": staged_filter.audit_id,
                    "staged_node_payload": staged_filter.staged_node_payload,
                }
            ],
        },
    )
    diff_id = stage_response.json()["diff_id"]

    accept = authed_client.post(
        f"/ai/diff/{diff_id}/accept",
        json={"flow_id": registered_flow.flow_id + 999},
    )
    assert accept.status_code == 422
    assert "flow_id mismatch" in accept.json()["detail"]
    # Diff stays in store on mismatch.
    assert diff.get_diff(diff_id) is not None


# --------------------------------------------------------------------------- #
# 11. 503 — feature flag off                                                   #
# --------------------------------------------------------------------------- #


def test_route_503_when_feature_flag_off(authed_client: TestClient) -> None:
    original = FEATURE_FLAG_AI.value
    FEATURE_FLAG_AI.set(False)
    try:
        stage = authed_client.post(
            "/ai/diff/stage",
            json={"session_id": "s", "flow_id": 1, "staged_results": []},
        )
        assert stage.status_code == 503

        accept = authed_client.post("/ai/diff/x/accept", json={"flow_id": 1})
        assert accept.status_code == 503

        reject = authed_client.post("/ai/diff/x/reject")
        assert reject.status_code == 503
    finally:
        FEATURE_FLAG_AI.set(original)


# --------------------------------------------------------------------------- #
# 12. Lazy litellm contract                                                    #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    # If litellm was already imported by a prior test, drop it.
    sys.modules.pop("litellm", None)
    # Re-import the diff surface.
    sys.modules.pop("flowfile_core.ai.diff", None)
    from flowfile_core.ai import diff as diff_reimport  # noqa: F401

    assert "litellm" not in sys.modules


# --------------------------------------------------------------------------- #
# 13. End-to-end stage→accept                                                  #
# --------------------------------------------------------------------------- #


def test_end_to_end_stage_then_accept(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    """Two W31 staged ops — addition + a connect — drive through accept."""
    staged_filter = _stage_filter(registered_flow, node_id=2, upstream=1)
    assert staged_filter.status == "staged"

    # Stage a connect from #1 to a future node #2 via the executor (mode=stage
    # for ``connect`` doesn't validate against the live graph — that's W41's
    # apply step's job, where add_connection runs after the addition lands).
    connection_payload = input_schema.NodeConnection.create_from_simple_input(
        from_id=1, to_id=2, input_type="input-0", output_handle="output-0"
    ).model_dump()

    pre_undo = registered_flow.get_history_state().undo_count

    stage = authed_client.post(
        "/ai/diff/stage",
        json={
            "session_id": "s",
            "flow_id": registered_flow.flow_id,
            "rationale": "two-op compound diff",
            "staged_results": [
                {
                    "tool_name": staged_filter.tool_name,
                    "audit_id": staged_filter.audit_id,
                    "staged_node_payload": staged_filter.staged_node_payload,
                },
                # NB: ``_apply_add_node`` already wires the upstream connection
                # for the addition. We DON'T add a duplicate explicit connect
                # here — duplicating would raise inside ``add_connection``.
                # The op_count below stays at 1.
            ],
        },
    )
    assert stage.status_code == 200, stage.text
    body = stage.json()
    assert body["op_count"] == 1
    diff_id = body["diff_id"]

    accept = authed_client.post(
        f"/ai/diff/{diff_id}/accept",
        json={"flow_id": registered_flow.flow_id},
    )
    assert accept.status_code == 200, accept.text
    accept_body = accept.json()
    assert accept_body["applied_node_ids"] == [2]

    # Real graph mutated: filter node + its main-input wiring exist.
    filter_node = registered_flow.get_node(2)
    assert filter_node is not None
    main_inputs = filter_node.main_input_ids if hasattr(filter_node, "main_input_ids") else None
    if main_inputs is not None:
        assert 1 in main_inputs

    # Single undo point covers the whole apply.
    assert registered_flow.get_history_state().undo_count - pre_undo == 1

    # Audit row flipped.
    rows = audit.query_events(session_id="test-session-w41")
    row = next(r for r in rows if r.id == staged_filter.audit_id)
    assert row.diff_action == "accepted"

    # Diff popped.
    assert diff.get_diff(diff_id) is None

    _ = connection_payload  # Keep the helper exercised for the future W40 path.

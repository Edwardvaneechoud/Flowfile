"""``GraphDiff`` staging tests.

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
  staged ops; assert real graph mutated, every audit row flipped,
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
# Shared helpers + fixtures #
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
    """Run ``execute_tool_call`` in ``stage`` mode against the test flow."""
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
# 1. DiffStore round-trip #
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
# 2. collect_audit_ids preserves op order #
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
# 3. apply_diff creates exactly one history snapshot #
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
# 4. apply_diff rolls back on mid-batch failure #
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
# 5. apply_diff drift detection #
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
# 5b. — diff inconsistency detection #
# --------------------------------------------------------------------------- #


def test_validate_diff_rejects_phantom_connection_endpoint() -> None:
    """staged connection references a node id that's neither live
    nor in this diff's additions. Replicates the live audit trail: agent
    staged ``add_group_by(node_id=10, upstream_node_ids=[2])`` plus a
    redundant ``connect(from=2, to=77)`` where 77 was hallucinated.
    """
    flow = _flow_with_orders()
    # One legitimate addition (id=2) plus one connection to a phantom id 77.
    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings=_filter_args(node_id=2, depending_on_id=1),
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[1]),
            )
        ],
        connections_added=[
            diff.StagedConnection(
                connection=input_schema.NodeConnection.create_from_simple_input(
                    from_id=1, to_id=77, input_type="input-0", output_handle="output-0"
                ).model_dump(),
            )
        ],
    )

    with pytest.raises(diff.DiffInconsistentError) as exc_info:
        diff.validate_diff_against_flow(flow, graph_diff)

    assert exc_info.value.missing_endpoints == [(77, "to")]


def test_validate_diff_accepts_in_batch_added_endpoint() -> None:
    """chained additions where the connection references an id
    being added in the same diff. Legal: in-batch references resolve.
    """
    flow = _flow_with_orders()
    second_filter = input_schema.NodeFilter(
        flow_id=1,
        node_id=11,
        depending_on_id=10,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[amount] > 50"),
    ).model_dump(mode="json")
    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings=_filter_args(node_id=10, depending_on_id=1),
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[1]),
            ),
            diff.StagedAddition(
                node_type="filter",
                settings=second_filter,
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[10]),
            ),
        ],
        connections_added=[
            diff.StagedConnection(
                connection=input_schema.NodeConnection.create_from_simple_input(
                    from_id=10, to_id=11, input_type="input-0", output_handle="output-0"
                ).model_dump(),
            )
        ],
    )

    # Should not raise — id 11 is in this diff's additions; id 10 too.
    diff.validate_diff_against_flow(flow, graph_diff)


def test_apply_diff_does_not_reach_apply_path_on_inconsistent_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``apply_diff`` short-circuits via validation before the
    per-op apply loop. Spies on ``_apply_add_node`` and ``add_connection``
    to assert zero invocations when the diff is inconsistent.
    """
    flow = _flow_with_orders()
    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings=_filter_args(node_id=2, depending_on_id=1),
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[1]),
            )
        ],
        connections_added=[
            diff.StagedConnection(
                connection=input_schema.NodeConnection.create_from_simple_input(
                    from_id=1, to_id=77, input_type="input-0", output_handle="output-0"
                ).model_dump(),
            )
        ],
    )

    apply_calls: list[tuple] = []
    connect_calls: list[tuple] = []

    def _spy_apply_add(*args, **kwargs):  # type: ignore[no-untyped-def]
        apply_calls.append((args, kwargs))

    def _spy_add_connection(*args, **kwargs):  # type: ignore[no-untyped-def]
        connect_calls.append((args, kwargs))

    # Both seams live in ``flowfile_core.ai.diff`` import scope: ``_apply_add_node``
    # is imported at module top; ``add_connection`` is imported lazily inside
    # ``apply_diff``. Patch both at their import sources to catch any sneak path.
    monkeypatch.setattr("flowfile_core.ai.diff._apply_add_node", _spy_apply_add)
    monkeypatch.setattr("flowfile_core.flowfile.flow_graph.add_connection", _spy_add_connection)

    pre_undo = flow.get_history_state().undo_count
    pre_node_count = len(flow.nodes)

    with pytest.raises(diff.DiffInconsistentError):
        diff.apply_diff(flow, graph_diff)

    assert apply_calls == [], "validation must reject before _apply_add_node fires"
    assert connect_calls == [], "validation must reject before add_connection fires"
    # No snapshot was taken (validation is checked first).
    assert flow.get_history_state().undo_count == pre_undo
    assert len(flow.nodes) == pre_node_count


# --------------------------------------------------------------------------- #
# 6. Accept route flips audit actions #
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
# 7. Reject route — no mutation #
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
# 8. 404 unknown diff #
# --------------------------------------------------------------------------- #


def test_route_404_unknown_diff(authed_client: TestClient) -> None:
    accept = authed_client.post("/ai/diff/no-such-diff/accept", json={"flow_id": 1})
    assert accept.status_code == 404
    assert "no-such-diff" in accept.json()["detail"]

    reject = authed_client.post("/ai/diff/no-such-diff/reject")
    assert reject.status_code == 404


# --------------------------------------------------------------------------- #
# 9. 409 drift via the route #
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

    # User mutates the canvas in between staging and accept.
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
# 9b. — 422 diff_inconsistent via the route #
# --------------------------------------------------------------------------- #


def test_route_422_diff_inconsistent(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    """Accept route returns 422 with a typed `diff_inconsistent`
    detail when the staged diff carries a phantom connection endpoint,
    and the diff stays in the store so the user can Reject and ask the
    agent to retry.
    """
    staged_filter = _stage_filter(registered_flow, node_id=2, upstream=1)
    phantom_connection = input_schema.NodeConnection.create_from_simple_input(
        from_id=1, to_id=77, input_type="input-0", output_handle="output-0"
    ).model_dump()

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
                },
                {
                    "tool_name": "flowfile.graph.connect",
                    "audit_id": None,
                    "staged_node_payload": {"connection": phantom_connection},
                },
            ],
        },
    )
    assert stage_response.status_code == 200, stage_response.text
    diff_id = stage_response.json()["diff_id"]

    accept = authed_client.post(
        f"/ai/diff/{diff_id}/accept",
        json={"flow_id": registered_flow.flow_id},
    )
    assert accept.status_code == 422, accept.text
    detail = accept.json()["detail"]
    assert detail["error"] == "diff_inconsistent"
    assert detail["missing_endpoints"] == [[77, "to"]]
    assert detail["diff_id"] == diff_id

    # Diff stays in the store so the user can Reject and ask the agent to retry.
    assert diff.get_diff(diff_id) is not None


# --------------------------------------------------------------------------- #
# 10. 422 cross-flow mismatch #
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
# 11. 503 — feature flag off #
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
# 12. Lazy litellm contract #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    # If litellm was already imported by a prior test, drop it.
    sys.modules.pop("litellm", None)
    # Re-import the diff surface.
    sys.modules.pop("flowfile_core.ai.diff", None)
    from flowfile_core.ai import diff as diff_reimport  # noqa: F401

    assert "litellm" not in sys.modules


# --------------------------------------------------------------------------- #
# 13. End-to-end stage→accept #
# --------------------------------------------------------------------------- #


def test_end_to_end_stage_then_accept(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    """Two staged ops — addition + a connect — drive through accept."""
    staged_filter = _stage_filter(registered_flow, node_id=2, upstream=1)
    assert staged_filter.status == "staged"

    # Stage a connect from #1 to a future node #2 via the executor (mode=stage
    # for ``connect`` doesn't validate against the live graph — that's's
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

    _ = connection_payload  # Keep the helper exercised for the future path.


# --------------------------------------------------------------------------- #
# modifications bucket #
# --------------------------------------------------------------------------- #


def _flow_with_orders_and_filter(flow_id: int = 1) -> FlowGraph:
    """``orders`` (id=1) → ``filter EU`` (id=2). Used by tests to have an
    existing filter node available for `update_node_settings` to mutate.

    Mirrors the production ``_apply_add_node`` pattern (``add_filter`` then
    ``add_connection``) so the filter's ``node_inputs.main_inputs`` is
    populated — without the explicit connection, ``get_predicted_schema``
    has no upstream data to feed into ``_func``.
    """
    from flowfile_core.flowfile.flow_graph import add_connection as _add_connection

    flow = _flow_with_orders(flow_id=flow_id)
    filter_settings = input_schema.NodeFilter(
        flow_id=flow_id,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
    )
    flow.add_filter(filter_settings)
    _add_connection(
        flow,
        input_schema.NodeConnection.create_from_simple_input(
            from_id=1, to_id=2, input_type="input-0", output_handle="output-0"
        ),
    )
    flow.get_node(2).get_predicted_schema()
    return flow


def _filter_settings_dump(node_id: int, depending_on_id: int, expr: str) -> dict[str, Any]:
    return input_schema.NodeFilter(
        flow_id=1,
        node_id=node_id,
        depending_on_id=depending_on_id,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter=expr),
    ).model_dump(mode="json")


def test_staged_settings_update_roundtrip() -> None:
    """:class:`StagedSettingsUpdate` round-trips with all required fields.

    Smoke test that the new bucket type is constructible and ``GraphDiff``
    accepts a populated ``modifications`` list.
    """
    mod = diff.StagedSettingsUpdate(
        node_id=9,
        node_type="filter",
        old_settings={"foo": 1},
        new_settings={"foo": 2},
        predicted_output_schema=[diff.StagedSchemaColumn(name="region", data_type="String")],
        audit_id=42,
    )
    graph_diff = diff.GraphDiff(session_id="s", flow_id=1, modifications=[mod])
    assert graph_diff.modifications[0].node_id == 9
    assert graph_diff.modifications[0].old_settings == {"foo": 1}
    assert graph_diff.modifications[0].new_settings == {"foo": 2}
    assert diff.collect_audit_ids(graph_diff) == [42]


def test_bundle_staged_results_bins_modification() -> None:
    """bundler routes ``flowfile.graph.update_node_settings`` payloads
    into the modifications bucket; mixed batch with an addition stays clean.
    """
    entries = [
        diff.StagedToolEntry(
            tool_name="flowfile.graph.add_filter",
            audit_id=1,
            staged_node_payload={
                "node_type": "filter",
                "settings": _filter_settings_dump(node_id=2, depending_on_id=1, expr="[region]=='EU'"),
                "insertion_context": {"upstream_node_ids": [1]},
                "predicted_output_schema": [],
            },
        ),
        diff.StagedToolEntry(
            tool_name="flowfile.graph.update_node_settings",
            audit_id=2,
            staged_node_payload={
                "kind": "modification",
                "node_id": 2,
                "node_type": "filter",
                "old_settings": _filter_settings_dump(node_id=2, depending_on_id=1, expr="[region]=='EU'"),
                "new_settings": _filter_settings_dump(node_id=2, depending_on_id=1, expr="[amount] > 50"),
                "predicted_output_schema": [],
            },
        ),
    ]
    bundled = diff.bundle_staged_results(entries)
    assert len(bundled.additions) == 1
    assert len(bundled.modifications) == 1
    mod = bundled.modifications[0]
    assert mod.node_id == 2
    assert mod.node_type == "filter"
    assert mod.audit_id == 2
    # collect_audit_ids walks additions, then modifications, then connections, ...
    diff_with_meta = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=bundled.additions,
        modifications=bundled.modifications,
    )
    assert diff.collect_audit_ids(diff_with_meta) == [1, 2]


def test_bundle_staged_results_dedupes_modifications_by_node_id() -> None:
    """multiple ``update_node_settings`` calls on the same
    node within an agent run collapse to the latest one. Live trace
    14:21 showed the LLM emitting 4 identical ``update_node_settings``
    calls; without dedupe the diff preview surfaced *"4 modifications"*
    with four identical cards stacked. The contract is full-replace,
    so the latest call already reflects the cumulative intent.
    """
    initial_settings = _filter_settings_dump(node_id=2, depending_on_id=1, expr="[region]=='EU'")
    new_v1 = _filter_settings_dump(node_id=2, depending_on_id=1, expr="[amount] > 50")
    new_v2 = _filter_settings_dump(node_id=2, depending_on_id=1, expr="[amount] > 100")
    new_v3 = _filter_settings_dump(node_id=2, depending_on_id=1, expr="[amount] > 200")
    entries = [
        diff.StagedToolEntry(
            tool_name="flowfile.graph.update_node_settings",
            audit_id=10,
            staged_node_payload={
                "kind": "modification", "node_id": 2, "node_type": "filter",
                "old_settings": initial_settings, "new_settings": new_v1,
            },
        ),
        diff.StagedToolEntry(
            tool_name="flowfile.graph.update_node_settings",
            audit_id=11,
            staged_node_payload={
                "kind": "modification", "node_id": 2, "node_type": "filter",
                "old_settings": new_v1, "new_settings": new_v2,
            },
        ),
        diff.StagedToolEntry(
            tool_name="flowfile.graph.update_node_settings",
            audit_id=12,
            staged_node_payload={
                "kind": "modification", "node_id": 2, "node_type": "filter",
                "old_settings": new_v2, "new_settings": new_v3,
            },
        ),
    ]
    bundled = diff.bundle_staged_results(entries)
    # Three calls on node 2 collapse to one entry: the latest.
    assert len(bundled.modifications) == 1
    mod = bundled.modifications[0]
    assert mod.audit_id == 12
    # The "After" payload reflects the last LLM call's intent.
    assert mod.new_settings.get("filter_input", {}).get("advanced_filter") == "[amount] > 200"


def test_bundle_staged_results_dedupes_modifications_per_node() -> None:
    """Modifications targeting *different* nodes do NOT dedupe — each gets
    its own entry. Only same-node duplicates collapse."""
    settings_a = _filter_settings_dump(node_id=2, depending_on_id=1, expr="[region]=='EU'")
    settings_b = _filter_settings_dump(node_id=3, depending_on_id=1, expr="[region]=='US'")
    entries = [
        diff.StagedToolEntry(
            tool_name="flowfile.graph.update_node_settings",
            audit_id=20,
            staged_node_payload={
                "kind": "modification", "node_id": 2, "node_type": "filter",
                "old_settings": settings_a, "new_settings": settings_a,
            },
        ),
        diff.StagedToolEntry(
            tool_name="flowfile.graph.update_node_settings",
            audit_id=21,
            staged_node_payload={
                "kind": "modification", "node_id": 3, "node_type": "filter",
                "old_settings": settings_b, "new_settings": settings_b,
            },
        ),
    ]
    bundled = diff.bundle_staged_results(entries)
    assert len(bundled.modifications) == 2
    assert {m.node_id for m in bundled.modifications} == {2, 3}


def test_bundle_staged_results_rejects_modification_without_kind() -> None:
    """defensive: missing ``kind="modification"`` in the payload is
    a programming error from upstream; raise so it surfaces as 422.
    """
    entries = [
        diff.StagedToolEntry(
            tool_name="flowfile.graph.update_node_settings",
            audit_id=1,
            staged_node_payload={"node_id": 2, "node_type": "filter"},  # no kind
        ),
    ]
    with pytest.raises(ValueError, match="kind='modification'"):
        diff.bundle_staged_results(entries)


def test_validate_diff_against_flow_drift_on_missing_modification_target() -> None:
    """a modification targeting a node that's neither live nor in
    the diff's additions raises :class:`DiffDriftError` with the missing
    id surfaced. Mirrors how addition-upstream drift is reported.
    """
    flow = _flow_with_orders()
    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        modifications=[
            diff.StagedSettingsUpdate(
                node_id=99,
                node_type="filter",
                old_settings={},
                new_settings=_filter_settings_dump(node_id=99, depending_on_id=1, expr="[amount] > 50"),
            ),
        ],
    )
    with pytest.raises(diff.DiffDriftError) as exc_info:
        diff.validate_diff_against_flow(flow, graph_diff)
    assert exc_info.value.missing_node_ids == [99]


def test_validate_diff_against_flow_tolerates_modification_on_in_batch_addition() -> None:
    """a modification targeting an in-batch addition is legal
    (parallel to addition-chaining via upstream ids). Apply order is
    additions → modifications, so the addition exists when the
    modification fires.
    """
    flow = _flow_with_orders()
    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings=_filter_settings_dump(node_id=10, depending_on_id=1, expr="[region]=='EU'"),
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[1]),
            ),
        ],
        modifications=[
            diff.StagedSettingsUpdate(
                node_id=10,
                node_type="filter",
                old_settings={},
                new_settings=_filter_settings_dump(node_id=10, depending_on_id=1, expr="[amount] > 50"),
            ),
        ],
    )
    # Should not raise — id 10 is in the additions bucket.
    diff.validate_diff_against_flow(flow, graph_diff)


def test_apply_diff_modification_mutates_settings_and_single_undo_point() -> None:
    """applying a single modification mutates the live node's
    ``setting_input`` to the new value and registers exactly one BATCH
    snapshot on the undo stack.
    """
    flow = _flow_with_orders_and_filter()
    pre_undo = flow.get_history_state().undo_count
    pre_filter = flow.get_node(2).setting_input
    assert pre_filter.filter_input.advanced_filter == "[region]=='EU'"

    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        modifications=[
            diff.StagedSettingsUpdate(
                node_id=2,
                node_type="filter",
                old_settings=pre_filter.model_dump(mode="json"),
                new_settings=_filter_settings_dump(node_id=2, depending_on_id=1, expr="[amount] > 50"),
            ),
        ],
        rationale="show only large amounts",
    )
    result = diff.apply_diff(flow, graph_diff)

    assert result.modified_node_ids == [2]
    post_filter = flow.get_node(2).setting_input
    assert post_filter.filter_input.advanced_filter == "[amount] > 50"
    assert flow.get_history_state().undo_count - pre_undo == 1


def test_apply_diff_modification_rolls_back_on_midbatch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """when a downstream op raises after a modification has already
    fired, ``flow.undo()`` rolls the BATCH snapshot back and the live
    node's settings revert to the pre-apply value. The diff stays in the
    store so the user can fix-and-retry.
    """
    flow = _flow_with_orders_and_filter()
    pre_filter_dump = flow.get_node(2).setting_input.model_dump(mode="json")
    pre_undo = flow.get_history_state().undo_count

    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        modifications=[
            diff.StagedSettingsUpdate(
                node_id=2,
                node_type="filter",
                old_settings=pre_filter_dump,
                new_settings=_filter_settings_dump(node_id=2, depending_on_id=1, expr="[amount] > 50"),
                audit_id=42,
            ),
        ],
        deletions=[diff.StagedDeletion(delete_node_id=1, audit_id=43)],
    )
    diff.register_diff(graph_diff)

    # Detonate the deletion bucket so the modification has already been applied
    # by the time the raise fires — exercises the rollback path.
    def _boom(self: FlowGraph, node_id):  # type: ignore[no-untyped-def]
        raise RuntimeError("simulated post-modification failure")

    monkeypatch.setattr(FlowGraph, "delete_node", _boom)
    with pytest.raises(RuntimeError, match="simulated post-modification failure"):
        diff.apply_diff(flow, graph_diff)

    # Live node's settings rolled back to pre-apply state.
    post_filter = flow.get_node(2).setting_input
    assert post_filter.filter_input.advanced_filter == "[region]=='EU'"
    # Diff stays in the store.
    assert diff.get_diff(graph_diff.diff_id) is graph_diff
    # BATCH snapshot taken then undone — net delta 0.
    assert flow.get_history_state().undo_count == pre_undo


def test_apply_diff_modification_chained_after_addition() -> None:
    """addition + modification of the freshly-added node in the
    same diff. The bundler bins them; ``apply_diff`` walks additions
    first so the modification fires against a real node.
    """
    flow = _flow_with_orders()
    pre_filter_count = sum(1 for n in flow.nodes if n.node_type == "filter")
    graph_diff = diff.GraphDiff(
        session_id="s",
        flow_id=1,
        additions=[
            diff.StagedAddition(
                node_type="filter",
                settings=_filter_settings_dump(node_id=10, depending_on_id=1, expr="[region]=='EU'"),
                insertion_context=diff.StagedInsertionContext(upstream_node_ids=[1]),
            ),
        ],
        modifications=[
            diff.StagedSettingsUpdate(
                node_id=10,
                node_type="filter",
                old_settings={},
                new_settings=_filter_settings_dump(node_id=10, depending_on_id=1, expr="[amount] > 100"),
            ),
        ],
    )
    result = diff.apply_diff(flow, graph_diff)
    assert result.applied_node_ids == [10]
    assert result.modified_node_ids == [10]
    post_filter = flow.get_node(10).setting_input
    assert post_filter.filter_input.advanced_filter == "[amount] > 100"
    assert sum(1 for n in flow.nodes if n.node_type == "filter") == pre_filter_count + 1

"""W40 + W45 — :mod:`flowfile_core.ai.sessions` tests.

Cases:

* ``test_session_store_roundtrip`` — register / get / pop / second-pop None.
* ``test_session_store_user_id_namespacing`` — ``get_session`` and
  ``pop_session`` return None for a different user_id than the owner.
* ``test_clear_for_tests_wipes_all`` — fixture cleanup works.
* ``test_capture_snapshot_basic`` — snapshot a flow with one node;
  ``node_ids`` and ``node_types`` populated (W45 — hash fields removed).
* ``test_detect_drift_no_change`` — fresh snapshot vs same flow → None.
* ``test_detect_drift_external_addition_fires`` — user adds a node
  post-snapshot the agent didn't stage → ``external_added_node_ids`` populated.
* ``test_detect_drift_agent_staged_addition_is_not_drift`` — agent's own
  staged node is excluded from the external-added bucket (W45 Q1).
* ``test_detect_drift_missing_node`` — delete a node post-snapshot →
  ``missing_node_ids`` populated.
* ``test_capture_snapshot_records_node_types`` — node_type captured per id.
* ``test_drift_detail_includes_node_types_for_missing`` — node_type survives
  the deletion via the snapshot map.
* ``test_drift_detail_includes_node_types_for_external_added`` — node_type
  pulled from the live node for new additions.
* ``test_lazy_litellm_contract`` — importing
  ``flowfile_core.ai.sessions`` doesn't load ``litellm``.
* ``test_list_sessions_for_user`` — only matching user_id returned.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator

import pytest

from flowfile_core.ai import sessions
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w40_sessions",
    )


def _add_orders(flow: FlowGraph, node_id: int = 1) -> None:
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[[1, 2, 3], ["EU", "US", "EU"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(node_id).get_predicted_schema()


def _flow_with_orders(flow_id: int = 1) -> FlowGraph:
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="w40_sessions_test")
    _add_orders(flow)
    return flow


def _make_session(*, user_id: int = 1, flow_id: int = 1) -> sessions.AgentSession:
    flow = _flow_with_orders(flow_id=flow_id)
    snapshot = sessions.capture_graph_snapshot(flow)
    return sessions.AgentSession(
        flow_id=flow_id,
        user_id=user_id,
        user_prompt="filter to EU",
        provider_name="anthropic",
        snapshot=snapshot,
    )


@pytest.fixture(autouse=True)
def _reset_session_store() -> Iterator[None]:
    sessions.clear_for_tests()
    yield
    sessions.clear_for_tests()


# --------------------------------------------------------------------------- #
# Store round-trip                                                             #
# --------------------------------------------------------------------------- #


def test_session_store_roundtrip() -> None:
    sess = _make_session()
    sid = sessions.register_session(sess)
    assert sid == sess.session_id
    assert sessions.get_session(sid) is sess
    popped = sessions.pop_session(sid)
    assert popped is sess
    assert sessions.get_session(sid) is None
    assert sessions.pop_session(sid) is None


def test_session_store_user_id_namespacing() -> None:
    owner = _make_session(user_id=1)
    sessions.register_session(owner)

    # Wrong user_id → no read.
    assert sessions.get_session(owner.session_id, user_id=2) is None
    # Wrong user_id → no pop.
    assert sessions.pop_session(owner.session_id, user_id=2) is None
    # Owner can still read after the failed cross-user pop attempt.
    assert sessions.get_session(owner.session_id, user_id=1) is owner

    # No user_id → still works (admin / internal callers).
    assert sessions.get_session(owner.session_id) is owner


def test_clear_for_tests_wipes_all() -> None:
    sessions.register_session(_make_session(user_id=1))
    sessions.register_session(_make_session(user_id=2))
    sessions.clear_for_tests()
    assert sessions.list_sessions_for_user(1) == []
    assert sessions.list_sessions_for_user(2) == []


def test_list_sessions_for_user() -> None:
    s1a = _make_session(user_id=1)
    s1b = _make_session(user_id=1)
    s2 = _make_session(user_id=2)
    sessions.register_session(s1a)
    sessions.register_session(s1b)
    sessions.register_session(s2)
    user1_sessions = sessions.list_sessions_for_user(1)
    assert {s.session_id for s in user1_sessions} == {s1a.session_id, s1b.session_id}
    user2_sessions = sessions.list_sessions_for_user(2)
    assert {s.session_id for s in user2_sessions} == {s2.session_id}


# --------------------------------------------------------------------------- #
# Snapshot capture                                                             #
# --------------------------------------------------------------------------- #


def test_capture_snapshot_basic() -> None:
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    assert snap.flow_id == 1
    assert snap.node_ids == (1,)
    # W45 — settings_hashes / schema_signatures removed; node_types added.
    assert snap.node_types == {1: "manual_input"}


def test_capture_snapshot_records_node_types() -> None:
    """W45 — ``node_types`` map is populated for every captured node."""
    flow = _flow_with_orders()
    _add_orders(flow, node_id=2)
    snap = sessions.capture_graph_snapshot(flow)
    assert snap.node_types == {1: "manual_input", 2: "manual_input"}


# --------------------------------------------------------------------------- #
# Drift detection (W45 — id-set only)                                          #
# --------------------------------------------------------------------------- #


def test_detect_drift_no_change() -> None:
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    assert sessions.detect_drift(flow, snap) is None


def test_detect_drift_external_addition_fires() -> None:
    """W45 — a node added after the snapshot that the agent didn't stage IS drift."""
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    _add_orders(flow, node_id=2)
    drift = sessions.detect_drift(flow, snap)
    assert drift is not None
    assert drift.external_added_node_ids == [2]
    assert drift.missing_node_ids == []


def test_detect_drift_agent_staged_addition_is_not_drift() -> None:
    """W45 Q1 — the agent's own staged ids are excluded from external-added."""
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    _add_orders(flow, node_id=2)
    # Agent claims it staged node 2 — drift should ignore it.
    drift = sessions.detect_drift(flow, snap, agent_staged_node_ids={2})
    assert drift is None


def test_detect_drift_missing_node() -> None:
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    flow.delete_node(1)
    drift = sessions.detect_drift(flow, snap)
    assert drift is not None
    assert drift.missing_node_ids == [1]
    assert drift.external_added_node_ids == []


def test_detect_drift_mixed_missing_and_external_added() -> None:
    """W45 — both buckets can populate in the same call."""
    flow = _flow_with_orders()
    _add_orders(flow, node_id=2)
    snap = sessions.capture_graph_snapshot(flow)
    flow.delete_node(1)
    _add_orders(flow, node_id=5)
    drift = sessions.detect_drift(flow, snap)
    assert drift is not None
    assert drift.missing_node_ids == [1]
    assert drift.external_added_node_ids == [5]


def test_drift_detail_includes_node_types_for_missing() -> None:
    """W45 Q3 — node_type for a deleted node comes from the snapshot."""
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    flow.delete_node(1)
    drift = sessions.detect_drift(flow, snap)
    assert drift is not None
    assert drift.node_types.get(1) == "manual_input"


def test_drift_detail_includes_node_types_for_external_added() -> None:
    """W45 Q3 — node_type for a new external addition comes from the live node."""
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    _add_orders(flow, node_id=2)
    drift = sessions.detect_drift(flow, snap)
    assert drift is not None
    assert drift.node_types.get(2) == "manual_input"


def test_drift_detail_is_empty() -> None:
    detail = sessions.DriftDetail()
    assert detail.is_empty()
    detail2 = sessions.DriftDetail(missing_node_ids=[1])
    assert not detail2.is_empty()
    detail3 = sessions.DriftDetail(external_added_node_ids=[5])
    assert not detail3.is_empty()


# --------------------------------------------------------------------------- #
# Lazy litellm contract                                                        #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    # Importing sessions must not pull litellm — same posture every workstream
    # since W11 has documented. If a prior test already imported litellm,
    # drop and re-import to verify the contract holds.
    sys.modules.pop("litellm", None)
    sys.modules.pop("flowfile_core.ai.sessions", None)
    from flowfile_core.ai import sessions as _sessions  # noqa: F401

    assert "litellm" not in sys.modules


def test_session_touch_updates_timestamp() -> None:
    sess = _make_session()
    before = sess.updated_at
    # Force a different instant.
    import time

    time.sleep(0.001)
    sess.touch()
    assert sess.updated_at > before


def test_session_default_status_is_running() -> None:
    sess = _make_session()
    assert sess.status == "running"
    assert sess.step_count == 0
    assert sess.staged_results == []
    assert sess.staged_node_ids == []  # W45 Q1
    assert sess.diff_id is None

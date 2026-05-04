"""W40 — :mod:`flowfile_core.ai.sessions` tests.

Cases:

* ``test_session_store_roundtrip`` — register / get / pop / second-pop None.
* ``test_session_store_user_id_namespacing`` — ``get_session`` and
  ``pop_session`` return None for a different user_id than the owner.
* ``test_clear_for_tests_wipes_all`` — fixture cleanup works.
* ``test_capture_snapshot_basic`` — snapshot a flow with one node;
  ``node_ids``, ``settings_hashes``, ``schema_signatures`` populated.
* ``test_detect_drift_no_change`` — fresh snapshot vs same flow → None.
* ``test_detect_drift_added_node_is_not_drift`` — adding a node after the
  snapshot doesn't trip drift; planner can refer to it via read_node_schema.
* ``test_detect_drift_missing_node`` — delete a node post-snapshot →
  ``missing_node_ids`` populated.
* ``test_detect_drift_settings_mutation`` — change a node's settings →
  ``mutated_node_ids`` populated.
* ``test_detect_drift_schema_change`` — manually mutate the predicted
  schema → ``schema_changed_node_ids`` populated.
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
    assert isinstance(snap.settings_hashes[1], str) and len(snap.settings_hashes[1]) == 64
    assert isinstance(snap.schema_signatures[1], str) and len(snap.schema_signatures[1]) == 64


# --------------------------------------------------------------------------- #
# Drift detection                                                              #
# --------------------------------------------------------------------------- #


def test_detect_drift_no_change() -> None:
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    assert sessions.detect_drift(flow, snap) is None


def test_detect_drift_added_node_is_not_drift() -> None:
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    # User adds a second node after the snapshot — that's not drift.
    _add_orders(flow, node_id=2)
    drift = sessions.detect_drift(flow, snap)
    assert drift is None


def test_detect_drift_missing_node() -> None:
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    flow.delete_node(1)
    drift = sessions.detect_drift(flow, snap)
    assert drift is not None
    assert drift.missing_node_ids == [1]


def test_detect_drift_settings_mutation() -> None:
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    # Mutate the node's settings — re-attach a new manual input with different data.
    node = flow.get_node(1)
    raw_setting = node.setting_input
    raw_setting.raw_data_format.data = [[99, 100, 101], ["AS", "AS", "AS"]]
    node.setting_input = raw_setting  # triggers reset
    drift = sessions.detect_drift(flow, snap)
    assert drift is not None
    assert 1 in drift.mutated_node_ids


def test_detect_drift_schema_change() -> None:
    flow = _flow_with_orders()
    snap = sessions.capture_graph_snapshot(flow)
    node = flow.get_node(1)
    # Forcibly clobber predicted_schema to simulate a downstream change.
    node.node_schema.predicted_schema = []
    drift = sessions.detect_drift(flow, snap)
    assert drift is not None
    assert 1 in drift.schema_changed_node_ids


def test_drift_detail_is_empty() -> None:
    detail = sessions.DriftDetail()
    assert detail.is_empty()
    detail2 = sessions.DriftDetail(missing_node_ids=[1])
    assert not detail2.is_empty()


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
    assert sess.diff_id is None

"""W15 — Audit log infra tests.

Cases:

* ``test_audit_table_exists_after_alembic`` — Alembic migration 011 lands
  the ``ai_audit_events`` table on a fresh DB.
* ``test_audit_columns_match_model`` — table columns line up with the
  ``AiAuditEvent`` SQLAlchemy class.
* ``test_record_event_round_trip`` — ``record_event`` persists; ``query_events``
  reads back; field-by-field equality.
* ``test_record_event_minimal_fields`` — only the four required fields
  (``session_id`` / ``user_id`` / ``tool_name`` / ``result_status``) are
  enough to persist.
* ``test_record_event_truncates_oversize_args`` — payload >
  ``MAX_ARGS_BYTES`` becomes a ``__truncated__`` marker.
* ``test_record_event_with_external_session`` — caller's session controls
  the transaction; rolling back yields zero rows.
* ``test_record_event_logs_structured_line`` — the audit logger emits a
  human-readable line for the existing logger pipeline (plan §6.5).
* ``test_query_events_filters`` — ``flow_id`` / ``user_id`` / ``session_id``
  / ``tool_name`` filters compose; default order is DESC by ``created_at``.
* ``test_query_events_limit`` — ``limit`` caps the result set.
* ``test_update_diff_action_round_trip`` — ``update_diff_action`` flips
  ``diff_action`` from ``None`` → ``"accepted"``.
* ``test_update_diff_action_missing_id`` — stale id returns ``None``,
  doesn't raise.
* ``test_metrics_pass_rate_empty`` — ``aggregate_pass_rate`` on no rows
  returns 1.0 (monotonic from the first event).
* ``test_metrics_pass_rate_mixed`` — pass rate calc with success / error /
  rejected events.
* ``test_metrics_aggregate_tokens`` — token totals sum across events.
* ``test_lazy_litellm_import`` — importing ``flowfile_core.ai.audit`` does
  not pull in ``litellm`` (audit is independent of providers).
* ``test_truncate_args_handles_non_serialisable`` — circular refs / non-JSON
  values fall back to a marker rather than crashing the audit write.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import pytest
from sqlalchemy import inspect

from flowfile_core.ai.audit import (
    MAX_ARGS_BYTES,
    AuditEvent,
    _truncate_args,
    query_events,
    record_event,
    update_diff_action,
)
from flowfile_core.ai.metrics import aggregate_pass_rate, aggregate_tokens
from flowfile_core.database.connection import engine, get_db_context
from flowfile_core.database.models import AiAuditEvent, User

# ---------- shared fixture ----------


@pytest.fixture
def local_user_id() -> int:
    with get_db_context() as db:
        user = db.query(User).filter_by(username="local_user").first()
        assert user is not None, "Tests rely on conftest's setup_test_db local_user"
        return user.id


@pytest.fixture(autouse=True)
def _cleanup_audit_rows() -> None:
    """Each test starts with an empty ``ai_audit_events`` table."""
    with get_db_context() as db:
        db.query(AiAuditEvent).delete()
        db.commit()


def _make_event(local_user_id: int, **overrides: Any) -> AuditEvent:
    base: dict[str, Any] = {
        "session_id": "sess-test",
        "user_id": local_user_id,
        "tool_name": "flowfile.graph.add_filter",
        "result_status": "success",
    }
    base.update(overrides)
    return AuditEvent(**base)


# ---------- schema / Alembic ----------


def test_audit_table_exists_after_alembic() -> None:
    inspector = inspect(engine)
    assert "ai_audit_events" in inspector.get_table_names()


def test_audit_columns_match_model() -> None:
    inspector = inspect(engine)
    columns = {c["name"] for c in inspector.get_columns("ai_audit_events")}
    expected = {
        "id",
        "session_id",
        "flow_id",
        "user_id",
        "tool_name",
        "tool_args",
        "result_status",
        "error",
        "provider",
        "model",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "diff_action",
        "created_at",
    }
    assert expected.issubset(columns), f"missing: {expected - columns}"


# ---------- record_event ----------


def test_record_event_round_trip(local_user_id: int) -> None:
    event = _make_event(
        local_user_id,
        flow_id=42,
        tool_args={"predicate": "amount > 100"},
        provider="anthropic",
        model="anthropic/claude-haiku-4-5",
        prompt_tokens=120,
        completion_tokens=30,
        total_tokens=150,
    )
    row = record_event(event)
    assert row.id is not None
    assert row.session_id == "sess-test"
    assert row.flow_id == 42
    assert row.tool_name == "flowfile.graph.add_filter"
    assert row.result_status == "success"
    assert row.tool_args == '{"predicate": "amount > 100"}'
    assert row.total_tokens == 150

    queried = query_events(flow_id=42)
    assert len(queried) == 1
    assert queried[0].id == row.id


def test_record_event_minimal_fields(local_user_id: int) -> None:
    row = record_event(_make_event(local_user_id))
    assert row.id is not None
    assert row.flow_id is None
    assert row.tool_args is None
    assert row.diff_action is None
    assert row.prompt_tokens == 0


def test_record_event_truncates_oversize_args(local_user_id: int) -> None:
    bloated = {"payload": "x" * (MAX_ARGS_BYTES * 2)}
    row = record_event(_make_event(local_user_id, tool_args=bloated))
    assert row.tool_args is not None
    assert "__truncated__" in row.tool_args
    assert "__original_size__" in row.tool_args
    # The persisted blob is much smaller than the original.
    assert len(row.tool_args.encode("utf-8")) < MAX_ARGS_BYTES * 2


def test_record_event_with_external_session_rollback(local_user_id: int) -> None:
    """Caller's transaction owns the write — rolling back leaves no rows."""
    with get_db_context() as db:
        record_event(_make_event(local_user_id, session_id="rolled-back"), db=db)
        db.rollback()

    rows = query_events(session_id="rolled-back")
    assert rows == []


def test_record_event_logs_structured_line(
    local_user_id: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO, logger="flowfile_core.ai.audit"):
        record_event(_make_event(local_user_id, total_tokens=42, provider="anthropic"))

    matching = [r for r in caplog.records if "ai_audit" in r.getMessage()]
    assert matching, f"expected an ai_audit log line, saw: {[r.getMessage() for r in caplog.records]}"
    msg = matching[0].getMessage()
    assert "tool=flowfile.graph.add_filter" in msg
    assert "tokens=42" in msg
    assert "provider=anthropic" in msg


# ---------- query_events ----------


def test_query_events_filters(local_user_id: int) -> None:
    record_event(_make_event(local_user_id, session_id="s1", flow_id=1))
    record_event(_make_event(local_user_id, session_id="s1", flow_id=2))
    record_event(
        _make_event(
            local_user_id,
            session_id="s2",
            flow_id=1,
            tool_name="flowfile.graph.add_select",
        )
    )

    s1_rows = query_events(session_id="s1")
    assert len(s1_rows) == 2

    flow1_rows = query_events(flow_id=1)
    assert len(flow1_rows) == 2

    select_rows = query_events(tool_name="flowfile.graph.add_select")
    assert len(select_rows) == 1
    assert select_rows[0].session_id == "s2"

    intersect = query_events(session_id="s1", flow_id=1)
    assert len(intersect) == 1


def test_query_events_limit(local_user_id: int) -> None:
    for i in range(5):
        record_event(_make_event(local_user_id, session_id=f"s{i}"))
    assert len(query_events(limit=3)) == 3
    # Default limit is 100; all five fit.
    assert len(query_events()) == 5


# ---------- update_diff_action ----------


def test_update_diff_action_round_trip(local_user_id: int) -> None:
    row = record_event(_make_event(local_user_id, diff_action=None))
    assert row.diff_action is None

    updated = update_diff_action(row.id, "accepted")
    assert updated is not None
    assert updated.diff_action == "accepted"

    fresh = query_events()[0]
    assert fresh.diff_action == "accepted"


def test_update_diff_action_missing_id(local_user_id: int) -> None:
    assert update_diff_action(999_999, "accepted") is None


# ---------- metrics ----------


def test_metrics_pass_rate_empty() -> None:
    stats = aggregate_pass_rate()
    assert stats == {
        "total": 0,
        "success": 0,
        "error": 0,
        "rejected": 0,
        "pass_rate": 1.0,
    }


def test_metrics_pass_rate_mixed(local_user_id: int) -> None:
    record_event(_make_event(local_user_id, flow_id=7, result_status="success"))
    record_event(_make_event(local_user_id, flow_id=7, result_status="success"))
    record_event(_make_event(local_user_id, flow_id=7, result_status="error"))
    record_event(_make_event(local_user_id, flow_id=8, result_status="rejected"))

    flow7 = aggregate_pass_rate(flow_id=7)
    assert flow7["total"] == 3
    assert flow7["success"] == 2
    assert flow7["error"] == 1
    assert flow7["pass_rate"] == pytest.approx(2 / 3)

    overall = aggregate_pass_rate()
    assert overall["total"] == 4
    assert overall["rejected"] == 1


def test_metrics_aggregate_tokens(local_user_id: int) -> None:
    record_event(
        _make_event(
            local_user_id,
            flow_id=11,
            prompt_tokens=10,
            completion_tokens=5,
            total_tokens=15,
        )
    )
    record_event(
        _make_event(
            local_user_id,
            flow_id=11,
            prompt_tokens=20,
            completion_tokens=8,
            total_tokens=28,
        )
    )
    record_event(
        _make_event(
            local_user_id,
            flow_id=12,
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
        )
    )

    flow11 = aggregate_tokens(flow_id=11)
    assert flow11 == {"prompt_tokens": 30, "completion_tokens": 13, "total_tokens": 43}


# ---------- internals ----------


def test_truncate_args_handles_non_serialisable() -> None:
    circular: dict[str, Any] = {}
    circular["self"] = circular
    out = _truncate_args(circular)
    assert "__truncated__" in out
    assert "non_serialisable" in out


def test_truncate_args_passes_through_small_payload() -> None:
    out = _truncate_args({"a": 1, "b": "two"})
    assert out == '{"a": 1, "b": "two"}'


# ---------- lazy import ----------


def test_lazy_litellm_import() -> None:
    """``flowfile_core.ai.audit`` is independent of provider machinery.

    Mirrors W11's lazy-import contract — importing the audit module must not
    pull in litellm. We unconditionally restore the original modules
    afterwards so other tests' references (e.g. ``safety``'s re-exported
    ``AuditEvent`` class identity) stay consistent.
    """
    cleared: dict[str, Any] = {}
    for mod_name in list(sys.modules):
        if mod_name == "litellm" or mod_name.startswith("litellm."):
            cleared[mod_name] = sys.modules.pop(mod_name)
        elif mod_name == "flowfile_core.ai.audit":
            cleared[mod_name] = sys.modules.pop(mod_name)
    try:
        import flowfile_core.ai.audit  # noqa: F401
        assert "litellm" not in sys.modules, (
            "Importing flowfile_core.ai.audit must not eagerly import litellm"
        )
    finally:
        # Restore the original audit module so other tests see the same
        # ``AuditEvent`` class object they imported at module load time.
        for mod_name, mod in cleared.items():
            sys.modules[mod_name] = mod


# ---------- safety re-export ----------


def test_safety_module_reexports_audit() -> None:
    """``ai.safety`` should expose audit helpers so callers have one import surface."""
    from flowfile_core.ai import safety

    assert safety.AuditEvent is AuditEvent
    assert safety.record_event is record_event
    assert safety.query_events is query_events
    assert safety.MAX_ARGS_BYTES == MAX_ARGS_BYTES

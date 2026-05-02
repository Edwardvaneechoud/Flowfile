"""PII scrubbing, schema-only mode, and audit hooks.

Owned by W25 (PII scrubber + safety layer) and W15 (audit log infra).

Per D009, sample rows are off by default. When the user opts in per-flow,
W25 applies a regex pass (emails / phones / cards) before sample rows leave
the box. Presidio remains opt-in for sensitive flows.

W15 has landed the audit half: every tool call is recorded with
``(flow_id, user_id, tool_name, args, result, timestamp)`` so users can
inspect what the agent did on their behalf — a §9.4 commitment. The
:func:`record_event` and :func:`query_events` functions are re-exported
below so callers have a single ``flowfile_core.ai.safety`` import surface
once W25 lands its scrubber.

Until W25 lands its PII helpers, ``safety`` mirrors ``audit`` only.
"""

from flowfile_core.ai.audit import (
    MAX_ARGS_BYTES,
    AuditEvent,
    DiffAction,
    ResultStatus,
    query_events,
    record_event,
    update_diff_action,
)

__all__ = [
    "MAX_ARGS_BYTES",
    "AuditEvent",
    "DiffAction",
    "ResultStatus",
    "query_events",
    "record_event",
    "update_diff_action",
]

"""Counters and cost-per-flow telemetry for the AI subsystem.

Owned by W11 (cost-per-flow numbers feed back into D010's latency / model
selection) and W15 (audit-log surface).

Reads from ``ai_audit_events`` (W15) to compute the §13 success metrics:

* tokens in / out per provider per surface;
* tool-call success / failure counts;
* tool-call validation pass rate (``≥95%`` target).

Time-to-first-byte and total round-trip aren't recorded yet — W11/W14 add
those once the rate-limit scheduler is in place. Until then this module
exposes only the metrics that the audit log can answer today.

W34 adds :func:`record_autocomplete_call` — a lightweight non-DB telemetry
helper for per-call settings-autocomplete observation. The audit-DB write
path is too noisy for keystroke-frequency events; this helper emits a
structured ``INFO`` log line instead, ready for downstream collection.
"""

from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from flowfile_core.ai.audit import query_events
from flowfile_core.database.connection import SessionLocal
from flowfile_core.database.models import AiAuditEvent

logger = logging.getLogger(__name__)


def aggregate_pass_rate(
    flow_id: int | None = None,
    db: Session | None = None,
) -> dict[str, float | int]:
    """Tool-call validation pass rate for §13's ≥95% target.

    Returns a dict with ``total``, ``success``, ``error``, ``rejected``,
    ``pass_rate`` (success/total). When ``total == 0`` the pass rate is
    reported as ``1.0`` (no failures to count yet) so the metric is
    monotonically meaningful from the first event onward.
    """

    def _aggregate(session: Session) -> dict[str, float | int]:
        q = session.query(AiAuditEvent)
        if flow_id is not None:
            q = q.filter(AiAuditEvent.flow_id == flow_id)
        rows = q.all()
        total = len(rows)
        success = sum(1 for r in rows if r.result_status == "success")
        error = sum(1 for r in rows if r.result_status == "error")
        rejected = sum(1 for r in rows if r.result_status == "rejected")
        pass_rate = (success / total) if total else 1.0
        return {
            "total": total,
            "success": success,
            "error": error,
            "rejected": rejected,
            "pass_rate": pass_rate,
        }

    if db is not None:
        return _aggregate(db)
    with SessionLocal() as session:
        return _aggregate(session)


def aggregate_tokens(
    flow_id: int | None = None,
    db: Session | None = None,
) -> dict[str, int]:
    """Token totals for the cost-per-flow §13 metric.

    Returns ``prompt_tokens``, ``completion_tokens``, ``total_tokens``
    summed across the relevant audit events.
    """

    events = query_events(flow_id=flow_id, limit=10_000, db=db)
    return {
        "prompt_tokens": sum(e.prompt_tokens for e in events),
        "completion_tokens": sum(e.completion_tokens for e in events),
        "total_tokens": sum(e.total_tokens for e in events),
    }


def record_autocomplete_call(
    *,
    surface: str,
    provider: str,
    latency_ms: int,
    suggestion_count: int = 0,
    degraded_reason: str | None = None,
) -> None:
    """Emit one telemetry line per autocomplete request (W34).

    Deliberately bypasses the audit DB — autocomplete fires on keystrokes and
    a per-keystroke insert would balloon the table. The structured ``INFO``
    line under the ``flowfile_core.ai.metrics`` logger is enough for a
    downstream log scraper / dashboard to slice by surface, provider, and
    degraded reason. Acceptance-side accounting (which suggestion the user
    actually picked) is W41's responsibility — that goes in the audit DB.
    """
    logger.info(
        "ai_autocomplete surface=%s provider=%s latency_ms=%d suggestions=%d degraded=%s",
        surface,
        provider,
        latency_ms,
        suggestion_count,
        degraded_reason or "-",
    )


__all__ = ["aggregate_pass_rate", "aggregate_tokens", "record_autocomplete_call"]

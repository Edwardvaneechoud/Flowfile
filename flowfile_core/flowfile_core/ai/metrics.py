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

W59 adds :func:`record_provider_call` + :func:`get_provider_call_counts` — an
in-process counter labelled by ``(provider, surface, model, status)``. Bumped
on every ``LiteLLMProvider.chat`` / ``.stream`` call regardless of whether
prompt-logging is enabled (the JSONL log is opt-in; basic call traffic
counters are always-on so dashboards don't go dark when logging is off).
The implementation is a plain ``collections.Counter`` — no Prometheus client
dep gets pulled in for v0; a future workstream can swap it for the proper
metric backend without changing the call signature.
"""

from __future__ import annotations

import logging
from collections import Counter
from threading import Lock

from sqlalchemy.orm import Session

from flowfile_core.ai.audit import query_events
from flowfile_core.database.connection import SessionLocal
from flowfile_core.database.models import AiAuditEvent

logger = logging.getLogger(__name__)

#: Status label for the ``flowfile_ai_provider_call_total`` counter.
ProviderCallStatus = str  # "success" | "error"

#: Process-local counter keyed by ``(provider, surface, model, status)``.
#: Reset between test runs via :func:`reset_provider_call_counts`.
_PROVIDER_CALL_COUNTER: Counter[tuple[str, str, str, str]] = Counter()
_PROVIDER_CALL_LOCK = Lock()


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


def record_provider_call(
    *,
    provider: str,
    surface: str | None,
    model: str,
    status: str,
) -> None:
    """Increment the ``flowfile_ai_provider_call_total`` counter.

    Called from the W11 ``LiteLLMProvider`` wrap on every ``chat`` /
    ``stream`` call, success or error. ``surface`` may be ``None`` for
    callers that haven't been threaded through yet — coerced to ``"unknown"``
    so the counter key stays a 4-tuple.
    """
    key = (provider, surface or "unknown", model, status)
    with _PROVIDER_CALL_LOCK:
        _PROVIDER_CALL_COUNTER[key] += 1
    logger.info(
        "ai_provider_call provider=%s surface=%s model=%s status=%s",
        provider,
        surface or "unknown",
        model,
        status,
    )


def get_provider_call_counts() -> dict[tuple[str, str, str, str], int]:
    """Snapshot of the in-process counter — keyed by full label tuple."""
    with _PROVIDER_CALL_LOCK:
        return dict(_PROVIDER_CALL_COUNTER)


def reset_provider_call_counts() -> None:
    """Clear the in-process counter (test helper)."""
    with _PROVIDER_CALL_LOCK:
        _PROVIDER_CALL_COUNTER.clear()


__all__ = [
    "aggregate_pass_rate",
    "aggregate_tokens",
    "get_provider_call_counts",
    "record_autocomplete_call",
    "record_provider_call",
    "reset_provider_call_counts",
]

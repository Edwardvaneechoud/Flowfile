"""Builds the metadata-only event payload carried by an outbox row.

Never include row data or node output — a webhook URL is a low-trust destination.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy.orm import Session

from shared.models import FlowRegistration, FlowRun, FlowSchedule
from shared.run_logs import SUBPROCESS_RUN_TYPES, run_log_path

logger = logging.getLogger("flowfile.notifications")

MAX_FAILED_NODES = 5
MAX_ERROR_CHARS = 300


def _isoformat(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def _failed_nodes(run: FlowRun) -> list[dict[str, Any]]:
    """Non-successful node results carrying an error, capped and truncated.

    Node errors routinely embed data values, so they are trimmed hard before
    leaving the install.
    """
    if not run.node_results_json:
        return []
    try:
        results = json.loads(run.node_results_json)
    except (TypeError, ValueError):
        logger.debug("Run %s has unparseable node_results_json", run.id)
        return []
    if not isinstance(results, list):
        return []

    failed: list[dict[str, Any]] = []
    for entry in results:
        if not isinstance(entry, dict) or entry.get("success"):
            continue
        error = entry.get("error")
        if not error:
            continue
        failed.append(
            {
                "node_id": entry.get("node_id"),
                "node_name": entry.get("node_name"),
                "error": str(error)[:MAX_ERROR_CHARS],
            }
        )
        if len(failed) >= MAX_FAILED_NODES:
            break
    return failed


def _log_path(run: FlowRun) -> str | None:
    if run.run_type not in SUBPROCESS_RUN_TYPES:
        return None
    path = run_log_path(run.id)
    return str(path) if path.exists() else None


def build_run_event_payload(
    session: Session,
    run: FlowRun,
    event_type: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Metadata describing one run outcome, ready to be rendered per channel type."""
    flow_name = run.flow_name
    if run.registration_id is not None:
        registration = session.get(FlowRegistration, run.registration_id)
        if registration is not None:
            flow_name = registration.name

    schedule_name = None
    if run.schedule_id is not None:
        schedule = session.get(FlowSchedule, run.schedule_id)
        if schedule is not None:
            schedule_name = schedule.name

    return {
        "event_type": event_type,
        "flow_name": flow_name,
        "registration_id": run.registration_id,
        "run_id": run.id,
        "run_type": run.run_type,
        "schedule_id": run.schedule_id,
        "schedule_name": schedule_name,
        "success": run.success,
        "started_at": _isoformat(run.started_at),
        "ended_at": _isoformat(run.ended_at),
        "duration_seconds": run.duration_seconds,
        "nodes_completed": run.nodes_completed,
        "number_of_nodes": run.number_of_nodes,
        "failed_nodes": _failed_nodes(run),
        "log_path": _log_path(run),
        "reason": reason,
    }

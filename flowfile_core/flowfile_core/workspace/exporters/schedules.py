"""Export schedule *definitions* (cron / interval) keyed by flow_uuid.

Runtime-state columns (``last_triggered_at``, ``last_cron_slot``,
``last_trigger_table_updated_at``) and audit timestamps are never exported -- only
the definition. Table-trigger schedules depend on catalog tables (Phase 3) and
are skipped with a warning in the MVP.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from flowfile_core.database.models import FlowRegistration, FlowSchedule
from flowfile_core.workspace.exporters import ExportBundle, drop_none
from flowfile_core.workspace.layout import ProjectLayout, slugify
from flowfile_core.workspace.normalize import canonical_yaml_dump

_MVP_SCHEDULE_TYPES = {"interval", "cron"}


def _schedule_doc(sched: FlowSchedule) -> dict:
    return drop_none(
        {
            "name": sched.name,
            "schedule_type": sched.schedule_type,
            "enabled": sched.enabled,
            "interval_seconds": sched.interval_seconds,
            "cron_expression": sched.cron_expression,
            "cron_timezone": sched.cron_timezone,
            "description": sched.description,
        }
    )


def _sort_key(doc: dict) -> tuple:
    return (
        doc.get("name") or "",
        doc.get("schedule_type") or "",
        doc.get("cron_expression") or "",
        doc.get("interval_seconds") or 0,
    )


def export_schedules(
    db: Session, user_id: int, layout: ProjectLayout, flow_index: dict[str, str]
) -> ExportBundle:
    bundle = ExportBundle()

    rows = (
        db.query(FlowSchedule, FlowRegistration)
        .join(FlowRegistration, FlowSchedule.registration_id == FlowRegistration.id)
        .filter(FlowSchedule.owner_id == user_id)
        .all()
    )

    by_flow: dict[str, list[dict]] = {}
    flow_names: dict[str, str] = {}
    for sched, reg in rows:
        if sched.schedule_type not in _MVP_SCHEDULE_TYPES:
            bundle.warnings.append(
                f"schedule '{sched.name or sched.id}' on flow '{reg.name}' is type "
                f"'{sched.schedule_type}' (catalog-table triggers are Phase 3); skipped"
            )
            continue
        by_flow.setdefault(reg.flow_uuid, []).append(_schedule_doc(sched))
        flow_names[reg.flow_uuid] = reg.name

    for flow_uuid, docs in by_flow.items():
        slug = flow_index.get(flow_uuid) or slugify(flow_names.get(flow_uuid, flow_uuid))
        doc = {"flow_uuid": flow_uuid, "schedules": sorted(docs, key=_sort_key)}
        bundle.artifacts[layout.rel(layout.schedules_path(slug))] = canonical_yaml_dump(doc)

    return bundle

"""Recreate cron/interval schedule definitions, keyed by flow_uuid.

Apply is replace-all *for managed schedule types only* (interval, cron): existing
interval/cron schedules for a flow are dropped and recreated from the file, while
catalog-table triggers (Phase 3) are left untouched. Runtime-state columns are
left NULL by design -- the scheduler re-establishes its cursors by polling.
"""

from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from flowfile_core.database.models import FlowRegistration, FlowSchedule
from flowfile_core.workspace.layout import ProjectLayout
from flowfile_core.workspace.normalize import canonical_yaml_load

logger = logging.getLogger(__name__)

_MANAGED_TYPES = ("interval", "cron")


class _ScheduleImportResult:
    def __init__(self) -> None:
        self.counts: dict[str, int] = {"schedule": 0}
        self.warnings: list[str] = []


def _registration_id(db: Session, reg_map: dict[str, int], flow_uuid: str) -> int | None:
    if flow_uuid in reg_map:
        return reg_map[flow_uuid]
    reg = db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first()
    return reg.id if reg else None


def import_schedules(
    db: Session, user_id: int, layout: ProjectLayout, reg_map: dict[str, int]
) -> _ScheduleImportResult:
    result = _ScheduleImportResult()

    for sched_file in layout.iter_schedule_files():
        rel = layout.rel(sched_file)
        doc = canonical_yaml_load(sched_file.read_text(encoding="utf-8")) or {}
        flow_uuid = doc.get("flow_uuid")
        if not flow_uuid:
            result.warnings.append(f"{rel}: missing flow_uuid; skipped")
            continue
        registration_id = _registration_id(db, reg_map, flow_uuid)
        if registration_id is None:
            result.warnings.append(f"{rel}: no flow registered for flow_uuid {flow_uuid}; schedules skipped")
            continue

        # Replace managed schedule types only; leave Phase-3 table triggers intact.
        db.query(FlowSchedule).filter(
            FlowSchedule.registration_id == registration_id,
            FlowSchedule.schedule_type.in_(_MANAGED_TYPES),
        ).delete(synchronize_session=False)

        for entry in doc.get("schedules", []) or []:
            schedule_type = entry.get("schedule_type")
            if schedule_type not in _MANAGED_TYPES:
                result.warnings.append(f"{rel}: unsupported schedule_type '{schedule_type}'; skipped")
                continue
            db.add(
                FlowSchedule(
                    registration_id=registration_id,
                    owner_id=user_id,
                    enabled=entry.get("enabled", True),
                    name=entry.get("name"),
                    description=entry.get("description"),
                    schedule_type=schedule_type,
                    interval_seconds=entry.get("interval_seconds"),
                    cron_expression=entry.get("cron_expression"),
                    cron_timezone=entry.get("cron_timezone"),
                )
            )
            result.counts["schedule"] += 1

    db.commit()
    return result

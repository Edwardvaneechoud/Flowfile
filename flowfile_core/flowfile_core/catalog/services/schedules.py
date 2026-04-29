"""Schedule CRUD, trigger-now, table-trigger fan-out."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from flowfile_core.catalog.exceptions import (
    FlowAlreadyRunningError,
    FlowNotFoundError,
    ScheduleNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.catalog.services.runs import FlowRunService
from flowfile_core.catalog.validators import (
    format_full_name,
    validate_schedule_create,
    validate_schedule_update,
)
from flowfile_core.database.models import FlowSchedule
from flowfile_core.schemas.catalog_schema import FlowRunOut, FlowScheduleOut

logger = logging.getLogger(__name__)


class ScheduleService:
    """Owns schedule CRUD, manual triggers, and the push path of table_trigger fan-out."""

    def __init__(
        self,
        repo: CatalogRepository,
        runs: FlowRunService,
        namespaces: NamespaceService,
    ) -> None:
        self.repo = repo
        self._runs = runs
        self._namespaces = namespaces

    def _schedule_to_out(self, schedule: FlowSchedule) -> FlowScheduleOut:
        """Convert a FlowSchedule ORM instance to its DTO, populating trigger info."""
        trigger_table_name: str | None = None
        trigger_namespace_id: int | None = None
        trigger_namespace_name: str | None = None
        trigger_full_table_name: str | None = None
        if schedule.trigger_table_id is not None:
            table = self.repo.get_table(schedule.trigger_table_id)
            if table is not None:
                trigger_table_name = table.name
                trigger_namespace_id = table.namespace_id
                trigger_namespace_name = self._namespaces.resolve_namespace_name(table.namespace_id)
                trigger_full_table_name = format_full_name(trigger_namespace_name, table.name)

        trigger_table_ids: list[int] = []
        trigger_table_names: list[str] = []
        trigger_full_table_names: list[str] = []
        if schedule.schedule_type == "table_set_trigger":
            trigger_table_ids = self.repo.get_trigger_table_ids(schedule.id)
            for table_id in trigger_table_ids:
                table = self.repo.get_table(table_id)
                if table is None:
                    trigger_table_names.append(f"#{table_id}")
                    trigger_full_table_names.append(f"#{table_id}")
                    continue
                trigger_table_names.append(table.name)
                ns_name = self._namespaces.resolve_namespace_name(table.namespace_id)
                trigger_full_table_names.append(format_full_name(ns_name, table.name))

        return FlowScheduleOut(
            id=schedule.id,
            registration_id=schedule.registration_id,
            owner_id=schedule.owner_id,
            enabled=schedule.enabled,
            name=schedule.name,
            description=schedule.description,
            schedule_type=schedule.schedule_type,
            interval_seconds=schedule.interval_seconds,
            trigger_table_id=schedule.trigger_table_id,
            trigger_table_name=trigger_table_name,
            trigger_namespace_id=trigger_namespace_id,
            trigger_namespace_name=trigger_namespace_name,
            trigger_full_table_name=trigger_full_table_name,
            trigger_table_ids=trigger_table_ids,
            trigger_table_names=trigger_table_names,
            trigger_full_table_names=trigger_full_table_names,
            last_triggered_at=schedule.last_triggered_at,
            last_trigger_table_updated_at=schedule.last_trigger_table_updated_at,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at,
        )

    def create_schedule(
        self,
        registration_id: int,
        owner_id: int,
        schedule_type: str,
        interval_seconds: int | None = None,
        trigger_table_id: int | None = None,
        trigger_table_ids: list[int] | None = None,
        enabled: bool = True,
        name: str | None = None,
        description: str | None = None,
    ) -> FlowScheduleOut:
        """Create a new schedule for a registered flow."""
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)

        validate_schedule_create(
            schedule_type=schedule_type,
            interval_seconds=interval_seconds,
            trigger_table_id=trigger_table_id,
            trigger_table_ids=trigger_table_ids,
            table_exists=lambda table_id: self.repo.get_table(table_id) is not None,
        )

        schedule = FlowSchedule(
            registration_id=registration_id,
            owner_id=owner_id,
            enabled=enabled,
            name=name,
            description=description,
            schedule_type=schedule_type,
            interval_seconds=interval_seconds,
            trigger_table_id=trigger_table_id,
        )
        schedule = self.repo.create_schedule(schedule)

        if schedule_type == "table_set_trigger" and trigger_table_ids:
            self.repo.set_trigger_table_ids(schedule.id, trigger_table_ids)

        return self._schedule_to_out(schedule)

    def update_schedule(
        self,
        schedule_id: int,
        enabled: bool | None = None,
        interval_seconds: int | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> FlowScheduleOut:
        """Update a schedule."""
        schedule = self.repo.get_schedule(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(schedule_id=schedule_id)
        if enabled is not None:
            schedule.enabled = enabled
        if interval_seconds is not None:
            validate_schedule_update(interval_seconds)
            schedule.interval_seconds = interval_seconds
        if name is not None:
            schedule.name = name
        if description is not None:
            schedule.description = description
        schedule = self.repo.update_schedule(schedule)
        return self._schedule_to_out(schedule)

    def delete_schedule(self, schedule_id: int) -> None:
        """Delete a schedule and its associated trigger table links."""
        schedule = self.repo.get_schedule(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(schedule_id=schedule_id)
        self.repo.delete_schedule(schedule_id)

    def get_schedule(self, schedule_id: int) -> FlowScheduleOut:
        """Get a schedule by ID."""
        schedule = self.repo.get_schedule(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(schedule_id=schedule_id)
        return self._schedule_to_out(schedule)

    def list_schedules(self, registration_id: int | None = None) -> list[FlowScheduleOut]:
        """List schedules, optionally filtered by flow."""
        schedules = self.repo.list_schedules(registration_id=registration_id)
        return [self._schedule_to_out(s) for s in schedules]

    def trigger_schedule_now(self, schedule_id: int, user_id: int) -> FlowRunOut:
        """Manually trigger a scheduled flow immediately."""
        schedule = self.repo.get_schedule(schedule_id)
        if schedule is None:
            raise ScheduleNotFoundError(schedule_id=schedule_id)

        flow = self.repo.get_flow(schedule.registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=schedule.registration_id)

        if self.repo.has_active_run(schedule.registration_id):
            raise FlowAlreadyRunningError(registration_id=schedule.registration_id)

        run = self._runs.spawn_flow_run(flow, user_id=user_id, run_type="on_demand", schedule_id=schedule.id)
        return self._runs.run_to_out(run)

    def fire_table_trigger_schedules(self, table_id: int, table_updated_at: datetime) -> int:
        """Fire enabled table_trigger schedules watching *table_id* (push path).

        This is the **push path** of the dual trigger mechanism for
        ``table_trigger`` schedules.  It runs synchronously inside
        ``overwrite_table_data`` — i.e. immediately when a catalog table's
        data is replaced — so the downstream flow starts without waiting
        for the next scheduler poll tick.

        A parallel **poll path** exists in
        ``FlowScheduler._process_table_trigger_schedules`` (engine.py).
        The poll path runs every ~30 s and compares
        ``CatalogTable.updated_at`` against
        ``FlowSchedule.last_trigger_table_updated_at``.  It acts as a
        **safety net**: if the push path fails (exception, process crash,
        etc.) the poll path will still detect the table change on its next
        tick and launch the flow.

        Double-firing is prevented by two guards:

        1. ``has_active_run`` — if the push path already spawned a
           subprocess, the poll path (and any concurrent push) sees an
           active run and skips the schedule.
        2. ``last_trigger_table_updated_at`` — the push path commits
           this timestamp (equal to the table's ``updated_at``) via
           ``repo.update_schedule`` *before* returning.  When the poll
           path later compares ``table.updated_at`` against this value
           it finds them equal and skips the schedule.

        Returns the number of flows launched.
        """
        schedules = self.repo.list_table_trigger_schedules_for_table(table_id)
        launched = 0
        for schedule in schedules:
            flow = self.repo.get_flow(schedule.registration_id)
            if flow is None:
                logger.warning("Schedule %s references missing flow %s", schedule.id, schedule.registration_id)
                continue

            if self.repo.has_active_run(schedule.registration_id):
                logger.info("Skipping push trigger for flow %s — active run exists", schedule.registration_id)
                continue

            schedule.last_triggered_at = datetime.now(timezone.utc)
            schedule.last_trigger_table_updated_at = table_updated_at
            self.repo.update_schedule(schedule)

            run = self._runs.spawn_flow_run(
                flow, user_id=schedule.owner_id, run_type="scheduled", schedule_id=schedule.id
            )
            if run.pid is not None:
                launched += 1

        return launched

    def safely_fire_table_trigger_schedules(self, table_id: int, table_updated_at: datetime) -> None:
        """Best-effort push-trigger fan-out.

        Broad catch is intentional: the poll path in ``FlowScheduler`` is
        the safety net if push fails. Logging here gives visibility without
        surfacing a transient worker hiccup as a 500 to the route caller.
        """
        try:
            self.fire_table_trigger_schedules(table_id, table_updated_at)
        except Exception:
            logger.exception("Push trigger fan-out failed for table %s", table_id)

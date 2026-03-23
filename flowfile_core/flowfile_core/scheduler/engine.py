"""Scheduler engine that polls for due schedules and triggers flow runs.

The scheduler runs as an asyncio background task. Every poll cycle it checks:
1. Interval schedules: has enough time elapsed since last trigger?
2. Table-trigger schedules: has the trigger table been updated since last check?

Runs are executed as subprocesses via ``flowfile run flow <path> --run-id <id>``
to isolate each run in its own process.
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
from datetime import datetime, timezone

from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.service import CatalogService
from flowfile_core.database.connection import get_db_context

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 30


class FlowScheduler:
    """Background scheduler that triggers flow runs based on schedules."""

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        """Start the scheduler background task."""
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("FlowScheduler started (poll interval=%ds)", _POLL_INTERVAL_SECONDS)

    async def stop(self) -> None:
        """Stop the scheduler background task."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("FlowScheduler stopped")

    async def _poll_loop(self) -> None:
        """Main loop: sleep then check schedules."""
        while self._running:
            try:
                await asyncio.sleep(_POLL_INTERVAL_SECONDS)
                await asyncio.to_thread(self._check_and_trigger)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Scheduler poll error")

    def _check_and_trigger(self) -> None:
        """Single poll tick: check all due schedules and trigger runs."""
        now = datetime.now(timezone.utc)

        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            service = CatalogService(repo)

            # -- Interval schedules --
            for schedule in repo.list_due_interval_schedules():
                try:
                    self._check_interval_schedule(schedule, now, service, repo)
                except Exception:
                    logger.exception("Error checking interval schedule %d", schedule.id)

            # -- Table trigger schedules --
            for schedule in repo.list_table_trigger_schedules():
                try:
                    self._check_table_trigger_schedule(schedule, now, service, repo)
                except Exception:
                    logger.exception("Error checking table trigger schedule %d", schedule.id)

    def _check_interval_schedule(
        self,
        schedule,
        now: datetime,
        service: CatalogService,
        repo: SQLAlchemyCatalogRepository,
    ) -> None:
        """Check if an interval schedule is due and trigger if so."""
        if schedule.interval_seconds is None:
            return

        if schedule.last_triggered_at is not None:
            last = schedule.last_triggered_at
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            elapsed = (now - last).total_seconds()
            if elapsed < schedule.interval_seconds:
                return

        # Skip if flow already has an active run
        if repo.has_active_run(schedule.registration_id):
            logger.debug(
                "Skipping interval schedule %d: flow %d already running",
                schedule.id,
                schedule.registration_id,
            )
            return

        self._trigger_scheduled_run(schedule, now, service, repo)

    def _check_table_trigger_schedule(
        self,
        schedule,
        now: datetime,
        service: CatalogService,
        repo: SQLAlchemyCatalogRepository,
    ) -> None:
        """Check if a table-trigger schedule's table has been updated."""
        if schedule.trigger_table_id is None:
            return

        table = repo.get_table(schedule.trigger_table_id)
        if table is None:
            logger.warning(
                "Table trigger schedule %d references missing table %d",
                schedule.id,
                schedule.trigger_table_id,
            )
            return

        table_updated_at = table.updated_at
        if table_updated_at is not None and table_updated_at.tzinfo is None:
            table_updated_at = table_updated_at.replace(tzinfo=timezone.utc)

        # Check if table has been updated since last trigger
        if schedule.last_trigger_table_updated_at is not None:
            last = schedule.last_trigger_table_updated_at
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            if table_updated_at is None or table_updated_at <= last:
                return

        # Skip if flow already has an active run
        if repo.has_active_run(schedule.registration_id):
            logger.debug(
                "Skipping table trigger schedule %d: flow %d already running",
                schedule.id,
                schedule.registration_id,
            )
            return

        # Record the table's updated_at so we don't re-trigger
        schedule.last_trigger_table_updated_at = table_updated_at
        self._trigger_scheduled_run(schedule, now, service, repo)

    def _trigger_scheduled_run(
        self,
        schedule,
        now: datetime,
        service: CatalogService,
        repo: SQLAlchemyCatalogRepository,
    ) -> None:
        """Create a run record and spawn a subprocess to execute the flow."""
        flow = repo.get_flow(schedule.registration_id)
        if flow is None:
            logger.warning("Schedule %d references missing flow %d", schedule.id, schedule.registration_id)
            return

        # Create the run record (phase 1)
        db_run = service.start_run(
            registration_id=flow.id,
            flow_name=flow.name,
            flow_path=flow.flow_path,
            user_id=schedule.owner_id,
            number_of_nodes=0,  # will be updated by the subprocess
            run_type="scheduled",
        )

        # Update schedule tracking
        schedule.last_triggered_at = now
        repo.update_schedule(schedule)

        # Spawn subprocess
        cmd = [
            sys.executable,
            "-m",
            "flowfile",
            "run",
            "flow",
            flow.flow_path,
            "--run-id",
            str(db_run.id),
        ]
        logger.info(
            "Triggering scheduled run: schedule=%d, flow=%s, run_id=%d, cmd=%s",
            schedule.id,
            flow.name,
            db_run.id,
            " ".join(cmd),
        )

        try:
            subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception:
            logger.exception("Failed to spawn subprocess for schedule %d", schedule.id)
            # Mark the run as failed
            try:
                service.complete_run(run_id=db_run.id, success=False, nodes_completed=0)
            except Exception:
                logger.exception("Failed to mark run %d as failed", db_run.id)

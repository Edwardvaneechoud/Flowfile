"""FlowScheduler — polls for due schedules and spawns flow runs.

This module is intentionally free of flowfile_core imports.  It talks
directly to the shared SQLite database using lightweight SQLAlchemy
table reflections defined in ``flowfile_scheduler.models``.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
import zoneinfo
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from croniter import croniter
from sqlalchemy.orm import Session, sessionmaker

from flowfile_scheduler.models import (
    CatalogTable,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    SchedulerLock,
    ScheduleTriggerTable,
)
from shared.database import create_catalog_engine
from shared.notifications.processor import process_pending_notifications
from shared.run_completion import reap_orphaned_runs, terminate_pid
from shared.run_logs import cleanup_old_logs
from shared.storage_config import get_database_url, require_flow_in_storage

logger = logging.getLogger("flowfile.scheduler")

DEFAULT_POLL_INTERVAL = 30

STALE_THRESHOLD = 90

LOG_SWEEP_INTERVAL = 3600


class LaunchOutcome(Enum):
    """Result of a ``_maybe_launch`` attempt.

    ``SKIPPED`` (the active-run guard declined) and ``FAILED`` (missing
    registration or a spawn that failed) are distinct because trigger watermarks
    must advance on ``FAILED`` but not on ``SKIPPED``.
    """

    LAUNCHED = "launched"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(frozen=True)
class Launch:
    """What a launcher answers for one fire of a schedule.

    ``pid`` is the process it started on this machine. ``remote_run_id`` is the run's id where it was sent instead:
    such a run has no process here, and the id on its record keeps the orphan reaper from closing it as dead.
    ``refused`` is the reason the schedule may not run at all: the scheduler disables it with that reason and
    records no run. ``error`` is why this one fire failed; so does an answer with neither a pid nor a run id.
    """

    pid: int | None = None
    remote_run_id: str | None = None
    refused: str | None = None
    error: str | None = None


Launcher = Callable[[Session, FlowSchedule, FlowRegistration, datetime], Launch]
"""Starts one run of a due schedule's flow, given the session, the schedule, the flow and the fire time."""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FlowScheduler:
    """Async scheduler that polls for due schedules and launches flows.

    ``launcher`` starts a due schedule's run in place of the subprocess spawned here; without one a run is spawned
    on this machine from the flow's file.
    """

    def __init__(self, poll_interval: int = DEFAULT_POLL_INTERVAL, launcher: Launcher | None = None):
        self._poll_interval = poll_interval
        self._launcher = launcher
        self._holder_id = uuid.uuid4().hex[:12]
        self._task: asyncio.Task | None = None
        self._stopping = False
        # None, not 0.0: time.monotonic() is seconds-since-boot on Linux, so a
        # zero seed would skip the first sweep for an hour on a freshly booted host.
        self._last_log_sweep: float | None = None

        self._engine = create_catalog_engine(get_database_url())
        self._session_factory = sessionmaker(bind=self._engine)

        # Ensure scheduler-specific tables exist (safe no-op if they already do)
        from flowfile_scheduler.models import Base

        Base.metadata.create_all(self._engine, checkfirst=True)

        logger.info("Scheduler %s targeting %s", self._holder_id, self._engine.url)

    # Public API (called from core's lifespan)

    async def start(self) -> None:
        """Start the background polling loop. Can only be called once."""
        if self._task is not None:
            raise RuntimeError("Scheduler has already been started")
        self._stopping = False
        self._task = asyncio.get_event_loop().create_task(self._run_loop())
        logger.info("Scheduler %s started", self._holder_id)

    async def run_once(self) -> None:
        """Execute a single scheduler tick and return.

        Acquires the lock, processes all due schedules, then releases
        the lock. Useful for testing or one-shot CLI invocations.
        """
        logger.info("Scheduler %s running single tick", self._holder_id)
        await asyncio.to_thread(self._tick)
        self._release_lock()
        logger.info("Scheduler %s single tick complete", self._holder_id)

    async def stop(self) -> None:
        """Signal the loop to stop and release the lock."""
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._release_lock()
        logger.info("Scheduler %s stopped", self._holder_id)

    # Main loop

    async def _run_loop(self) -> None:
        while not self._stopping:
            try:
                await asyncio.to_thread(self._tick)
            except Exception:
                logger.exception("Scheduler tick failed")
            await asyncio.sleep(self._poll_interval)

    def _tick(self) -> None:
        """Single scheduler tick — acquire lock, check schedules, launch."""
        with self._session_factory() as db:
            if not self._acquire_lock(db):
                logger.info("Tick skipped — lock held by another instance")
                return
            try:
                reap_orphaned_runs()
            except Exception:
                logger.exception("Orphaned-run reap failed")

            # Throttled: the tick runs every 30s and the sweep globs the logs dir.
            now_monotonic = time.monotonic()
            if self._last_log_sweep is None or now_monotonic - self._last_log_sweep > LOG_SWEEP_INTERVAL:
                self._last_log_sweep = now_monotonic
                try:
                    cleanup_old_logs()
                except Exception:
                    logger.exception("Log retention sweep failed")

            # Evaluates newly-ended runs into the outbox and drains it over webhooks.
            try:
                process_pending_notifications()
            except Exception:
                logger.exception("Notification processing failed")

            launched = self._process_interval_schedules(db)
            launched += self._process_cron_schedules(db)
            launched += self._process_table_trigger_schedules(db)
            launched += self._process_table_set_trigger_schedules(db)
            if launched:
                logger.info("Tick complete — launched %d flow(s)", launched)
            else:
                logger.info("Tick complete — no schedules due")

    # Lock management

    def _acquire_lock(self, db: Session) -> bool:
        """Try to acquire or refresh the advisory lock.

        Returns ``True`` if this instance holds the lock.
        """
        now = _utcnow()
        lock: SchedulerLock | None = db.get(SchedulerLock, 1)

        if lock is None:
            lock = SchedulerLock(id=1, holder_id=self._holder_id, started_at=now, heartbeat_at=now)
            db.add(lock)
            db.commit()
            return True

        if lock.holder_id == self._holder_id:
            lock.heartbeat_at = now
            db.commit()
            return True

        if lock.heartbeat_at is not None:
            elapsed = (now - lock.heartbeat_at.replace(tzinfo=timezone.utc)).total_seconds()
            if elapsed > STALE_THRESHOLD:
                logger.warning("Taking over stale lock from %s (%.0fs old)", lock.holder_id, elapsed)
                lock.holder_id = self._holder_id
                lock.started_at = now
                lock.heartbeat_at = now
                db.commit()
                return True

        return False

    def _release_lock(self) -> None:
        try:
            with self._session_factory() as db:
                lock: SchedulerLock | None = db.get(SchedulerLock, 1)
                if lock is not None and lock.holder_id == self._holder_id:
                    db.delete(lock)
                    db.commit()
        except Exception:
            logger.exception("Failed to release scheduler lock")

    # Interval schedules

    def _process_interval_schedules(self, db: Session) -> int:
        schedules: list[FlowSchedule] = (
            db.query(FlowSchedule)
            .filter(FlowSchedule.enabled.is_(True), FlowSchedule.schedule_type == "interval")
            .all()
        )
        logger.info("Evaluating %d interval schedule(s)", len(schedules))

        launched = 0
        now = _utcnow()
        for sched in schedules:
            if sched.interval_seconds is None:
                continue

            if sched.last_triggered_at is not None:
                last = sched.last_triggered_at.replace(tzinfo=timezone.utc)
                elapsed = (now - last).total_seconds()
                remaining = sched.interval_seconds - elapsed
                if remaining > 0:
                    logger.info("Schedule %s not due yet (%.0fs remaining)", sched.id, remaining)
                    continue

            if self._maybe_launch(db, sched, now) is LaunchOutcome.LAUNCHED:
                launched += 1
        return launched

    # Cron schedules

    def _process_cron_schedules(self, db: Session) -> int:
        """Evaluate cron schedules and launch those whose next fire time has passed.

        Cron is evaluated in the schedule's own timezone as *naive local
        wall-clock* time: ``last_cron_slot`` holds the local-time cursor and
        ``croniter`` advances it, so "every day at 2 AM" fires at 2 AM local
        and stays correct across DST.  Because local wall-clock carries no UTC
        offset, the repeated hour of a fall-back transition is a single naive
        instant — "02:30" fires once, not twice — and a skipped spring-forward
        time is simply caught at the next tick.

        The cursor is advanced to *now* on each fire (not to the slot), so a
        scheduler that was down catches up with a single fire rather than
        backfilling every missed slot.  ``last_triggered_at`` continues to
        record the actual UTC fire time (used for display).
        """
        schedules: list[FlowSchedule] = (
            db.query(FlowSchedule)
            .filter(FlowSchedule.enabled.is_(True), FlowSchedule.schedule_type == "cron")
            .all()
        )
        logger.debug("Evaluating %d cron schedule(s)", len(schedules))

        launched = 0
        now = _utcnow()
        for sched in schedules:
            if not sched.cron_expression:
                logger.warning("Cron schedule %s has no expression, skipping", sched.id)
                continue

            try:
                tz = zoneinfo.ZoneInfo(sched.cron_timezone or "UTC")
            except Exception:
                logger.warning(
                    "Cron schedule %s has invalid timezone %r, skipping", sched.id, sched.cron_timezone
                )
                continue

            # Cron runs in *naive local wall-clock* time: the cursor and
            # croniter both work in local time with no UTC offset, so the
            # repeated hour of a fall-back transition is a single instant
            # ("02:30" fires once) and skipped spring-forward times are caught
            # at the next tick.
            base_local = sched.last_cron_slot
            if base_local is None:
                # Never fired under the cursor model (or a pre-existing row):
                # seed from the last real trigger / creation time in local time.
                base_utc = sched.last_triggered_at or sched.created_at
                if base_utc is None:
                    logger.warning(
                        "Cron schedule %s has no last_triggered_at/created_at, skipping", sched.id
                    )
                    continue
                try:
                    base_local = base_utc.replace(tzinfo=timezone.utc).astimezone(tz).replace(tzinfo=None)
                except Exception:
                    logger.warning("Cron schedule %s has an unusable base timestamp, skipping", sched.id)
                    continue

            try:
                next_run = croniter(sched.cron_expression, base_local).get_next(datetime)
            except Exception:
                logger.warning(
                    "Cron schedule %s has invalid expression %r, skipping", sched.id, sched.cron_expression
                )
                continue

            now_local = now.astimezone(tz).replace(tzinfo=None)
            if next_run <= now_local:
                logger.info(
                    "Cron schedule %s due (next_run=%s local, now=%s) — triggering", sched.id, next_run, now
                )
                if self._maybe_launch(db, sched, now) is LaunchOutcome.LAUNCHED:
                    # Advance the cursor to *now* (not next_run) so a scheduler
                    # that was down catches up with a single fire.
                    sched.last_cron_slot = now_local
                    db.commit()
                    launched += 1
            else:
                logger.debug("Cron schedule %s not due yet (next_run=%s local)", sched.id, next_run)
        return launched

    # Table-trigger schedules

    def _process_table_trigger_schedules(self, db: Session) -> int:
        """Detect table changes and launch table_trigger flows (poll path).

        This is the **poll path** of the dual trigger mechanism for
        ``table_trigger`` schedules.  It runs on every scheduler tick
        (~30 s) and compares each watched table's ``updated_at`` against
        the schedule's ``last_trigger_table_updated_at``.

        A parallel **push path** exists in
        ``CatalogService._fire_table_trigger_schedules`` (service.py).
        The push path fires synchronously inside ``overwrite_table_data``
        and is the primary/fast trigger.  This poll path serves as a
        **safety net** — if the push path fails (exception, process crash,
        network error, etc.) this method will still detect the stale
        timestamp and launch the flow on the next tick.

        Double-firing is prevented by two guards:

        1. ``has_active_run`` (checked inside ``_maybe_launch``) — if the
           push path already spawned a run, ``_maybe_launch`` sees it and
           skips.
        2. ``last_trigger_table_updated_at`` — the push path commits this
           value equal to ``table.updated_at`` before returning, so this
           method's ``table_updated > last_seen`` comparison evaluates to
           ``False`` and the schedule is skipped.

        A NULL watermark means "never observed", not "changed": the first tick
        that sees such a schedule arms it with the table's current
        ``updated_at`` and launches nothing.
        """
        schedules: list[FlowSchedule] = (
            db.query(FlowSchedule)
            .filter(FlowSchedule.enabled.is_(True), FlowSchedule.schedule_type == "table_trigger")
            .all()
        )
        logger.info("Evaluating %d table-trigger schedule(s)", len(schedules))

        launched = 0
        for sched in schedules:
            if sched.trigger_table_id is None:
                continue

            table: CatalogTable | None = db.get(CatalogTable, sched.trigger_table_id)
            if table is None:
                logger.warning("Schedule %s references missing table %s", sched.id, sched.trigger_table_id)
                continue

            table_updated = table.updated_at.replace(tzinfo=timezone.utc) if table.updated_at else None
            last_seen = (
                sched.last_trigger_table_updated_at.replace(tzinfo=timezone.utc)
                if sched.last_trigger_table_updated_at
                else None
            )

            if table_updated is None:
                continue

            if last_seen is None:
                # Arm, don't replay: a pre-existing updated_at is not a change we saw happen.
                sched.last_trigger_table_updated_at = table_updated
                db.commit()
                continue

            if table_updated <= last_seen:
                continue

            logger.info(
                "Table '%s' (id=%s) updated at %s (last seen %s) — triggering schedule %s",
                table.name,
                table.id,
                table_updated,
                last_seen,
                sched.id,
            )
            outcome = self._maybe_launch(db, sched, _utcnow())
            if outcome is LaunchOutcome.SKIPPED:
                # Leave the watermark so the trigger re-fires once the blocking run ends.
                continue

            sched.last_trigger_table_updated_at = table_updated
            db.commit()
            if outcome is LaunchOutcome.LAUNCHED:
                launched += 1
        return launched

    # Table-set-trigger schedules

    def _process_table_set_trigger_schedules(self, db: Session) -> int:
        """Launch when *every* linked table has advanced past the watermark.

        Like the single-table path, the watermark is
        ``last_trigger_table_updated_at`` and is armed (never replayed) the first
        time a schedule is seen. It always carries an *observed*
        ``max(table.updated_at)`` — never ``now()``, which would swallow a write
        landing between the reads and the stamp.
        """
        schedules: list[FlowSchedule] = (
            db.query(FlowSchedule)
            .filter(FlowSchedule.enabled.is_(True), FlowSchedule.schedule_type == "table_set_trigger")
            .all()
        )
        logger.info("Evaluating %d table-set-trigger schedule(s)", len(schedules))

        launched = 0
        for sched in schedules:
            trigger_links: list[ScheduleTriggerTable] = (
                db.query(ScheduleTriggerTable).filter(ScheduleTriggerTable.schedule_id == sched.id).all()
            )
            table_ids = [link.table_id for link in trigger_links]
            if len(table_ids) < 2:
                logger.warning("Schedule %s has fewer than 2 trigger tables, skipping", sched.id)
                continue

            last_seen = (
                sched.last_trigger_table_updated_at.replace(tzinfo=timezone.utc)
                if sched.last_trigger_table_updated_at
                else None
            )

            stamps: list[datetime] = []
            for tid in table_ids:
                table: CatalogTable | None = db.get(CatalogTable, tid)
                if table is None:
                    logger.warning("Schedule %s references missing table %s", sched.id, tid)
                    break
                if table.updated_at is None:
                    break
                stamps.append(table.updated_at.replace(tzinfo=timezone.utc))
            if len(stamps) != len(table_ids):
                continue

            observed = max(stamps)
            if last_seen is None:
                # Arm, don't replay: pre-existing timestamps are not changes we saw happen.
                sched.last_trigger_table_updated_at = observed
                db.commit()
                continue

            if not all(stamp > last_seen for stamp in stamps):
                continue

            logger.info(
                "All %d trigger tables updated for schedule %s — triggering",
                len(table_ids),
                sched.id,
            )
            outcome = self._maybe_launch(db, sched, _utcnow())
            if outcome is LaunchOutcome.SKIPPED:
                # Leave the watermark so the trigger re-fires once the blocking run ends.
                continue

            sched.last_trigger_table_updated_at = observed
            db.commit()
            if outcome is LaunchOutcome.LAUNCHED:
                launched += 1
        return launched

    # Launch helpers

    def _has_active_run(self, db: Session, reg: FlowRegistration) -> bool:
        """Whether the flow has an unfinished run, matched on either key.

        The ``flow_uuid`` arm mirrors core's registration-scoped queries; the
        ``registration_id`` arm still counts rows written before runs carried a uuid
        (a NULL uuid never satisfies the equality, so the OR stays safe).
        """
        return (
            db.query(FlowRun)
            .filter(
                (FlowRun.registration_id == reg.id) | (FlowRun.flow_uuid == reg.flow_uuid),
                FlowRun.ended_at.is_(None),
            )
            .first()
            is not None
        )

    def _maybe_launch(self, db: Session, sched: FlowSchedule, now: datetime) -> LaunchOutcome:
        """Create a run record and spawn the flow if no run is active."""
        reg: FlowRegistration | None = db.get(FlowRegistration, sched.registration_id)
        if reg is None:
            logger.warning("Schedule %s references missing flow registration %s", sched.id, sched.registration_id)
            return LaunchOutcome.FAILED

        if self._has_active_run(db, reg):
            logger.info("Skipping schedule %s — flow '%s' already has an active run", sched.id, reg.name)
            return LaunchOutcome.SKIPPED

        launch: Launch | None = None
        flow_path = reg.flow_path
        if self._launcher is not None:
            try:
                launch = self._launcher(db, sched, reg, now)
            except Exception as exc:
                logger.exception("Launcher raised for schedule %s", sched.id)
                launch = Launch(error=str(exc))
            if launch.refused is not None:
                logger.warning("Schedule %s disabled — %s", sched.id, launch.refused)
                sched.enabled = False
                sched.disabled_reason = launch.refused
                db.commit()
                return LaunchOutcome.FAILED
        else:
            try:
                flow_path = str(require_flow_in_storage(Path(reg.flow_path)))
            except ValueError as exc:
                # On a multi-user server a flow outside the flow folders is not run, scheduled or not.
                logger.warning("Schedule %s not launched — flow '%s': %s", sched.id, reg.name, exc)
                return LaunchOutcome.FAILED

        # Create a run record *before* spawning so it shows as active
        run = FlowRun(
            registration_id=reg.id,
            flow_uuid=reg.flow_uuid,
            flow_name=reg.name,
            flow_path=reg.flow_path,
            user_id=sched.owner_id,
            started_at=now,
            number_of_nodes=0,
            run_type="scheduled",
            schedule_id=sched.id,
            remote_run_id=launch.remote_run_id if launch else None,
        )
        db.add(run)
        sched.last_triggered_at = now
        db.commit()
        db.refresh(run)

        logger.info(
            "Launching schedule %s → flow '%s' (registration=%s, run=%s, path=%s)",
            sched.id,
            reg.name,
            reg.id,
            run.id,
            flow_path,
        )

        if launch is not None:
            pid = launch.pid
            launched = launch.error is None and (pid is not None or launch.remote_run_id is not None)
        else:
            try:
                pid = self._spawn_flow(flow_path, run.id, backend=sched.backend)
            except Exception:
                logger.exception("Spawn raised for run %s", run.id)
                pid = None
            launched = pid is not None

        if not launched:
            logger.error("Failed to launch run %s — marking as failed%s", run.id, f": {launch.error}" if launch else "")
            run.ended_at = _utcnow()
            run.success = False
            db.commit()
            return LaunchOutcome.FAILED

        run.pid = pid
        db.commit()
        db.refresh(run)
        if run.ended_at is not None and pid is not None:
            # Cancelled while it was starting: that cancel saw no pid to signal.
            logger.info("Run %s was cancelled while starting: %s", run.id, terminate_pid(pid))
        return LaunchOutcome.LAUNCHED

    def _spawn_flow(self, flow_path: str, run_id: int, backend: str | None = None) -> int | None:
        """Fire-and-forget a ``flowfile run flow`` subprocess.

        ``backend`` is the schedule's backend id, passed as ``--backend``; ``None`` spawns the command unchanged.
        Returns the child PID on success, or ``None`` on failure.
        """
        from shared.subprocess_utils import spawn_flow_subprocess

        return spawn_flow_subprocess(flow_path, run_id, backend=backend)

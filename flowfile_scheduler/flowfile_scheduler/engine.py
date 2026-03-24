"""FlowScheduler — polls for due schedules and spawns flow runs.

This module is intentionally free of flowfile_core imports.  It talks
directly to the shared SQLite database using lightweight SQLAlchemy
table reflections defined in ``flowfile_scheduler.models``.
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from flowfile_scheduler.models import (
    CatalogTable,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    SchedulerLock,
    ScheduleTriggerTable,
)

logger = logging.getLogger("flowfile.scheduler")

# How often the main loop runs (seconds)
DEFAULT_POLL_INTERVAL = 30

# A lock heartbeat older than this is considered stale (seconds)
STALE_THRESHOLD = 90


def _get_database_url() -> str:
    """Resolve the database URL using the same logic as flowfile_core.

    Reads ``FLOWFILE_DB_PATH`` or falls back to
    ``~/.flowfile/database/flowfile.db``.
    """
    import os

    custom = os.environ.get("FLOWFILE_DB_PATH")
    if custom:
        return f"sqlite:///{custom}"

    storage_dir = os.environ.get("FLOWFILE_STORAGE_DIR")
    if storage_dir:
        db_path = Path(storage_dir) / "database" / "flowfile.db"
    elif os.environ.get("FLOWFILE_MODE") == "docker":
        db_path = Path("/app/internal_storage/database/flowfile.db")
    else:
        db_path = Path.home() / ".flowfile" / "database" / "flowfile.db"

    return f"sqlite:///{db_path}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FlowScheduler:
    """Async scheduler that polls for due schedules and launches flows."""

    def __init__(self, poll_interval: int = DEFAULT_POLL_INTERVAL):
        self._poll_interval = poll_interval
        self._holder_id = uuid.uuid4().hex[:12]
        self._task: asyncio.Task | None = None
        self._stopping = False

        url = _get_database_url()
        connect_args = {"check_same_thread": False} if "sqlite" in url else {}
        self._engine = create_engine(url, connect_args=connect_args)
        self._session_factory = sessionmaker(bind=self._engine)

        # Ensure scheduler-specific tables exist (safe no-op if they already do)
        from flowfile_scheduler.models import Base

        Base.metadata.create_all(self._engine, checkfirst=True)

        logger.info("Scheduler %s targeting %s", self._holder_id, url)

    # ------------------------------------------------------------------
    # Public API (called from core's lifespan)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

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
            launched = self._process_interval_schedules(db)
            launched += self._process_table_trigger_schedules(db)
            launched += self._process_table_set_trigger_schedules(db)
            if launched:
                logger.info("Tick complete — launched %d flow(s)", launched)
            else:
                logger.info("Tick complete — no schedules due")

    # ------------------------------------------------------------------
    # Lock management
    # ------------------------------------------------------------------

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

        # Another holder — check staleness
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

    # ------------------------------------------------------------------
    # Interval schedules
    # ------------------------------------------------------------------

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

            if self._maybe_launch(db, sched, now):
                launched += 1
        return launched

    # ------------------------------------------------------------------
    # Table-trigger schedules
    # ------------------------------------------------------------------

    def _process_table_trigger_schedules(self, db: Session) -> int:
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

            if table_updated is not None and (last_seen is None or table_updated > last_seen):
                logger.info(
                    "Table '%s' (id=%s) updated at %s (last seen %s) — triggering schedule %s",
                    table.name,
                    table.id,
                    table_updated,
                    last_seen,
                    sched.id,
                )
                now = _utcnow()
                sched.last_trigger_table_updated_at = table_updated
                if self._maybe_launch(db, sched, now):
                    launched += 1
        return launched

    # ------------------------------------------------------------------
    # Table-set-trigger schedules
    # ------------------------------------------------------------------

    def _process_table_set_trigger_schedules(self, db: Session) -> int:
        schedules: list[FlowSchedule] = (
            db.query(FlowSchedule)
            .filter(FlowSchedule.enabled.is_(True), FlowSchedule.schedule_type == "table_set_trigger")
            .all()
        )
        logger.info("Evaluating %d table-set-trigger schedule(s)", len(schedules))

        launched = 0
        for sched in schedules:
            # Load linked table IDs from the join table
            trigger_links: list[ScheduleTriggerTable] = (
                db.query(ScheduleTriggerTable).filter(ScheduleTriggerTable.schedule_id == sched.id).all()
            )
            table_ids = [link.table_id for link in trigger_links]
            if len(table_ids) < 2:
                logger.warning("Schedule %s has fewer than 2 trigger tables, skipping", sched.id)
                continue

            last_triggered = sched.last_triggered_at.replace(tzinfo=timezone.utc) if sched.last_triggered_at else None

            # Check if ALL tables have been updated since last trigger
            all_updated = True
            for tid in table_ids:
                table: CatalogTable | None = db.get(CatalogTable, tid)
                if table is None:
                    logger.warning("Schedule %s references missing table %s", sched.id, tid)
                    all_updated = False
                    break

                table_updated = table.updated_at.replace(tzinfo=timezone.utc) if table.updated_at else None
                if table_updated is None:
                    all_updated = False
                    break
                if last_triggered is not None and table_updated <= last_triggered:
                    all_updated = False
                    break

            if all_updated:
                logger.info(
                    "All %d trigger tables updated for schedule %s — triggering",
                    len(table_ids),
                    sched.id,
                )
                now = _utcnow()
                if self._maybe_launch(db, sched, now):
                    launched += 1
        return launched

    # ------------------------------------------------------------------
    # Launch helpers
    # ------------------------------------------------------------------

    def _has_active_run(self, db: Session, registration_id: int) -> bool:
        return (
            db.query(FlowRun).filter(FlowRun.registration_id == registration_id, FlowRun.ended_at.is_(None)).first()
            is not None
        )

    def _maybe_launch(self, db: Session, sched: FlowSchedule, now: datetime) -> bool:
        """Create a run record and spawn the flow if no run is active.

        Returns ``True`` if a flow was launched.
        """
        reg: FlowRegistration | None = db.get(FlowRegistration, sched.registration_id)
        if reg is None:
            logger.warning("Schedule %s references missing flow registration %s", sched.id, sched.registration_id)
            return False

        if self._has_active_run(db, sched.registration_id):
            logger.info("Skipping schedule %s — flow '%s' already has an active run", sched.id, reg.name)
            return False

        # Create a run record *before* spawning so it shows as active
        run = FlowRun(
            registration_id=reg.id,
            flow_name=reg.name,
            flow_path=reg.flow_path,
            user_id=sched.owner_id,
            started_at=now,
            number_of_nodes=0,
            run_type="scheduled",
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
            reg.flow_path,
        )

        self._spawn_flow(reg.flow_path, run.id)
        return True

    def _spawn_flow(self, flow_path: str, run_id: int) -> None:
        """Fire-and-forget a ``flowfile run flow`` subprocess."""
        cmd = [sys.executable, "-m", "flowfile", "run", "flow", flow_path, "--run-id", str(run_id)]
        logger.info("Spawning: %s", " ".join(cmd))
        try:
            log_dir = Path.home() / ".flowfile" / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"scheduled_run_{run_id}.log"
            fh = open(log_file, "w")  # noqa: SIM115
            subprocess.Popen(
                cmd,
                stdout=fh,
                stderr=fh,
                start_new_session=True,
            )
            fh.close()  # Parent releases its copy; child still has the fd
            logger.info("Subprocess log: %s", log_file)
        except Exception:
            logger.exception("Failed to spawn flow subprocess: %s", flow_path)

"""Flow-run lifecycle: create, complete, list, cancel, snapshot retrieval."""

from __future__ import annotations

import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

from flowfile_core.catalog.exceptions import (
    FlowAlreadyRunningError,
    FlowNotFoundError,
    NoSnapshotError,
    RunNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import run_to_out
from flowfile_core.database.models import FlowRegistration, FlowRun, RunType
from flowfile_core.schemas.catalog_schema import (
    ActiveFlowRun,
    FlowRunDetail,
    FlowRunOut,
    PaginatedFlowRuns,
)
from shared.subprocess_utils import spawn_flow_subprocess

logger = logging.getLogger(__name__)


class FlowRunService:
    """Owns FlowRun lifecycle: list, fetch, start, complete, cancel."""

    def __init__(self, repo: CatalogRepository) -> None:
        self.repo = repo

    @staticmethod
    def _resolve_log_path(run_id: int, run_type: str) -> str | None:
        """Return the log file path if it exists for subprocess-spawned runs."""
        if run_type not in ("scheduled", "manual", "on_demand"):
            return None
        log_file = Path.home() / ".flowfile" / "logs" / f"scheduled_run_{run_id}.log"
        if log_file.exists():
            return str(log_file)
        return None

    def run_to_out(self, run: FlowRun) -> FlowRunOut:
        """Convert a FlowRun ORM row to FlowRunOut, computing has_log via FS."""
        return run_to_out(run, has_log=self._resolve_log_path(run.id, run.run_type) is not None)

    def list_runs(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> PaginatedFlowRuns:
        """List run summaries (without snapshots) with total count for pagination."""
        runs = self.repo.list_runs(
            registration_id=registration_id,
            schedule_id=schedule_id,
            run_type=run_type,
            limit=limit,
            offset=offset,
        )
        counts = self.repo.count_runs_by_status(
            registration_id=registration_id, schedule_id=schedule_id, run_type=run_type
        )
        return PaginatedFlowRuns(
            items=[self.run_to_out(r) for r in runs],
            total=counts["total"],
            total_success=counts["success"],
            total_failed=counts["failed"],
            total_running=counts["running"],
        )

    def get_run_detail(self, run_id: int) -> FlowRunDetail:
        """Get a single run including the YAML snapshot."""
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        return FlowRunDetail(
            id=run.id,
            registration_id=run.registration_id,
            flow_name=run.flow_name,
            flow_path=run.flow_path,
            user_id=run.user_id,
            started_at=run.started_at,
            ended_at=run.ended_at,
            success=run.success,
            nodes_completed=run.nodes_completed,
            number_of_nodes=run.number_of_nodes,
            duration_seconds=run.duration_seconds,
            run_type=run.run_type,
            schedule_id=run.schedule_id,
            has_snapshot=run.flow_snapshot is not None,
            has_log=self._resolve_log_path(run.id, run.run_type) is not None,
            flow_snapshot=run.flow_snapshot,
            node_results_json=run.node_results_json,
        )

    def get_run(self, run_id: int) -> FlowRun:
        """Get a raw FlowRun model."""
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        return run

    def start_run(
        self,
        registration_id: int | None,
        flow_name: str,
        flow_path: str | None,
        user_id: int,
        number_of_nodes: int,
        run_type: RunType,
        flow_snapshot: str | None = None,
    ) -> FlowRun:
        """Record a new flow run start."""
        run = FlowRun(
            registration_id=registration_id,
            flow_name=flow_name,
            flow_path=flow_path,
            user_id=user_id,
            started_at=datetime.now(timezone.utc),
            number_of_nodes=number_of_nodes,
            run_type=run_type,
            flow_snapshot=flow_snapshot,
        )
        return self.repo.create_run(run)

    def complete_run(
        self,
        run_id: int,
        success: bool,
        nodes_completed: int,
        number_of_nodes: int | None = None,
        node_results_json: str | None = None,
    ) -> FlowRun:
        """Mark a run as completed."""
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        now = datetime.now(timezone.utc)
        run.ended_at = now
        run.success = success
        run.nodes_completed = nodes_completed
        if number_of_nodes is not None and number_of_nodes > 0:
            run.number_of_nodes = number_of_nodes
        if run.started_at:
            # SQLite stores naive datetimes, so started_at may lack tzinfo —
            # normalise both sides for the duration calc.
            started_utc = run.started_at.replace(tzinfo=None)
            now_utc = now.replace(tzinfo=None)
            run.duration_seconds = (now_utc - started_utc).total_seconds()
        if node_results_json is not None:
            run.node_results_json = node_results_json
        return self.repo.update_run(run)

    def create_completed_run(
        self,
        registration_id: int | None,
        flow_name: str,
        flow_path: str | None,
        user_id: int,
        started_at: datetime | None,
        ended_at: datetime | None,
        success: bool,
        nodes_completed: int,
        number_of_nodes: int,
        run_type: RunType,
        node_results_json: str | None = None,
        flow_snapshot: str | None = None,
    ) -> FlowRun:
        """Record a fully completed run in one step (fallback when start_run was skipped)."""
        duration = None
        if started_at and ended_at:
            duration = (ended_at - started_at).total_seconds()
        run = FlowRun(
            registration_id=registration_id,
            flow_name=flow_name,
            flow_path=flow_path,
            user_id=user_id,
            started_at=started_at,
            ended_at=ended_at,
            success=success,
            nodes_completed=nodes_completed,
            number_of_nodes=number_of_nodes,
            duration_seconds=duration,
            run_type=run_type,
            node_results_json=node_results_json,
            flow_snapshot=flow_snapshot,
        )
        return self.repo.create_run(run)

    def get_run_snapshot(self, run_id: int) -> str:
        """Return the flow snapshot text for a run."""
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        if not run.flow_snapshot:
            raise NoSnapshotError(run_id=run_id)
        return run.flow_snapshot

    @staticmethod
    def _spawn_flow_subprocess(flow_path: str, run_id: int) -> int | None:
        """Fire-and-forget a ``flowfile run flow`` subprocess.

        Returns the child PID on success, or ``None`` on failure.
        """
        return spawn_flow_subprocess(flow_path, run_id)

    def spawn_flow_run(
        self,
        flow: FlowRegistration,
        user_id: int,
        run_type: RunType,
        schedule_id: int | None = None,
    ) -> FlowRun:
        """Create a FlowRun record and spawn the subprocess.

        On spawn failure the run is marked as failed immediately.
        """
        now = datetime.now(timezone.utc)
        run = FlowRun(
            registration_id=flow.id,
            flow_name=flow.name,
            flow_path=flow.flow_path,
            user_id=user_id,
            started_at=now,
            number_of_nodes=0,
            run_type=run_type,
            schedule_id=schedule_id,
        )
        run = self.repo.create_run(run)

        pid = self._spawn_flow_subprocess(flow.flow_path, run.id)
        if pid is not None:
            run.pid = pid
        else:
            logger.error("Failed to spawn subprocess for run %s — marking as failed", run.id)
            run.ended_at = datetime.now(timezone.utc)
            run.success = False
        self.repo.update_run(run)
        return run

    def run_flow_now(self, registration_id: int, user_id: int) -> FlowRunOut:
        """Trigger a registered flow immediately without a schedule."""
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)

        if self.repo.has_active_run(registration_id):
            raise FlowAlreadyRunningError(registration_id=registration_id)

        run = self.spawn_flow_run(flow, user_id=user_id, run_type="manual")
        return self.run_to_out(run)

    def list_active_runs(self) -> list[ActiveFlowRun]:
        """List all currently running flows (ended_at IS NULL)."""
        runs = self.repo.list_active_runs()
        return [
            ActiveFlowRun(
                id=r.id,
                registration_id=r.registration_id,
                flow_name=r.flow_name,
                flow_path=r.flow_path,
                user_id=r.user_id,
                started_at=r.started_at,
                nodes_completed=r.nodes_completed,
                number_of_nodes=r.number_of_nodes,
                run_type=r.run_type,
            )
            for r in runs
        ]

    def cancel_run(self, run_id: int) -> None:
        """Cancel a running flow by terminating its subprocess and marking
        the database record as failed."""
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)

        if run.pid is not None:
            try:
                os.kill(run.pid, signal.SIGTERM)
                logger.info("Sent SIGTERM to pid %s for run %s", run.pid, run_id)
            except ProcessLookupError:
                logger.info("Process %s for run %s already exited", run.pid, run_id)
            except OSError:
                logger.warning("Failed to kill pid %s for run %s", run.pid, run_id, exc_info=True)

        now = datetime.now(timezone.utc)
        run.ended_at = now
        run.success = False
        if run.started_at:
            started_utc = run.started_at.replace(tzinfo=None)
            now_utc = now.replace(tzinfo=None)
            run.duration_seconds = (now_utc - started_utc).total_seconds()
        self.repo.update_run(run)

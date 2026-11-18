"""Flow-run lifecycle: create, complete, list, cancel, snapshot retrieval."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.orm.attributes import flag_modified

from flowfile_core.catalog import run_records
from flowfile_core.catalog.exceptions import (
    FlowAlreadyRunningError,
    FlowNotFoundError,
    NoSnapshotError,
    RunNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import run_cancelled, run_to_out
from flowfile_core.database.models import FlowRegistration, FlowRun, RunType
from flowfile_core.schemas.catalog_schema import (
    ActiveFlowRun,
    FlowRunDetail,
    FlowRunOut,
    PaginatedFlowRuns,
)
from shared.run_completion import terminate_pid
from shared.run_logs import LOGGED_RUN_TYPES, run_log_path
from shared.storage_config import require_flow_in_storage
from shared.subprocess_utils import spawn_flow_subprocess

if TYPE_CHECKING:
    from flowfile_core.catalog.access import AccessResolver

logger = logging.getLogger(__name__)


class FlowRunService:
    """Owns FlowRun lifecycle: list, fetch, start, complete, cancel.

    With a restricted ``access`` resolver, run history (lists, counts, active
    runs, run detail) covers only the runs the caller may see: runs they
    started, runs of a flow they own or were granted, and runs of a flow
    registered in a namespace they can see.
    """

    def __init__(self, repo: CatalogRepository, access: AccessResolver | None = None) -> None:
        self.repo = repo
        # None (internal callers, electron mode, tests) is unrestricted.
        self._access = access

    def visible_run_filters(self) -> dict:
        """Keyword filters that limit a repository run query to the caller's visible runs.

        Empty when the caller is unrestricted, so the query is unchanged.
        """
        if self._access is None or not self._access.restricted:
            return {}
        return {
            "visible_user_id": self._access.user_id,
            "visible_flow_ids": self._access.accessible_ids("flow"),
            "visible_namespace_ids": self._access.visible_namespace_ids(),
        }

    def is_visible(self, run: FlowRun) -> bool:
        """Whether the caller may see ``run``; the per-row form of ``visible_run_filters``."""
        if self._access is None or not self._access.restricted:
            return True
        if run.user_id == self._access.user_id:
            return True
        if run.registration_id is None:
            return False
        if run.registration_id in self._access.accessible_ids("flow"):
            return True
        reg = self.repo.get_flow(run.registration_id)
        return reg is not None and reg.namespace_id in self._access.visible_namespace_ids()

    @staticmethod
    def _resolve_log_path(run_id: int, run_type: str) -> str | None:
        """Return the log file path if it exists for subprocess-spawned runs."""
        if run_type not in LOGGED_RUN_TYPES:
            return None
        log_file = run_log_path(run_id)
        return str(log_file) if log_file.exists() else None

    def _display_names_for(self, runs: list[FlowRun]) -> dict[int, str]:
        """Bulk-resolve ``registration_id -> current registration name``.

        Used to override the run's snapshot ``flow_name`` (which after a save is
        the prefixed file stem like ``9_house_price``) with the catalog's
        user-typed name, so renames are reflected retroactively in run history.
        """
        ids = {r.registration_id for r in runs if r.registration_id is not None}
        if not ids:
            return {}
        return {reg.id: reg.name for reg in self.repo.list_flows_by_ids(list(ids)) if reg.name}

    def run_to_out(self, run: FlowRun, name_overrides: dict[int, str] | None = None) -> FlowRunOut:
        """Convert a FlowRun ORM row to FlowRunOut, computing has_log via FS."""
        override = (name_overrides or {}).get(run.registration_id) if run.registration_id else None
        return run_to_out(
            run,
            has_log=self._resolve_log_path(run.id, run.run_type) is not None,
            display_name=override,
        )

    def list_runs(
        self,
        registration_id: int | None = None,
        schedule_id: int | None = None,
        run_type: RunType | None = None,
        limit: int = 50,
        offset: int = 0,
        search: str | None = None,
    ) -> PaginatedFlowRuns:
        """List run summaries (without snapshots) with total count for pagination."""
        visible = self.visible_run_filters()
        runs = self.repo.list_runs(
            registration_id=registration_id,
            schedule_id=schedule_id,
            run_type=run_type,
            limit=limit,
            offset=offset,
            search=search,
            **visible,
        )
        counts = self.repo.count_runs_by_status(
            registration_id=registration_id, schedule_id=schedule_id, run_type=run_type, search=search, **visible
        )
        name_overrides = self._display_names_for(runs)
        return PaginatedFlowRuns(
            items=[self.run_to_out(r, name_overrides) for r in runs],
            total=counts["total"],
            total_success=counts["success"],
            total_failed=counts["failed"],
            total_running=counts["running"],
            total_cancelled=counts["cancelled"],
        )

    def get_run_detail(self, run_id: int) -> FlowRunDetail:
        """Get a single run including the YAML snapshot.

        A run the caller may not see is reported as not found, so its id reveals nothing.
        """
        run = self.repo.get_run(run_id)
        if run is None or not self.is_visible(run):
            raise RunNotFoundError(run_id=run_id)
        display_name = None
        if run.registration_id is not None:
            reg = self.repo.get_flow(run.registration_id)
            if reg is not None and reg.name:
                display_name = reg.name
        snapshot = run_records.snapshot_of(run)
        return FlowRunDetail(
            id=run.id,
            registration_id=run.registration_id,
            flow_uuid=run.flow_uuid,
            flow_name=display_name or run.flow_name,
            flow_path=run.flow_path,
            user_id=run.user_id,
            started_at=run.started_at,
            ended_at=run.ended_at,
            success=run.success,
            cancelled=run_cancelled(run),
            nodes_completed=run.nodes_completed,
            number_of_nodes=run.number_of_nodes,
            duration_seconds=run.duration_seconds,
            run_type=run.run_type,
            schedule_id=run.schedule_id,
            has_snapshot=snapshot is not None,
            has_log=self._resolve_log_path(run.id, run.run_type) is not None or run.log_ref is not None,
            flow_snapshot=snapshot,
            node_results_json=run.node_results_json,
            executed_on=run.executed_on,
            remote_run_id=run.remote_run_id,
            log_ref=run.log_ref,
            flow_version=run.flow_version,
            log_url=run_records.log_location(run.log_ref) if run.log_ref else None,
            log_page_url=run_records.log_page(run.log_ref) if run.log_ref else None,
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
        flow_uuid: str | None = None,
        executed_on: str | None = None,
        remote_run_id: str | None = None,
        log_ref: str | None = None,
        flow_version: int | None = None,
    ) -> FlowRun:
        """Record a new flow run start.

        ``flow_uuid``, if not supplied, is resolved from the registration so the
        run row stays attributable after a registration delete+recreate.
        """
        if flow_uuid is None and registration_id is not None:
            reg = self.repo.get_flow(registration_id)
            if reg is not None:
                flow_uuid = reg.flow_uuid
        run = FlowRun(
            registration_id=registration_id,
            flow_uuid=flow_uuid,
            flow_name=flow_name,
            flow_path=flow_path,
            user_id=user_id,
            started_at=datetime.now(timezone.utc),
            number_of_nodes=number_of_nodes,
            run_type=run_type,
            flow_snapshot=flow_snapshot,
            executed_on=executed_on,
            remote_run_id=remote_run_id,
            log_ref=log_ref,
            flow_version=flow_version,
        )
        return self.repo.create_run(run)

    def complete_run(
        self,
        run_id: int,
        success: bool,
        nodes_completed: int,
        number_of_nodes: int | None = None,
        node_results_json: str | None = None,
        cancelled: bool = False,
    ) -> FlowRun:
        """Mark a run as completed with the outcome the run itself reports.

        ``cancelled`` says the run was stopped on purpose and nothing in it failed. It is kept only with
        ``success`` False, and it replaces a cancel recorded earlier: a run that reports its own end was not
        stopped by that cancel.
        """
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        now = datetime.now(timezone.utc)
        run.ended_at = now
        run.success = success
        run.cancelled = cancelled and not success
        # Written even when it equals the loaded value: a cancel may have been recorded since the load.
        flag_modified(run, "cancelled")
        run.nodes_completed = nodes_completed
        if number_of_nodes is not None and number_of_nodes > 0:
            run.number_of_nodes = number_of_nodes
        if run.started_at:
            # SQLite stores naive datetimes, so compare both sides without tzinfo.
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
        flow_uuid: str | None = None,
        executed_on: str | None = None,
        remote_run_id: str | None = None,
        log_ref: str | None = None,
        flow_version: int | None = None,
        cancelled: bool = False,
    ) -> FlowRun:
        """Record a fully completed run in one step (fallback when start_run was skipped)."""
        duration = None
        if started_at and ended_at:
            duration = (ended_at - started_at).total_seconds()
        if flow_uuid is None and registration_id is not None:
            reg = self.repo.get_flow(registration_id)
            if reg is not None:
                flow_uuid = reg.flow_uuid
        run = FlowRun(
            registration_id=registration_id,
            flow_uuid=flow_uuid,
            flow_name=flow_name,
            flow_path=flow_path,
            user_id=user_id,
            started_at=started_at,
            ended_at=ended_at,
            success=success,
            cancelled=cancelled and not success,
            nodes_completed=nodes_completed,
            number_of_nodes=number_of_nodes,
            duration_seconds=duration,
            run_type=run_type,
            executed_on=executed_on,
            remote_run_id=remote_run_id,
            log_ref=log_ref,
            flow_version=flow_version,
            node_results_json=node_results_json,
            flow_snapshot=flow_snapshot,
        )
        return self.repo.create_run(run)

    def get_run_snapshot(self, run_id: int) -> str:
        """Return the flow snapshot text for a run."""
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)
        snapshot = run_records.snapshot_of(run)
        if not snapshot:
            raise NoSnapshotError(run_id=run_id)
        return snapshot

    @staticmethod
    def _spawn_flow_subprocess(
        flow_path: str, run_id: int, *, suppress_telemetry: bool = False, backend: str | None = None
    ) -> int | None:
        """Fire-and-forget a ``flowfile run flow`` subprocess.

        Returns the child PID on success, or ``None`` on failure.
        """
        return spawn_flow_subprocess(flow_path, run_id, suppress_telemetry=suppress_telemetry, backend=backend)

    def spawn_flow_run(
        self,
        flow: FlowRegistration,
        user_id: int,
        run_type: RunType,
        schedule_id: int | None = None,
        *,
        suppress_telemetry: bool = False,
        backend: str | None = None,
    ) -> FlowRun:
        """Create a FlowRun record and spawn the subprocess.

        On spawn failure the run is marked as failed immediately.

        ``suppress_telemetry`` is forwarded to the child only (the demo seeder's
        runs are ours, not the user's). ``backend`` names the compute backend the
        child runs the flow on; without it the child runs in its own process
        unless the flow saved a backend id. On a multi-user server a flow whose
        file lies outside the server's flow folders is refused before any run
        record exists.
        """
        flow_path = str(require_flow_in_storage(Path(flow.flow_path)))
        now = datetime.now(timezone.utc)
        run = FlowRun(
            registration_id=flow.id,
            flow_uuid=flow.flow_uuid,
            flow_name=flow.name,
            flow_path=flow.flow_path,
            user_id=user_id,
            started_at=now,
            number_of_nodes=0,
            run_type=run_type,
            schedule_id=schedule_id,
        )
        run = self.repo.create_run(run)

        pid = self._spawn_flow_subprocess(flow_path, run.id, suppress_telemetry=suppress_telemetry, backend=backend)
        if pid is not None:
            run.pid = pid
        else:
            logger.error("Failed to spawn subprocess for run %s — marking as failed", run.id)
            run.ended_at = datetime.now(timezone.utc)
            run.success = False
        self.repo.update_run(run)
        if pid is not None and run.ended_at is not None:
            # Cancelled while it was starting: that cancel saw no pid to signal.
            logger.info("Run %s was cancelled while starting: %s", run.id, terminate_pid(pid))
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
        runs = self.repo.list_active_runs(**self.visible_run_filters())
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
                schedule_id=r.schedule_id,
                remote_run_id=r.remote_run_id,
            )
            for r in runs
        ]

    def cancel_run(self, run_id: int) -> None:
        """Cancel a run that has not ended: record it as cancelled, then terminate its subprocess.

        The record is closed first, as cancelled and already evaluated for notifications, so nothing that polls
        for dead runs can close it as a failure between the signal and the write. A run that already ended keeps
        its outcome and nothing is signalled: its pid may belong to another process by now.
        """
        run = self.repo.get_run(run_id)
        if run is None:
            raise RunNotFoundError(run_id=run_id)

        if not self.repo.cancel_active_run(run_id, datetime.now(timezone.utc)):
            logger.info("Run %s already ended; not cancelling", run_id)
            return

        if run.pid is not None:
            logger.info("Cancelled run %s: %s", run_id, terminate_pid(run.pid))

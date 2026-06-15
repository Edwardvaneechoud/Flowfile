"""Sync orchestrator: ``export`` (DB->files), ``apply`` (files->DB), ``status``.

Export is idempotent (writes only changed bytes) and prunes stale managed files
so deletions in the DB surface as removals in the tree. Apply runs the importers
in dependency order: secrets -> connections -> flows -> schedules. Status reports
per-artifact drift against the last recorded fingerprints.
"""

from __future__ import annotations

import json
import logging

from sqlalchemy.orm import Session

from flowfile_core.workspace.exporters import ExportBundle
from flowfile_core.workspace.exporters.connections import export_connections
from flowfile_core.workspace.exporters.flows import export_flows
from flowfile_core.workspace.exporters.schedules import export_schedules
from flowfile_core.workspace.exporters.secrets import export_secret_manifest
from flowfile_core.workspace.importers.connections import import_connections
from flowfile_core.workspace.importers.flows import import_flows
from flowfile_core.workspace.importers.schedules import import_schedules
from flowfile_core.workspace.importers.secrets import import_secrets
from flowfile_core.workspace.layout import ProjectLayout
from flowfile_core.workspace.manifest import load_manifest
from flowfile_core.workspace.models import (
    DriftReport,
    SecretRequirement,
    WorkspaceApplyResult,
    WorkspaceExportResult,
    WorkspaceStatus,
)
from flowfile_core.workspace.normalize import (
    canonical_yaml_load,
    default_path_tokenizer,
    sha256_text,
)
from flowfile_core.workspace.secret_resolver import SecretResolver

logger = logging.getLogger(__name__)


class WorkspaceSync:
    """Stateful façade over the export/apply/status flows for one project root."""

    def __init__(self, db: Session, user_id: int, root: str) -> None:
        self.db = db
        self.user_id = user_id
        self.layout = ProjectLayout(root)

    # -- export ------------------------------------------------------------
    def _build_artifacts(self) -> ExportBundle:
        """Compute the full in-memory projection of the DB (no disk writes)."""
        tokenizer = default_path_tokenizer()
        flow_bundle, flow_index = export_flows(self.db, self.user_id, self.layout, tokenizer)
        conn_bundle = export_connections(self.db, self.user_id, self.layout)
        sched_bundle = export_schedules(self.db, self.user_id, self.layout, flow_index)
        secret_bundle = export_secret_manifest(conn_bundle.secret_refs, self.layout)

        bundle = ExportBundle()
        bundle.merge(flow_bundle).merge(conn_bundle).merge(sched_bundle).merge(secret_bundle)
        # secret_refs already aggregated into bundle; keep on the merged result.
        bundle.secret_refs = conn_bundle.secret_refs
        return bundle

    def export(self) -> WorkspaceExportResult:
        bundle = self._build_artifacts()
        artifacts = bundle.artifacts

        written: list[str] = []
        unchanged: list[str] = []
        for rel, content in sorted(artifacts.items()):
            abs_path = self.layout.root / rel
            if abs_path.exists() and abs_path.read_text(encoding="utf-8") == content:
                unchanged.append(rel)
                continue
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            abs_path.write_text(content, encoding="utf-8")
            written.append(rel)

        removed = self._prune_stale(set(artifacts))
        self._write_sync_state(artifacts)

        return WorkspaceExportResult(
            project_root=str(self.layout.root),
            written=written,
            unchanged=unchanged,
            removed=removed,
            secret_requirements=self._requirements(bundle.secret_refs),
            warnings=bundle.warnings,
            counts=self._counts(artifacts),
        )

    def _managed_files_on_disk(self) -> set[str]:
        rels: set[str] = set()
        for path in self.layout.iter_flow_files():
            rels.add(self.layout.rel(path))
        for path in self.layout.flows_dir.glob("*.layout.yaml") if self.layout.flows_dir.exists() else []:
            rels.add(self.layout.rel(path))
        for path in self.layout.iter_connection_files():
            rels.add(self.layout.rel(path))
        for path in self.layout.iter_schedule_files():
            rels.add(self.layout.rel(path))
        if self.layout.secrets_manifest_path.exists():
            rels.add(self.layout.rel(self.layout.secrets_manifest_path))
        return rels

    def _prune_stale(self, keep: set[str]) -> list[str]:
        removed: list[str] = []
        for rel in sorted(self._managed_files_on_disk() - keep):
            (self.layout.root / rel).unlink(missing_ok=True)
            removed.append(rel)
        return removed

    # -- apply -------------------------------------------------------------
    def apply(self) -> WorkspaceApplyResult:
        tokenizer = default_path_tokenizer()
        resolver = SecretResolver(self.layout.root)

        resolved, missing = import_secrets(self.db, self.user_id, self.layout, resolver)
        conn_stats = import_connections(self.db, self.user_id, self.layout)
        flow_result = import_flows(self.db, self.user_id, self.layout, tokenizer)
        sched_result = import_schedules(self.db, self.user_id, self.layout, flow_result.reg_map)

        counts = dict(conn_stats.counts)
        counts.update(flow_result.counts)
        counts.update(sched_result.counts)
        counts["secrets_resolved"] = len(resolved)

        warnings = conn_stats.warnings + flow_result.warnings + sched_result.warnings

        return WorkspaceApplyResult(
            project_root=str(self.layout.root),
            counts=counts,
            missing_secrets=missing,
            resolved_secrets=resolved,
            warnings=warnings,
        )

    # -- status / drift ----------------------------------------------------
    def status(self) -> WorkspaceStatus:
        return WorkspaceStatus(
            project_root=str(self.layout.root),
            manifest=load_manifest(self.layout),
            git_enabled=(self.layout.root / ".git").exists(),
            drift=self.diff_drift(),
            secret_requirements=self.required_secrets(),
        )

    def required_secrets(self) -> list[SecretRequirement]:
        return self._requirements(self._build_artifacts().secret_refs)

    def diff_drift(self) -> DriftReport:
        db_artifacts = self._build_artifacts().artifacts
        disk = self._managed_files_on_disk()
        fingerprints = self._read_sync_state()

        report = DriftReport()
        for rel in sorted(set(db_artifacts) | disk):
            db_content = db_artifacts.get(rel)
            file_content = None
            abs_path = self.layout.root / rel
            if abs_path.exists():
                file_content = abs_path.read_text(encoding="utf-8")
            if db_content == file_content:
                continue
            last_fp = fingerprints.get(rel)
            db_changed = db_content is not None and sha256_text(db_content) != last_fp
            file_changed = file_content is not None and sha256_text(file_content) != last_fp

            if db_content is None:
                report.files_ahead.append(rel)  # on disk, gone from DB
            elif file_content is None:
                report.db_ahead.append(rel)  # in DB, not yet on disk
            elif db_changed and file_changed:
                report.conflict.append(rel)
            elif db_changed:
                report.db_ahead.append(rel)
            else:
                report.files_ahead.append(rel)

        report.in_sync = not (report.db_ahead or report.files_ahead or report.conflict)
        return report

    # -- helpers -----------------------------------------------------------
    def _write_sync_state(self, artifacts: dict[str, str]) -> None:
        fingerprints = {rel: sha256_text(content) for rel, content in artifacts.items()}
        path = self.layout.sync_state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(fingerprints, indent=2, sort_keys=True), encoding="utf-8")

    def _read_sync_state(self) -> dict[str, str]:
        path = self.layout.sync_state_path
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}

    @staticmethod
    def _requirements(secret_refs: dict[str, list[str]]) -> list[SecretRequirement]:
        return [
            SecretRequirement(name=name, required_by=sorted(set(paths)))
            for name, paths in sorted(secret_refs.items())
        ]

    @staticmethod
    def _counts(artifacts: dict[str, str]) -> dict[str, int]:
        counts = {"flows": 0, "connections": 0, "schedules": 0, "secrets": 0}
        for rel in artifacts:
            if rel.endswith(".flow.yaml"):
                counts["flows"] += 1
            elif rel.endswith(".schedules.yaml"):
                counts["schedules"] += 1
            elif rel.startswith("connections/"):
                counts["connections"] += 1
        manifest_rel = "secrets/secrets.manifest.yaml"
        if manifest_rel in artifacts:
            data = canonical_yaml_load(artifacts[manifest_rel]) or {}
            counts["secrets"] = len(data.get("secrets", []) or [])
        return counts

"""Pydantic models for the workspace (project) subsystem.

These describe the on-disk project manifest and the structured results returned
by the sync engine (export / apply / status). They are deliberately decoupled
from the SQLAlchemy models in ``flowfile_core.database`` -- the manifest is a
file artifact, not a DB row.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

# Bump when the determinism rules in ``normalize.py`` change in a way that would
# alter exported bytes for unchanged DB state. Lets ``apply`` detect a tree that
# was written by an incompatible engine.
NORMALIZATION_VERSION = "1"
PROJECT_VERSION = "1"


class ProjectManifest(BaseModel):
    """``flowfile.project.yaml`` -- identifies a project tree.

    ``namespace_roots`` lets one DB host several projects (informational in the
    MVP; reserved for namespace-scoped export in Phase 3).
    """

    project_version: str = PROJECT_VERSION
    flowfile_version: str
    project_id: str  # uuid4
    name: str
    namespace_roots: list[str] = Field(default_factory=list)
    normalization: str = NORMALIZATION_VERSION


class SecretRequirement(BaseModel):
    """A secret the project needs but never stores the value of."""

    name: str
    required_by: list[str] = Field(default_factory=list)
    # Populated on apply: whether a value was found (env/.env) or already present
    # in the local DB.
    resolved: bool | None = None
    source: str | None = None  # "env" | "dotenv" | "existing" | None


class WorkspaceExportResult(BaseModel):
    """Outcome of an export (DB -> files)."""

    project_root: str
    written: list[str] = Field(default_factory=list)
    unchanged: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    secret_requirements: list[SecretRequirement] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    counts: dict[str, int] = Field(default_factory=dict)


class WorkspaceApplyResult(BaseModel):
    """Outcome of an apply (files -> DB)."""

    project_root: str
    counts: dict[str, int] = Field(default_factory=dict)
    missing_secrets: list[SecretRequirement] = Field(default_factory=list)
    resolved_secrets: list[SecretRequirement] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DriftReport(BaseModel):
    """Per-artifact divergence between the DB projection and the working tree.

    An artifact is identified by its project-relative path.
    """

    db_ahead: list[str] = Field(default_factory=list)  # DB changed since last sync
    files_ahead: list[str] = Field(default_factory=list)  # tree changed since last sync
    conflict: list[str] = Field(default_factory=list)  # both changed
    in_sync: bool = True


class WorkspaceStatus(BaseModel):
    """Aggregate status for ``/workspace/status``."""

    project_root: str
    manifest: ProjectManifest | None = None
    git_enabled: bool = False
    drift: DriftReport = Field(default_factory=DriftReport)
    secret_requirements: list[SecretRequirement] = Field(default_factory=list)

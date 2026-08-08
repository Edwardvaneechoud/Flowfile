"""Business-logic layer for the Global Artifacts system.

``ArtifactService`` encapsulates all domain rules (validation, versioning,
storage management) and delegates persistence to SQLAlchemy. It never
raises ``HTTPException`` — only domain-specific exceptions from
``artifacts.exceptions``.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, or_, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from flowfile_core.artifacts.exceptions import (
    AmbiguousArtifactError,
    AmbiguousNamespaceError,
    ArtifactNotFoundError,
    ArtifactStateError,
    ArtifactUploadError,
    CannotDeleteLastVersionError,
    CannotDeleteLatestVersionError,
    InvalidArtifactNameError,
    NamespaceNotFoundError,
)
from flowfile_core.auth import sharing
from flowfile_core.catalog.exceptions import FlowNotFoundError
from flowfile_core.database.models import CatalogNamespace, FlowRegistration, GlobalArtifact, ResourceGrant
from flowfile_core.schemas.artifact_schema import (
    ArtifactListItem,
    ArtifactOut,
    ArtifactPruneCandidate,
    ArtifactPruneResponse,
    ArtifactVersionInfo,
    ArtifactWithVersions,
    DownloadSource,
    FinalizeUploadResponse,
    PrepareUploadRequest,
    PrepareUploadResponse,
)

if TYPE_CHECKING:
    from flowfile_core.catalog.access import AccessResolver
    from shared.artifact_storage import ArtifactStorageBackend

logger = logging.getLogger(__name__)

# Separator between a namespace path and the artifact name in a qualified
# reference: "catalog.schema::name". Split on the LAST occurrence.
REFERENCE_SEPARATOR = "::"


def _project_sync_artifacts(owner_id: int) -> None:
    """Mirror an artifact finalize/delete into the active project's models.yaml (no-op when none active)."""
    from flowfile_core.project import project_sync

    project_sync.artifacts_changed(owner_id)


class ArtifactService:
    """Coordinates all artifact business logic.

    Parameters
    ----------
    db:
        SQLAlchemy database session.
    storage:
        Storage backend for blob operations.
    access:
        Per-request authorization resolver. ``None`` ⇒ unrestricted (electron,
        internal callers, tests), mirroring ``CatalogService``.

    TODO: Add a periodic cleanup task or TTL-based reaper to mark old "pending"
    artifacts as "failed". If a kernel crashes between prepare_upload and
    finalize_upload, the DB row stays in "pending" forever. Consider:
    - Background task that marks pending artifacts older than N minutes as failed
    - Startup check that cleans up stale pending artifacts
    - TTL column with automatic status transition
    """

    def __init__(
        self,
        db: Session,
        storage: ArtifactStorageBackend,
        access: AccessResolver | None = None,
    ) -> None:
        self.db = db
        self.storage = storage
        self.access = access

    # ------------------------------------------------------------------ #
    # Authorization
    # ------------------------------------------------------------------ #

    def _accessible_ids(self) -> set[int] | None:
        """Ids the caller may read, or None when unrestricted."""
        if self.access is None or not self.access.restricted:
            return None
        return self.access.accessible_ids("global_artifact")

    def _scope(self, query):
        """Restrict a GlobalArtifact query to the rows the caller may read."""
        ids = self._accessible_ids()
        if ids is None:
            return query
        return query.filter(GlobalArtifact.id.in_(ids))

    def _require_manage(self, artifact: GlobalArtifact) -> None:
        if self.access is None:
            return
        self.access.require_manage("global_artifact", artifact.id, artifact.owner_id)

    def _require_readable(self, artifact: GlobalArtifact) -> None:
        """404 a row the caller cannot even read, so mutations are no existence oracle.

        403 stays reserved for rows the caller can read but not manage.
        """
        ids = self._accessible_ids()
        if ids is not None and artifact.id not in ids:
            raise ArtifactNotFoundError(artifact_id=artifact.id)

    # ------------------------------------------------------------------ #
    # Upload workflow
    # ------------------------------------------------------------------ #

    def prepare_upload(
        self,
        request: PrepareUploadRequest,
        owner_id: int,
        _max_retries: int = 3,
    ) -> PrepareUploadResponse:
        """Create pending artifact record and return upload target.

        Step 1 of upload: Kernel calls this to get where to write the blob.

        Args:
            request: Upload request with artifact metadata.
            owner_id: User ID of the artifact owner.
            _max_retries: Internal retry count for version conflicts.

        Returns:
            PrepareUploadResponse with upload target information.

        Raises:
            FlowNotFoundError: If source registration doesn't exist.
            NamespaceNotFoundError: If specified namespace doesn't exist.
            InvalidArtifactNameError: If the name contains the reference separator.
        """
        if REFERENCE_SEPARATOR in request.name:
            raise InvalidArtifactNameError(
                request.name,
                f"'{REFERENCE_SEPARATOR}' is reserved as the namespace/name separator in artifact references",
            )

        registration = self.db.get(FlowRegistration, request.source_registration_id)
        if registration is None:
            raise FlowNotFoundError(registration_id=request.source_registration_id)

        if request.namespace_id is None:
            request.namespace_id = registration.namespace_id

        if request.namespace_id is not None:
            ns = self.db.get(CatalogNamespace, request.namespace_id)
            if ns is None:
                raise NamespaceNotFoundError(request.namespace_id)

        # Create artifact with retry logic for concurrent version conflicts.
        # If two prepare_upload calls race for the same name, one will fail
        # with IntegrityError on the unique constraint. We retry with a fresh
        # version number in that case.
        #
        # Clean up stale pending/failed artifacts for this name first so they
        # don't block version numbering.
        stale = (
            self.db.query(GlobalArtifact)
            .filter_by(name=request.name, namespace_id=request.namespace_id)
            .filter(GlobalArtifact.status.in_(("pending", "failed")))
            .all()
        )
        for row in stale:
            sharing.delete_grants_for_resource(self.db, "global_artifact", row.id)
            self.db.delete(row)
        if stale:
            self.db.commit()

        last_error: IntegrityError | None = None
        for attempt in range(_max_retries):
            # Determine next version across ALL statuses to avoid unique-constraint
            # collisions with pending/failed rows.
            latest = (
                self.db.query(GlobalArtifact)
                .filter_by(name=request.name, namespace_id=request.namespace_id)
                .order_by(GlobalArtifact.version.desc())
                .first()
            )
            next_version = (latest.version + 1) if latest else 1

            artifact = GlobalArtifact(
                name=request.name,
                namespace_id=request.namespace_id,
                version=next_version,
                status="pending",
                owner_id=owner_id,
                source_registration_id=request.source_registration_id,
                source_flow_id=request.source_flow_id,
                source_node_id=request.source_node_id,
                source_kernel_id=request.source_kernel_id,
                python_type=request.python_type,
                python_module=request.python_module,
                serialization_format=request.serialization_format,
                description=request.description,
                tags=json.dumps(request.tags) if request.tags else "[]",
            )
            self.db.add(artifact)

            try:
                self.db.commit()
                self.db.refresh(artifact)
                break  # Success - exit retry loop
            except IntegrityError as e:
                # Version conflict - another concurrent request got the same version
                self.db.rollback()
                last_error = e
                logger.warning(
                    "Version conflict for artifact '%s' v%d (attempt %d/%d), retrying...",
                    request.name,
                    next_version,
                    attempt + 1,
                    _max_retries,
                )
                continue
        else:
            # Exhausted all retries
            raise ArtifactUploadError(
                artifact_id=0,
                reason=(
                    f"Failed to create artifact after {_max_retries} attempts "
                    f"due to version conflicts: {last_error}"
                ),
            )

        ext_map = {
            "parquet": ".parquet",
            "joblib": ".joblib",
            "pickle": ".pkl",
        }
        ext = ext_map.get(request.serialization_format, ".bin")
        filename = f"{request.name}{ext}"

        target = self.storage.prepare_upload(artifact.id, filename)

        return PrepareUploadResponse(
            artifact_id=artifact.id,
            version=next_version,
            method=target.method,
            path=target.path,
            storage_key=target.storage_key,
        )

    def finalize_upload(
        self,
        artifact_id: int,
        storage_key: str,
        sha256: str,
        size_bytes: int,
    ) -> FinalizeUploadResponse:
        """Verify blob and activate artifact.

        Step 2 of upload: Kernel calls this after writing blob to storage.

        Args:
            artifact_id: Database ID of the artifact.
            storage_key: Storage key from prepare_upload response.
            sha256: SHA-256 hash of the uploaded blob.
            size_bytes: Size of the uploaded blob in bytes.

        Returns:
            FinalizeUploadResponse confirming activation.

        Raises:
            ArtifactNotFoundError: If artifact doesn't exist.
            ArtifactStateError: If artifact is not in pending state.
            ArtifactUploadError: If blob verification fails.
        """
        artifact = self.db.get(GlobalArtifact, artifact_id)
        if not artifact:
            raise ArtifactNotFoundError(artifact_id=artifact_id)

        if artifact.status != "pending":
            raise ArtifactStateError(artifact_id, artifact.status, expected_status="pending")

        try:
            verified_size = self.storage.finalize_upload(storage_key, sha256)
        except FileNotFoundError:
            artifact.status = "failed"
            self.db.commit()
            raise ArtifactUploadError(artifact_id, "Blob not found in storage") from None
        except ValueError as e:
            artifact.status = "failed"
            self.db.commit()
            raise ArtifactUploadError(artifact_id, str(e)) from e

        # Grants key on the version row id, so a direct share would be revoked by
        # every publish; carry the previous latest version's grants forward.
        previous = self._latest_active(artifact.name, artifact.namespace_id, exclude_id=artifact.id)
        if previous is not None:
            self._copy_grants(previous.id, artifact.id)

        artifact.status = "active"
        artifact.storage_key = storage_key
        artifact.sha256 = sha256
        artifact.size_bytes = verified_size
        self.db.commit()
        _project_sync_artifacts(artifact.owner_id)

        return FinalizeUploadResponse(
            status="ok",
            artifact_id=artifact.id,
            version=artifact.version,
        )

    # ------------------------------------------------------------------ #
    # Retrieval
    # ------------------------------------------------------------------ #

    def resolve_reference(self, ref: str, namespace_id: int | None = None) -> tuple[str, int | None]:
        """Split a possibly-qualified ``catalog.schema::name`` reference.

        Every by-name route parses references through here, so a qualified value
        resolves identically whichever endpoint receives it. An unqualified ref is
        returned untouched (today's behavior, ambiguity check included). When the
        namespace path no longer resolves (a renamed catalog) the bare name is
        accepted if it is unambiguous; otherwise the lookup fails.
        """
        if REFERENCE_SEPARATOR not in ref:
            return ref, namespace_id

        path, _, name = ref.rpartition(REFERENCE_SEPARATOR)
        if namespace_id is not None:
            return name, namespace_id

        resolved = self._resolve_namespace_path(path)
        if resolved is not None:
            return name, resolved

        namespaces = self._distinct_namespaces(name)
        if len(namespaces) == 1:
            return name, namespaces[0]
        raise NamespaceNotFoundError(name=path)

    def _resolve_namespace_path(self, path: str) -> int | None:
        """Resolve a dotted ``catalog[.schema]`` path to a namespace id (never creates)."""
        catalog_name, _, schema_name = path.partition(".")
        if not catalog_name:
            return None
        from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
        from flowfile_core.catalog.services.namespaces import NamespaceService

        service = NamespaceService(SQLAlchemyCatalogRepository(self.db))
        return service.resolve_namespace_id_by_path(catalog_name, schema_name or None)

    def _distinct_namespaces(self, name: str) -> list[int | None]:
        rows = self._scope(self.db.query(GlobalArtifact).filter_by(name=name, status="active")).with_entities(
            GlobalArtifact.namespace_id
        )
        return [r[0] for r in rows.distinct().all()]

    def get_artifact_by_name(
        self,
        name: str,
        namespace_id: int | None = None,
        version: int | None = None,
    ) -> ArtifactOut:
        """Lookup artifact by name with download source.

        Args:
            name: Artifact name (may be a qualified ``catalog.schema::name`` reference).
            namespace_id: Optional namespace filter.
            version: Optional specific version (latest if not specified).

        Returns:
            ArtifactOut with full metadata and download source.

        Raises:
            ArtifactNotFoundError: If artifact doesn't exist.
        """
        name, namespace_id = self.resolve_reference(name, namespace_id)
        self._assert_unambiguous(name, namespace_id)

        query = self._scope(
            self.db.query(GlobalArtifact).filter_by(
                name=name,
                status="active",
            )
        )
        if namespace_id is not None:
            query = query.filter_by(namespace_id=namespace_id)

        if version is not None:
            artifact = query.filter_by(version=version).first()
        else:
            artifact = query.order_by(GlobalArtifact.version.desc()).first()

        if not artifact:
            raise ArtifactNotFoundError(name=name, version=version)

        return self._artifact_to_out(artifact, include_download=True)

    def get_artifact_by_id(self, artifact_id: int) -> ArtifactOut:
        """Lookup artifact by ID with download source.

        Args:
            artifact_id: Database ID of the artifact.

        Returns:
            ArtifactOut with full metadata and download source.

        Raises:
            ArtifactNotFoundError: If artifact doesn't exist.
        """
        artifact = self.db.get(GlobalArtifact, artifact_id)
        if not artifact or artifact.status != "active":
            raise ArtifactNotFoundError(artifact_id=artifact_id)
        ids = self._accessible_ids()
        if ids is not None and artifact.id not in ids:
            raise ArtifactNotFoundError(artifact_id=artifact_id)

        return self._artifact_to_out(artifact, include_download=True)

    def get_artifact_with_versions(
        self,
        name: str,
        namespace_id: int | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> ArtifactWithVersions:
        """Get artifact with a (optionally paginated) list of its versions.

        Args:
            name: Artifact name (may be a qualified ``catalog.schema::name`` reference).
            namespace_id: Optional namespace filter.
            limit: Maximum versions to return; ``None`` returns them all.
            offset: Offset into the version list (newest first).

        Returns:
            ArtifactWithVersions with latest version and version list.

        Raises:
            ArtifactNotFoundError: If artifact doesn't exist.
        """
        name, namespace_id = self.resolve_reference(name, namespace_id)
        self._assert_unambiguous(name, namespace_id)

        query = self._scope(self.db.query(GlobalArtifact).filter_by(name=name, status="active"))
        if namespace_id is not None:
            query = query.filter_by(namespace_id=namespace_id)

        latest = query.order_by(GlobalArtifact.version.desc()).first()

        if not latest:
            raise ArtifactNotFoundError(name=name)

        total_versions = query.count()
        page = query.order_by(GlobalArtifact.version.desc()).offset(offset)
        if limit is not None:
            page = page.limit(limit)
        versions = page.all()

        version_infos = [
            ArtifactVersionInfo(
                version=v.version,
                id=v.id,
                created_at=v.created_at,
                size_bytes=v.size_bytes,
                sha256=v.sha256,
                python_type=v.python_type,
                blob_exists=self._blob_exists(v),
            )
            for v in versions
        ]

        out = self._artifact_to_out(latest, include_download=True)
        return ArtifactWithVersions(
            **out.model_dump(),
            all_versions=version_infos,
            total_versions=total_versions,
        )

    def resolve_namespace_id(self, namespace: str) -> int:
        """Resolve a namespace *name* to its id.

        Raises:
            NamespaceNotFoundError: If no namespace has that name.
            AmbiguousNamespaceError: If the name matches more than one namespace.
        """
        ids = [r[0] for r in self.db.query(CatalogNamespace.id).filter_by(name=namespace).all()]
        if not ids:
            raise NamespaceNotFoundError(name=namespace)
        if len(ids) > 1:
            raise AmbiguousNamespaceError(namespace, ids)
        return ids[0]

    # ------------------------------------------------------------------ #
    # Listing
    # ------------------------------------------------------------------ #

    def list_artifacts(
        self,
        namespace_id: int | None = None,
        tags: list[str] | None = None,
        name_contains: str | None = None,
        python_type_contains: str | None = None,
        limit: int = 100,
        offset: int = 0,
        latest_only: bool = False,
    ) -> list[ArtifactListItem]:
        """List artifacts with optional filtering.

        Args:
            namespace_id: Filter by namespace.
            tags: Filter by tags (AND logic).
            name_contains: Filter by name substring.
            python_type_contains: Filter by Python type substring.
            limit: Maximum results to return.
            offset: Offset for pagination.
            latest_only: Collapse to one row per (name, namespace_id) — the newest
                active version. Applied after access filtering, before limit/offset,
                so pagination counts artifacts rather than versions.

        Returns:
            List of ArtifactListItem objects.
        """
        query = self._scope(self.db.query(GlobalArtifact).filter_by(status="active"))

        if namespace_id is not None:
            query = query.filter_by(namespace_id=namespace_id)

        if name_contains:
            query = query.filter(GlobalArtifact.name.contains(name_contains))

        if python_type_contains:
            query = query.filter(GlobalArtifact.python_type.contains(python_type_contains))

        # Tag filtering using SQLite json_each for proper element matching
        # This avoids false positives (e.g., "ml" matching "html") and
        # applies filtering BEFORE pagination so limit/offset work correctly
        #
        # WARNING: SQLite-specific SQL using json_each() function.
        # For PostgreSQL, use: jsonb_array_elements_text(tags) or tags ? :tag
        # This needs to be abstracted if multi-database support is required.
        if tags:
            for tag in tags:
                query = query.filter(
                    text(
                        "EXISTS (SELECT 1 FROM json_each(global_artifacts.tags) " "WHERE json_each.value = :tag)"
                    ).bindparams(tag=tag)
                )

        if latest_only:
            newest = (
                query.with_entities(
                    GlobalArtifact.name.label("group_name"),
                    GlobalArtifact.namespace_id.label("group_namespace_id"),
                    func.max(GlobalArtifact.version).label("group_version"),
                )
                .group_by(GlobalArtifact.name, GlobalArtifact.namespace_id)
                .subquery()
            )
            query = query.join(
                newest,
                and_(
                    GlobalArtifact.name == newest.c.group_name,
                    GlobalArtifact.version == newest.c.group_version,
                    or_(
                        GlobalArtifact.namespace_id == newest.c.group_namespace_id,
                        and_(
                            GlobalArtifact.namespace_id.is_(None),
                            newest.c.group_namespace_id.is_(None),
                        ),
                    ),
                ),
            )

        # Order by name and version, most recent versions first
        # Pagination is applied AFTER all filtering
        artifacts = (
            query.order_by(
                GlobalArtifact.name,
                GlobalArtifact.version.desc(),
            )
            .offset(offset)
            .limit(limit)
            .all()
        )

        counts = self._active_version_counts(artifacts)
        paths = self._bulk_namespace_paths({a.namespace_id for a in artifacts if a.namespace_id is not None})
        return [
            self._artifact_to_list_item(
                a,
                namespace_path=paths.get(a.namespace_id) if a.namespace_id is not None else None,
                version_count=counts.get((a.name, a.namespace_id)),
            )
            for a in artifacts
        ]

    def list_artifact_names(
        self,
        namespace_id: int | None = None,
    ) -> list[str]:
        """List unique artifact names in a namespace.

        Args:
            namespace_id: Optional namespace filter.

        Returns:
            List of unique artifact names.
        """
        query = self._scope(self.db.query(GlobalArtifact).filter_by(status="active"))

        if namespace_id is not None:
            query = query.filter_by(namespace_id=namespace_id)

        names = query.with_entities(GlobalArtifact.name).distinct().all()
        return [n[0] for n in names]

    # ------------------------------------------------------------------ #
    # Deletion
    # ------------------------------------------------------------------ #

    def delete_artifact(
        self,
        artifact_id: int,
        force: bool = False,
        bypass_guards: bool = False,
    ) -> int:
        """Delete a specific artifact version (soft delete in DB, hard delete blob).

        Args:
            artifact_id: Database ID of the artifact to delete.
            force: Allow deleting the current latest version.
            bypass_guards: Skip the version-safety guards entirely (kernel/internal
                callers, whose released images delete "the latest version" by design).

        Returns:
            Number of versions deleted (always 1).

        Raises:
            ArtifactNotFoundError: If artifact doesn't exist or is not readable.
            CannotDeleteLatestVersionError: If the row is the current latest and force is False.
            CannotDeleteLastVersionError: If the row is the only remaining active version.
        """
        artifact = self.db.get(GlobalArtifact, artifact_id)
        if not artifact:
            raise ArtifactNotFoundError(artifact_id=artifact_id)

        self._require_readable(artifact)
        self._require_manage(artifact)

        if not bypass_guards and artifact.status == "active":
            siblings = self._active_versions(artifact.name, artifact.namespace_id, exclude_id=artifact.id)
            if not siblings:
                raise CannotDeleteLastVersionError(artifact.id, artifact.name, artifact.version)
            if not force and artifact.version > max(s.version for s in siblings):
                raise CannotDeleteLatestVersionError(artifact.id, artifact.name, artifact.version)

        owner_id = artifact.owner_id
        self._delete_row(artifact)
        self.db.commit()
        _project_sync_artifacts(owner_id)

        return 1

    def promote_version(self, artifact_id: int, _max_retries: int = 3) -> ArtifactOut:
        """Copy an older version forward as the new latest version.

        "Latest" is ``max(version)``, so restoring an old version means
        re-publishing its bytes under a new highest version — history stays
        append-only and every ``get_global(name)`` picks it up unchanged. The blob
        is copied by the storage backend; Core never touches the bytes.

        A concurrent publish can claim the same version number, so the mint is
        retried on the unique constraint like ``prepare_upload`` does.

        Raises:
            ArtifactNotFoundError: If the source row is missing, not active, or its blob is gone.
            ArtifactUploadError: If every attempt lost the version race.
        """
        source = self.db.get(GlobalArtifact, artifact_id)
        if not source or source.status != "active":
            raise ArtifactNotFoundError(artifact_id=artifact_id)

        self._require_readable(source)
        self._require_manage(source)

        if not source.storage_key:
            raise ArtifactNotFoundError(artifact_id=artifact_id)

        filename = source.storage_key.split("/", 1)[-1]
        last_error: IntegrityError | None = None
        for attempt in range(_max_retries):
            previous_latest = self._latest_active(source.name, source.namespace_id)
            latest_any = (
                self.db.query(GlobalArtifact)
                .filter_by(name=source.name, namespace_id=source.namespace_id)
                .order_by(GlobalArtifact.version.desc())
                .first()
            )
            next_version = (latest_any.version + 1) if latest_any else 1

            promoted = GlobalArtifact(
                name=source.name,
                namespace_id=source.namespace_id,
                version=next_version,
                status="active",
                owner_id=source.owner_id,
                source_registration_id=source.source_registration_id,
                source_flow_id=source.source_flow_id,
                source_node_id=source.source_node_id,
                source_kernel_id=source.source_kernel_id,
                python_type=source.python_type,
                python_module=source.python_module,
                serialization_format=source.serialization_format,
                sha256=source.sha256,
                size_bytes=source.size_bytes,
                description=source.description,
                tags=source.tags,
            )
            self.db.add(promoted)
            try:
                self.db.flush()
            except IntegrityError as e:
                self.db.rollback()
                last_error = e
                continue

            storage_key = f"{promoted.id}/{filename}"
            try:
                promoted.size_bytes = self.storage.copy(source.storage_key, storage_key)
            except FileNotFoundError:
                self.db.rollback()
                raise ArtifactNotFoundError(artifact_id=artifact_id) from None
            promoted.storage_key = storage_key

            if previous_latest is not None:
                self._copy_grants(previous_latest.id, promoted.id)

            try:
                self.db.commit()
            except IntegrityError as e:
                self.db.rollback()
                self._discard_blob(storage_key)
                last_error = e
                logger.warning(
                    "Version conflict promoting artifact '%s' v%d (attempt %d/%d), retrying...",
                    source.name,
                    next_version,
                    attempt + 1,
                    _max_retries,
                )
                continue

            self.db.refresh(promoted)
            _project_sync_artifacts(promoted.owner_id)
            return self._artifact_to_out(promoted, include_download=True)

        raise ArtifactUploadError(
            artifact_id=artifact_id,
            reason=f"Failed to promote after {_max_retries} attempts due to version conflicts: {last_error}",
        )

    def prune_versions(
        self,
        name: str,
        namespace_id: int | None = None,
        keep: int = 5,
        dry_run: bool = True,
    ) -> ArtifactPruneResponse:
        """Delete all but the newest ``keep`` active versions (the latest is always kept).

        Raises:
            ArtifactNotFoundError: If the artifact has no active versions.
        """
        keep = max(1, keep)
        name, namespace_id = self.resolve_reference(name, namespace_id)
        if namespace_id is None:
            # Versions must be grouped by namespace, so a bare name has to resolve to
            # exactly one (possibly the namespace-less one).
            namespaces = self._distinct_namespaces(name)
            if len(namespaces) > 1:
                raise AmbiguousArtifactError(name, namespaces)
            namespace_id = namespaces[0] if namespaces else None

        versions = self._active_versions(name, namespace_id)
        if not versions:
            raise ArtifactNotFoundError(name=name)

        self._require_manage(versions[0])

        candidates = versions[keep:]
        would_delete = [ArtifactPruneCandidate(id=v.id, version=v.version, size_bytes=v.size_bytes) for v in candidates]
        freed_bytes = sum(v.size_bytes or 0 for v in candidates)

        if dry_run or not candidates:
            return ArtifactPruneResponse(would_delete=would_delete, freed_bytes=freed_bytes, deleted=0)

        owner_id = candidates[0].owner_id
        for artifact in candidates:
            self._delete_row(artifact)
        self.db.commit()
        _project_sync_artifacts(owner_id)

        return ArtifactPruneResponse(
            would_delete=would_delete,
            freed_bytes=freed_bytes,
            deleted=len(candidates),
        )

    def delete_all_versions(
        self,
        name: str,
        namespace_id: int | None = None,
    ) -> int:
        """Delete all versions of an artifact.

        Args:
            name: Artifact name.
            namespace_id: Optional namespace filter.

        Returns:
            Number of versions deleted.

        Raises:
            ArtifactNotFoundError: If no artifacts found.
        """
        name, namespace_id = self.resolve_reference(name, namespace_id)
        query = self._scope(
            self.db.query(GlobalArtifact).filter_by(name=name).filter(GlobalArtifact.status != "deleted")
        )
        if namespace_id is not None:
            query = query.filter_by(namespace_id=namespace_id)
        artifacts = query.all()

        if not artifacts:
            raise ArtifactNotFoundError(name=name)

        # Every row, not just the first: a bare name can span namespaces and owners,
        # so one manageable row must never authorize deleting someone else's.
        for artifact in artifacts:
            self._require_manage(artifact)

        owner_id = artifacts[0].owner_id
        count = 0
        for artifact in artifacts:
            self._delete_row(artifact)
            count += 1

        self.db.commit()
        _project_sync_artifacts(owner_id)
        return count

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    def _assert_unambiguous(self, name: str, namespace_id: int | None) -> None:
        """Reject a name-only lookup that would span more than one namespace.

        The same name can be published into several namespaces, so resolving by
        name alone (``namespace_id is None``) is only safe when exactly one
        namespace holds an active artifact with that name. Otherwise the caller
        must disambiguate with an explicit ``namespace_id`` or a qualified
        ``catalog.schema::name`` reference.
        """
        if namespace_id is not None:
            return
        namespaces = self._distinct_namespaces(name)
        if len(namespaces) > 1:
            raise AmbiguousArtifactError(name, namespaces)

    def _active_versions(
        self,
        name: str,
        namespace_id: int | None,
        exclude_id: int | None = None,
    ) -> list[GlobalArtifact]:
        """Active versions of one artifact the caller may read, newest first."""
        query = self._scope(
            self.db.query(GlobalArtifact).filter_by(name=name, namespace_id=namespace_id, status="active")
        )
        if exclude_id is not None:
            query = query.filter(GlobalArtifact.id != exclude_id)
        return query.order_by(GlobalArtifact.version.desc()).all()

    def _latest_active(
        self,
        name: str,
        namespace_id: int | None,
        exclude_id: int | None = None,
    ) -> GlobalArtifact | None:
        versions = self._active_versions(name, namespace_id, exclude_id=exclude_id)
        return versions[0] if versions else None

    def _copy_grants(self, from_artifact_id: int, to_artifact_id: int) -> None:
        """Re-point a version's sharing grants onto a newly created version row."""
        grants = (
            self.db.query(ResourceGrant).filter_by(resource_type="global_artifact", resource_id=from_artifact_id).all()
        )
        existing = {
            g.group_id
            for g in self.db.query(ResourceGrant).filter_by(resource_type="global_artifact", resource_id=to_artifact_id)
        }
        for grant in grants:
            if grant.group_id in existing:
                continue
            self.db.add(
                ResourceGrant(
                    resource_type="global_artifact",
                    resource_id=to_artifact_id,
                    group_id=grant.group_id,
                    permission=grant.permission,
                    granted_by=grant.granted_by,
                )
            )

    def _delete_row(self, artifact: GlobalArtifact) -> None:
        """Hard-delete the blob, soft-delete the row, drop its grants. Caller commits."""
        if artifact.storage_key:
            self._discard_blob(artifact.storage_key)
        artifact.status = "deleted"
        sharing.delete_grants_for_resource(self.db, "global_artifact", artifact.id)

    def _discard_blob(self, storage_key: str) -> None:
        """Best-effort blob removal; a storage failure must not abort the DB work."""
        try:
            self.storage.delete(storage_key)
        except Exception as exc:
            logger.warning("Failed to delete artifact blob (storage_key=%s): %s", storage_key, exc)

    def _blob_exists(self, artifact: GlobalArtifact) -> bool:
        """Filesystem backends only — one cheap stat per row.

        An S3 HEAD per version row would put a network call in every versions page,
        which is exactly what ``bulk_enrich_artifacts`` refuses to do.
        """
        from shared.artifact_storage import SharedFilesystemStorage

        if not artifact.storage_key:
            return False
        if not isinstance(self.storage, SharedFilesystemStorage):
            return True
        try:
            return self.storage.exists(artifact.storage_key)
        except Exception:
            return False

    def _active_version_counts(self, artifacts: list[GlobalArtifact]) -> dict[tuple[str, int | None], int]:
        """Active-version count per (name, namespace_id) for a page of rows, in one query."""
        names = {a.name for a in artifacts}
        if not names:
            return {}
        rows = (
            self._scope(self.db.query(GlobalArtifact).filter(GlobalArtifact.status == "active"))
            .filter(GlobalArtifact.name.in_(names))
            .with_entities(
                GlobalArtifact.name,
                GlobalArtifact.namespace_id,
                func.count(GlobalArtifact.id),
            )
            .group_by(GlobalArtifact.name, GlobalArtifact.namespace_id)
            .all()
        )
        return {(name, namespace_id): count for name, namespace_id, count in rows}

    def _bulk_namespace_paths(self, namespace_ids: set[int]) -> dict[int, str]:
        """Dotted paths for a set of namespaces, walking parents in bulk (no per-row query)."""
        if not namespace_ids:
            return {}
        known: dict[int, tuple[str, int | None]] = {}
        pending = set(namespace_ids)
        attempted: set[int] = set()
        while pending:
            # Track what was looked up, not what came back: a dangling parent_id
            # (SQLite does not enforce FKs) would otherwise re-query forever.
            attempted |= pending
            rows = (
                self.db.query(CatalogNamespace.id, CatalogNamespace.name, CatalogNamespace.parent_id)
                .filter(CatalogNamespace.id.in_(pending))
                .all()
            )
            for ns_id, name, parent_id in rows:
                known[ns_id] = (name, parent_id)
            pending = {parent for _n, parent in known.values() if parent is not None and parent not in attempted}

        paths: dict[int, str] = {}
        for ns_id in namespace_ids:
            parts: list[str] = []
            seen: set[int] = set()
            current: int | None = ns_id
            while current is not None and current in known and current not in seen:
                seen.add(current)
                name, parent_id = known[current]
                parts.append(name)
                current = parent_id
            if parts:
                paths[ns_id] = ".".join(reversed(parts))
        return paths

    def _artifact_to_out(
        self,
        artifact: GlobalArtifact,
        include_download: bool = False,
    ) -> ArtifactOut:
        """Convert database model to output schema."""
        download_source = None
        if include_download and artifact.storage_key:
            try:
                source = self.storage.prepare_download(artifact.storage_key)
                download_source = DownloadSource(
                    method=source.method,
                    path=source.path,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to prepare download for artifact %s (storage_key=%s): %s",
                    artifact.id,
                    artifact.storage_key,
                    exc,
                )

        return ArtifactOut(
            id=artifact.id,
            name=artifact.name,
            namespace_id=artifact.namespace_id,
            version=artifact.version,
            status=artifact.status,
            owner_id=artifact.owner_id,
            source_registration_id=artifact.source_registration_id,
            source_flow_id=artifact.source_flow_id,
            source_node_id=artifact.source_node_id,
            source_kernel_id=artifact.source_kernel_id,
            python_type=artifact.python_type,
            python_module=artifact.python_module,
            serialization_format=artifact.serialization_format,
            storage_key=artifact.storage_key,
            size_bytes=artifact.size_bytes,
            sha256=artifact.sha256,
            description=artifact.description,
            tags=json.loads(artifact.tags) if artifact.tags else [],
            created_at=artifact.created_at,
            updated_at=artifact.updated_at,
            download_source=download_source,
        )

    def _artifact_to_list_item(
        self,
        artifact: GlobalArtifact,
        namespace_path: str | None = None,
        version_count: int | None = None,
    ) -> ArtifactListItem:
        """Convert database model to list item schema."""
        return ArtifactListItem(
            id=artifact.id,
            name=artifact.name,
            namespace_id=artifact.namespace_id,
            version=artifact.version,
            status=artifact.status,
            source_registration_id=artifact.source_registration_id,
            python_type=artifact.python_type,
            serialization_format=artifact.serialization_format,
            size_bytes=artifact.size_bytes,
            created_at=artifact.created_at,
            tags=json.loads(artifact.tags) if artifact.tags else [],
            owner_id=artifact.owner_id,
            namespace_path=namespace_path,
            version_count=version_count,
        )

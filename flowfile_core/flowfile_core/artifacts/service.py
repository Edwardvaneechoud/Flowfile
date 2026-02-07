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

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from flowfile_core.artifacts.exceptions import (
    ArtifactNotFoundError,
    ArtifactStateError,
    ArtifactUploadError,
    NamespaceNotFoundError,
)
from flowfile_core.catalog.exceptions import FlowNotFoundError
from flowfile_core.database.models import CatalogNamespace, FlowRegistration, GlobalArtifact
from flowfile_core.schemas.artifact_schema import (
    ArtifactListItem,
    ArtifactOut,
    ArtifactVersionInfo,
    ArtifactWithVersions,
    DownloadSource,
    FinalizeUploadResponse,
    PrepareUploadRequest,
    PrepareUploadResponse,
)

if TYPE_CHECKING:
    from shared.artifact_storage import ArtifactStorageBackend


class ArtifactService:
    """Coordinates all artifact business logic.

    Parameters
    ----------
    db:
        SQLAlchemy database session.
    storage:
        Storage backend for blob operations.

    TODO: Add a periodic cleanup task or TTL-based reaper to mark old "pending"
    artifacts as "failed". If a kernel crashes between prepare_upload and
    finalize_upload, the DB row stays in "pending" forever. Consider:
    - Background task that marks pending artifacts older than N minutes as failed
    - Startup check that cleans up stale pending artifacts
    - TTL column with automatic status transition
    """

    def __init__(self, db: Session, storage: ArtifactStorageBackend) -> None:
        self.db = db
        self.storage = storage

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
        """
        # Validate registration exists
        registration = self.db.get(FlowRegistration, request.source_registration_id)
        if registration is None:
            raise FlowNotFoundError(registration_id=request.source_registration_id)

        # Inherit namespace_id from registration if not explicitly provided
        if request.namespace_id is None:
            request.namespace_id = registration.namespace_id

        # Validate namespace if specified
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

            # Create pending artifact record
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
                reason=f"Failed to create artifact after {_max_retries} attempts due to version conflicts: {last_error}",
            )

        # Get upload target from storage backend
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

        # Verify and finalize storage
        try:
            verified_size = self.storage.finalize_upload(storage_key, sha256)
        except FileNotFoundError:
            artifact.status = "failed"
            self.db.commit()
            raise ArtifactUploadError(artifact_id, "Blob not found in storage")
        except ValueError as e:
            artifact.status = "failed"
            self.db.commit()
            raise ArtifactUploadError(artifact_id, str(e))

        # Activate artifact
        artifact.status = "active"
        artifact.storage_key = storage_key
        artifact.sha256 = sha256
        artifact.size_bytes = verified_size
        self.db.commit()

        return FinalizeUploadResponse(
            status="ok",
            artifact_id=artifact.id,
            version=artifact.version,
        )

    # ------------------------------------------------------------------ #
    # Retrieval
    # ------------------------------------------------------------------ #

    def get_artifact_by_name(
        self,
        name: str,
        namespace_id: int | None = None,
        version: int | None = None,
    ) -> ArtifactOut:
        """Lookup artifact by name with download source.

        Args:
            name: Artifact name.
            namespace_id: Optional namespace filter.
            version: Optional specific version (latest if not specified).

        Returns:
            ArtifactOut with full metadata and download source.

        Raises:
            ArtifactNotFoundError: If artifact doesn't exist.
        """
        query = self.db.query(GlobalArtifact).filter_by(
            name=name,
            status="active",
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

        return self._artifact_to_out(artifact, include_download=True)

    def get_artifact_with_versions(
        self,
        name: str,
        namespace_id: int | None = None,
    ) -> ArtifactWithVersions:
        """Get artifact with list of all available versions.

        Args:
            name: Artifact name.
            namespace_id: Optional namespace filter.

        Returns:
            ArtifactWithVersions with latest version and version list.

        Raises:
            ArtifactNotFoundError: If artifact doesn't exist.
        """
        # Get latest version
        query = self.db.query(GlobalArtifact).filter_by(name=name, status="active")
        if namespace_id is not None:
            query = query.filter_by(namespace_id=namespace_id)

        latest = query.order_by(GlobalArtifact.version.desc()).first()

        if not latest:
            raise ArtifactNotFoundError(name=name)

        # Get all versions
        versions = query.order_by(GlobalArtifact.version.desc()).all()

        version_infos = [
            ArtifactVersionInfo(
                version=v.version,
                id=v.id,
                created_at=v.created_at,
                size_bytes=v.size_bytes,
                sha256=v.sha256,
            )
            for v in versions
        ]

        out = self._artifact_to_out(latest, include_download=True)
        return ArtifactWithVersions(
            **out.model_dump(),
            all_versions=version_infos,
        )

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
    ) -> list[ArtifactListItem]:
        """List artifacts with optional filtering.

        Args:
            namespace_id: Filter by namespace.
            tags: Filter by tags (AND logic).
            name_contains: Filter by name substring.
            python_type_contains: Filter by Python type substring.
            limit: Maximum results to return.
            offset: Offset for pagination.

        Returns:
            List of ArtifactListItem objects.
        """
        query = self.db.query(GlobalArtifact).filter_by(status="active")

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
                # Use EXISTS with json_each to check if tag is in the JSON array
                # This works with SQLite's JSON1 extension
                query = query.filter(
                    text(
                        "EXISTS (SELECT 1 FROM json_each(global_artifacts.tags) " "WHERE json_each.value = :tag)"
                    ).bindparams(tag=tag)
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

        return [self._artifact_to_list_item(a) for a in artifacts]

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
        query = self.db.query(GlobalArtifact.name).filter_by(status="active")

        if namespace_id is not None:
            query = query.filter_by(namespace_id=namespace_id)

        names = query.distinct().all()
        return [n[0] for n in names]

    # ------------------------------------------------------------------ #
    # Deletion
    # ------------------------------------------------------------------ #

    def delete_artifact(
        self,
        artifact_id: int,
    ) -> int:
        """Delete a specific artifact version (soft delete in DB, hard delete blob).

        Args:
            artifact_id: Database ID of the artifact to delete.

        Returns:
            Number of versions deleted (always 1).

        Raises:
            ArtifactNotFoundError: If artifact doesn't exist.
        """
        artifact = self.db.get(GlobalArtifact, artifact_id)
        if not artifact:
            raise ArtifactNotFoundError(artifact_id=artifact_id)

        # Delete blob from storage
        if artifact.storage_key:
            try:
                self.storage.delete(artifact.storage_key)
            except Exception as exc:
                logger.warning(
                    "Failed to delete blob for artifact %s (storage_key=%s): %s",
                    artifact_id,
                    artifact.storage_key,
                    exc,
                )

        # Soft delete in DB
        artifact.status = "deleted"
        self.db.commit()

        return 1

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
        query = self.db.query(GlobalArtifact).filter_by(name=name).filter(GlobalArtifact.status != "deleted")
        if namespace_id is not None:
            query = query.filter_by(namespace_id=namespace_id)
        artifacts = query.all()

        if not artifacts:
            raise ArtifactNotFoundError(name=name)

        count = 0
        for artifact in artifacts:
            if artifact.storage_key:
                try:
                    self.storage.delete(artifact.storage_key)
                except Exception as exc:
                    logger.warning(
                        "Failed to delete blob for artifact %s v%s (storage_key=%s): %s",
                        name,
                        artifact.version,
                        artifact.storage_key,
                        exc,
                    )
            artifact.status = "deleted"
            count += 1

        self.db.commit()
        return count

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

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

    def _artifact_to_list_item(self, artifact: GlobalArtifact) -> ArtifactListItem:
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
        )

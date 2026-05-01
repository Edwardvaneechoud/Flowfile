"""Flow registration: register, update, list, enrich, auto-register, namespace seeding."""

from __future__ import annotations

import logging
import os

from flowfile_core.catalog.exceptions import (
    FlowHasArtifactsError,
    FlowNotFoundError,
    NamespaceNotFoundError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import artifact_to_out
from flowfile_core.catalog.services.namespaces import NamespaceService
from flowfile_core.database.models import CatalogNamespace, FlowRegistration
from flowfile_core.schemas.catalog_schema import (
    CatalogTableSummary,
    FlowRegistrationOut,
    GlobalArtifactOut,
)

logger = logging.getLogger(__name__)

# Process-local memo of (registration_id, flow_path) tuples we've already
# warned about. The first sighting of a missing file is genuinely useful (it
# can flag a misconfigured volume mount), but every subsequent /catalog/flows
# request would re-log the same line. Keeping it in-memory means the warning
# resurfaces after a restart, which is the right cadence for an operator.
_warned_missing_paths: set[tuple[int, str]] = set()


def _warn_missing_file_once(flow: FlowRegistration) -> None:
    """Log the missing-file warning at most once per (id, path) per process."""
    key = (flow.id, flow.flow_path or "")
    if key in _warned_missing_paths:
        return
    _warned_missing_paths.add(key)
    logger.warning(
        "Registered flow %s (id=%d) references missing file: %s",
        flow.name,
        flow.id,
        flow.flow_path,
    )


class FlowRegistrationService:
    """Owns flow registration CRUD, enrichment and auto-registration."""

    def __init__(self, repo: CatalogRepository, namespaces: NamespaceService) -> None:
        self.repo = repo
        self._namespaces = namespaces

    def enrich_flow_registration(self, flow: FlowRegistration, user_id: int) -> FlowRegistrationOut:
        """Attach favourite/follow flags and run stats to a single registration.

        Note: For bulk operations, prefer ``bulk_enrich_flows`` to avoid N+1 queries.
        """
        is_favorite = self.repo.get_favorite(user_id, flow.id) is not None
        is_following = self.repo.get_follow(user_id, flow.id) is not None
        run_count = self.repo.count_run_for_flow(flow.id)
        last_run = self.repo.last_run_for_flow(flow.id)
        artifact_count = self.repo.count_active_artifacts_for_flow(flow.id)
        produced_tables = self.repo.list_tables_for_flow(flow.id)
        read_tables = self.repo.list_read_tables_for_flow(flow.id)
        file_exists = os.path.exists(flow.flow_path) if flow.flow_path else False
        if not file_exists:
            _warn_missing_file_once(flow)
        return FlowRegistrationOut(
            id=flow.id,
            name=flow.name,
            description=flow.description,
            flow_path=flow.flow_path,
            namespace_id=flow.namespace_id,
            owner_id=flow.owner_id,
            created_at=flow.created_at,
            updated_at=flow.updated_at,
            is_favorite=is_favorite,
            is_following=is_following,
            run_count=run_count,
            last_run_at=last_run.started_at if last_run else None,
            last_run_success=last_run.success if last_run else None,
            file_exists=file_exists,
            artifact_count=artifact_count,
            tables_produced=[
                CatalogTableSummary(id=t.id, name=t.name, namespace_id=t.namespace_id) for t in produced_tables
            ],
            tables_read=[CatalogTableSummary(id=t.id, name=t.name, namespace_id=t.namespace_id) for t in read_tables],
        )

    def bulk_enrich_flows(self, flows: list[FlowRegistration], user_id: int) -> list[FlowRegistrationOut]:
        """Enrich multiple flows with favourites, follows, and run stats in bulk.

        Uses 6 bulk queries instead of 6×N, dramatically improving performance
        when listing many flows.
        """
        if not flows:
            return []

        flow_ids = [f.id for f in flows]
        favorite_ids = self.repo.bulk_get_favorite_flow_ids(user_id, flow_ids)
        follow_ids = self.repo.bulk_get_follow_flow_ids(user_id, flow_ids)
        run_stats = self.repo.bulk_get_run_stats(flow_ids)
        artifact_counts = self.repo.bulk_get_artifact_counts(flow_ids)
        tables_by_flow = self.repo.bulk_get_tables_for_flows(flow_ids)
        read_tables_by_flow = self.repo.bulk_get_read_tables_for_flows(flow_ids)

        result: list[FlowRegistrationOut] = []
        for flow in flows:
            run_count, last_run = run_stats.get(flow.id, (0, None))
            produced = tables_by_flow.get(flow.id, [])
            read = read_tables_by_flow.get(flow.id, [])
            file_exists = os.path.exists(flow.flow_path) if flow.flow_path else False
            if not file_exists:
                _warn_missing_file_once(flow)
            result.append(
                FlowRegistrationOut(
                    id=flow.id,
                    name=flow.name,
                    description=flow.description,
                    flow_path=flow.flow_path,
                    namespace_id=flow.namespace_id,
                    owner_id=flow.owner_id,
                    created_at=flow.created_at,
                    updated_at=flow.updated_at,
                    is_favorite=flow.id in favorite_ids,
                    is_following=flow.id in follow_ids,
                    run_count=run_count,
                    last_run_at=last_run.started_at if last_run else None,
                    last_run_success=last_run.success if last_run else None,
                    file_exists=file_exists,
                    artifact_count=artifact_counts.get(flow.id, 0),
                    tables_produced=[
                        CatalogTableSummary(id=t.id, name=t.name, namespace_id=t.namespace_id) for t in produced
                    ],
                    tables_read=[CatalogTableSummary(id=t.id, name=t.name, namespace_id=t.namespace_id) for t in read],
                )
            )
        return result

    def register_flow(
        self,
        name: str,
        flow_path: str,
        owner_id: int,
        namespace_id: int | None = None,
        description: str | None = None,
    ) -> FlowRegistrationOut:
        """Register a new flow in the catalog."""
        if namespace_id is not None:
            namespace = self.repo.get_namespace(namespace_id)
            if namespace is None:
                raise NamespaceNotFoundError(namespace_id=namespace_id)
        flow = FlowRegistration(
            name=name,
            description=description,
            flow_path=flow_path,
            namespace_id=namespace_id,
            owner_id=owner_id,
        )
        flow = self.repo.create_flow(flow)
        return self.enrich_flow_registration(flow, owner_id)

    def update_flow(
        self,
        registration_id: int,
        requesting_user_id: int,
        name: str | None = None,
        description: str | None = None,
        namespace_id: int | None = None,
    ) -> FlowRegistrationOut:
        """Update a flow registration."""
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        if name is not None:
            flow.name = name
        if description is not None:
            flow.description = description
        if namespace_id is not None:
            flow.namespace_id = namespace_id
        flow = self.repo.update_flow(flow)
        return self.enrich_flow_registration(flow, requesting_user_id)

    def delete_flow(self, registration_id: int) -> None:
        """Delete a flow and its related favourites/follows."""
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)

        artifact_count = self.repo.count_active_artifacts_for_flow(registration_id)
        if artifact_count > 0:
            raise FlowHasArtifactsError(registration_id, artifact_count)

        self.repo.delete_flow(registration_id)

    def get_flow(self, registration_id: int, user_id: int) -> FlowRegistrationOut:
        """Get an enriched flow registration."""
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        return self.enrich_flow_registration(flow, user_id)

    def list_flows(self, user_id: int, namespace_id: int | None = None) -> list[FlowRegistrationOut]:
        """List flows, optionally filtered by namespace, enriched with user context."""
        flows = self.repo.list_flows(namespace_id=namespace_id)
        return self.bulk_enrich_flows(flows, user_id)

    def list_artifacts_for_flow(self, registration_id: int) -> list[GlobalArtifactOut]:
        """List all active artifacts produced by a registered flow."""
        flow = self.repo.get_flow(registration_id)
        if flow is None:
            raise FlowNotFoundError(registration_id=registration_id)
        artifacts = self.repo.list_artifacts_for_flow(registration_id)
        return [artifact_to_out(a) for a in artifacts]

    def _ensure_general_child(self, name: str, description: str) -> CatalogNamespace | None:
        """Ensure 'General > {name}' namespace exists, creating it if needed."""
        general = self.repo.get_namespace_by_name("General", parent_id=None)
        if general is None:
            logger.info(f"Cannot ensure '{name}' namespace: parent 'General' not found")
            return None
        existing = self.repo.get_namespace_by_name(name, parent_id=general.id)
        if existing is not None:
            return existing
        namespace = CatalogNamespace(
            name=name,
            parent_id=general.id,
            level=1,
            description=description,
            owner_id=general.owner_id,
        )
        return self.repo.create_namespace(namespace)

    def ensure_unnamed_flows_namespace(self) -> CatalogNamespace | None:
        return self._ensure_general_child("Unnamed Flows", "Quick-created flows that have not yet been named")

    def ensure_local_flows_namespace(self) -> CatalogNamespace | None:
        return self._ensure_general_child("Local Flows", "Flows saved to disk at user-chosen paths")

    def ensure_python_flows_namespace(self) -> CatalogNamespace | None:
        return self._ensure_general_child("Python Flows", "Flows authored programmatically via flowfile_frame")

    def resolve_registration_id(self, flow_path: str) -> int | None:
        """Look up the registration ID for a flow by its file path."""
        reg = self.repo.get_flow_by_path(flow_path)
        return reg.id if reg else None

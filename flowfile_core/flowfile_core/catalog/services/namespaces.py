"""Namespace CRUD, tree assembly, default-namespace seeding."""

from __future__ import annotations

import logging
from collections.abc import Callable

from flowfile_core.catalog.exceptions import (
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    NestingLimitError,
)
from flowfile_core.catalog.repository import CatalogRepository
from flowfile_core.catalog.serializers import artifact_to_out
from flowfile_core.catalog.validators import reject_dot_in_name
from flowfile_core.database.models import CatalogNamespace, FlowRegistration, GlobalArtifact
from flowfile_core.schemas.catalog_schema import (
    CatalogTableOut,
    FlowRegistrationOut,
    GlobalArtifactOut,
    NamespaceTree,
    NotebookSummaryOut,
    VisualizationOut,
)

logger = logging.getLogger(__name__)


def bulk_enrich_artifacts(artifacts: list[GlobalArtifact]) -> list[GlobalArtifactOut]:
    """Serialize artifacts, flagging ones whose backing blob is missing on disk
    (filesystem backend only — exists() is one cheap stat; an S3 probe per item
    would be a network call on every load)."""
    from flowfile_core.artifacts import get_storage_backend
    from shared.artifact_storage import SharedFilesystemStorage

    storage = get_storage_backend()
    fs_storage = storage if isinstance(storage, SharedFilesystemStorage) else None
    items: list[GlobalArtifactOut] = []
    for a in artifacts:
        blob_exists = True
        if fs_storage is not None and a.storage_key:
            blob_exists = fs_storage.exists(a.storage_key)
        items.append(artifact_to_out(a, blob_exists=blob_exists))
    return items


class NamespaceService:
    """CRUD + lookup for namespaces. No peer-service dependencies."""

    def __init__(self, repo: CatalogRepository) -> None:
        self.repo = repo

    def resolve_namespace_name(self, namespace_id: int | None) -> str | None:
        if namespace_id is None:
            return None
        namespace = self.repo.get_namespace(namespace_id)
        return namespace.name if namespace is not None else None

    def create_namespace(
        self,
        name: str,
        owner_id: int,
        parent_id: int | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        """Create a catalog (level 0) or schema (level 1) namespace."""
        reject_dot_in_name(name, "Namespace")
        level = 0
        if parent_id is not None:
            parent = self.repo.get_namespace(parent_id)
            if parent is None:
                raise NamespaceNotFoundError(namespace_id=parent_id)
            if parent.level >= 1:
                raise NestingLimitError(parent_id=parent_id, parent_level=parent.level)
            level = parent.level + 1

        existing = self.repo.get_namespace_by_name(name, parent_id)
        if existing is not None:
            raise NamespaceExistsError(name=name, parent_id=parent_id)

        namespace = CatalogNamespace(
            name=name,
            parent_id=parent_id,
            level=level,
            description=description,
            owner_id=owner_id,
        )
        return self.repo.create_namespace(namespace)

    def update_namespace(
        self,
        namespace_id: int,
        name: str | None = None,
        description: str | None = None,
    ) -> CatalogNamespace:
        """Update a namespace's name and/or description."""
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        if name is not None:
            namespace.name = name
        if description is not None:
            namespace.description = description
        return self.repo.update_namespace(namespace)

    def delete_namespace(self, namespace_id: int) -> None:
        """Delete a namespace if it has no children, flows or tables."""
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        children = self.repo.count_children(namespace_id)
        flows = self.repo.count_flows_in_namespace(namespace_id)
        tables = self.repo.count_tables_in_namespace(namespace_id)
        if children > 0 or flows > 0 or tables > 0:
            raise NamespaceNotEmptyError(namespace_id=namespace_id, children=children, flows=flows, tables=tables)
        self.repo.delete_namespace(namespace_id)

    def get_namespace(self, namespace_id: int) -> CatalogNamespace:
        """Retrieve a single namespace by ID."""
        namespace = self.repo.get_namespace(namespace_id)
        if namespace is None:
            raise NamespaceNotFoundError(namespace_id=namespace_id)
        return namespace

    def list_namespaces(self, parent_id: int | None = None) -> list[CatalogNamespace]:
        """List namespaces, optionally filtered by parent."""
        return self.repo.list_namespaces(parent_id)

    def get_namespace_tree(
        self,
        user_id: int,
        *,
        list_visualizations: Callable[[int | None], list[VisualizationOut]],
        list_notebooks: Callable[[int | None], list[NotebookSummaryOut]],
        bulk_enrich_tables: Callable[[list, int], list[CatalogTableOut]],
        bulk_enrich_flows: Callable[[list[FlowRegistration], int], list[FlowRegistrationOut]],
    ) -> list[NamespaceTree]:
        """Build the full catalog tree with flows nested under schemas.

        Cross-cutting enrichment is injected via callbacks so this service
        stays a leaf — see ``CatalogService.get_namespace_tree`` for wiring.
        """
        catalogs = self.repo.list_root_namespaces()

        all_flows: list[FlowRegistration] = []
        namespace_flow_map: dict[int, list[FlowRegistration]] = {}
        namespace_artifact_map: dict[int, list[GlobalArtifactOut]] = {}
        namespace_table_map: dict[int, list[CatalogTableOut]] = {}

        def _artifacts_out(ns_id: int) -> list[GlobalArtifactOut]:
            return bulk_enrich_artifacts(self.repo.list_artifacts_for_namespace(ns_id))

        # Visualizations are surfaced as a peer of flows / tables / artifacts in
        # whatever namespace they were saved into (their own ``namespace_id``,
        # not the parent table's). Resolve once and bucket by namespace.
        viz_by_namespace: dict[int, list[VisualizationOut]] = {}
        for v in list_visualizations(user_id):
            if v.namespace_id is None:
                continue
            viz_by_namespace.setdefault(v.namespace_id, []).append(v)

        nb_by_namespace: dict[int, list[NotebookSummaryOut]] = {}
        for nb in list_notebooks(user_id):
            if nb.namespace_id is None:
                continue
            nb_by_namespace.setdefault(nb.namespace_id, []).append(nb)

        for cat in catalogs:
            cat_flows = self.repo.list_flows(namespace_id=cat.id)
            namespace_flow_map[cat.id] = cat_flows
            all_flows.extend(cat_flows)
            namespace_artifact_map[cat.id] = _artifacts_out(cat.id)
            namespace_table_map[cat.id] = bulk_enrich_tables(self.repo.list_tables_for_namespace(cat.id), user_id)

            for schema in self.repo.list_child_namespaces(cat.id):
                schema_flows = self.repo.list_flows(namespace_id=schema.id)
                namespace_flow_map[schema.id] = schema_flows
                all_flows.extend(schema_flows)
                namespace_artifact_map[schema.id] = _artifacts_out(schema.id)
                namespace_table_map[schema.id] = bulk_enrich_tables(
                    self.repo.list_tables_for_namespace(schema.id), user_id
                )

        enriched = bulk_enrich_flows(all_flows, user_id)
        enriched_map = {e.id: e for e in enriched}

        result: list[NamespaceTree] = []
        for cat in catalogs:
            schemas = self.repo.list_child_namespaces(cat.id)
            children: list[NamespaceTree] = []
            for schema in schemas:
                schema_flows = namespace_flow_map.get(schema.id, [])
                flow_outs = [enriched_map[f.id] for f in schema_flows if f.id in enriched_map]
                children.append(
                    NamespaceTree(
                        id=schema.id,
                        name=schema.name,
                        parent_id=schema.parent_id,
                        level=schema.level,
                        description=schema.description,
                        owner_id=schema.owner_id,
                        created_at=schema.created_at,
                        updated_at=schema.updated_at,
                        children=[],
                        flows=flow_outs,
                        artifacts=namespace_artifact_map.get(schema.id, []),
                        tables=namespace_table_map.get(schema.id, []),
                        visualizations=viz_by_namespace.get(schema.id, []),
                        notebooks=nb_by_namespace.get(schema.id, []),
                    )
                )
            cat_flows = namespace_flow_map.get(cat.id, [])
            root_flow_outs = [enriched_map[f.id] for f in cat_flows if f.id in enriched_map]
            result.append(
                NamespaceTree(
                    id=cat.id,
                    name=cat.name,
                    parent_id=cat.parent_id,
                    level=cat.level,
                    description=cat.description,
                    owner_id=cat.owner_id,
                    created_at=cat.created_at,
                    updated_at=cat.updated_at,
                    children=children,
                    flows=root_flow_outs,
                    artifacts=namespace_artifact_map.get(cat.id, []),
                    tables=namespace_table_map.get(cat.id, []),
                    visualizations=viz_by_namespace.get(cat.id, []),
                    notebooks=nb_by_namespace.get(cat.id, []),
                )
            )
        return result

    def get_default_namespace_id(self) -> int | None:
        """Return the ID of the default 'default' schema under 'General'."""
        general = self.repo.get_namespace_by_name("General", parent_id=None)
        if general is None:
            return None
        default_schema = self.repo.get_namespace_by_name("default", parent_id=general.id)
        if default_schema is None:
            return None
        return default_schema.id
